#ifndef DDCKV_CLIENT_MM_H_
#define DDCKV_CLIENT_MM_H_

#include <infiniband/verbs.h>

#include <vector>
#include <boost/fiber/all.hpp>
#include <thread>
#include <mutex>
#include <queue>
#include <deque>
#include <unordered_map>

#include "kv_utils.h"
#include "nm.h"
#include "spinlock.h"
#include "hashtable.h"

#define MAX_NUM_SUBBLOCKS 4
#define MAX_WATER_MARK 0.7

typedef struct TagClientMMBlock {
    struct MrInfo mr_info_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
    bool * bmap;
    uint32_t num_allocated;
    int32_t  prev_free_subblock_idx;
    int32_t  next_free_subblock_idx;
    int32_t  next_free_subblock_cnt;

    uint64_t next_mmblock_addr[MAX_REP_NUM];
} ClientMMBlock;

enum ClientAllocType {
    TYPE_SUBTABLE = 1,
    TYPE_KVBLOCK  = 2,
};

typedef struct TagClientMMAllocCtx {
    uint8_t  server_id_list[MAX_REP_NUM];
    uint64_t addr_list[MAX_REP_NUM];
    uint64_t prev_addr_list[MAX_REP_NUM];
    uint64_t next_addr_list[MAX_REP_NUM];
    uint32_t rkey_list[MAX_REP_NUM];
    uint32_t prev_rkey_list[MAX_REP_NUM];
    uint32_t next_rkey_list[MAX_REP_NUM];

    uint32_t num_subblocks;
    bool     need_change_prev;
} ClientMMAllocCtx;

typedef struct TagClientMMAllocSubtableCtx {
    uint8_t  server_id;
    uint64_t addr;
} ClientMMAllocSubtableCtx;

typedef struct TagRecoverLogInfo {
    KVLogTail * local_tail_addr;
    uint32_t    key_len;
    uint32_t    val_len;
    uint64_t    remote_addr;
    uint8_t     server_id;
} RecoverLogInfo;

typedef struct TagSubblockInfo {
    uint64_t addr_list[MAX_REP_NUM];
    uint32_t rkey_list[MAX_REP_NUM];
    uint8_t  server_id_list[MAX_REP_NUM];
} SubblockInfo;

class ClientMM {
private:
    uint32_t num_replication_;
    uint32_t num_idx_rep_;
    uint32_t num_memory_;

    std::vector<ClientMMBlock *> mm_blocks_;
    spinlock_t mm_blocks_lock_;
    uint32_t cur_mm_block_idx_;

    uint32_t subblock_num_;
    uint32_t last_allocated_;

    uint32_t bmap_block_num_;

    uint8_t  pr_log_server_id_;
    uint64_t pr_log_head_;

    uint64_t client_meta_addr_;
    uint64_t client_gc_addr_;

    uint64_t server_limit_addr_;
    uint64_t server_kv_area_off_;
    uint64_t server_kv_area_addr_;
    uint64_t server_num_blocks_;

    std::mutex alloc_new_block_lock_;
    bool is_allocing_new_block_;

    // for recovery
    void * recover_buf_;
    struct ibv_mr             * recover_mr_;
    std::vector<RecoverLogInfo> recover_log_info_list_;
    std::unordered_map<uint64_t, bool> recover_addr_is_allocated_map_;
    KVLogTail * log_tail_st_ptr_;
    void * tmp_buf_;

    // modification
    std::deque<SubblockInfo> subblock_free_queue_;
    SubblockInfo             last_allocated_info_;



    // std::map<std::string, std::queue<SubblockInfo>> allocated_subblock_key_map_;

    struct timeval local_recover_space_et_;
    struct timeval get_addr_meta_et_;
    struct timeval traverse_log_et_;

// private methods
private:
    int init_get_new_block_from_server(UDPNetworkManager * nm);
    int init_reg_space(struct MrInfo mr_inf_list[][MAX_REP_NUM], uint8_t server_id_list[][MAX_REP_NUM],
            UDPNetworkManager * nm, int reg_type);
    int dyn_get_new_block_from_server(UDPNetworkManager * nm);
    int get_new_block_from_server(UDPNetworkManager * nm);
    int local_reg_blocks(const struct MrInfo * mr_info_list, const uint8_t * server_id_list);
    int reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list,
            UDPNetworkManager * nm, int reg_type);
    int dyn_reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list,
            UDPNetworkManager * nm, int reg_type);
    int32_t alloc_from_sid(uint32_t server_id, UDPNetworkManager * nm, int alloc_type,
            __OUT struct MrInfo * mr_info);
    void update_mm_block_next(ClientMMBlock * mm_block);
    int remote_write_meta_addr(UDPNetworkManager * nm);

    int mm_recovery(UDPNetworkManager * nm);
    int mm_recover_prepare_space(UDPNetworkManager * nm);
    int get_remote_log_header(UDPNetworkManager * nm, uint8_t server_id, uint64_t r_addr, 
            KVLogHeader * local_addr);
    int mm_traverse_log(UDPNetworkManager * nm);
    int mm_get_addr_meta(UDPNetworkManager * nm);
    int mm_recover_mm_blocks(UDPNetworkManager * nm);

    uint32_t get_subblock_idx(uint64_t addr, ClientMMBlock * cur_block);
    ClientMMBlock * get_new_mmblock();

    void gen_subblock_info(ClientMMBlock * mm_block, uint32_t subblock_idx, __OUT SubblockInfo * subblock_info);

    void get_block_map();

// inline private methods
private:
    inline uint32_t get_alloc_hint_rr() {
#ifndef SERVER_MM
        return last_allocated_ ++;
#else   
        // last_allocated_ ++;
        // return last_allocated_ / 65536;
        return last_allocated_ ++;
#endif
    }

    inline float get_water_mark() {
        float num_used = 0;
        for (size_t i = 0; i < mm_blocks_.size(); i ++) {
            num_used += mm_blocks_[i]->num_allocated;
        }
        return num_used / (mm_blocks_.size() * subblock_num_);
    }

// public methods
public:
    uint64_t mm_block_sz_;
    uint64_t subblock_sz_;
    // block_mapping
    std::unordered_map<uint64_t, std::vector<uint64_t> > alloc_block_map_;
    std::unordered_map<uint64_t, std::vector<uint64_t> > total_block_map_;

    // for free
    std::unordered_map<std::string, uint64_t> free_faa_map_;

    ClientMM(const struct GlobalConfig * conf, 
        UDPNetworkManager * nm);
    ~ClientMM();

    void get_log_head(__OUT uint64_t * pr_log_head, __OUT uint64_t * bk_log_head);
    
    void mm_alloc(size_t size, UDPNetworkManager * nm, __OUT ClientMMAllocCtx * ctx);
    void mm_alloc(size_t size, UDPNetworkManager * nm, std::string key, __OUT ClientMMAllocCtx * ctx);
    void mm_alloc_log_info(RecoverLogInfo * log_info, __OUT ClientMMAllocCtx * ctx);

    void mm_free_cur(const ClientMMAllocCtx * ctx);
    void mm_free(uint64_t orig_slot_value);
    
    void mm_alloc_subtable(UDPNetworkManager * nm, __OUT ClientMMAllocSubtableCtx * ctx);

    int  get_last_log_recover_info(__OUT RecoverLogInfo * recover_log_info);
    void free_recover_buf();

    void get_time_bread_down(std::vector<struct timeval> & time_vec);

// inline public methods
public:
    inline uint64_t get_remote_meta_ptr() {
        return client_meta_addr_;
    }

    inline uint32_t get_num_mm_blocks() {
        return mm_blocks_.size();
    }

    inline bool should_alloc_new() {
        float water_mark = get_water_mark();
        return water_mark > MAX_WATER_MARK;
    }

    inline bool should_start_gc() {
        ClientMMBlock * cur_mmblock = mm_blocks_[cur_mm_block_idx_];
        return cur_mmblock->next_free_subblock_cnt < MAX_NUM_SUBBLOCKS;
    }

    inline ClientMMBlock * get_cur_mm_block() {
        return mm_blocks_[cur_mm_block_idx_];
    }

    inline void get_log_head(__OUT uint8_t * pr_log_server_id, __OUT uint64_t * pr_log_head) {
        *pr_log_server_id = pr_log_server_id_;
        *pr_log_head = pr_log_head_;
    }

    inline size_t get_aligned_size(size_t size) {
        if ((size % subblock_sz_) == 0) {
            return size;
        }
        size_t aligned = ((size / subblock_sz_) + 1) * subblock_sz_;
        return aligned;
    }
};

#endif