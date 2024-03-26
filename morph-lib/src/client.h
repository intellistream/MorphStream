#ifndef DDCKV_CLIENT_H_
#define DDCKV_CLIENT_H_

#include <map>

#include <pthread.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <sys/time.h>

#include <string>
#include <unordered_map>
#include <boost/fiber/all.hpp>

#include "client_mm.h"
#include "nm.h"
#include "kv_utils.h"
#include "hashtable.h"
#include "ib.h"
#include "kv_debug.h"

#define CLINET_INPUT_BUF_LEN  (512 * 1024 * 1024ULL)
#define CORO_LOCAL_BUF_LEN    (32 * 1024 * 1024)

enum KVOpsCrashPoint {
    KV_CRASH_UNCOMMITTED_BK_CONSENSUS_0,
    KV_CRASH_UNCOMMITTED_BK_CONSENSUS_1,
    KV_CRASH_COMMITTED_PR_CAS,
    KV_CRASH_COMMITTED_ALL_FINISH,
};

enum KVRequestType {
    KV_REQ_SEARCH,
    KV_REQ_INSERT,
    KV_REQ_UPDATE,
    KV_REQ_DELETE,
    KV_REQ_RECOVER_COMMITTED,
    KV_REQ_RECOVER_UNCOMMITTED
};

enum KVConsensusState {
    KV_CONSENSUS_WIN_ALL = 0,
    KV_CONSENSUS_WIN_MAJOR,
    KV_CONSENSUS_WIN_LITTLE,
    KV_CONSENSUS_FAIL,
    KV_CONSENSUS_FINISH,
    KV_CONSENSUS_NON
};

enum KVOpsRetCode {
    KV_OPS_SUCCESS = 0,
    KV_OPS_FAIL_RETURN,
    KV_OPS_FAIL_REDO
};

typedef struct TagCacheSaveSlot {
    char key[32];
    LocalCacheEntry entry;
} CacheSaveSlot;

typedef struct TagKVReqCtx {
    // input
    uint64_t         coro_id;
    uint8_t          req_type;
    KVInfo         * kv_info;
    RaceHashBucket * local_bucket_addr;  // need to be allocated to each coro ib_mr
    void           * local_cache_addr;   // need to be allocated to each coro ib_mr
    void           * local_kv_addr;      // need to be allocated to each coro ib_mr
    void           * local_cas_target_value_addr;     // need to be allocated to each coro ib_mr
    void           * local_cas_return_value_addr;     // need to be allocated to each coro ib_mr
    void           * op_laddr;    // tmp results
    uint32_t         lkey;
    bool             use_cache;

    // bucket_info
    RaceHashBucket * bucket_arr[4];
    RaceHashBucket * f_com_bucket;
    RaceHashBucket * s_com_bucket;
    RaceHashSlot   * slot_arr[4]; 

    // for preparation
    KVHashInfo      hash_info;
    KVTableAddrInfo tbl_addr_info;
    
    // for key-value pair read
    std::vector<KVRWAddr>                     kv_read_addr_list;
    std::vector<std::pair<int32_t, int32_t> > kv_idx_list;

    // for kv insert/update/delete
    ClientMMAllocCtx mm_alloc_ctx;
    uint8_t consensus_state;
    std::vector<KVCASAddr> kv_modify_pr_cas_list;
    std::vector<KVCASAddr> kv_modify_bk_0_cas_list; // all backup cas
    std::vector<KVCASAddr> kv_modify_bk_1_cas_list; // all not modified backups

    // for insert
    int32_t bucket_idx;
    int32_t slot_idx;

    // for kv update/delete
    KVRWAddr kv_invalid_addr;
    std::vector<KVRWAddr> log_commit_addr_list;
    std::vector<KVRWAddr> write_unused_addr_list;
    std::vector<std::pair<int32_t, int32_t> > recover_match_idx_list;

    // on crash
    std::vector<uint8_t> healthy_idx_server_id_list;    // record the replication idx of the healthy index servers

    bool is_finished;
    bool is_cache_hit;
    bool is_local_cache_hit;
    bool committed_need_cas_pr;
    bool has_modified_bk_idx;
    bool failed_pr_index; // set when the primary index failed
    bool failed_pr_addr;  // set when the primary data failed
    union {
        void * value_addr;    // for search return value
        int    ret_code;
    } ret_val;

    volatile bool * should_stop;

    std::string key_str;
    LocalCacheEntry * cache_entry;
} KVReqCtx;

class Client {
private:
    ClientMM          * mm_;
    UDPNetworkManager * nm_;

    uint32_t my_server_id_;
    uint32_t num_replication_;
    uint32_t num_memory_;
    uint32_t num_idx_rep_;

    uint8_t  pr_log_server_id_;
    uint64_t pr_log_head_;
    uint64_t pr_log_tail_;

    uint64_t remote_global_meta_addr_;
    uint64_t remote_meta_addr_;
    uint64_t remote_gc_addr_;
    uint64_t remote_root_addr_;

    uint64_t server_st_addr_;
    uint64_t server_data_len_;

    float miss_rate_threash_;

    RaceHashRoot * race_root_;
    struct ibv_mr * race_root_mr_;

    void * local_buf_;
    struct ibv_mr * local_buf_mr_;

    void * input_buf_;
    struct ibv_mr * input_buf_mr_;

    void * transient_buf_;
    struct ibv_mr * transient_buf_mr_;

    uint64_t * coro_local_addr_list_;

    std::unordered_map<std::string, LocalCacheEntry *> addr_cache_;
    std::map<uint32_t, struct MrInfo *> server_mr_info_map_;

    // core bind information
    uint32_t main_core_id_;
    uint32_t poll_core_id_;
    uint32_t bg_core_id_;
    uint32_t gc_core_id_;

    // crash testing information
    std::map<uint8_t, bool> server_crash_map_;
    std::vector<ClientMetaAddrInfo> meta_addr_info_;

    // timer for profile
    struct timeval recover_st_;
    struct timeval connection_recover_et_;
    struct timeval mm_recover_et_;
    struct timeval local_mr_reg_et_;
    struct timeval kv_ops_recover_et_;
    
    // gc
    boost::fibers::fiber gc_fb_;

// private inline methods
private:
    inline int get_race_root() {
        int ret = nm_->nm_rdma_read_from_sid((void *)race_root_, race_root_mr_->lkey, sizeof(RaceHashRoot),
                remote_root_addr_, server_mr_info_map_[0]->rkey, 0);
        assert(ret == 0);
        return 0;
    }

    inline int write_race_root() {
        int ret = 0;
        for (int i = 0; i < num_replication_; i ++) {
            ret = nm_->nm_rdma_write_to_sid((void *)race_root_, race_root_mr_->lkey, sizeof(RaceHashRoot),
                    remote_root_addr_, server_mr_info_map_[i]->rkey, i);
            // assert(ret == 0);
        }
        return 0;
    }

    inline char * get_key(KVInfo * kv_info) {
        return (char *)((uint64_t)kv_info->l_addr + sizeof(KVLogHeader));
    }

    inline char * get_value(KVInfo * kv_info) {
        return (char *)((uint64_t)kv_info->l_addr + sizeof(KVLogHeader) + kv_info->key_len);
    }

    inline KVLogHeader * get_header(KVInfo * kv_info) {
        return (KVLogHeader *)kv_info->l_addr;
    }

    inline void update_cache(std::string key_str, RaceHashSlot * slot_info, uint64_t * r_slot_addr_list) {
        // char key_buf[128] = {0};
        // memcpy(key_buf, get_key(kv_info), kv_info->key_len);
        // std::string tmp_key(key_buf);

        auto it = addr_cache_.find(key_str);
        if (it != addr_cache_.end()) {
            LocalCacheEntry * entry = it->second;
            // check if is miss
            if (*(uint64_t *)(&entry->l_slot_ptr) != *(uint64_t *)slot_info) {
                entry->miss_cnt ++;
                memcpy(&entry->l_slot_ptr, slot_info, sizeof(RaceHashSlot));
                for (int i = 0; i < num_idx_rep_; i ++) {
                    entry->r_slot_addr[i] = r_slot_addr_list[i];
                }
            }
            // update access cnt
            entry->acc_cnt ++;
            return;
        }
        
        LocalCacheEntry * tmp_value = (LocalCacheEntry *)malloc(sizeof(LocalCacheEntry));
        memcpy(&tmp_value->l_slot_ptr, slot_info, sizeof(RaceHashSlot));
        tmp_value->acc_cnt  = 1;
        tmp_value->miss_cnt = 0;
        
        for (int i = 0; i < num_idx_rep_; i ++) {
            tmp_value->r_slot_addr[i] = r_slot_addr_list[i];
        }

        addr_cache_[key_str] = tmp_value;
        // print_log(DEBUG, "\t[%s] %s->slot(%lx) kv(%lx)", __FUNCTION__, key_buf, r_slot_addr_list[0], HashIndexConvert40To64Bits(tmp_value->l_slot_ptr.pointer));
    }

    inline LocalCacheEntry * check_cache(std::string key_str) {
        // char key_buf[128] = {0};
        // memcpy(key_buf, get_key(kv_info), kv_info->key_len);
        // std::string tmp_key(key_buf);

        auto it = addr_cache_.find(key_str);
        if (it == addr_cache_.end()) {
            // print_log(DEBUG, "\t\t[%s] cache miss", __FUNCTION__);
            return NULL;
        }
        if (HashIndexConvert40To64Bits(it->second->l_slot_ptr.pointer) == 0) {
            free(it->second);
            addr_cache_.erase(it);
            // print_log(DEBUG, "\t\t[%s] cache empty pointer miss", __FUNCTION__);
            return NULL;
        }

        float miss_rate = ((float)it->second->miss_cnt / it->second->acc_cnt);
        if (miss_rate > miss_rate_threash_) {
            return NULL;
        }
        // print_log(DEBUG, "\t\t[%s] cache hit", __FUNCTION__);
        return it->second;
    }

    inline void remove_cache(std::string key_str) {
        auto it = addr_cache_.find(key_str);
        if (it != addr_cache_.end()) {
            addr_cache_.erase(it);
        }
    }

    inline bool check_key(KVLogHeader * log_header, KVInfo * kv_info) {
        uint64_t r_key_addr = (uint64_t)log_header + sizeof(log_header);
        uint64_t l_key_addr = (uint64_t)kv_info->l_addr + sizeof(KVLogHeader);
        return CheckKey((void *)r_key_addr, log_header->key_length, (void *)l_key_addr, kv_info->key_len);
    }

    inline int poll_completion(std::map<uint64_t, struct ibv_wc *> & wait_wrid_wc_map) {
        int ret = 0;
        while (ib_is_all_wrid_finished(wait_wrid_wc_map) == false) {
            boost::this_fiber::sleep_for(std::chrono::microseconds(10));
            ret = nm_->nm_check_completion(wait_wrid_wc_map);
        }
        return ret;
    }

    inline int poll_completion(std::map<uint64_t, struct ibv_wc *> & wait_wrid_wc_map, volatile bool * should_stop) {
        int ret = 0;
        while (ib_is_all_wrid_finished(wait_wrid_wc_map) == false && (*should_stop) == false) {
            // print_log(DEBUG, "\t\t[%s] fiber: %ld yielding", __FUNCTION__, boost::this_fiber::get_id());
            if (*(should_stop)) {
                return ret;
            }
            boost::this_fiber::yield();
            ret = nm_->nm_check_completion(wait_wrid_wc_map);
            // kv_assert(ret == 0);
        }
        return ret;
    }

// private methods
private:
    bool init_is_finished();
    int  sync_init_finish();
    int  connect_ib_qps();
    int  write_client_meta_info();

    void get_kv_addr_info(KVHashInfo * a_kv_hash_info, __OUT KVTableAddrInfo * a_kv_addr_info);
    void get_kv_hash_info(KVInfo * a_kv_info, __OUT KVHashInfo * a_kv_hash_info);
    void fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, 
        __OUT RaceHashSlot * local_slot);
    void fill_cas_addr(KVTableAddrInfo * addr_info, uint64_t remote_slot_addr, RaceHashSlot * old_local_slot_addr, RaceHashSlot * new_local_slot_addr,
        __OUT KVCASAddr * pr_cas_addr, __OUT KVCASAddr * bk_cas_addr);
    void fill_cas_addr(KVReqCtx * ctx, uint64_t * remote_slot_addr, RaceHashSlot * old_local_slot_addr, RaceHashSlot * new_local_slot_addr);
    void fill_invalid_addr(KVReqCtx * ctx, RaceHashSlot * local_slot);
    
    IbvSrList * gen_read_bucket_sr_lists(KVReqCtx * ctx, 
        __OUT uint32_t * num_sr_lists);
    void        free_read_bucket_sr_lists(IbvSrList * sr_list);
    IbvSrList * gen_read_all_bucket_sr_lists(KVReqCtx * ctx, 
        __OUT uint32_t * num_sr_lists);
    void        free_read_all_bucket_sr_lists(IbvSrList * sr_list);
    IbvSrList * gen_read_bucket_sr_lists_on_crash(KVReqCtx * ctx, 
        __OUT uint32_t * num_sr_lists);
    void        free_read_bucket_sr_lists_on_crash(IbvSrList * sr_lists, int num_sr_lists);
    IbvSrList * gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info,    
        ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists);
    void        free_write_kv_sr_lists(IbvSrList * sr_list);
    IbvSrList * gen_write_del_log_sr_lists(uint32_t coro_id, KVInfo * a_kv_info,
        ClientMMAllocCtx * r_mm_info, __OUT uint32_t * num_sr_lists);
    void        free_write_del_log_sr_lists(IbvSrList * sr_list);
    IbvSrList * gen_read_kv_sr_lists(uint32_t coro_id, 
        const std::vector<KVRWAddr> & r_addr_list, __OUT uint32_t * num_sr_lists);
    void        free_read_kv_sr_lists(IbvSrList * sr_lists, int num_sr_lists);
    IbvSrList * gen_cas_sr_lists(uint32_t coro_id, 
        const std::vector<KVCASAddr> & cas_addr_list, __OUT uint32_t * num_sr_lists);
    void        free_cas_sr_lists(IbvSrList * sr_lists, int num_sr_lists);
    IbvSrList * gen_invalid_sr_lists(uint32_t coro_id, 
        KVRWAddr * r_addr, uint64_t local_data_addr);
    void        free_invalid_sr_lists(IbvSrList * sr_list);
    IbvSrList * gen_read_cache_kv_sr_lists(uint32_t coro_id, 
        RaceHashSlot * local_slot_ptr, uint64_t local_addr);
    void        free_read_cache_kv_sr_lists(IbvSrList * sr_lists);
    IbvSrList * gen_write_hb_sr_lists(uint32_t coro_id, 
        std::vector<KVRWAddr> & rw_addr_list, __OUT uint32_t * num_sr_lists);
    void        free_write_hb_sr_lists(IbvSrList * sr_lists, int num_sr_lists);
    IbvSrList * gen_log_commit_sr_lists(uint32_t coro_id, void * addr, uint32_t size, 
        std::vector<KVRWAddr> & rw_addr_list, __OUT uint32_t * num_sr_lists);
    void        free_log_commit_sr_lists(IbvSrList * sr_lists, int num_sr_lists);
    IbvSrList * gen_read_primary_sr_list(KVReqCtx * ctx);
    void        free_read_primary_sr_list(IbvSrList * sr_list);

    void prepare_request(KVReqCtx * ctx);
    void prepare_log_commit_addrs(KVReqCtx * ctx);
    void find_kv_in_buckets(KVReqCtx * ctx);
    void find_kv_in_buckets_on_crash(KVReqCtx * ctx);
    void find_empty_slot(KVReqCtx * ctx);
    int  find_match_kv_idx(KVReqCtx * ctx);
    void get_local_bucket_info(KVReqCtx * ctx);
    void modify_backup_idx_consensus_1(KVReqCtx * ctx);
    void modify_backup_idx_consensus_1_sync(KVReqCtx * ctx);
    void modify_primary_idx(KVReqCtx * ctx);
    void modify_primary_idx_sync(KVReqCtx * ctx);
    void check_cas_consensus_0(KVReqCtx * ctx);
    void check_cas_consensus_0_sync(KVReqCtx * ctx);
    int  check_cas_consensus_1(KVReqCtx * ctx);
    void kv_log_commit(KVReqCtx * ctx);
    void kv_log_commit_sync(KVReqCtx * ctx);
    void check_recover_need_cas_pr(KVReqCtx * ctx);
    void recover_modified_slots(KVReqCtx * ctx);
    void check_failed_index(KVReqCtx * ctx);
    void check_failed_data(KVReqCtx * ctx);
    RaceHashSlot * check_failed_cache(LocalCacheEntry * local_cache_entry);
    int  find_healthy_idx(uint8_t target_server, uint64_t target_addr);
    ClientMetaAddrInfo * find_corresponding_addr_info(uint8_t target_server, uint64_t target_addr);

    void kv_search_read_buckets(KVReqCtx * ctx);
    void kv_search_read_buckets_sync(KVReqCtx * ctx);
    void kv_search_read_buckets_on_crash(KVReqCtx * ctx);
    void kv_search_read_kv(KVReqCtx * ctx);
    void kv_search_read_kv_sync(KVReqCtx * ctx);
    void kv_search_check_kv(KVReqCtx * ctx);
    void kv_search_read_all_healthy_index(KVReqCtx * ctx);
    void kv_search_read_failed_kv(KVReqCtx * ctx);

    void kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx);
    void kv_insert_read_buckets_and_write_kv_sync(KVReqCtx * ctx);
    void kv_insert_backup_consensus_0(KVReqCtx * ctx);
    void kv_insert_backup_consensus_0_sync(KVReqCtx * ctx);
    void kv_insert_backup_consensus_1(KVReqCtx * ctx);
    void kv_insert_backup_consensus_1_sync(KVReqCtx * ctx);
    void kv_insert_commit_log(KVReqCtx * ctx);
    void kv_insert_commit_log_sync(KVReqCtx * ctx);
    void kv_insert_cas_primary(KVReqCtx * ctx);
    void kv_insert_cas_primary_sync(KVReqCtx * ctx);

    void kv_update_read_buckets_and_write_kv(KVReqCtx * ctx);
    void kv_update_read_buckets_and_write_kv_sync(KVReqCtx * ctx);
    void kv_update_read_kv(KVReqCtx * ctx);
    void kv_update_read_kv_sync(KVReqCtx * ctx);
    void kv_update_backup_consensus_0(KVReqCtx * ctx);
    void kv_update_backup_consensus_0_sync(KVReqCtx * ctx);
    void kv_update_backup_consensus_1(KVReqCtx * ctx);
    void kv_update_backup_consensus_1_sync(KVReqCtx * ctx);
    void kv_update_commit_log(KVReqCtx * ctx);
    void kv_update_commit_log_sync(KVReqCtx * ctx);
    void kv_update_cas_primary(KVReqCtx * ctx);
    void kv_update_cas_primary_sync(KVReqCtx * ctx);

    void kv_delete_read_buckets_write_log(KVReqCtx * ctx);
    void kv_delete_read_buckets_write_log_sync(KVReqCtx * ctx);
    void kv_delete_read_kv(KVReqCtx * ctx);
    void kv_delete_read_kv_sync(KVReqCtx * ctx);
    void kv_delete_backup_consensus_0(KVReqCtx * ctx);
    void kv_delete_backup_consensus_0_sync(KVReqCtx * ctx);
    void kv_delete_backup_consensus_1(KVReqCtx * ctx);
    void kv_delete_backup_consensus_1_sync(KVReqCtx * ctx);
    void kv_delete_commit_log(KVReqCtx * ctx);
    void kv_delete_commit_log_sync(KVReqCtx * ctx);
    void kv_delete_cas_primary(KVReqCtx * ctx);
    void kv_delete_cas_primary_sync(KVReqCtx * ctx);

    void kv_recover_read_all_buckets(KVReqCtx * ctx);

    void kv_recover_insert_read_all_buckets(KVReqCtx * ctx);
    void kv_recover_insert_backup_consensus_0(KVReqCtx * ctx);
    void kv_recover_insert_backup_consensus_1(KVReqCtx * ctx);
    void kv_recover_insert_commit_log(KVReqCtx * ctx);
    void kv_recover_insert_cas_primary(KVReqCtx * ctx);

    void kv_recover_update_read_all_buckets(KVReqCtx * ctx);
    void kv_recover_update_read_kv(KVReqCtx * ctx);
    void kv_recover_update_backup_consensus_0(KVReqCtx * ctx);
    void kv_recover_update_backup_consensus_1(KVReqCtx * ctx);
    void kv_recover_update_commit_log(KVReqCtx * ctx);
    void kv_recover_update_cas_primary(KVReqCtx * ctx);
    void recover_update_gen_cas_pr_addr(KVReqCtx * ctx);

    int post_sr_lists_unsignaled(IbvSrList * sr_lists, uint32_t sr_lsits_num);
    int post_sr_lists_and_yield_wait(IbvSrList * sr_lists, uint32_t sr_lists_num);
    int post_sr_lists_and_yield_wait(IbvSrList * sr_lists, uint32_t sr_lists_num, volatile bool * should_stop);
    int post_sr_list_batch_and_yield_wait(std::vector<IbvSrList *> sr_list_batch, 
        std::vector<uint32_t> sr_list_batch_num);
    int post_sr_list_batch_and_yield_wait(std::vector<IbvSrList *> sr_list_batch, std::vector<uint32_t> sr_list_num_batch, volatile bool * should_stop);

    void init_kv_req_ctx(KVReqCtx * req_ctx, KVInfo * kv_info, char * operation);
    void update_log_tail(KVLogTail * log_header, ClientMMAllocCtx * alloc_ctx);

    int  client_recovery();
    void init_recover_req_ctx(KVInfo * kv_info, __OUT KVReqCtx * rec_ctx);

    int init_hash_table();

// inline methods
public:
    inline void * get_input_buf() {
        return input_buf_;
    }

    inline uint32_t get_input_buf_lkey() {
        return input_buf_mr_->lkey;
    }

    inline struct ibv_mr * get_local_buf_mr() {
        return local_buf_mr_;
    }

    // public methods
public:
    Client(const struct GlobalConfig * conf);
    ~Client();

    KVInfo   * kv_info_list_;
    KVReqCtx * kv_req_ctx_list_;
    uint32_t   num_total_operations_;
    uint32_t   num_local_operations_;
    uint32_t   num_coroutines_;
    int        workload_run_time_;
    int        micro_workload_num_;

    bool stop_gc_;

    int kv_update(KVInfo * kv_info);
    int kv_update(KVReqCtx * ctx);
    int kv_update_w_cache(KVInfo * kv_info);
    int kv_update_w_crash(KVReqCtx * ctx, int crash_point);
    int kv_update_sync(KVReqCtx * ctx);
    
    int kv_insert(KVInfo * kv_info);
    int kv_insert(KVReqCtx * ctx);
    int kv_insert_w_cache(KVInfo * kv_info);
    int kv_insert_w_crash(KVReqCtx * ctx, int crash_point);
    int kv_insert_sync(KVReqCtx * ctx);

    void * kv_search(KVInfo * kv_info);
    void * kv_search(KVReqCtx * ctx);
    void * kv_search_w_cache(KVInfo * kv_info);
    void * kv_search_on_crash(KVReqCtx * ctx);
    void * kv_search_sync(KVReqCtx * ctx);
    
    int kv_delete(KVInfo * kv_info);
    int kv_delete(KVReqCtx * ctx);
    int kv_delete_w_cache(KVInfo * kv_info);
    int kv_delete_sync(KVReqCtx * ctx);

    int kv_recover(KVReqCtx * ctx);
    int kv_recover_update(KVReqCtx * ctx);
    int kv_recover_insert(KVReqCtx * ctx);
    
    pthread_t start_polling_thread();
    boost::fibers::fiber start_polling_fiber();
    void stop_polling_thread();
    void start_gc_fiber();
    void stop_gc_fiber();

    void free_batch();

    void init_kvreq_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops);
    void init_kv_insert_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_insert_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);
    void init_kv_search_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_search_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);
    void init_kv_update_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_update_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);
    void init_kv_delete_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_delete_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);

    void crash_server(const std::vector<uint8_t> & fail_server_list);

    void dump_cache();
    void load_cache();
    int  load_seq_kv_requests(uint32_t num_keys, char * op_type);
    int  load_kv_requests(const char * fname, uint32_t st_idx, int32_t num_ops);

    int get_num_rep();
    int get_my_server_id();
    int get_num_memory();
    int get_num_idx_rep();

    void get_recover_time(std::vector<struct timeval> & recover_time);

// for testing
public:
    int test_get_root(__OUT RaceHashRoot * race_root);
    int test_get_log_meta_info(__OUT ClientLogMetaInfo * remote_log_meta_info_list, 
            __OUT ClientLogMetaInfo * local_meta);
    int test_get_pr_log_meta_info(__OUT ClientLogMetaInfo * pr_log_meta_info);
    int test_get_remote_log_header(uint8_t server_id, uint64_t raddr, uint32_t buf_size,
            __OUT void * buf);
    int test_get_local_mm_blocks(__OUT ClientMMBlock * mm_block_list, __OUT uint64_t * list_len);
    ClientMetaAddrInfo ** test_get_meta_addr_info(__OUT uint64_t * list_len);

    inline ClientMM * get_mm() {
        return mm_;
    }

    inline UDPNetworkManager * get_nm() {
        return nm_;
    }
};

class ClientCR;
typedef struct TagClientFiberArgs {
    Client   * client;
    ClientCR * client_cr;
    uint32_t ops_st_idx;
    uint32_t ops_num;
    uint32_t coro_id;

    uint32_t num_failed;
    
    // for count time
    struct timeval * st;
    struct timeval * et;

    // for count ops
    boost::fibers::barrier * b;
    volatile bool   * should_stop;
    uint32_t ops_cnt;
    uint32_t thread_id;
} ClientFiberArgs;

void * client_main(void * arg);
void * client_ops_fb_cnt_time(void * arg);
void * client_ops_fb_cnt_ops(void * arg);
void * client_ops_fb_cnt_ops_micro(void * arg);
void * client_ops_fb_cnt_ops_on_crash(void * arg);
void * client_ops_fb_cnt_ops_cont(void * arg);
void * client_ops_fb_lat(void * arg);
void * client_gc_fb(void * arg);

#endif