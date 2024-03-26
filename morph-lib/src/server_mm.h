#ifndef DDCKV_SERVER_MM_
#define DDCKV_SERVER_MM_

#include <infiniband/verbs.h>

#include <map>
#include <unordered_map>
#include <vector>
#include <queue>

#include "kv_utils.h"
#include "hashtable.h"

class ServerMM {
private:
    uint16_t my_sid_;

    uint64_t base_addr_;
    uint64_t base_len_;
    uint64_t client_meta_area_off_;
    uint64_t client_meta_area_len_;
    uint64_t client_gc_area_off_;
    uint64_t client_gc_area_len_;
    uint64_t client_hash_area_off_;
    uint64_t client_hash_area_len_;
    uint64_t client_kv_area_off_;
    uint64_t client_kv_area_len_;
    uint64_t client_kv_area_limit_;

    uint32_t num_memory_;
    uint32_t num_replication_;
    
    uint32_t block_size_;
    uint32_t num_blocks_;
    struct ibv_mr  * mr_;
#ifdef SERVER_MM
    uint64_t next_free_block_addr_;
#endif

    std::vector<bool> subtable_alloc_map_;
    std::queue<uint64_t> allocable_blocks_;
    std::unordered_map<uint64_t, bool> allocated_blocks_;

    void   * data_;

    // private methods
private:
    //init hash table index stored at client_hash_area_off_
    int init_root(void * root_addr);
    int init_subtable(void * subtable_addr);
    int init_hashtable();
    void get_allocable_blocks();

public:
    ServerMM(uint64_t server_base_addr, uint64_t base_len, 
        uint32_t block_size, const struct IbInfo * ib_info,
        const struct GlobalConfig * conf);
    ~ServerMM();
    
    uint64_t mm_alloc();

    int mm_free(uint64_t st_addr);

    uint64_t mm_alloc_subtable();

    uint32_t get_rkey();

    int get_client_gc_info(uint32_t client_id, __OUT struct MrInfo * mr_info);
    int get_mr_info(__OUT struct MrInfo * mr_info);
    
    uint64_t get_kv_area_addr();
    uint64_t get_subtable_st_addr();
};

#endif