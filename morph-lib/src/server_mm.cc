#include "server_mm.h"

#include <unistd.h>
#include <sys/mman.h>
#include <assert.h>

#include "kv_debug.h"

#define MAP_HUGE_2MB        (21 << MAP_HUGE_SHIFT)
#define MAP_HUGE_1GB        (30 << MAP_HUGE_SHIFT)

ServerMM::ServerMM(uint64_t server_base_addr, uint64_t base_len, 
    uint32_t block_size, const struct IbInfo * ib_info,
    const struct GlobalConfig * conf) {
    this->block_size_ = block_size;
    this->base_addr_ = server_base_addr;
    this->base_len_  = base_len;
    int port_flag = PROT_READ | PROT_WRITE;
    int mm_flag   = MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB | MAP_HUGE_2MB;
    this->data_ = mmap((void *)this->base_addr_, this->base_len_, port_flag, mm_flag, -1, 0);
    // assert((uint64_t)this->data_ == this->base_addr_);

    client_meta_area_off_ = 0;
    client_meta_area_len_ = META_AREA_LEN;
    client_gc_area_off_ = this->client_meta_area_len_;
    client_gc_area_len_ = GC_AREA_LEN;
    client_hash_area_off_ = this->client_gc_area_off_ + this->client_gc_area_len_;
    client_hash_area_len_ = HASH_AREA_LEN;
    client_kv_area_off_ = this->client_hash_area_off_ + this->client_hash_area_len_;
    client_kv_area_off_ = round_up(client_kv_area_off_, block_size_);
    client_kv_area_len_ = base_len_ - client_kv_area_off_;
    client_kv_area_limit_ = base_len_ + base_addr_;
    printf("kv_area_addr: %lx, block_size: %x\n", client_kv_area_off_, block_size_);

    //init hash index
    init_hashtable();

    int access_flag = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | 
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    this->mr_ = ibv_reg_mr(ib_info->ib_pd, this->data_, this->base_len_, access_flag);
    // print_log(DEBUG, "addr %lx rkey %x", mr_->addr, mr_->rkey);

    num_memory_ = conf->memory_num;
    num_replication_ = conf->num_replication;
    my_sid_ = conf->server_id;
    printf("my_sid_: %d, num_memory_: %d\n", my_sid_, num_memory_);

    // init blocks
    num_blocks_ = client_kv_area_len_ / block_size_;
    get_allocable_blocks();
}

ServerMM::~ServerMM() {
    munmap(data_, this->base_len_);
}

void ServerMM::get_allocable_blocks() {
    uint64_t kv_area_addr = base_addr_ + client_kv_area_off_;
    std::vector<uint64_t> mn_addr_ptr;
    for (int i = 0; i < num_memory_; i ++)
        mn_addr_ptr.push_back(kv_area_addr);
    
    uint32_t num_rep_blocks = (num_blocks_ * num_memory_) / num_replication_;
    printf("num_rep_blocks: %d, num_blocks: %d, limit: %lx\n", 
        num_rep_blocks, num_blocks_, client_kv_area_limit_);

    uint32_t block_cnt = 0;
    while (block_cnt < num_rep_blocks) {
        uint32_t st_sid = block_cnt % num_memory_;
        while (mn_addr_ptr[st_sid] == client_kv_area_limit_)
            st_sid = (st_sid + 1) % num_memory_;

        uint64_t addr_list[num_replication_];
        for (int i = 0; i < num_replication_; i ++) {
            uint8_t sid = (st_sid + i) % num_memory_;

            if (mn_addr_ptr[sid] >= client_kv_area_limit_) {
                printf("Error addr map %d %d %d\n", block_cnt, sid, st_sid);
                for (int j = 0; j < num_memory_; j ++)
                    printf("server: %lx\n", mn_addr_ptr[j]);
                exit(1);
            }
            if (mn_addr_ptr[sid] & 0xFF != 0) {
                printf("Error addr map addr\n");
                exit(1);
            }

            addr_list[i] = mn_addr_ptr[sid];
            mn_addr_ptr[sid] += block_size_;
        }
        if (st_sid == my_sid_) {
            allocable_blocks_.push(addr_list[0]);
        }
        block_cnt ++;
    }
}

uint64_t ServerMM::mm_alloc() {
    if (allocable_blocks_.size() == 0) {
        return 0;
    }

    uint64_t ret_addr = allocable_blocks_.front();
    allocable_blocks_.pop();
    allocated_blocks_[ret_addr] = true;
    return ret_addr;
}

int ServerMM::mm_free(uint64_t st_addr) {
    if (allocated_blocks_[st_addr] != true)
        return -1;

    allocated_blocks_[st_addr] = false;
    allocable_blocks_.push(st_addr);
    return 0;
}

uint64_t ServerMM::mm_alloc_subtable() {
    int ret = 0;
    uint64_t subtable_st_addr = base_addr_ + client_hash_area_off_ + roundup_256(ROOT_RES_LEN);
    for (size_t i = 0; i < subtable_alloc_map_.size(); i ++) {
        if (subtable_alloc_map_[i] == 0) {
            subtable_alloc_map_[i] = 1;
            return subtable_st_addr + i * roundup_256(SUBTABLE_LEN);
        }
    }
    return 0;
}

uint32_t ServerMM::get_rkey() {
    return this->mr_->rkey;
}

int ServerMM::get_client_gc_info(uint32_t client_id, __OUT struct MrInfo * mr_info) {
    uint64_t single_gc_len = 1024 * 1024;
    uint64_t client_gc_off = client_id * single_gc_len;
    if (client_gc_off + single_gc_len >= this->client_gc_area_len_) {
        return -1;
    }
    mr_info->addr = this->client_gc_area_off_ + client_gc_off + this->base_addr_;
    mr_info->rkey = this->mr_->rkey;
    return 0;
}

int ServerMM::get_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = this->base_addr_;
    mr_info->rkey = this->mr_->rkey;
    return 0;
}

int ServerMM::init_root(void * root_addr) {
    RaceHashRoot * root = (RaceHashRoot *)root_addr;
    root->global_depth = RACE_HASH_GLOBAL_DEPTH;
    root->init_local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
    root->max_global_depth = RACE_HASH_MAX_GLOBAL_DEPTH;
    root->prefix_num = 1 << RACE_HASH_MAX_GLOBAL_DEPTH;
    root->subtable_res_num = root->prefix_num;
    root->subtable_init_num = RACE_HASH_INIT_SUBTABLE_NUM;
    root->subtable_hash_range = RACE_HASH_ADDRESSABLE_BUCKET_NUM;
    root->subtable_bucket_num = RACE_HASH_SUBTABLE_BUCKET_NUM;
    root->seed = rand();
    root->root_offset = client_hash_area_off_;
    root->subtable_offset = root->root_offset + roundup_256(ROOT_RES_LEN);
    root->kv_offset = client_kv_area_off_;
    root->kv_len = client_kv_area_len_;
    root->lock = 0;

    return 0;
}

int ServerMM::init_subtable(void * subtable_addr) {
    // RaceHashBucket * bucket = (RaceHashBucket *)subtable_addr;
    uint64_t max_subtables = (base_addr_ + client_hash_area_off_ + client_hash_area_len_ - (uint64_t)subtable_addr) / roundup_256(SUBTABLE_LEN);

    subtable_alloc_map_.resize(max_subtables);
    for (int i = 0; i < max_subtables; i ++) {
        uint64_t cur_subtable_addr = (uint64_t)subtable_addr + i * roundup_256(SUBTABLE_LEN);
        subtable_alloc_map_[i] = 0;
        for (int j = 0; j < RACE_HASH_ADDRESSABLE_BUCKET_NUM; j ++) {
            RaceHashBucket * bucket = (RaceHashBucket *)cur_subtable_addr + j;
            bucket->local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
            bucket->prefix = i;
            bucket ++;
        }
    }

    return 0;
}

int ServerMM::init_hashtable() {
    uint64_t root_addr = base_addr_ + client_hash_area_off_;
    uint64_t subtable_st_addr = get_subtable_st_addr();
    init_root((void *)(root_addr));
    init_subtable((void *)(subtable_st_addr));
    return 0;
}

uint64_t ServerMM::get_kv_area_addr() {
    return client_kv_area_off_ + base_addr_;
}

uint64_t ServerMM::get_subtable_st_addr() {
    return client_hash_area_off_ + base_addr_ + roundup_256(ROOT_RES_LEN);
}