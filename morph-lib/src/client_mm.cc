#include "client_mm.h"

#include <unistd.h>
#include <assert.h>
#include <sys/time.h>

#include "kv_debug.h"
#include "hashtable.h"

#define REC_SPACE_SIZE (64 * 1024 * 1024)

ClientMM::ClientMM(const struct GlobalConfig * conf, UDPNetworkManager * nm) {
    int ret = 0;
    client_meta_addr_ = conf->server_base_addr 
        + CLIENT_META_LEN * (conf->server_id - conf->memory_num + 1) 
        + sizeof(ClientLogMetaInfo);
    client_gc_addr_   = conf->server_base_addr 
        + META_AREA_LEN + CLIENT_GC_LEN * (conf->server_id - conf->memory_num + 1);

    server_limit_addr_ = conf->server_base_addr + conf->server_data_len;
    server_kv_area_off_ = round_up(META_AREA_LEN 
        + GC_AREA_LEN + HASH_AREA_LEN, conf->block_size);
    server_kv_area_addr_ = conf->server_base_addr + server_kv_area_off_;
    server_num_blocks_   = (conf->server_data_len - server_kv_area_off_)
        / conf->block_size;
    printf("server_kv_area_addr: %lx %ld\n", 
        server_kv_area_addr_, server_num_blocks_);

    num_replication_ = conf->num_replication;
    num_idx_rep_     = conf->num_idx_rep;
    num_memory_      = conf->memory_num;

    last_allocated_ = conf->server_id;
    mm_block_sz_ = conf->block_size;
    subblock_sz_ = conf->subblock_size;
    subblock_num_ = mm_block_sz_ / subblock_sz_;

    bmap_block_num_ = subblock_num_ / 8 / subblock_sz_;
    if (bmap_block_num_ * 8 * subblock_sz_ < subblock_num_)
        bmap_block_num_ += 1;

    mm_blocks_lock_ = 0;
    cur_mm_block_idx_ = 0;

    alloc_new_block_lock_.unlock();
    is_allocing_new_block_ = false;

    // construct block mapping
    get_block_map();

    if (conf->is_recovery == false) {
        // allocate initial blocks
        alloc_new_block_lock_.lock();
        is_allocing_new_block_ = true;
        int ret;
        if (conf->server_id - conf->memory_num == 0) {
            ret = init_get_new_block_from_server(nm);
        } else {
            ret = get_new_block_from_server(nm);
        }
        alloc_new_block_lock_.unlock();
        // assert(ret == 0);

        // assert(mm_blocks_.size() == 1);
        cur_mm_block_idx_ = 0;

        pr_log_server_id_ = mm_blocks_[cur_mm_block_idx_]->server_id_list[0];
        pr_log_head_ = mm_blocks_[cur_mm_block_idx_]->mr_info_list[0].addr;
    } else {
        // start recovery
        ret = mm_recovery(nm);
        // assert(ret == 0);
    }
    // printf("end\n");
}

ClientMM::~ClientMM() {
    for (size_t i = 0; i < mm_blocks_.size(); i ++) {
        free(mm_blocks_[i]);
    }
    mm_blocks_.clear();
    // printf("!!!!!!!!n_dyn_req: %d\n", n_dyn_req_);
}

// Statically map memory blocks
void ClientMM::get_block_map() {
    std::vector<uint64_t> mn_addr_ptr;
    for (int i = 0; i < num_memory_; i ++)
        mn_addr_ptr.push_back(server_kv_area_addr_);
    uint32_t num_rep_blocks = (server_num_blocks_ * num_memory_) / num_replication_;
    printf("num_rep_blocks: %d\n", num_rep_blocks);

    uint32_t block_cnt = 0;
    while (block_cnt < num_rep_blocks) {
        uint32_t st_sid = block_cnt % num_memory_;
        while (mn_addr_ptr[st_sid] == server_limit_addr_)
            st_sid = (st_sid + 1) % num_memory_;

        uint64_t addr_list[num_replication_];
        for (int i = 0; i < num_replication_; i ++) {
            uint8_t sid = (st_sid + i) % num_memory_; 
            
            if (mn_addr_ptr[sid] >= server_limit_addr_) {
                printf("Error addr map\n");
                exit(1);
            }
            if (mn_addr_ptr[sid] & 0xFF != 0) {
                printf("Error addr map addr\n");
                exit(1);
            }

            addr_list[i] = mn_addr_ptr[sid] | sid;
            mn_addr_ptr[sid] += mm_block_sz_;
        }

        for (int i = 0; i < num_replication_; i ++) {
            for (int j = 0; j < num_replication_; j ++) {
                if (j == i) continue;
                total_block_map_[addr_list[i]].push_back(addr_list[j]);
                if (i == 0) {
                    alloc_block_map_[addr_list[i]].push_back(addr_list[j]);
                    if (addr_list[j] == 0) {
                        printf("Failed initialization!\n");
                        exit(1);
                    }
                }
            }
            if (num_replication_ == 1) {
                alloc_block_map_[addr_list[0]].push_back(0);
            }
        }
        block_cnt ++;
    }
}

int32_t ClientMM::dyn_get_new_block_from_server(UDPNetworkManager * nm) {
    // n_dyn_req_ ++;
    int ret = 0;
    uint32_t my_server_id = nm->get_server_id();
    uint32_t alloc_hint   = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers  = nm->get_num_servers();

    struct MrInfo mr_info_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
    server_id_list[0] = pr_server_id;
    ret = alloc_from_sid(pr_server_id, nm, TYPE_KVBLOCK, &mr_info_list[0]);
    if (mr_info_list[0].addr == 0) {
        return -1;
    }

    if ((mr_info_list[0].addr & 0xFF) != 0) {
        printf("error addr: %lx\n", mr_info_list[0].addr);
        exit(1);
    }
    uint64_t allocated_addr = mr_info_list[0].addr | pr_server_id;
    auto it = alloc_block_map_.find(allocated_addr);
    if (it == alloc_block_map_.end()) {
        printf("cannot have allocated %lx\n", allocated_addr);
        exit(1);
    }
    for (int i = 0; i < num_replication_ - 1; i ++) {
        uint8_t  sid = it->second[i] & 0xFF;
        uint64_t addr = (it->second[i] >> 8) << 8;
        uint32_t rkey = nm->get_server_rkey(sid);
        mr_info_list[i + 1].addr = addr;
        mr_info_list[i + 1].rkey = rkey;
        server_id_list[i + 1] = sid;
    }

    ret = dyn_reg_new_space(mr_info_list, server_id_list, nm, TYPE_KVBLOCK);
    // assert(ret == 0);
    return 0;
}

int ClientMM::get_new_block_from_server(UDPNetworkManager * nm) {
    int ret = 0;
    uint32_t my_server_id = nm->get_server_id();
    uint32_t alloc_hint   = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers  = nm->get_num_servers();

    struct MrInfo mr_info_list[MAX_REP_NUM];
    uint8_t server_id_list[MAX_REP_NUM];
    ret = alloc_from_sid(pr_server_id, nm, TYPE_KVBLOCK, &mr_info_list[0]);
    server_id_list[0] = pr_server_id;
    if ((mr_info_list[0].addr & 0xFF) != 0) {
        printf("error addr: %lx\n", mr_info_list[0].addr);
        exit(1);
    }
    uint64_t allocated_addr = mr_info_list[0].addr | pr_server_id;
    auto it = alloc_block_map_.find(allocated_addr);
    if (it == alloc_block_map_.end()) {
        printf("Cannot have allocated %lx\n", allocated_addr);
        exit(1);
    }
    for (int i = 0; i < num_replication_ - 1; i ++) {
        uint8_t sid = it->second[i] & 0xFF;
        uint64_t addr = (it->second[i] >> 8) << 8;
        uint32_t rkey = nm->get_server_rkey(sid);
        mr_info_list[i + 1].addr = addr;
        mr_info_list[i + 1].rkey = rkey;
        server_id_list[i + 1] = sid;
    }

    ret = reg_new_space(mr_info_list, server_id_list, nm, TYPE_KVBLOCK);
    // assert(ret == 0);
    return 0;
}

int ClientMM::init_get_new_block_from_server(UDPNetworkManager * nm) {
    int ret = 0;
    uint32_t my_server_id = nm->get_server_id();
    
    struct MrInfo mr_info_list[num_memory_][MAX_REP_NUM];
    uint8_t server_id_list[num_memory_][MAX_REP_NUM];

    for (int i = 0; i < num_memory_; i ++) {
        uint32_t alloc_hint = get_alloc_hint_rr();
        uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
        server_id_list[i][0] = pr_server_id;
        ret = alloc_from_sid(pr_server_id, nm, TYPE_KVBLOCK, &mr_info_list[i][0]);
        if ((mr_info_list[i][0].addr & 0xFF) != 0) {
            printf("error addr: %lx\n", mr_info_list[i][0].addr);
            exit(1);
        }
        uint64_t allocated_addr = mr_info_list[i][0].addr | pr_server_id;
        auto it = alloc_block_map_.find(allocated_addr);
        if (it == alloc_block_map_.end()) {
            printf("cannot have allocated %lx\n", allocated_addr);
            exit(1);
        }
        for (int j = 0; j < num_replication_ - 1; j ++) {
            uint8_t sid = it->second[j] & 0xFF;
            uint64_t addr = (it->second[j] >> 8) << 8;
            uint32_t rkey = nm->get_server_rkey(sid);
            mr_info_list[i][j + 1].addr = addr;
            mr_info_list[i][j + 1].rkey = rkey;
            server_id_list[i][j + 1] = sid;
        }
    }

    ret = init_reg_space(mr_info_list, server_id_list, nm, TYPE_KVBLOCK);
    return 0;
}

void ClientMM::update_mm_block_next(ClientMMBlock * mm_block) {
    int32_t max_idx = -1;
    int32_t max_cnt = -1;
    bool * mm_block_bmap = mm_block->bmap;

    for (int i = 1; i < subblock_num_; ) {
        if (mm_block_bmap[i] == 1) {
            i ++;
            continue;
        }
        
        int j;
        for (j = 0; i + j < subblock_num_; j ++) {
            if (mm_block_bmap[i + j] == 1) {
                break;
            }
        }
        if (j > max_cnt) {
            max_cnt = j;
            max_idx = i;
        }
        i += j;
    }

    mm_block->prev_free_subblock_idx = mm_block->next_free_subblock_idx;
    mm_block->next_free_subblock_idx = max_idx;
    mm_block->next_free_subblock_cnt = max_cnt;
}

void ClientMM::mm_free(uint64_t orig_slot_val) {
    RaceHashSlot slot = *(RaceHashSlot *)&orig_slot_val;
    uint64_t kv_raddr = HashIndexConvert40To64Bits(slot.pointer);
    uint32_t subblock_id = (kv_raddr % mm_block_sz_) / subblock_sz_;
    uint64_t block_raddr = kv_raddr - (kv_raddr % mm_block_sz_);
    uint32_t subblock_8byte_offset = subblock_id / 64;
    
    uint64_t bmap_addr = block_raddr + subblock_8byte_offset * sizeof(uint64_t);
    uint64_t add_value = 1 << (subblock_id % 64);
    if (bmap_addr > block_raddr + subblock_sz_ * bmap_block_num_) {
        printf("Error free!\n");
        exit(1);
    }

    char tmp[256] = {0};
    sprintf(tmp, "%ld@%d", bmap_addr, slot.server_id);
    std::string addr_str(tmp);
    free_faa_map_[addr_str] += add_value;
}

void ClientMM::mm_free_cur(const ClientMMAllocCtx * ctx) {
    // 1. reconstruct the last_allocated
    SubblockInfo last_allocated;
    memset(&last_allocated, 0, sizeof(SubblockInfo));
    for (int i = 0; i < num_replication_; i ++) {
        last_allocated.server_id_list[i] = ctx->server_id_list[i];
        last_allocated.addr_list[i] = ctx->addr_list[i];
        last_allocated.rkey_list[i] = ctx->rkey_list[i];
    }
    subblock_free_queue_.push_front(last_allocated);

    // 2. reconstruct last allocated to be last-last allocated
    SubblockInfo last_last_allocated;
    memset(&last_last_allocated, 0, sizeof(SubblockInfo));
    for (int i = 0; i < num_replication_; i ++) {
        last_last_allocated.server_id_list[i] = ctx->prev_addr_list[i] & 0xFF;
        last_last_allocated.addr_list[i] = ((ctx->prev_addr_list[i] >> 8) << 8);
        last_last_allocated.rkey_list[i] = ctx->prev_rkey_list[i];
    }
    last_allocated_info_ = last_last_allocated;
}

void ClientMM::mm_alloc(size_t size, UDPNetworkManager * nm, std::string key, __OUT ClientMMAllocCtx * ctx) {
    mm_alloc(size, nm, ctx);
}

void ClientMM::mm_alloc(size_t size, UDPNetworkManager * nm, __OUT ClientMMAllocCtx * ctx) {
    // struct timeval st, et;
    // gettimeofday(&st, NULL);
    int ret = 0;
    size_t aligned_size = get_aligned_size(size);
    int num_subblocks_required = aligned_size / subblock_sz_;
    assert(num_subblocks_required == 1);

    assert(subblock_free_queue_.size() > 0);
    SubblockInfo alloc_subblock = subblock_free_queue_.front();
    subblock_free_queue_.pop_front();

    if (subblock_free_queue_.size() == 0) {
        ret = dyn_get_new_block_from_server(nm);
        if (ret == -1) {
            ctx->addr_list[0] = 0;
            return;
        }
    }

    SubblockInfo next_subblock = subblock_free_queue_.front();
    
    ctx->need_change_prev = false;
    ctx->num_subblocks = num_subblocks_required;
    for (int i = 0; i < num_replication_; i ++) {
        ctx->addr_list[i] = alloc_subblock.addr_list[i];
        ctx->rkey_list[i] = alloc_subblock.rkey_list[i];
        ctx->server_id_list[i] = alloc_subblock.server_id_list[i];

        ctx->next_addr_list[i] = next_subblock.addr_list[i];
        ctx->next_addr_list[i] |= next_subblock.server_id_list[i];
        ctx->next_rkey_list[i] = next_subblock.rkey_list[i];
        
        ctx->prev_addr_list[i] =  last_allocated_info_.addr_list[i];
        ctx->prev_addr_list[i] |= last_allocated_info_.server_id_list[i];
        ctx->prev_rkey_list[i] =  last_allocated_info_.rkey_list[i];
        // print_log(DEBUG, "\t   [%s] allocating %lx on server(%d)", __FUNCTION__,
        //     ctx->addr_list[i], ctx->server_id_list[i]);
    }

    last_allocated_info_ = alloc_subblock;
}

void ClientMM::mm_alloc_log_info(RecoverLogInfo * log_info, __OUT ClientMMAllocCtx * ctx) {
    uint32_t size = log_info->key_len+ log_info->val_len 
        + sizeof(KVLogHeader) + sizeof(KVLogTail);
    int num_subblock_required = get_aligned_size(size) / subblock_sz_;
    uint64_t pr_addr = log_info->remote_addr;
    uint8_t  server_id = log_info->server_id;

    for (int i = 0; i < num_replication_; i ++) {
        ctx->addr_list[i] = last_allocated_info_.addr_list[i];
        ctx->rkey_list[i] = last_allocated_info_.rkey_list[i];
        ctx->server_id_list[i] = last_allocated_info_.server_id_list[i];
    }
    ctx->num_subblocks = num_subblock_required;

    KVLogTail * tail = log_info->local_tail_addr;

    ctx->prev_addr_list[0] = HashIndexConvert40To64Bits(tail->prev_addr) 
        | tail->prev_addr[5];
}

void ClientMM::mm_alloc_subtable(UDPNetworkManager * nm, __OUT ClientMMAllocSubtableCtx * ctx) {
    uint32_t my_server_id = nm->get_server_id();
    uint32_t alloc_hint   = get_alloc_hint_rr();
    uint32_t pr_server_id = nm->get_one_server_id(alloc_hint);
    uint32_t num_servers  = nm->get_num_servers();
    int ret = 0;
    
    struct MrInfo mr_info_list[num_idx_rep_];
    uint8_t server_id_list[num_idx_rep_];
    for (int i = 0; i < num_idx_rep_; i ++) {
        uint32_t server_id = (pr_server_id + i) % num_servers;
        server_id_list[i] = server_id;
        ret = alloc_from_sid(server_id, nm, TYPE_SUBTABLE, &mr_info_list[i]);
        // assert(ret == 0);
        ctx[i].addr = mr_info_list[i].addr;
        ctx[i].server_id = server_id;
        // print_log(DEBUG, "allocating subtable on %d %lx", server_id, mr_info_list[i].addr);
    }

    ret = reg_new_space(mr_info_list, server_id_list, nm, TYPE_SUBTABLE);
    // assert(ret == 0);
}

int ClientMM::get_last_log_recover_info(__OUT RecoverLogInfo * recover_log_info) {
    size_t last_idx = recover_log_info_list_.size();
    if (last_idx > 1) {
        memcpy(recover_log_info, &recover_log_info_list_[last_idx - 2], sizeof(RecoverLogInfo));
    } else {
        memset(recover_log_info, 0, sizeof(RecoverLogInfo));
    }
    return 0;
}

void ClientMM::free_recover_buf() {
    ibv_dereg_mr(recover_mr_);
    recover_log_info_list_.clear();
    free(recover_buf_);
}

int ClientMM::alloc_from_sid(uint32_t server_id, UDPNetworkManager * nm, int alloc_type,
        __OUT struct MrInfo * mr_info) {
    struct KVMsg request, reply;
    memset(&request, 0, sizeof(struct KVMsg));
    memset(&reply, 0, sizeof(struct KVMsg));

    request.id = nm->get_server_id();
    if (alloc_type == TYPE_KVBLOCK) {
        request.type = REQ_ALLOC;
    } else {
        // assert(alloc_type == TYPE_SUBTABLE);
        request.type = REQ_ALLOC_SUBTABLE;
    }
    serialize_kvmsg(&request);

    int ret = nm->nm_send_udp_msg_to_server(&request, server_id);
    // assert(ret == 0);
    ret = nm->nm_recv_udp_msg(&reply, NULL, NULL);
    // assert(ret == 0);
    deserialize_kvmsg(&reply);

    // if (alloc_type == TYPE_KVBLOCK) {
    //     // assert(reply.type == REP_ALLOC);
    // } else {
    //     // assert(reply.type == REP_ALLOC_SUBTABLE);
    // }

    memcpy(mr_info, &reply.body.mr_info, sizeof(struct MrInfo));
    return 0;
}

int ClientMM::local_reg_blocks(const struct MrInfo * mr_info_list, const uint8_t * server_id_list) {
    ClientMMBlock * new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
    memset(new_mm_block , 0, sizeof(ClientMMBlock));

    for (int i = 0; i < num_replication_; i ++) {
        memcpy(&new_mm_block->mr_info_list[i], &mr_info_list[i], sizeof(struct MrInfo));
        new_mm_block->server_id_list[i] = server_id_list[i];
    }

    // add subblocks to subblock list
    // Start from bmap_block_num_ to pass the leading bit map blocks
    for (int i = bmap_block_num_; i < subblock_num_; i ++) {
        SubblockInfo tmp_info;
        for (int r = 0; r < num_replication_; r ++) {
            tmp_info.addr_list[r] = new_mm_block->mr_info_list[r].addr + i * subblock_sz_;
            // assert((tmp_info.addr_list[r] & 0xFF) == 0);
            tmp_info.rkey_list[r] = new_mm_block->mr_info_list[r].rkey;
            tmp_info.server_id_list[r] = new_mm_block->server_id_list[r];
        }
        subblock_free_queue_.push_back(tmp_info);
    }

    mm_blocks_.push_back(new_mm_block);
    
    return 0;
}

int ClientMM::reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list, 
        UDPNetworkManager * nm, int alloc_type) {
    int ret = 0;
    ClientMetaAddrInfo meta_info;
    if (alloc_type == TYPE_KVBLOCK) {
        meta_info.meta_info_type = TYPE_KVBLOCK;
    } else {
        assert(alloc_type == TYPE_SUBTABLE);
        meta_info.meta_info_type = TYPE_SUBTABLE;
    }

    // prepare meta info
    // print_log(DEBUG, "[%s] prepare meta info", __FUNCTION__);
    for (int i = 0; i < num_replication_; i ++) {
        meta_info.server_id_list[i] = server_id_list[i];
        meta_info.addr_list[i] = mr_info_list[i].addr;
    }

    // send meta info to remote
    // print_log(DEBUG, "[%s] send meta info to remote", __FUNCTION__);
    for (int i = 0; i < num_replication_; i ++) {
        uint32_t rkey = nm->get_server_rkey(i);
        // print_log(DEBUG, "[%s] writing to server(%d) addr(%lx) rkey(%x)", 
        //     __FUNCTION__, i, client_meta_addr_, rkey);
        ret = nm->nm_rdma_write_inl_to_sid(&meta_info, sizeof(ClientMetaAddrInfo),
            client_meta_addr_, rkey, i);
        // assert(ret == 0);
    }
    client_meta_addr_ += sizeof(ClientMetaAddrInfo);

    // locally register blocks
    if (alloc_type == TYPE_KVBLOCK) {
        // print_log(DEBUG, "[%s] register locally", __FUNCTION__);
        ret = local_reg_blocks(mr_info_list, server_id_list);
        // assert(ret == 0);
    }
    return 0;
}

int ClientMM::init_reg_space(struct MrInfo mr_info_list[][MAX_REP_NUM], uint8_t server_id_list[][MAX_REP_NUM],
        UDPNetworkManager * nm, int alloc_type) {
    int ret = 0;
    assert(alloc_type == TYPE_KVBLOCK);

    std::queue<SubblockInfo> tmp_queue[num_memory_];
    for (int m = 0; m < num_memory_; m ++) {
        ClientMetaAddrInfo meta_info;
        
        meta_info.meta_info_type = TYPE_KVBLOCK;
        for (int i = 0; i < num_replication_; i ++) {
            meta_info.server_id_list[i] = server_id_list[m][i];
            meta_info.addr_list[i] = mr_info_list[m][i].addr;
        }

        for (int i = 0; i < num_replication_; i ++) {
            uint32_t rkey = nm->get_server_rkey(i);
            // print_log(DEBUG, "[%s] write meta to server(%d) raddr(%lx) rkey(%x)", __FUNCTION__, i, client_meta_addr_, rkey);
            ret = nm->nm_rdma_write_inl_to_sid(&meta_info, sizeof(ClientMetaAddrInfo),
                client_meta_addr_, rkey, i);
            // assert(ret == 0);
        }
        client_meta_addr_ += sizeof(ClientMetaAddrInfo);
        
        ClientMMBlock * new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
        memset(new_mm_block, 0, sizeof(ClientMMBlock));
        for (int i = 0; i < num_replication_; i ++) {
            memcpy(&new_mm_block->mr_info_list[i], &mr_info_list[m][i], sizeof(struct MrInfo));
            new_mm_block->server_id_list[i] = server_id_list[m][i];
            printf("mmblock %d: %lx\n", i, new_mm_block->mr_info_list[i].addr);
        }
        
        // start from bmap_block_num to pass the leading bitmap blocks
        for (int i = bmap_block_num_; i < subblock_num_; i ++) {
            SubblockInfo tmp_info;
            for (int r = 0; r < num_replication_; r ++) {
                tmp_info.addr_list[r] = new_mm_block->mr_info_list[r].addr + i * subblock_sz_;
                tmp_info.rkey_list[r] = new_mm_block->mr_info_list[r].rkey;
                tmp_info.server_id_list[r] = new_mm_block->server_id_list[r];
            }
            tmp_queue[m].push(tmp_info);
        }
        mm_blocks_.push_back(new_mm_block);
    }

    // merge queues
    uint32_t data_block_num = subblock_num_ - bmap_block_num_;
    for (int i = 0; i < num_memory_ * data_block_num; i ++) {
        int queue_id = i % num_memory_;
        SubblockInfo tmp_info = tmp_queue[queue_id].front();
        subblock_free_queue_.push_back(tmp_info);
        tmp_queue[queue_id].pop();
    }

    for (int i = 0; i < num_memory_; i ++) {
        if (tmp_queue[i].size() > 0) {
            printf("!!!!err\n");
            exit(1);
        }
    }
    return 0;
}

int ClientMM::dyn_reg_new_space(const struct MrInfo * mr_info_list, const uint8_t * server_id_list, 
        UDPNetworkManager * nm, int alloc_type) {
    int ret = 0;
    ClientMetaAddrInfo meta_info;
    if (alloc_type == TYPE_KVBLOCK) {
        meta_info.meta_info_type = TYPE_KVBLOCK;
    } else {
        // assert(alloc_type == TYPE_SUBTABLE);
        meta_info.meta_info_type = TYPE_SUBTABLE;
    }

    // prepare meta info
    // print_log(DEBUG, "[%s] prepare meta info", __FUNCTION__);
    for (int i = 0; i < num_replication_; i ++) {
        meta_info.server_id_list[i] = server_id_list[i];
        meta_info.addr_list[i] = mr_info_list[i].addr;
    }

    // send meta info to remote
    // print_log(DEBUG, "[%s] send meta info to remote", __FUNCTION__);
    for (int i = 0; i < num_replication_; i ++) {
        uint32_t rkey = nm->get_server_rkey(i);
        // print_log(DEBUG, "[%s] writing to server(%d) addr(%lx) rkey(%x)", __FUNCTION__, i, client_meta_addr_, rkey);
        ret = nm->nm_rdma_write_inl_to_sid_sync(&meta_info, sizeof(ClientMetaAddrInfo), client_meta_addr_, rkey, i);
        // assert(ret == 0);
    }
#ifndef SERVE_MM
    client_meta_addr_ += sizeof(ClientMetaAddrInfo);
#endif

    // locally register blocks
    if (alloc_type == TYPE_KVBLOCK) {
        // print_log(DEBUG, "[%s] register locally", __FUNCTION__);
        ret = local_reg_blocks(mr_info_list, server_id_list);
        // assert(ret == 0);
    }
    return 0;
}

int ClientMM::mm_recover_prepare_space(UDPNetworkManager * nm) {
    int ret = 0;
    // print_log(DEBUG, "  [%s] 1. create recoer space and register mr", __FUNCTION__);
    IbInfo ib_info;
    nm->get_ib_info(&ib_info);
    recover_buf_ = malloc(REC_SPACE_SIZE);
    recover_mr_  = ibv_reg_mr(ib_info.ib_pd, recover_buf_, REC_SPACE_SIZE, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    tmp_buf_ = (void *)((uint64_t)recover_buf_ + CLIENT_META_LEN);
    log_tail_st_ptr_ = (KVLogTail *)((uint64_t)tmp_buf_ + CLIENT_META_LEN);
    return 0;
}

int ClientMM::mm_traverse_log(UDPNetworkManager * nm) {
    int ret = 0;
    
    // 1. traverse remote chained log
    // print_log(DEBUG, "  [%s] 3. traverse remote chained log", __FUNCTION__);
    ClientLogMetaInfo * log_meta_info = (ClientLogMetaInfo *)recover_buf_;

    uint64_t ptr_addr = log_meta_info->pr_log_head + bmap_block_num_ * subblock_sz_;
    uint64_t ptr_sid  = log_meta_info->pr_server_id;
    int cnt = 0;
    while (ptr_addr != 0) {
        uint32_t rkey = nm->get_server_rkey(ptr_sid);
        ret = nm->nm_rdma_read_from_sid(tmp_buf_, recover_mr_->lkey, subblock_sz_, 
            ptr_addr, rkey, ptr_sid);

        KVLogHeader * head = (KVLogHeader *)tmp_buf_;
        KVLogTail   * tail = (KVLogTail *)((uint64_t)tmp_buf_
            + sizeof(KVLogHeader) + head->key_length + head->value_length);
        memcpy(log_tail_st_ptr_ + cnt, tail, sizeof(KVLogTail));
        // assert(ret == 0);
        // print_log(DEBUG, "  [%s]      valid(%d) gc(%d) commit(%d)", __FUNCTION__,
        //     log_is_valid(log_header_ptr + cnt), log_is_gc(log_header_ptr + cnt), log_is_committed(log_header_ptr + cnt));

        // save the read log header info
        RecoverLogInfo recover_info;
        recover_info.local_tail_addr = log_tail_st_ptr_ + cnt;
        recover_info.remote_addr = ptr_addr;
        recover_info.server_id   = ptr_sid;
        recover_info.key_len     = head->key_length;
        recover_info.val_len     = head->value_length;
        recover_log_info_list_.push_back(recover_info);
        
        // record the allocated information
        // printf("%lx\n", ptr_addr);
        assert((ptr_addr & 0xFF) == 0);
        uint64_t allocated_pr_addr = ptr_addr | ptr_sid;
        recover_addr_is_allocated_map_[allocated_pr_addr] = true;

        // print_log(DEBUG, "  [%s]      header read: next addr(%lx) press to continue", __FUNCTION__, cur_log_header->next_addr);
        // set pointer to next log header
        ptr_sid = tail->next_addr[5];
        ptr_addr = HashIndexConvert40To64Bits(tail->next_addr);
        cnt ++;
    }
    // printf("traversed: %d\n", cnt);
    return 0;
}

int ClientMM::mm_get_addr_meta(UDPNetworkManager * nm) {
    // print_log(DEBUG, "  [%s] 2. read meta addr info", __FUNCTION__);
    int ret = 0;
    uint64_t r_meta_addr = client_meta_addr_ - sizeof(ClientLogMetaInfo);
    uint32_t rkey = nm->get_server_rkey(0);
    ret = nm->nm_rdma_read_from_sid(recover_buf_, recover_mr_->lkey, 
        CLIENT_META_LEN, r_meta_addr, rkey, 0);
    // assert(ret == 0);
    return 0;
}

int ClientMM::mm_recover_mm_blocks(UDPNetworkManager * nm) {
    // print_log(DEBUG, "  [%s] start", __FUNCTION__);
    ClientMetaAddrInfo * meta_addr_info_list = (ClientMetaAddrInfo *)((uint64_t)recover_buf_ + sizeof(ClientLogMetaInfo));
    uint32_t num_log_entries = recover_log_info_list_.size();

    cur_mm_block_idx_ = -1;

    // recover the last allocated subblock info and the queue head info
    uint64_t last_allocated_pr_addr;
    uint64_t queue_head_pr_addr = recover_log_info_list_[num_log_entries - 1].remote_addr | recover_log_info_list_[num_log_entries - 1].server_id;
    if (num_log_entries > 1) {
        last_allocated_pr_addr = recover_log_info_list_[num_log_entries - 2].remote_addr | recover_log_info_list_[num_log_entries - 2].server_id;
    }

    // construct all subblock info
    std::queue<SubblockInfo> all_subblock_queue[num_memory_];
    SubblockInfo queue_head_info;
    while (meta_addr_info_list->meta_info_type != 0) {
        if (meta_addr_info_list->meta_info_type == TYPE_SUBTABLE) {
            meta_addr_info_list ++;
            continue;
        }

        // print_log(DEBUG, "[%s] recovering %lx", __FUNCTION__, meta_addr_info_list->addr_list[0]);

        // create a new mmblock
        ClientMMBlock * new_mmblock = get_new_mmblock();
        for (int i = 0; i < num_replication_; i ++) {
            new_mmblock->mr_info_list[i].addr = meta_addr_info_list->addr_list[i];
            new_mmblock->mr_info_list[i].rkey = nm->get_server_rkey(meta_addr_info_list->server_id_list[i]);
            new_mmblock->server_id_list[i] = meta_addr_info_list->server_id_list[i];
        }
        
        uint64_t pr_subblock_st_addr = new_mmblock->mr_info_list[0].addr;
        for (uint32_t i = bmap_block_num_; i < subblock_num_; i ++) {
            uint64_t cur_pr_addr = (pr_subblock_st_addr + subblock_sz_ * i) | new_mmblock->server_id_list[0];
            if (cur_pr_addr == queue_head_pr_addr) {
                gen_subblock_info(new_mmblock, i, &queue_head_info);
            } else if (cur_pr_addr == last_allocated_pr_addr) {
                gen_subblock_info(new_mmblock, i, &last_allocated_info_);
            } else if (recover_addr_is_allocated_map_[cur_pr_addr] == true) {
                // print_log(DEBUG, "[%s] %lx is allocated\n", __FUNCTION__, cur_pr_addr);
                continue;
            } else {
                SubblockInfo cur_subblock_info;
                gen_subblock_info(new_mmblock, i, &cur_subblock_info);
                all_subblock_queue[new_mmblock->server_id_list[0]].push(cur_subblock_info);
            }
        }

        mm_blocks_.push_back(new_mmblock);
        meta_addr_info_list ++;
    }

    // construct local allocation queue
    subblock_free_queue_.push_back(queue_head_info);
    while (true) {
        // stop condition
        bool need_stop = true;
        for (int i = 0; i < num_memory_; i ++) {
            if (all_subblock_queue[i].size() > 0) {
                subblock_free_queue_.push_back(all_subblock_queue[i].front());
                all_subblock_queue[i].pop();
                need_stop = false;
            }
        }
        if (need_stop) {
            break;
        }
    }
    
    return 0;
}

void ClientMM::gen_subblock_info(ClientMMBlock * mm_block, uint32_t subblock_idx,
        __OUT SubblockInfo * subblock_info) {
    for (int i = 0; i < num_replication_; i ++) {
        subblock_info->addr_list[i] = mm_block->mr_info_list[i].addr + subblock_idx * subblock_sz_;
        subblock_info->rkey_list[i] = mm_block->mr_info_list[i].rkey;
        subblock_info->server_id_list[i] = mm_block->server_id_list[i];
    }
}

int ClientMM::mm_recovery(UDPNetworkManager * nm) {
    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    int ret = 0;

    // 0. prepare recover buf
    ret = mm_recover_prepare_space(nm);
    // assert(ret == 0);
    gettimeofday(&local_recover_space_et_, NULL);

    // 1. get addr meta info
    ret = mm_get_addr_meta(nm);
    // assert(ret == 0);
    gettimeofday(&get_addr_meta_et_, NULL);

    // 2. traverse log
    ret = mm_traverse_log(nm);
    // assert(ret == 0);
    gettimeofday(&traverse_log_et_, NULL);

    // 3. recover mm blocks
    ret = mm_recover_mm_blocks(nm);
    // assert(ret == 0);

    return 0;
}

ClientMMBlock * ClientMM::get_new_mmblock() {
    ClientMMBlock * new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
    memset(new_mm_block, 0, sizeof(ClientMMBlock));

    return new_mm_block;
}

uint32_t ClientMM::get_subblock_idx(uint64_t addr, ClientMMBlock * mm_block) {
    return (addr - mm_block->mr_info_list[0].addr) / subblock_sz_;
}

void ClientMM::get_time_bread_down(std::vector<struct timeval> & time_vec) {
    time_vec.push_back(local_recover_space_et_);
    time_vec.push_back(get_addr_meta_et_);
    time_vec.push_back(traverse_log_et_);
}