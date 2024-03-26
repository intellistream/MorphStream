#include "client_cr.h"

#include <assert.h>
#include <sys/mman.h>
#include <stdio.h>
#include <sys/time.h>

// #include <boost/fiber/all.hpp>

#include <vector>
#include <fstream>

#include "kv_debug.h"

#define READ_BUCKET_ST_WRID 100
#define WRITE_KV_ST_WRID    200
#define READ_KV_ST_WRID     300
#define CAS_ST_WRID         400
#define INVALID_ST_WRID     500
#define READ_CACHE_ST_WRID  600
#define WRITE_HB_ST_WRID    700
#define LOG_COMMIT_ST_WRID  800
#define UPDATE_PREV_ST_WRID 900
#define READ_ALL_BUCKET_ST_WRID 150
#define READ_PR_ST_WRID     250
#define FAA_WRID            350

ClientCR::ClientCR(const struct GlobalConfig * conf) {
    num_idx_rep_             = conf->num_idx_rep;
    num_replication_         = conf->num_replication;
    remote_global_meta_addr_ = conf->server_base_addr;
    remote_meta_addr_ = conf->server_base_addr + CLIENT_META_LEN * (conf->server_id - conf->memory_num + 1);
    remote_gc_addr_   = conf->server_base_addr + META_AREA_LEN + CLIENT_GC_LEN * (conf->server_id - conf->memory_num + 1);
    remote_root_addr_ = conf->server_base_addr + META_AREA_LEN + GC_AREA_LEN;
    my_server_id_     = conf->server_id;
    num_memory_       = conf->memory_num;
    workload_run_time_ = conf->workload_run_time;

    num_coroutines_   = conf->num_coroutines;
    num_total_operations_   = 0;
    num_local_operations_   = 0;
    kv_info_list_     = NULL;
    kv_req_ctx_list_  = NULL;

    // bind core information
    main_core_id_ = conf->main_core_id;
    poll_core_id_ = conf->poll_core_id;
    bg_core_id_   = conf->bg_core_id;
    gc_core_id_   = conf->gc_core_id;

    miss_rate_threash_ = conf->miss_rate_threash;

    server_st_addr_     = conf->server_base_addr;
    server_data_len_    = conf->server_data_len;
    micro_workload_num_ = conf->micro_workload_num;

    // create cm
    nm_ = new UDPNetworkManager(conf);

    int ret = connect_ib_qps();
    // assert(ret == 0);

    // create mm
    mm_ = new ClientMM(conf, nm_);

    // alloc mr
    IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    local_buf_ = mmap(NULL, CORO_LOCAL_BUF_LEN * num_coroutines_, PROT_READ | PROT_WRITE, 
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    // assert(local_buf_ != MAP_FAILED);
    local_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, local_buf_, CORO_LOCAL_BUF_LEN * num_coroutines_, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // print_log(DEBUG, "register mr addr(0x%lx) rkey(%x)", local_buf_mr_->addr, local_buf_mr_->rkey);
    
    race_root_ = (RaceHashRoot *)malloc(sizeof(RaceHashRoot));
    // assert(race_root_ != NULL);
    race_root_mr_ = ibv_reg_mr(ib_info.ib_pd, race_root_, sizeof(RaceHashRoot),
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // assert(race_root_mr_ != NULL);
    // print_log(DEBUG, "register mr addr(0x%lx) rkey(%x)", race_root_mr_->addr, race_root_mr_->rkey);

    input_buf_ = malloc(CLINET_INPUT_BUF_LEN);
    // assert(input_buf_ != NULL);
    input_buf_mr_ = ibv_reg_mr(ib_info.ib_pd, input_buf_, CLINET_INPUT_BUF_LEN, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // assert(input_buf_mr_ != NULL);

    coro_local_addr_list_ = (uint64_t *)malloc(sizeof(uint64_t) * num_coroutines_);
    for (int i = 0; i < num_coroutines_; i ++) {
        coro_local_addr_list_[i] = (uint64_t)local_buf_ + CORO_LOCAL_BUF_LEN * i;
    }

    // record meta info
    if (conf->is_recovery == false) {
        mm_->get_log_head(&pr_log_server_id_, &pr_log_head_);
        pr_log_tail_ = pr_log_head_;
        ret = write_client_meta_info();
        // assert(ret == 0);

        // get root
        ret = get_race_root();
        // kv_assert(ret == 0);

        if (my_server_id_ - conf->memory_num == 0) {
            // init table
            ret = init_hash_table();
            // kv_assert(ret == 0);

            ret = sync_init_finish();
            // kv_assert(ret == 0);

            ret = get_race_root();
            // kv_assert(ret == 0);
        } else {
            while (!init_is_finished());
            ret = get_race_root();
            // kv_assert(ret == 0);
        }
    } else {
        ret = get_race_root();
        // kv_assert(ret == 0);
        
        ret = client_recovery();
        // assert(ret == 0);
    }
}

ClientCR::~ClientCR() {
    delete nm_;
    delete mm_;
}

int ClientCR::connect_ib_qps() {
    uint32_t num_servers = nm_->get_num_servers();
    int ret = 0;
    for (int i = 0; i < num_servers; i ++) {
        struct MrInfo * gc_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
        ret = nm_->client_connect_one_rc_qp(i, gc_info);
        // assert(ret == 0);
        server_mr_info_map_[i] = gc_info;
        // print_log(DEBUG, "connect to server(%d) addr(%lx) rkey(%x)", i, server_mr_info_map_[i]->addr, server_mr_info_map_[i]->rkey);
    }
    return 0;
}

int ClientCR::write_client_meta_info() {
    ClientLogMetaInfo meta_info;
    int ret;
    meta_info.pr_server_id = pr_log_server_id_;
    meta_info.pr_log_head  = pr_log_head_;
    meta_info.pr_log_tail  = pr_log_tail_;

    for (int i = 0; i < num_replication_; i ++) {
        struct MrInfo * cur_mr_info = server_mr_info_map_[i];
        // print_log(DEBUG, "write meta info to server(%d) addr(0x%lx) rkey(%x) len(%d)", i, cur_mr_info->addr, cur_mr_info->rkey, sizeof(ClientLogMetaInfo));
        ret = nm_->nm_rdma_write_inl_to_sid(&meta_info, sizeof(ClientLogMetaInfo), remote_meta_addr_, cur_mr_info->rkey, i);
        // assert(ret == 0);
    }

    return 0;
}

void * ClientCR::kv_search_w_cache(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_SEARCH;
    ctx.use_cache = true;
    ctx.kv_info  = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_cache_addr  = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_kv_addr     = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket) + 256);
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] kv_search start", __FUNCTION__);
    prepare_request(&ctx);
    kv_search_read_buckets(&ctx);
    kv_search_read_kv(&ctx);
    kv_search_check_kv(&ctx);
    return ctx.ret_val.value_addr;
}

void * ClientCR::kv_search(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_SEARCH;
    ctx.use_cache = false;
    ctx.kv_info = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_kv_addr     = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(&ctx);
    kv_search_read_buckets(&ctx);
    kv_search_read_kv(&ctx);
    kv_search_check_kv(&ctx);
    return ctx.ret_val.value_addr;
}

void * ClientCR::kv_search(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    prepare_request(ctx);
    kv_search_read_buckets(ctx);
    kv_search_read_kv(ctx); 
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void * ClientCR::kv_search_on_crash(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    prepare_request(ctx);
    check_failed_index(ctx);
    if (ctx->failed_pr_index) {
        kv_search_read_all_healthy_index(ctx);
    } else {
        kv_search_read_buckets(ctx);
    }

    kv_search_read_failed_kv(ctx);
    
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

void * ClientCR::kv_search_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    prepare_request(ctx);
    kv_search_read_buckets_sync(ctx);
    kv_search_read_kv_sync(ctx); 
    kv_search_check_kv(ctx);
    return ctx->ret_val.value_addr;
}

int ClientCR::kv_insert_w_cache(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_INSERT;
    ctx.use_cache = true;
    ctx.kv_info = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_cas_target_value_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_return_value_addr = (void *)((uint64_t)ctx.local_cas_target_value_addr + sizeof(uint64_t));
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(&ctx);
    kv_insert_read_buckets_and_write_kv(&ctx);
    kv_insert_backup_consensus_0(&ctx);
    if (num_idx_rep_ > 1)
        kv_insert_commit_log(&ctx);
    kv_insert_cas_primary(&ctx);
    return ctx.ret_val.ret_code;
}

int ClientCR::kv_insert(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_INSERT;
    ctx.use_cache = false;
    ctx.kv_info = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_cas_target_value_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_return_value_addr = (void *)((uint64_t)ctx.local_cas_target_value_addr + sizeof(uint64_t));
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(&ctx);
    kv_insert_read_buckets_and_write_kv(&ctx);
    kv_insert_backup_consensus_0(&ctx);
    if (num_idx_rep_ > 1)
        kv_insert_commit_log(&ctx);
    kv_insert_cas_primary(&ctx);
    return ctx.ret_val.ret_code;
}

int ClientCR::kv_insert(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s fb%d %lld] start (fb %d)", __FUNCTION__, ctx->coro_id,
    //     ctx->coro_id, boost::this_fiber::get_id());
    prepare_request(ctx);
    kv_insert_read_buckets_and_write_kv(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_insert_backup_consensus_0(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    if (num_idx_rep_ > 1) {
        kv_insert_commit_log(ctx);
    }
    kv_insert_cas_primary(ctx);
    return ctx->ret_val.ret_code;
}

int ClientCR::kv_insert_w_crash(KVReqCtx * ctx, int crash_point) {
    // print_log(DEBUG, "[%s fb%d %lld] start (fb %d)", __FUNCTION__, ctx->coro_id,
    //     ctx->coro_id, boost::this_fiber::get_id());
    prepare_request(ctx);
    kv_insert_read_buckets_and_write_kv(ctx);
    kv_insert_backup_consensus_0(ctx);
    if (crash_point == KV_CRASH_UNCOMMITTED_BK_CONSENSUS_0) {
        // print_log(DEBUG, "[%s] crash after backup consensus 0", __FUNCTION__);
        exit(1);
    }
    kv_insert_commit_log(ctx);
    if (crash_point == KV_CRASH_COMMITTED_PR_CAS) {
        // print_log(DEBUG, "[%s] crash before cas primary", __FUNCTION__);
        exit(1);
    }
    kv_insert_cas_primary(ctx);
    if (crash_point == KV_CRASH_COMMITTED_ALL_FINISH) {
        // print_log(DEBUG, "[%s] crash after all finish", __FUNCTION__);
        exit(1);
    }
    return ctx->ret_val.ret_code;
}

int ClientCR::kv_insert_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s fb%d %lld] start (fb %d)", __FUNCTION__, ctx->coro_id,
    //     ctx->coro_id, boost::this_fiber::get_id());
    prepare_request(ctx);
    kv_insert_read_buckets_and_write_kv_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_insert_backup_consensus_0_sync(ctx);
    if (num_idx_rep_ > 1) {
        kv_insert_commit_log_sync(ctx);
    }
    kv_insert_cas_primary_sync(ctx);
    return ctx->ret_val.ret_code;
}

int ClientCR::kv_update_w_cache(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_UPDATE;
    ctx.use_cache = true;
    ctx.kv_info = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_kv_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_target_value_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_return_value_addr = (void *)((uint64_t)ctx.local_cas_target_value_addr + sizeof(uint64_t));
    ctx.local_cache_addr = (void *)((uint64_t)ctx.local_cas_target_value_addr + sizeof(uint64_t) * num_replication_);
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(&ctx);
    kv_update_read_buckets_and_write_kv(&ctx);
    kv_update_read_kv(&ctx);
    kv_update_backup_consensus_0(&ctx);
    if (num_idx_rep_ > 1)
        kv_update_commit_log(&ctx);
    kv_update_cas_primary(&ctx);
    kv_update_bg_operations(&ctx);
    return ctx.ret_val.ret_code;
}

int ClientCR::kv_update(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_UPDATE;
    ctx.use_cache = false;
    ctx.kv_info = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_kv_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_target_value_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_return_value_addr = (void *)((uint64_t)ctx.local_cas_target_value_addr + sizeof(uint64_t));
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(&ctx);
    kv_update_read_buckets_and_write_kv(&ctx);
    kv_update_read_kv(&ctx);
    kv_update_backup_consensus_0(&ctx);
    if (num_idx_rep_ > 1)
        kv_update_commit_log(&ctx);
    kv_update_cas_primary(&ctx);
    kv_update_bg_operations(&ctx);
    return ctx.ret_val.ret_code;
}

int ClientCR::kv_update(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());

    prepare_request(ctx);
    kv_update_read_buckets_and_write_kv(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_update_read_kv(ctx);
    kv_update_backup_consensus_0(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    if (num_idx_rep_ > 1) {
        kv_update_commit_log(ctx);
    }
    kv_update_cas_primary(ctx);
    kv_update_bg_operations(ctx);
    return ctx->ret_val.ret_code;
}

int ClientCR::kv_update_w_crash(KVReqCtx * ctx, int crash_point) {
    // print_log(DEBUG, "[%s] start", __FUNCTION__);

    prepare_request(ctx);
    kv_update_read_buckets_and_write_kv(ctx);
    kv_update_read_kv(ctx);
    kv_update_backup_consensus_0(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    if (crash_point == KV_CRASH_UNCOMMITTED_BK_CONSENSUS_0) {
        // print_log(DEBUG, "[%s] crash after backup consensus 0", __FUNCTION__);
        exit(1);
    }
    if (crash_point == KV_CRASH_UNCOMMITTED_BK_CONSENSUS_1) {
        // print_log(DEBUG, "[%s] crash after backup consensus 1", __FUNCTION__);
        exit(1);
    }
    kv_update_commit_log(ctx);
    if (crash_point == KV_CRASH_COMMITTED_PR_CAS) {
        // print_log(DEBUG, "[%s] crash before cas primary", __FUNCTION__);
        exit(1);
    }
    kv_update_cas_primary(ctx);
    kv_update_bg_operations(ctx);
    if (crash_point == KV_CRASH_COMMITTED_ALL_FINISH) {
        // print_log(DEBUG, "[%s] crash after all finished", __FUNCTION__);
        exit(1);
    }
    return ctx->ret_val.ret_code;
}

int ClientCR::kv_update_sync(KVReqCtx * ctx) {
    prepare_request(ctx);
    kv_update_read_buckets_and_write_kv_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_update_read_kv_sync(ctx);
    kv_update_backup_consensus_0_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    if (num_idx_rep_ > 1) {
        kv_update_commit_log_sync(ctx);
    }
    kv_update_cas_primary_sync(ctx);
    kv_update_bg_operations(ctx);
    return ctx->ret_val.ret_code;
}

int ClientCR::kv_delete_w_cache(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_DELETE;
    ctx.use_cache = true;
    ctx.kv_info = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_cache_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_kv_addr    = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket) + 256);
    ctx.local_cas_target_value_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_return_value_addr = (void *)((uint64_t)ctx.local_cas_target_value_addr + sizeof(uint64_t));
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(&ctx);
    kv_delete_read_buckets_write_log(&ctx);
    kv_delete_read_kv(&ctx);
    kv_delete_backup_consensus_0(&ctx);
    if (num_idx_rep_ > 1) 
        kv_delete_commit_log(&ctx);
    kv_delete_cas_primary(&ctx);
    kv_delete_bg_operations(&ctx);
    return ctx.ret_val.ret_code;
}

int ClientCR::kv_delete(KVInfo * kv_info) {
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_DELETE;
    ctx.use_cache = false;
    ctx.kv_info = kv_info;
    ctx.local_bucket_addr = (RaceHashBucket *)local_buf_;
    ctx.local_kv_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_target_value_addr = (void *)((uint64_t)local_buf_ + 4 * sizeof(RaceHashBucket));
    ctx.local_cas_return_value_addr = (void *)((uint64_t)ctx.local_cas_target_value_addr + sizeof(uint64_t));
    ctx.lkey = local_buf_mr_->lkey;

    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(&ctx);
    kv_delete_read_buckets_write_log(&ctx);
    kv_delete_read_kv(&ctx);
    kv_delete_backup_consensus_0(&ctx);
    if (num_idx_rep_ > 1)
        kv_delete_commit_log(&ctx);
    kv_delete_cas_primary(&ctx);
    kv_delete_bg_operations(&ctx);
    return ctx.ret_val.ret_code;
}

int ClientCR::kv_delete(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(ctx);
    kv_delete_read_buckets_write_log(ctx);
    kv_delete_read_kv(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_delete_backup_consensus_0(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    if (num_idx_rep_ > 1) {
        kv_delete_commit_log(ctx);
    }
    kv_delete_cas_primary(ctx);
    kv_delete_bg_operations(ctx);
    return ctx->ret_val.ret_code;
}

int ClientCR::kv_delete_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    prepare_request(ctx);
    kv_delete_read_buckets_write_log_sync(ctx);
    kv_delete_read_kv_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    kv_delete_backup_consensus_0_sync(ctx);
    if (ctx->is_finished) {
        return ctx->ret_val.ret_code;
    }
    if (num_idx_rep_ > 1) {
        kv_delete_commit_log_sync(ctx);
    }
    kv_delete_cas_primary_sync(ctx);
    return ctx->ret_val.ret_code;
}

void ClientCR::get_kv_hash_info(KVInfo * a_kv_info, __OUT KVHashInfo * a_kv_hash_info) {
    uint64_t key_addr = (uint64_t)a_kv_info->l_addr + sizeof(KVLogHeader);
    a_kv_hash_info->hash_value = VariableLengthHash((void *)key_addr, a_kv_info->key_len, 0);
    a_kv_hash_info->prefix = (a_kv_hash_info->hash_value >> SUBTABLE_USED_HASH_BIT_NUM) & RACE_HASH_MASK(race_root_->global_depth);
    a_kv_hash_info->local_depth = race_root_->subtable_entry[a_kv_hash_info->prefix][0].local_depth;
    a_kv_hash_info->fp = HashIndexComputeFp(a_kv_hash_info->hash_value);
}

void ClientCR::get_kv_addr_info(KVHashInfo * a_kv_hash_info, __OUT KVTableAddrInfo * a_kv_addr_info) {
    uint64_t hash_value = a_kv_hash_info->hash_value;
    uint64_t prefix     = a_kv_hash_info->prefix;

    uint8_t  pr_server_id = race_root_->subtable_entry[prefix][0].server_id;
    // uint64_t r_subtable_off = HashIndexConvert40To64Bits(race_root_->subtable_entry[prefix][0].pointer);
    uint64_t f_index_value = SubtableFirstIndex(hash_value, race_root_->subtable_hash_range);
    uint64_t s_index_value = SubtableSecondIndex(hash_value, f_index_value, race_root_->subtable_hash_range);
    uint64_t f_idx, s_idx;

    if (f_index_value % 2 == 0) 
        f_idx = f_index_value / 2 * 3;
    else
        f_idx = f_index_value / 2 * 3 + 1;
    
    if (s_index_value % 2 == 0)
        s_idx = s_index_value / 2 * 3;
    else 
        s_idx = s_index_value / 2 * 3 + 1;
    
    // get combined bucket off
    a_kv_addr_info->f_main_idx = f_index_value % 2;
    a_kv_addr_info->s_main_idx = s_index_value % 2;
    a_kv_addr_info->f_idx = f_idx;
    a_kv_addr_info->s_idx = s_idx;

    uint64_t r_subtable_off[MAX_REP_NUM];

    for (int i = 0; i < num_idx_rep_; i ++) {
        uint8_t target_server_id = race_root_->subtable_entry[prefix][i].server_id;
        r_subtable_off[i] = HashIndexConvert40To64Bits(race_root_->subtable_entry[prefix][i].pointer);
        a_kv_addr_info->server_id_list[i] = target_server_id;
        a_kv_addr_info->f_bucket_addr[i]  = r_subtable_off[i] + f_idx * sizeof(RaceHashBucket);
        a_kv_addr_info->s_bucket_addr[i]  = r_subtable_off[i] + s_idx * sizeof(RaceHashBucket);
        a_kv_addr_info->f_bucket_addr_rkey[i] = server_mr_info_map_[target_server_id]->rkey;
        a_kv_addr_info->s_bucket_addr_rkey[i] = server_mr_info_map_[target_server_id]->rkey;
    }
    // print_log(DEBUG, "\t  [%s] search from server(%d) subtable_addr(%lx)", __FUNCTION__, pr_server_id, r_subtable_off);
}

IbvSrList * ClientCR::gen_read_bucket_sr_lists_on_crash(KVReqCtx * ctx, __OUT uint32_t * num_sr_lists) {
    const std::vector<uint8_t> & bk_idx_id_list = ctx->healthy_idx_server_id_list;
    uint32_t num_healthy = bk_idx_id_list.size();
    IbvSrList * ret = (IbvSrList *)malloc(sizeof(IbvSrList) * num_healthy);
    for (int i = 0; i < num_healthy; i ++) {
        uint32_t orig_idx = bk_idx_id_list[i];
        struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * 2);
        struct ibv_sge * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * 2);
        memset(sr, 0, sizeof(struct ibv_send_wr) * 2);
        memset(sge, 0, sizeof(struct ibv_sge) * 2);

        uint64_t local_addr = (uint64_t)ctx->local_bucket_addr + 4 * sizeof(RaceHashBucket) * i;
        sge[0].addr   = local_addr;
        sge[0].length = 2 * sizeof(RaceHashBucket);
        sge[0].lkey   = ctx->lkey;

        sge[1].addr   = local_addr + 2 * sizeof(RaceHashBucket);
        sge[1].length = 2 * sizeof(RaceHashBucket);
        sge[1].lkey   = ctx->lkey;

        sr[0].wr_id = ib_gen_wr_id(ctx->coro_id, 
            ctx->tbl_addr_info.server_id_list[orig_idx], READ_ALL_BUCKET_ST_WRID, 2 * (i + 1));
        sr[0].sg_list = &sge[0];
        sr[0].num_sge = 1;
        sr[0].opcode  = IBV_WR_RDMA_READ;
        sr[0].wr.rdma.remote_addr = ctx->tbl_addr_info.f_bucket_addr[orig_idx];
        sr[0].wr.rdma.rkey        = ctx->tbl_addr_info.f_bucket_addr_rkey[orig_idx];
        sr[0].next = &sr[1];
        
        sr[1].wr_id = ib_gen_wr_id(ctx->coro_id,
            ctx->tbl_addr_info.server_id_list[orig_idx], READ_ALL_BUCKET_ST_WRID, 2 * (i + 1) - 1);
        sr[1].sg_list = &sge[1];
        sr[1].num_sge = 1;
        sr[1].opcode  = IBV_WR_RDMA_READ;
        sr[1].wr.rdma.remote_addr = ctx->tbl_addr_info.s_bucket_addr[orig_idx];
        sr[1].wr.rdma.rkey        = ctx->tbl_addr_info.s_bucket_addr_rkey[orig_idx];
        sr[1].next = NULL;

        ret[i].num_sr = 2;
        ret[i].sr_list = sr;
        ret[i].server_id = ctx->tbl_addr_info.server_id_list[orig_idx];

        // print_log(DEBUG, "\t[%s] read bucket on server(%d) raddr(%lx, %lx) rkey(%x %x)", __FUNCTION__,
        //     ret[i].server_id, sr[0].wr.rdma.remote_addr, sr[1].wr.rdma.remote_addr, sr[0].wr.rdma.rkey, sr[1].wr.rdma.rkey);
    }
    *num_sr_lists = num_healthy;
    return ret;
}

void ClientCR::free_read_bucket_sr_lists_on_crash(IbvSrList * sr_list, int num_sr_lists) {
    for (int i = 0; i < num_sr_lists; i ++) {
        free(sr_list[i].sr_list[0].sg_list);
        free(sr_list[i].sr_list);
    }
    free(sr_list);
}

IbvSrList * ClientCR::gen_read_bucket_sr_lists(KVReqCtx * ctx, __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret = (IbvSrList *)malloc(sizeof(IbvSrList));
    struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * 2);
    struct ibv_sge * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * 2);
    memset(sr, 0, sizeof(struct ibv_send_wr) * 2);
    memset(sge, 0, sizeof(struct ibv_sge) * 2);

    sge[0].addr   = (uint64_t)ctx->local_bucket_addr;
    sge[0].length = 2 * sizeof(RaceHashBucket);
    sge[0].lkey   = ctx->lkey;

    sge[1].addr   = (uint64_t)ctx->local_bucket_addr + 2 * sizeof(RaceHashBucket);
    sge[1].length = 2 * sizeof(RaceHashBucket);
    sge[1].lkey   = ctx->lkey;

    sr[0].wr_id   = ib_gen_wr_id(ctx->coro_id, ctx->tbl_addr_info.server_id_list[0], READ_BUCKET_ST_WRID, 1);
    sr[0].sg_list = &sge[0];
    sr[0].num_sge = 1;
    sr[0].opcode  = IBV_WR_RDMA_READ;
    sr[0].wr.rdma.remote_addr = ctx->tbl_addr_info.f_bucket_addr[0];
    sr[0].wr.rdma.rkey        = ctx->tbl_addr_info.f_bucket_addr_rkey[0];
    sr[0].next    = &sr[1];

    sr[1].wr_id   = ib_gen_wr_id(ctx->coro_id, ctx->tbl_addr_info.server_id_list[0], READ_BUCKET_ST_WRID, 2);
    sr[1].sg_list = &sge[1];
    sr[1].num_sge = 1;
    sr[1].opcode  = IBV_WR_RDMA_READ;
    sr[1].wr.rdma.remote_addr = ctx->tbl_addr_info.s_bucket_addr[0];
    sr[1].wr.rdma.rkey        = ctx->tbl_addr_info.s_bucket_addr_rkey[0];
    sr[1].next    = NULL;

    ret->num_sr = 2;
    ret->sr_list = sr;
    ret->server_id = ctx->tbl_addr_info.server_id_list[0];
    *num_sr_lists = 1;

    // print_log(DEBUG, "\t[%s] read server(%d) raddr(%lx %lx) rkey(%lx %lx)", __FUNCTION__, 
    //     ret->server_id, sr[0].wr.rdma.remote_addr, sr[1].wr.rdma.remote_addr, sr[0].wr.rdma.rkey, sr[1].wr.rdma.rkey);

    return ret;
}

void ClientCR::free_read_bucket_sr_lists(IbvSrList * sr_list) {
    free(sr_list->sr_list[0].sg_list);
    free(sr_list->sr_list);
    free(sr_list);
}

IbvSrList * ClientCR::gen_write_kv_sr_lists(uint32_t coro_id, KVInfo * a_kv_info, ClientMMAllocCtx * r_mm_info,
        __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret_sr_list = (IbvSrList *)malloc(sizeof(IbvSrList) * num_replication_);
    struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * num_replication_);
    struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * num_replication_);
    memset(sr, 0, sizeof(struct ibv_send_wr) * num_replication_);
    memset(sge, 0, sizeof(struct ibv_sge) * num_replication_);

    for (int i = 0; i < num_replication_; i ++) {
        sge[i].addr   = (uint64_t)a_kv_info->l_addr;
        sge[i].length = r_mm_info->num_subblocks * mm_->subblock_sz_;
        sge[i].lkey   = a_kv_info->lkey;

        sr[i].wr_id   = ib_gen_wr_id(coro_id, r_mm_info->server_id_list[i], WRITE_KV_ST_WRID, i + 1);
        sr[i].sg_list = &sge[i];
        sr[i].num_sge = 1;
        sr[i].opcode  = IBV_WR_RDMA_WRITE;
        sr[i].wr.rdma.remote_addr = r_mm_info->addr_list[i];
        sr[i].wr.rdma.rkey        = r_mm_info->rkey_list[i];
        sr[i].next    = NULL;

        ret_sr_list[i].sr_list   = &sr[i];
        ret_sr_list[i].num_sr    = 1;
        ret_sr_list[i].server_id = r_mm_info->server_id_list[i];

        // print_log(DEBUG, "\t  [%s] write kv to server(%d) addr(%lx) rkey(%x)", __FUNCTION__, 
        //     ret_sr_list[i].server_id, sr[i].wr.rdma.remote_addr, sr[i].wr.rdma.rkey);
    }

    *num_sr_lists = num_replication_;
    return ret_sr_list;
}

void ClientCR::free_write_kv_sr_lists(IbvSrList * sr_list) {
    free(sr_list[0].sr_list[0].sg_list);
    free(sr_list[0].sr_list);
    free(sr_list);
}

IbvSrList * ClientCR::gen_write_del_log_sr_lists(uint32_t coro_id, KVInfo * a_kv_info, ClientMMAllocCtx * r_mm_info,
        __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret_sr_list = (IbvSrList *)malloc(sizeof(IbvSrList) * num_replication_);
    struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * num_replication_);
    struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * num_replication_);
    memset(sr, 0, sizeof(struct ibv_send_wr) * num_replication_);
    memset(sge, 0, sizeof(struct ibv_sge) * num_replication_);

    for (int i = 0; i < num_replication_; i ++) {
        sge[i].addr   = (uint64_t)a_kv_info->l_addr;
        sge[i].length = sizeof(KVLogHeader) + sizeof(KVLogTail) + a_kv_info->key_len;
        sge[i].lkey   = a_kv_info->lkey;

        sr[i].wr_id   = ib_gen_wr_id(coro_id, r_mm_info->server_id_list[i], WRITE_KV_ST_WRID, i + 1);
        sr[i].sg_list = &sge[i];
        sr[i].num_sge = 1;
        sr[i].opcode  = IBV_WR_RDMA_WRITE;
        sr[i].wr.rdma.remote_addr = r_mm_info->addr_list[i];
        sr[i].wr.rdma.rkey        = r_mm_info->rkey_list[i];
        sr[i].next    = NULL;

        ret_sr_list[i].sr_list   = &sr[i];
        ret_sr_list[i].num_sr    = 1;
        ret_sr_list[i].server_id = r_mm_info->server_id_list[i];
    }

    *num_sr_lists = num_replication_;
    return ret_sr_list;
}

void ClientCR::free_write_del_log_sr_lists(IbvSrList * sr_list) {
    free(sr_list[0].sr_list[0].sg_list);
    free(sr_list[0].sr_list);
    free(sr_list);
}

IbvSrList * ClientCR::gen_read_kv_sr_lists(uint32_t coro_id, const std::vector<KVRWAddr> & r_addr_list, 
        __OUT uint32_t * num_sr_lists) {
    // print_log(DEBUG, "\t[%s] read kvs:", __FUNCTION__);
    std::map<uint8_t, std::vector<KVRWAddr> > server_id_kv_addr_map;
    for (size_t i = 0; i < r_addr_list.size(); i ++) {
        server_id_kv_addr_map[r_addr_list[i].server_id].push_back(r_addr_list[i]);
        // print_log(DEBUG, "\t[%s]    server_id(%d) addr(%lx) rkey(%x)", __FUNCTION__, 
        //     r_addr_list[i].server_id, r_addr_list[i].r_kv_addr, r_addr_list[i].rkey);
    }

    IbvSrList * ret_sr_list = (IbvSrList *)malloc(sizeof(IbvSrList) * server_id_kv_addr_map.size());
    std::map<uint8_t, std::vector<KVRWAddr> >::iterator it;
    uint32_t sr_num_cnt = 0;
    for (it = server_id_kv_addr_map.begin(); it != server_id_kv_addr_map.end(); it++) {
        size_t cur_num_sr = it->second.size();
        struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * cur_num_sr);
        struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * cur_num_sr);
        memset(sr, 0, sizeof(struct ibv_send_wr) * cur_num_sr);
        memset(sge, 0, sizeof(struct ibv_sge) * cur_num_sr);
        for (size_t i = 0; i < it->second.size(); i ++) {
            sge[i].addr   = it->second[i].l_kv_addr;
            sge[i].length = it->second[i].length;
            sge[i].lkey   = it->second[i].lkey;

            sr[i].wr_id =  ib_gen_wr_id(coro_id, it->first, READ_KV_ST_WRID, i + 1);
            sr[i].sg_list = &sge[i];
            sr[i].num_sge = 1;
            sr[i].opcode  = IBV_WR_RDMA_READ;
            sr[i].wr.rdma.remote_addr = it->second[i].r_kv_addr;
            sr[i].wr.rdma.rkey = it->second[i].rkey;
            if (i != it->second.size() - 1) {
                sr[i].next = &sr[i + 1];
            }
        }
        ret_sr_list[sr_num_cnt].sr_list   = sr;
        ret_sr_list[sr_num_cnt].server_id = it->first;
        ret_sr_list[sr_num_cnt].num_sr    = it->second.size();
        sr_num_cnt ++;
    }

    *num_sr_lists = server_id_kv_addr_map.size();
    return ret_sr_list;
}

void ClientCR::free_read_kv_sr_lists(IbvSrList * sr_list, int num_sr_lists) {
    for (int i = 0; i < num_sr_lists; i ++) {
        free(sr_list[i].sr_list[0].sg_list);
        free(sr_list[i].sr_list);
    }
    free(sr_list);
}

IbvSrList * ClientCR::gen_cas_sr_lists(uint32_t coro_id, const std::vector<KVCASAddr> & cas_addr_list, __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret_sr_lists = (IbvSrList *)malloc(sizeof(IbvSrList) * cas_addr_list.size());
    for (size_t i = 0; i < cas_addr_list.size(); i ++) {
        struct ibv_send_wr * sr  = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr));
        struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge));
        memset(sr, 0, sizeof(struct ibv_send_wr));
        memset(sge, 0, sizeof(struct ibv_sge));

        sge->addr = cas_addr_list[i].l_kv_addr;
        sge->length = 8;
        sge->lkey = cas_addr_list[i].lkey;

        sr->wr_id = ib_gen_wr_id(coro_id, cas_addr_list[i].server_id, CAS_ST_WRID, i + 1);
        sr->sg_list = sge;
        sr->num_sge = 1;
        sr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        sr->wr.atomic.remote_addr = cas_addr_list[i].r_kv_addr;
        sr->wr.atomic.rkey        = cas_addr_list[i].rkey;
        sr->wr.atomic.compare_add = cas_addr_list[i].orig_value;
        sr->wr.atomic.swap        = cas_addr_list[i].swap_value;
        sr->next = NULL;

        ret_sr_lists[i].sr_list = sr;
        ret_sr_lists[i].num_sr  = 1;
        ret_sr_lists[i].server_id = cas_addr_list[i].server_id;
        // print_log(DEBUG, "\t    [%s fb%d] cas server(%d) raddr(%lx) orig(%lx) target(%lx)", __FUNCTION__, coro_id, ret_sr_lists[i].server_id, 
        //     sr->wr.atomic.remote_addr, sr->wr.atomic.compare_add, sr->wr.atomic.swap);
    }
    *num_sr_lists = cas_addr_list.size();
    return ret_sr_lists;
}

void ClientCR::free_cas_sr_lists(IbvSrList * sr_lists, int num_sr_lists) {
    for (int i = 0; i < num_sr_lists; i ++) {
        free(sr_lists[i].sr_list[0].sg_list);
        free(sr_lists[i].sr_list);
    }
    free(sr_lists);
}

void ClientCR::fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info,
        __OUT RaceHashSlot * local_slot) {
    local_slot->fp = a_kv_hash_info->fp;
    local_slot->kv_len = mm_alloc_ctx->num_subblocks;
    local_slot->server_id = mm_alloc_ctx->server_id_list[0];
    HashIndexConvert64To40Bits(mm_alloc_ctx->addr_list[0], local_slot->pointer);
}

void ClientCR::fill_cas_addr(KVReqCtx * ctx, uint64_t * remote_slot_addr, RaceHashSlot * old_local_slot_addr, RaceHashSlot * new_local_slot_addr) {
    for (int i = 0; i < num_idx_rep_; i ++) {
        KVCASAddr * cur_cas_addr;
        if (i == 0) {
            cur_cas_addr = &ctx->kv_modify_pr_cas_list[0];
        } else {
            cur_cas_addr = &ctx->kv_modify_bk_0_cas_list[i - 1];
        }
        cur_cas_addr->r_kv_addr = remote_slot_addr[i];
        cur_cas_addr->rkey      = server_mr_info_map_[ctx->tbl_addr_info.server_id_list[i]]->rkey;
        cur_cas_addr->l_kv_addr = (uint64_t)ctx->local_cas_return_value_addr + i * sizeof(uint64_t);
        cur_cas_addr->lkey      = ctx->lkey;
        cur_cas_addr->orig_value = ConvertSlotToInt(old_local_slot_addr);
        cur_cas_addr->swap_value = ConvertSlotToInt(new_local_slot_addr);
        cur_cas_addr->server_id  = ctx->tbl_addr_info.server_id_list[i];

        // push to ctx
        // print_log(DEBUG, "\t\t[%s] cas raddr(%lx) laddr(%lx) orig(%lx) target(%lx)", __FUNCTION__, cur_cas_addr->r_kv_addr, cur_cas_addr->l_kv_addr, 
        //     cur_cas_addr->orig_value, cur_cas_addr->swap_value);
    }
    // assert(sizeof(RaceHashSlot) == sizeof(uint64_t));
    // print_log(DEBUG, "\t\t[%s] old_local_slot_info: fp(%d) kv_len(%d) addr(%lx) server_id(%d)", __FUNCTION__,  
    //     old_local_slot_addr->fp, old_local_slot_addr->kv_len, HashIndexConvert40To64Bits(old_local_slot_addr->pointer), old_local_slot_addr->server_id);
    // print_log(DEBUG, "\t\t[%s] new_local_slot_info: fp(%d) kv_len(%d) addr(%lx) server_id(%d)", __FUNCTION__,  
    //     new_local_slot_addr->fp, new_local_slot_addr->kv_len, HashIndexConvert40To64Bits(new_local_slot_addr->pointer), new_local_slot_addr->server_id);
    // assert(ctx->kv_modify_pr_cas_list.size() == 1);
    // assert(ctx->kv_modify_bk_0_cas_list.size() == num_idx_rep_ - 1);
}

void ClientCR::fill_cas_addr(KVTableAddrInfo * addr_info, uint64_t remote_slot_addr, RaceHashSlot * old_local_slot, 
        RaceHashSlot * new_local_slot, __OUT KVCASAddr * pr_cas_addr, __OUT KVCASAddr * bk_cas_addr) {
    pr_cas_addr->r_kv_addr = remote_slot_addr;
    pr_cas_addr->rkey = server_mr_info_map_[addr_info->server_id_list[0]]->rkey;
    pr_cas_addr->l_kv_addr = (uint64_t)new_local_slot + sizeof(RaceHashSlot);
    pr_cas_addr->lkey = local_buf_mr_->lkey;
    pr_cas_addr->orig_value = ConvertSlotToInt(old_local_slot);
    pr_cas_addr->swap_value = ConvertSlotToInt(new_local_slot);
    pr_cas_addr->server_id  = addr_info->server_id_list[0];

    memcpy(bk_cas_addr, pr_cas_addr, sizeof(KVCASAddr));
    bk_cas_addr->rkey = server_mr_info_map_[addr_info->server_id_list[1]]->rkey;
    bk_cas_addr->server_id = addr_info->server_id_list[1];
}

void ClientCR::fill_heartbeat_addr(uint8_t server_id, __OUT KVRWAddr * hb_addr) {
    hb_addr->r_kv_addr = remote_global_meta_addr_;
    hb_addr->rkey      = server_mr_info_map_[server_id]->rkey;
    hb_addr->l_kv_addr = (uint64_t)local_buf_;
    hb_addr->lkey      = local_buf_mr_->lkey;
    hb_addr->length    = 0;
    hb_addr->server_id = server_id;
    // print_log(DEBUG, "\t  [%s] write heart beat to server(%d)", __FUNCTION__, server_id);
}

void ClientCR::fill_invalid_addr(KVReqCtx * ctx, RaceHashSlot * local_slot) {
    ctx->kv_invalid_addr.r_kv_addr = HashIndexConvert40To64Bits(local_slot->pointer);
    ctx->kv_invalid_addr.server_id = local_slot->server_id;
    ctx->kv_invalid_addr.rkey = server_mr_info_map_[ctx->kv_invalid_addr.server_id]->rkey;
    // print_log(DEBUG, "\t  [%s fb%d %ld] invalid raddr(%lx) server(%d) rkey(%x)", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id(),
    //     ctx->kv_invalid_addr.r_kv_addr, ctx->kv_invalid_addr.server_id, ctx->kv_invalid_addr.rkey);
}

IbvSrList * ClientCR::gen_read_cache_kv_sr_lists(uint32_t coro_id, RaceHashSlot * local_slot_ptr, uint64_t local_addr) {
    IbvSrList * ret_sr_list = (IbvSrList *)malloc(sizeof(IbvSrList));
    struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr));
    struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge));

    memset(sr, 0, sizeof(struct ibv_send_wr));
    memset(sge, 0, sizeof(struct ibv_sge));

    sge->addr   = local_addr;
    sge->length = (local_slot_ptr->kv_len) * mm_->subblock_sz_;
    sge->lkey   = local_buf_mr_->lkey;

    sr->wr_id = ib_gen_wr_id(coro_id, local_slot_ptr->server_id, READ_CACHE_ST_WRID, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = IBV_WR_RDMA_READ;
    sr->wr.rdma.remote_addr = HashIndexConvert40To64Bits(local_slot_ptr->pointer);
    sr->wr.rdma.rkey        = server_mr_info_map_[local_slot_ptr->server_id]->rkey;
    sr->next = NULL;

    ret_sr_list->sr_list = sr;
    ret_sr_list->num_sr = 1;
    ret_sr_list->server_id = local_slot_ptr->server_id;

    // print_log(DEBUG, "\t[%s] read cached server(%d) addr(%lx) rkey(%x) len(%d) to local(%lx)", __FUNCTION__, 
    //     local_slot_ptr->server_id, sr->wr.rdma.remote_addr, sr->wr.rdma.rkey, sge->length, (uint64_t)sge->addr);
    return ret_sr_list;
}

void ClientCR::free_read_cache_kv_sr_lists(IbvSrList * sr_lists) {
    free(sr_lists[0].sr_list[0].sg_list);
    free(sr_lists[0].sr_list);
    free(sr_lists);
}

IbvSrList * ClientCR::gen_invalid_sr_lists(uint32_t coro_id, KVRWAddr * r_addr, uint64_t local_data_addr) {
    IbvSrList * ret_sr_lists = (IbvSrList *)malloc(sizeof(IbvSrList));
    struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr));
    struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge));

    memset(sr, 0, sizeof(struct ibv_send_wr));
    memset(sge, 0, sizeof(struct ibv_sge));

    sge->addr   = local_data_addr;
    sge->length = 1;
    sge->lkey   = 0;

    sr->wr_id = ib_gen_wr_id(coro_id, r_addr->server_id, INVALID_ST_WRID, 1);
    sr->sg_list = sge;
    sr->num_sge = 1;
    sr->opcode = IBV_WR_RDMA_WRITE;
    sr->send_flags = IBV_SEND_INLINE;
    sr->wr.rdma.remote_addr = r_addr->r_kv_addr;
    sr->wr.rdma.rkey        = r_addr->rkey;
    sr->next = NULL;

    ret_sr_lists->sr_list = sr;
    ret_sr_lists->num_sr = 1;
    ret_sr_lists->server_id = r_addr->server_id;

    // print_log(DEBUG, "\t\t[%s] invalid kv in server(%d) addr(%lx)", __FUNCTION__, ret_sr_lists->server_id, ret_sr_lists->sr_list->wr.rdma.remote_addr);
    return ret_sr_lists;
}

void ClientCR::free_invalid_sr_lists(IbvSrList * sr_lists) {
    free(sr_lists[0].sr_list[0].sg_list);
    free(sr_lists[0].sr_list);
    free(sr_lists);
}

IbvSrList * ClientCR::gen_write_hb_sr_lists(uint32_t coro_id, std::vector<KVRWAddr> & rw_addr_list, __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret_sr_lists = (IbvSrList *)malloc(sizeof(IbvSrList) * rw_addr_list.size());
    for (size_t i = 0; i < rw_addr_list.size(); i ++) {
        struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr));
        struct ibv_sge     * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge));
        memset(sr, 0, sizeof(struct ibv_send_wr));
        memset(sge, 0, sizeof(struct ibv_sge));

        sge->addr = rw_addr_list[i].l_kv_addr;
        sge->addr = rw_addr_list[i].length;
        sge->lkey = 0;

        sr->wr_id = ib_gen_wr_id(coro_id, rw_addr_list[i].server_id, WRITE_HB_ST_WRID, i + 1);
        sr->sg_list = sge;
        sr->num_sge = 1;
        sr->opcode = IBV_WR_RDMA_WRITE;
        sr->send_flags = IBV_SEND_INLINE;
        sr->wr.rdma.remote_addr = rw_addr_list[i].r_kv_addr;
        sr->wr.rdma.rkey        = rw_addr_list[i].rkey;
        sr->next = NULL;

        ret_sr_lists[i].sr_list = sr;
        ret_sr_lists[i].num_sr = 1;
        ret_sr_lists[i].server_id = rw_addr_list[i].server_id;
    }
    *num_sr_lists = rw_addr_list.size();
    return ret_sr_lists;
}

void ClientCR::free_write_hb_sr_lists(IbvSrList * sr_lists, int num_sr_lists) {
    for (int i = 0; i < num_sr_lists; i ++) {
        free(sr_lists[i].sr_list[0].sg_list);
        free(sr_lists[i].sr_list);
    }
    free(sr_lists);
}

IbvSrList * ClientCR::gen_log_commit_sr_lists(uint32_t coro_id,
        void * local_addr, uint32_t size,
        std::vector<KVRWAddr> & rw_addr_list, __OUT uint32_t * num_sr_lists) {
    IbvSrList * ret_sr_lists = (IbvSrList *)malloc(sizeof(IbvSrList) * num_replication_);
    // *local_val_addr = (KV_LOG_VALID | KV_LOG_COMMITTED);
    for (int i = 0; i < num_replication_; i ++) {
        struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr));
        struct ibv_sge * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge));

        memset(sr, 0, sizeof(struct ibv_send_wr));
        memset(sge, 0, sizeof(struct ibv_sge));

        sge->addr = (uint64_t)local_addr;
        sge->length = size;
        sge->lkey = 0;

        sr->wr_id = ib_gen_wr_id(coro_id, rw_addr_list[i].server_id, LOG_COMMIT_ST_WRID, i + 1);
        sr->sg_list = sge;
        sr->num_sge = 1;
        sr->opcode = IBV_WR_RDMA_WRITE;
        sr->send_flags = IBV_SEND_INLINE;
        sr->wr.rdma.remote_addr = rw_addr_list[i].r_kv_addr;
        sr->wr.rdma.rkey        = rw_addr_list[i].rkey;
        sr->next = NULL;

        ret_sr_lists[i].sr_list = sr;
        ret_sr_lists[i].num_sr = 1;
        ret_sr_lists[i].server_id = rw_addr_list[i].server_id;

        // print_log(DEBUG, "  [%s fb%d %ld] commit to server(%d) raddr(%lx) value(%d)", __FUNCTION__, coro_id, boost::this_fiber::get_id(),
        //     ret_sr_lists[i].server_id, sr->wr.rdma.remote_addr, *local_val_addr);
    }
    
    *num_sr_lists = num_replication_;
    return ret_sr_lists;
}

void ClientCR::free_log_commit_sr_lists(IbvSrList * sr_lists, int num_sr_lists) {
    for (int i = 0; i < num_sr_lists; i ++) {
        free(sr_lists[i].sr_list[0].sg_list);
        free(sr_lists[i].sr_list);
    }
    free(sr_lists);
}

IbvSrList * ClientCR::gen_read_all_bucket_sr_lists(KVReqCtx * ctx, __OUT uint32_t * num_sr_list) {
    IbvSrList * ret = (IbvSrList *)malloc(sizeof(IbvSrList) * num_replication_);
    for (int i = 0; i < num_idx_rep_; i ++) {
        struct ibv_send_wr * sr = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * 2);
        struct ibv_sge * sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * 2);
        memset(sr, 0, sizeof(struct ibv_send_wr) * 2);
        memset(sge, 0, sizeof(struct ibv_sge) * 2);

        uint64_t local_addr = (uint64_t)ctx->local_bucket_addr + 4 * sizeof(RaceHashBucket) * i;
        sge[0].addr   = local_addr;
        sge[0].length = 2 * sizeof(RaceHashBucket);
        sge[0].lkey   = ctx->lkey;

        sge[1].addr   = local_addr + 2 * sizeof(RaceHashBucket);
        sge[1].length = 2 * sizeof(RaceHashBucket);
        sge[1].lkey   = ctx->lkey;

        sr[0].wr_id = ib_gen_wr_id(ctx->coro_id, 
            ctx->tbl_addr_info.server_id_list[i], READ_ALL_BUCKET_ST_WRID, i + 1);
        sr[0].sg_list = &sge[0];
        sr[0].num_sge = 1;
        sr[0].opcode  = IBV_WR_RDMA_READ;
        sr[0].wr.rdma.remote_addr = ctx->tbl_addr_info.f_bucket_addr[i];
        sr[0].wr.rdma.rkey        = ctx->tbl_addr_info.f_bucket_addr_rkey[i];
        sr[0].next = &sr[1];
        
        sr[1].wr_id = ib_gen_wr_id(ctx->coro_id,
            ctx->tbl_addr_info.server_id_list[i], READ_ALL_BUCKET_ST_WRID, i + 1);
        sr[1].sg_list = &sge[1];
        sr[1].num_sge = 1;
        sr[1].opcode  = IBV_WR_RDMA_READ;
        sr[1].wr.rdma.remote_addr = ctx->tbl_addr_info.s_bucket_addr[i];
        sr[1].wr.rdma.rkey        = ctx->tbl_addr_info.s_bucket_addr_rkey[i];
        sr[1].next = NULL;

        ret[i].num_sr = 2;
        ret[i].sr_list = sr;
        ret[i].server_id = ctx->tbl_addr_info.server_id_list[i];

        // print_log(DEBUG, "\t[%s] read bucket on server(%d) raddr(%lx, %lx) rkey(%x %x)", __FUNCTION__,
        //     ret[i].server_id, sr[0].wr.rdma.remote_addr, sr[1].wr.rdma.remote_addr, sr[0].wr.rdma.rkey, sr[1].wr.rdma.rkey);
    }
    *num_sr_list = num_replication_;
    return ret;
}

void ClientCR::free_read_all_bucket_sr_lists(IbvSrList * sr_list) {
    for (int i = 0; i < num_replication_; i ++) {
        free(sr_list[i].sr_list[0].sg_list);
        free(sr_list[i].sr_list);
    }
    free(sr_list);
}

// test functions
int ClientCR::test_get_root(__OUT RaceHashRoot * race_root) {
    int ret = get_race_root();
    if (ret == 0)
        memcpy(race_root, race_root_, sizeof(RaceHashRoot));
    return ret;
}

int ClientCR::test_get_remote_log_header(uint8_t server_id, uint64_t raddr, uint32_t buf_size,
        __OUT void * buf) {
    int ret = 0;
    ret = nm_->nm_rdma_read_from_sid_sync(local_buf_, local_buf_mr_->lkey, buf_size, 
        raddr, server_mr_info_map_[server_id]->rkey, server_id);
    // kv_assert(ret == 0);
    memcpy(buf, local_buf_, buf_size);
    return 0;
}

int ClientCR::test_get_pr_log_meta_info(__OUT ClientLogMetaInfo * pr_log_meta_info) {
    int ret = 0;
    ret = nm_->nm_rdma_read_from_sid_sync(local_buf_, local_buf_mr_->lkey, sizeof(ClientLogMetaInfo), 
        remote_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    // kv_assert(ret == 0);
    memcpy(pr_log_meta_info, local_buf_, sizeof(ClientLogMetaInfo));
    return 0;
}

int ClientCR::test_get_log_meta_info(__OUT ClientLogMetaInfo * remote_log_meta_info_list, 
        __OUT ClientLogMetaInfo * local_meta) {    
    int ret = 0;
    for (int i = 0; i < num_replication_; i ++) {
        ret = nm_->nm_rdma_read_from_sid_sync(local_buf_, local_buf_mr_->lkey, sizeof(ClientLogMetaInfo), 
            remote_meta_addr_, server_mr_info_map_[i]->rkey, i);
        // assert(ret == 0);

        memcpy(&remote_log_meta_info_list[i], local_buf_, sizeof(ClientLogMetaInfo));
    }

    local_meta->pr_server_id = pr_log_server_id_;
    local_meta->pr_log_head  = pr_log_head_;
    local_meta->pr_log_tail  = pr_log_tail_;
    return 0;
}

ClientMetaAddrInfo ** ClientCR::test_get_meta_addr_info(__OUT uint64_t * list_len) {
    int ret = 0;
    uint64_t remote_meta_ptr = mm_->get_remote_meta_ptr();
    uint32_t read_len = remote_meta_ptr - remote_meta_addr_ - sizeof(ClientLogMetaInfo);
    uint64_t remote_read_addr = remote_global_meta_addr_ + sizeof(ClientLogMetaInfo);
    uint32_t num_meta_addr = read_len / sizeof(ClientMetaAddrInfo);

    // assert(read_len % sizeof(ClientMetaAddrInfo) == 0);

    ClientMetaAddrInfo ** ret_info = (ClientMetaAddrInfo **)malloc(sizeof(ClientMetaAddrInfo *) * num_replication_);

    for (int i = 0; i < num_replication_; i ++) {
        ret_info[i] = (ClientMetaAddrInfo *)malloc(sizeof(ClientMetaAddrInfo) * num_meta_addr);
        ret = nm_->nm_rdma_read_from_sid((void *)local_buf_, local_buf_mr_->lkey, read_len, remote_read_addr, 
            server_mr_info_map_[i]->rkey, i);
        // assert(ret == 0);

        // print_log(DEBUG, "read from server(%d) addr(0x%lx) rkey(%x)", i, remote_read_addr, server_mr_info_map_[i]->rkey);
        memcpy(ret_info[i], local_buf_, read_len);
    }

    *list_len = num_meta_addr;
    return ret_info;
}

int ClientCR::test_get_local_mm_blocks(__OUT ClientMMBlock * mm_block_list, __OUT uint64_t * list_len) {
    ClientMMBlock * tmp = mm_->get_cur_mm_block();
    memcpy(mm_block_list, tmp, sizeof(ClientMMBlock));
    *list_len = mm_->get_num_mm_blocks();
    return 0;
}

int ClientCR::init_hash_table() {
    // initialize remote subtable entry after getting root information
    for (int i = 0; i < RACE_HASH_INIT_SUBTABLE_NUM; i ++) {
        for (int j = 0; j < RACE_HASH_SUBTABLE_NUM / RACE_HASH_INIT_SUBTABLE_NUM; j ++) {
            uint64_t subtable_idx = j * RACE_HASH_INIT_SUBTABLE_NUM + i;
            ClientMMAllocSubtableCtx subtable_info[num_idx_rep_];
            mm_->mm_alloc_subtable(nm_, subtable_info);
            for (int r = 0; r < num_idx_rep_; r ++) {
                // print_log(DEBUG, "[%s] subtable(%lx) on server(%d)", __FUNCTION__, subtable_info[r].addr, subtable_info[r].server_id);
                race_root_->subtable_entry[subtable_idx][r].lock        = 0;
                race_root_->subtable_entry[subtable_idx][r].local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
                race_root_->subtable_entry[subtable_idx][r].server_id   = subtable_info[r].server_id;
                // assert((subtable_info[r].addr & 0xFF) == 0);
                HashIndexConvert64To40Bits(subtable_info[r].addr, race_root_->subtable_entry[subtable_idx][r].pointer);
            }
        }   
    }

    // write root information back to all replicas
    int ret = write_race_root();
    // assert(ret == 0);
    return 0;
}

int ClientCR::sync_init_finish() {
    uint64_t local_msg = 1;
    int ret = 0;
    for (int i = num_replication_ - 1; i >= 0; i --) {
        ret = nm_->nm_rdma_write_inl_to_sid(&local_msg, sizeof(uint64_t), 
            remote_global_meta_addr_, server_mr_info_map_[i]->rkey, i);
        // assert(ret == 0);
    }
    return 0;
}

bool ClientCR::init_is_finished() {
    int ret = 0;
    ret = nm_->nm_rdma_read_from_sid(local_buf_, local_buf_mr_->lkey, 
        sizeof(uint64_t), remote_global_meta_addr_, server_mr_info_map_[0]->rkey, 0);
    // assert(ret == 0);
    uint64_t read_value = *(uint64_t *)local_buf_;
    if (read_value == 1) {
        return true;
    }
    return false;
}

void ClientCR::prepare_request(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[1. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    // memset(&ctx->tbl_addr_info, 0, sizeof(KVTableAddrInfo));
    // memset(&ctx->hash_info, 0, sizeof(KVHashInfo));

    get_kv_hash_info(ctx->kv_info, &ctx->hash_info);
    get_kv_addr_info(&ctx->hash_info, &ctx->tbl_addr_info);
}

void ClientCR::prepare_log_commit_addrs(KVReqCtx * ctx) {
    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    uint32_t tail_offset = sizeof(KVLogHeader) + header->key_length + header->value_length;
    uint32_t commit_offset = tail_offset + offsetof(KVLogTail, old_value);
    for (int i = 0; i < num_replication_; i ++) {
        KVRWAddr * cur_rw_addr = &(ctx->log_commit_addr_list[i]);
        cur_rw_addr->r_kv_addr = ctx->mm_alloc_ctx.addr_list[i] + commit_offset;
        cur_rw_addr->server_id = ctx->mm_alloc_ctx.server_id_list[i];
        cur_rw_addr->rkey = ctx->mm_alloc_ctx.rkey_list[i];
    }
}

void ClientCR::find_kv_in_buckets(KVReqCtx * ctx) {
    get_local_bucket_info(ctx);
    uint64_t local_kv_buf_addr = (uint64_t)ctx->local_kv_addr;
    ctx->kv_read_addr_list.clear();
    ctx->kv_idx_list.clear();

    // search all kv pair that finger print matches
    // print_log(DEBUG, "\t  [%s] start", __FUNCTION__);
    for (int i = 0; i < 4; i ++) {
        // we do not consider resizing
        // print_log(DEBUG, "\t    [%s] remote_local_depth: %d cache_local_depth: %d", __FUNCTION__, 
        //     ctx->bucket_arr[i]->local_depth, ctx->hash_info.local_depth);
        // kv_assert(ctx->bucket_arr[i]->local_depth == ctx->hash_info.local_depth);

        for (int j = 0; j < RACE_HASH_ASSOC_NUM; j ++) {
            if (ctx->slot_arr[i][j].fp == ctx->hash_info.fp 
                    && ctx->slot_arr[i][j].kv_len != 0) {
                // push the offset to the lists
                KVRWAddr cur_kv_addr;
                cur_kv_addr.r_kv_addr = HashIndexConvert40To64Bits(ctx->slot_arr[i][j].pointer);
                cur_kv_addr.rkey      = server_mr_info_map_[ctx->slot_arr[i][j].server_id]->rkey;
                cur_kv_addr.l_kv_addr = local_kv_buf_addr;
                cur_kv_addr.lkey      = local_buf_mr_->lkey;
                cur_kv_addr.length    = ctx->slot_arr[i][j].kv_len * mm_->subblock_sz_;
                cur_kv_addr.server_id = ctx->slot_arr[i][j].server_id;

                // print_log(DEBUG, "\t      [%s]  find (%d, %d) raddr(%lx) rkey(%x)", __FUNCTION__, i, j, cur_kv_addr.r_kv_addr, cur_kv_addr.rkey);

                ctx->kv_read_addr_list.push_back(cur_kv_addr);
                ctx->kv_idx_list.push_back(std::make_pair(i, j));
                local_kv_buf_addr += cur_kv_addr.length;
            }
        }
    }
}

void ClientCR::find_kv_in_buckets_on_crash(KVReqCtx * ctx) {
    size_t num_healthy_idx = ctx->healthy_idx_server_id_list.size();
    uint64_t local_kv_buf_addr = (uint64_t)ctx->local_kv_addr;
    std::map<std::string, KVRWAddr> idx_addr_list;
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < RACE_HASH_ASSOC_NUM; j ++) {
            KVRWAddr found_addr[num_replication_];
            bool found_addr_idx_bmap[num_replication_];
            std::vector<int> found_addr_idx;
            memset(found_addr, 0, sizeof(KVRWAddr) * num_replication_);
            memset(found_addr_idx_bmap, 0, sizeof(bool) * num_replication_);
            found_addr_idx.clear();
            for (int r = 0; r < num_healthy_idx; r ++) {
                RaceHashBucket * f_com_bucket = ctx->local_bucket_addr + r * 4;
                RaceHashBucket * s_com_bucket = ctx->local_bucket_addr + 2 + r * 4;
                RaceHashSlot   * slot_arr[4];

                slot_arr[0] = f_com_bucket[0].slots;
                slot_arr[1] = f_com_bucket[1].slots;
                slot_arr[2] = s_com_bucket[0].slots;
                slot_arr[3] = s_com_bucket[1].slots;

                if (slot_arr[i][j].fp == ctx->hash_info.fp && ctx->slot_arr[i][j].kv_len != 0) {
                    uint8_t orig_bk_idx     = ctx->healthy_idx_server_id_list[r];
                    found_addr[orig_bk_idx].r_kv_addr = HashIndexConvert40To64Bits(slot_arr[i][j].pointer);
                    found_addr[orig_bk_idx].rkey      = server_mr_info_map_[slot_arr[i][j].server_id]->rkey;
                    found_addr[orig_bk_idx].l_kv_addr = local_kv_buf_addr;
                    found_addr[orig_bk_idx].length    = slot_arr[i][j].kv_len << 8;
                    found_addr[orig_bk_idx].server_id = slot_arr[i][j].server_id;
                    
                    found_addr_idx_bmap[orig_bk_idx] = true;
                    found_addr_idx.push_back(r);
                }
            }

            // continue to find other slots
            if (found_addr_idx.size() == 0) {
                continue;
            }

            // virtually recover failed servers
            int first_found_idx = found_addr_idx[0];
            std::map<uint64_t, std::vector<int> > target_addr_server_id_list_map;
            for (int r = 0; r < num_replication_; r ++) {
                if (found_addr_idx_bmap[r] == false) {
                    memcpy(&found_addr[r], &found_addr[first_found_idx], sizeof(KVRWAddr));
                }
                uint64_t target_addr = found_addr[r].r_kv_addr | found_addr[r].server_id;
                target_addr_server_id_list_map[target_addr].push_back(r);
            }

            // gain consensus
            std::map<uint64_t, std::vector<int> >::iterator it = target_addr_server_id_list_map.begin();
            std::map<uint64_t, std::vector<int> >::iterator win_it = target_addr_server_id_list_map.end();
            for (; it != target_addr_server_id_list_map.end(); it ++) {
                if (it->second.size() > (num_replication_ / 2)) {
                    win_it = it;
                    break;
                }
            }
            if (win_it == target_addr_server_id_list_map.end()) {
                win_it = target_addr_server_id_list_map.begin();
            }

            // construct read kv addr
            KVRWAddr * consensus_addr = &found_addr[win_it->second[0]];
            ctx->kv_read_addr_list.push_back(*consensus_addr);
            ctx->kv_idx_list.push_back(std::make_pair(i, j));
            local_kv_buf_addr += consensus_addr->length;
        }
    }
}

void ClientCR::find_empty_slot(KVReqCtx * ctx) {
    get_local_bucket_info(ctx);

    uint32_t f_main_idx = ctx->tbl_addr_info.f_main_idx;
    uint32_t s_main_idx = ctx->tbl_addr_info.s_main_idx;
    uint32_t f_free_num, s_free_num;
    uint32_t f_free_slot_idx, s_free_slot_idx;
    int32_t bucket_idx = -1;
    int32_t slot_idx = -1;
    uint32_t f_free_slot_idx_list[RACE_HASH_ASSOC_NUM];
    uint32_t s_free_slot_idx_list[RACE_HASH_ASSOC_NUM];
    std::vector<std::pair<uint32_t, uint32_t>> empty_idx_pair_vec;
    for (int i = 0; i < 2; i ++) {
        f_free_num = GetFreeSlotNum(ctx->f_com_bucket + f_main_idx, &f_free_slot_idx);
        s_free_num = GetFreeSlotNum(ctx->s_com_bucket + s_main_idx, &s_free_slot_idx);
        if (f_free_num > 0 || s_free_num > 0) {
            if (f_free_num >= s_free_num) {
                bucket_idx = f_main_idx;
                slot_idx = f_free_slot_idx;
            } else {
                bucket_idx = 2 + s_main_idx;
                slot_idx = s_free_slot_idx;
            }
        }
        f_main_idx = (f_main_idx + 1) % 2;
        s_main_idx = (s_main_idx + 1) % 2;
    }
    // print_log(DEBUG, "\t\t[%s]  found (%d, %d)", __FUNCTION__, bucket_idx, slot_idx);

    // assert(bucket_idx != -1);
    // assert(slot_idx != -1);

    ctx->bucket_idx = bucket_idx;
    ctx->slot_idx   = slot_idx;
}

void ClientCR::recover_modified_slots(KVReqCtx * ctx) {
    // assert(ctx->recover_match_idx_list.size() > 0);
    // for (size_t i = 0; i < ctx->recover_match_idx_list.size(); i ++) {
    //     assert(ctx->recover_match_idx_list[i].first == ctx->recover_match_idx_list[0].first);
    //     assert(ctx->recover_match_idx_list[i].second == ctx->recover_match_idx_list[0].second);
    // }

    ctx->bucket_idx = ctx->recover_match_idx_list[0].first;
    ctx->slot_idx = ctx->recover_match_idx_list[0].second;
}

void ClientCR::check_failed_index(KVReqCtx * ctx) {
    ctx->healthy_idx_server_id_list.clear();
    ctx->failed_pr_index = false;
    for (int i = 0; i < num_idx_rep_; i ++) {
        uint8_t server_id = ctx->tbl_addr_info.server_id_list[i];
        if (server_crash_map_[server_id] == false) {
            ctx->healthy_idx_server_id_list.push_back(i);
            continue;
        }
        // the server is crashed
        if(i == 0) {
            ctx->failed_pr_index = true;
        }
    }
}

// directly modify the kv rw addr to make it read a healthy server
void ClientCR::check_failed_data(KVReqCtx * ctx) {
    std::vector<KVRWAddr> & kv_read_addr_list = ctx->kv_read_addr_list;
    for (size_t i = 0; i < kv_read_addr_list.size(); i ++) {
        uint8_t target_server = kv_read_addr_list[i].server_id;
        if (server_crash_map_[target_server] == false) {
            continue;
        }
        // the server is crashed
        uint64_t target_addr = kv_read_addr_list[i].r_kv_addr;
        ClientMetaAddrInfo * addr_info = find_corresponding_addr_info(target_server, target_addr);

        // find a healthy server
        int healthy_idx = find_healthy_idx(target_server, target_addr);

        // modify the RW info
        uint32_t orig_rkey = kv_read_addr_list[i].rkey;
        kv_read_addr_list[i].server_id = addr_info->server_id_list[healthy_idx];
        kv_read_addr_list[i].r_kv_addr = addr_info->addr_list[healthy_idx] + (target_addr - addr_info->addr_list[0]);
        kv_read_addr_list[i].rkey      = server_mr_info_map_[kv_read_addr_list[i].server_id]->rkey;

        // print_log(DEBUG, "\t[%s fb%d %ld] modified kv rw addr server(%d->%d) raddr(%lx->%lx) rkey(%x->%x)", __FUNCTION__,
        //     ctx->coro_id, boost::this_fiber::get_id(), target_server, kv_read_addr_list[i].server_id, target_addr, 
        //     kv_read_addr_list[i].r_kv_addr, orig_rkey, kv_read_addr_list[i].rkey);
    }
}

// directly construct a local cache slot to make it points to a healthy server
RaceHashSlot * ClientCR::check_failed_cache(LocalCacheEntry * local_cache_entry) {
    RaceHashSlot * cached_slot = &local_cache_entry->l_slot_ptr;
    uint8_t target_server = cached_slot->server_id;
    if (server_crash_map_[target_server] == false) {
        return cached_slot;
    }
    // the server is crashed
    RaceHashSlot * modified_slot = (RaceHashSlot *)malloc(sizeof(RaceHashSlot));
    memset(modified_slot, 0, sizeof(RaceHashSlot));
    modified_slot->fp = cached_slot->fp;
    modified_slot->kv_len = cached_slot->kv_len;
    uint64_t target_addr = HashIndexConvert40To64Bits(cached_slot->pointer);
    ClientMetaAddrInfo * addr_info = find_corresponding_addr_info(target_server, target_addr);
    if (addr_info == NULL) {
        printf("cannot find addr_info\n");
    }

    // find a healthy server
    int healthy_idx = find_healthy_idx(target_server, target_addr);
    uint64_t new_addr = addr_info->addr_list[healthy_idx] + (target_addr - addr_info->addr_list[0]);
    modified_slot->server_id = addr_info->server_id_list[healthy_idx];
    HashIndexConvert64To40Bits(new_addr, modified_slot->pointer);
    return modified_slot;
}

int32_t ClientCR::find_healthy_idx(uint8_t target_server, uint64_t target_addr) {
    ClientMetaAddrInfo * addr_info;
    for (size_t j = 0; j < meta_addr_info_.size(); j ++) {
        if (meta_addr_info_[j].meta_info_type != TYPE_KVBLOCK || 
                meta_addr_info_[j].server_id_list[0] != target_server) {
            continue;
        }
        uint64_t base_addr = meta_addr_info_[j].addr_list[0];
        if (base_addr <= target_addr && target_addr < base_addr + mm_->mm_block_sz_) {
            addr_info = &meta_addr_info_[j];
            break;
        }   
    }

    // find a healthy server
    int healthy_idx = 1;
    for (; healthy_idx < num_idx_rep_; healthy_idx ++) {
        uint8_t bk_server_id = addr_info->server_id_list[healthy_idx];
        if (server_crash_map_[bk_server_id] == false) {
            break;
        }
    }
    return healthy_idx;
}

ClientMetaAddrInfo * ClientCR::find_corresponding_addr_info(uint8_t target_server, uint64_t target_addr) {
    ClientMetaAddrInfo * addr_info = NULL;
    for (size_t j = 0; j < meta_addr_info_.size(); j ++) {
        if (meta_addr_info_[j].meta_info_type != TYPE_KVBLOCK || 
                meta_addr_info_[j].server_id_list[0] != target_server) {
            continue;
        }
        uint64_t base_addr = meta_addr_info_[j].addr_list[0];
        if (base_addr <= target_addr && target_addr < base_addr + mm_->mm_block_sz_) {
            addr_info = &meta_addr_info_[j];
            break;
        }   
    }
    return addr_info;
}

int32_t ClientCR::find_match_kv_idx(KVReqCtx * ctx) {
    int32_t ret = 0;
    
    for (size_t i = 0; i < ctx->kv_read_addr_list.size(); i ++) {
        uint64_t read_key_addr = ctx->kv_read_addr_list[i].l_kv_addr + sizeof(KVLogHeader);
        uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
        KVLogHeader * header = (KVLogHeader *)ctx->kv_read_addr_list[i].l_kv_addr;
        if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
            return i;
        }
    }
    return -1;
}

void ClientCR::get_local_bucket_info(KVReqCtx * ctx) {
    ctx->f_com_bucket = ctx->local_bucket_addr;
    ctx->s_com_bucket = ctx->local_bucket_addr + 2;

    ctx->bucket_arr[0] = ctx->f_com_bucket;
    ctx->bucket_arr[1] = ctx->f_com_bucket + 1;
    ctx->bucket_arr[2] = ctx->s_com_bucket;
    ctx->bucket_arr[3] = ctx->s_com_bucket + 1;

    ctx->slot_arr[0] = ctx->f_com_bucket[0].slots;
    ctx->slot_arr[1] = ctx->f_com_bucket[1].slots;
    ctx->slot_arr[2] = ctx->s_com_bucket[0].slots;
    ctx->slot_arr[3] = ctx->s_com_bucket[1].slots;
}

void ClientCR::modify_primary_idx(KVReqCtx * ctx) {
    int ret = 0;
    // print_log(DEBUG, "\t  [%s fb%d %ld] state: %d", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id(), ctx->consensus_state);
    // assert(ctx->consensus_state == KV_CONSENSUS_WIN_ALL);
    if (ctx->consensus_state == KV_CONSENSUS_FAIL) {
        // print_log(DEBUG, "\t [%s fb%d %ld] !!failed!!", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        if (ctx->req_type == KV_REQ_INSERT) {
            ctx->ret_val.ret_code = KV_OPS_FAIL_REDO;
            mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        }
        return;
    }

    // 1. update primary index
    // print_log(DEBUG, "\t  [%s fb%d %ld] 1. update primary index", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t pr_cas_sr_list_num;
    IbvSrList * pr_cas_sr_list = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_pr_cas_list, &pr_cas_sr_list_num);
    // assert(pr_cas_sr_list_num == 1);

    IbvSrList * invalid_sr_list = NULL;
    if ((ctx->req_type == KV_REQ_UPDATE || ctx->req_type == KV_REQ_DELETE)) {
        // print_log(DEBUG, "\t  [%s fb%d %ld] generate cache invalid wr", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        // need to generate invalid wr
        uint8_t is_valid = false;
        invalid_sr_list = gen_invalid_sr_lists(ctx->coro_id, &ctx->kv_invalid_addr, (uint64_t)&is_valid);
    }

    std::vector<IbvSrList *> phase2_sr_list_batch;
    std::vector<uint32_t>    phase2_sr_list_num_batch;

    if ((ctx->req_type == KV_REQ_UPDATE || ctx->req_type == KV_REQ_DELETE)) {
        phase2_sr_list_batch.push_back(invalid_sr_list);
        phase2_sr_list_num_batch.push_back(1);
    }

    // assert(pr_cas_sr_list_num == 1);
    iteratively_cas_async(pr_cas_sr_list, pr_cas_sr_list_num);

    ret = post_sr_list_batch_and_yield_wait(phase2_sr_list_batch, phase2_sr_list_num_batch);

    // check update success
    if (*(uint64_t *)ctx->kv_modify_pr_cas_list[0].l_kv_addr != ctx->kv_modify_pr_cas_list[0].orig_value) {
        if (ctx->req_type == KV_REQ_INSERT) {
            ctx->ret_val.ret_code = KV_OPS_FAIL_REDO;
            ctx->is_finished = true;
            mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        } else if (ctx->req_type == KV_REQ_UPDATE) {
            ctx->ret_val.ret_code = KV_OPS_SUCCESS;
            ctx->is_finished = true;
            if (num_idx_rep_ == 1)
                mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        } else if (ctx->req_type == KV_REQ_DELETE) {
            ctx->ret_val.ret_code = KV_OPS_SUCCESS;
            ctx->is_finished = true;
        }
        return;
    }

    ctx->is_finished = true;
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;

    if (ctx->use_cache && ctx->req_type != KV_REQ_DELETE) {
        uint64_t r_slot_addr_list[num_idx_rep_];
        r_slot_addr_list[0] = ctx->kv_modify_pr_cas_list[0].r_kv_addr;
        for (int i = 1; i < num_idx_rep_; i ++) {
            r_slot_addr_list[i] = ctx->kv_modify_bk_0_cas_list[i - 1].r_kv_addr;
        }
        uint64_t local_cas_val = ctx->kv_modify_pr_cas_list[0].swap_value;
        update_cache(ctx->key_str, (RaceHashSlot *)&local_cas_val, 
            r_slot_addr_list);
    } else if (ctx->use_cache && ctx->req_type == KV_REQ_DELETE) {
        remove_cache(ctx->key_str);
    }
    return;
}

void ClientCR::modify_primary_idx_sync(KVReqCtx * ctx) {
    int ret = 0;
    // print_log(DEBUG, "\t  [%s] state: %d", __FUNCTION__, ctx->consensus_state);
    if (ctx->consensus_state == KV_CONSENSUS_FAIL) {
        // print_log(DEBUG, "\t [%s] !!failed!!", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        if (ctx->req_type == KV_REQ_INSERT) {
            ctx->ret_val.ret_code = KV_OPS_FAIL_REDO;
            mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        }
        return;
    }

    // 1. update primary index
    // print_log(DEBUG, "\t  [%s] 1. update primary index", __FUNCTION__);
    uint32_t pr_cas_sr_list_num;
    IbvSrList * pr_cas_sr_list = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_pr_cas_list, &pr_cas_sr_list_num);
    // assert(pr_cas_sr_list_num == 1);

    IbvSrList * invalid_sr_list = NULL;
    if ((ctx->req_type == KV_REQ_UPDATE || ctx->req_type == KV_REQ_DELETE)) {
        // print_log(DEBUG, "\t  [%s] generate cache invalid wr", __FUNCTION__);
        // need to generate invalid wr
        uint8_t is_valid = false;
        invalid_sr_list = gen_invalid_sr_lists(ctx->coro_id, &ctx->kv_invalid_addr, (uint64_t)&is_valid);
    }

    std::vector<IbvSrList *> phase2_sr_list_batch;
    std::vector<uint32_t>    phase2_sr_list_num_batch;

    if ((ctx->req_type == KV_REQ_UPDATE || ctx->req_type == KV_REQ_DELETE)) {
        phase2_sr_list_batch.push_back(invalid_sr_list);
        phase2_sr_list_num_batch.push_back(1);
    }

    // iteratively modify primary index
    // assert(pr_cas_sr_list_num == 1);
    // assert(pr_cas_sr_list[0].num_sr == 1);
    iteratively_cas_sync(pr_cas_sr_list, pr_cas_sr_list_num);

    struct ibv_wc phase2_wc;
    ret = nm_->rdma_post_sr_list_batch_sync(phase2_sr_list_batch, phase2_sr_list_num_batch, &phase2_wc);
    // kv_assert(ret == 0);
    free_cas_sr_lists(pr_cas_sr_list, pr_cas_sr_list_num);
    if (ctx->req_type == KV_REQ_UPDATE || ctx->req_type == KV_REQ_DELETE) {
        free_invalid_sr_lists(invalid_sr_list);
    }

    // check update success
    if (*(uint64_t *)ctx->kv_modify_pr_cas_list[0].l_kv_addr != ctx->kv_modify_pr_cas_list[0].orig_value) {
        if (ctx->req_type == KV_REQ_INSERT) {
            ctx->ret_val.ret_code = KV_OPS_FAIL_REDO;
            ctx->is_finished = true;
            mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        } else if (ctx->req_type == KV_REQ_UPDATE) {
            ctx->ret_val.ret_code = KV_OPS_SUCCESS;
            ctx->is_finished = true;
            if (num_idx_rep_ == 1)
                mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        } else if (ctx->req_type == KV_REQ_DELETE) {
            ctx->ret_val.ret_code = KV_OPS_SUCCESS;
            ctx->is_finished = true;
        }
        return;
    }

    ctx->is_finished = true;
    ctx->ret_val.ret_code = 0;

    if (ctx->use_cache && ctx->req_type != KV_REQ_DELETE) {
        uint64_t r_slot_addr_list[num_idx_rep_];
        r_slot_addr_list[0] = ctx->kv_modify_pr_cas_list[0].r_kv_addr;
        for (int i = 1; i < num_idx_rep_; i ++) {
            r_slot_addr_list[i] = ctx->kv_modify_bk_0_cas_list[i - 1].r_kv_addr;
        }
        uint64_t local_cas_val = ctx->kv_modify_pr_cas_list[0].swap_value;
        update_cache(ctx->key_str, (RaceHashSlot *)&local_cas_val, 
            r_slot_addr_list);
    } else if (ctx->use_cache && ctx->req_type == KV_REQ_DELETE) {
        remove_cache(ctx->key_str);
    }
    return;
}

void ClientCR::check_recover_need_cas_pr(KVReqCtx * ctx) {
    RaceHashSlot target_slot;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, &target_slot);
    uint64_t target_addr = ConvertSlotToInt(&target_slot);
    // print_log(DEBUG, "\t[%s] target_addr: %lx", __FUNCTION__, target_addr);
    // print_log(DEBUG, "\t[%s] target slot: fp(%d) kv_len(%d) server_id(%d) addr(%lx)", __FUNCTION__,     
    //     target_slot.fp, target_slot.kv_len, target_slot.server_id, HashIndexConvert40To64Bits(target_slot.pointer));
    int  num_bk_match = 0;

    for (int r = 0; r < num_idx_rep_; r ++) {
        RaceHashBucket * f_com_bucket = ctx->local_bucket_addr + r * 4;
        RaceHashBucket * s_com_bucket = ctx->local_bucket_addr + 2 + r * 4;
        RaceHashSlot   * slot_arr[4];

        slot_arr[0] = f_com_bucket[0].slots;
        slot_arr[1] = f_com_bucket[1].slots;
        slot_arr[2] = s_com_bucket[0].slots;
        slot_arr[3] = s_com_bucket[1].slots;

        for (int i = 0; i < 4; i ++) {
            for (int j = 0; j < RACE_HASH_ASSOC_NUM; j ++) {
                uint64_t remote_addr = ConvertSlotToInt(&slot_arr[i][j]);
                // print_log(DEBUG, "\t[%s] comapre(target: %lx, remote: %lx)", __FUNCTION__,
                //     target_addr, remote_addr);
                // print_log(DEBUG, "\t[%s] remote slot: fp(%d) kv_len(%d) server_id(%d) addr(%lx)", __FUNCTION__,     
                //     slot_arr[i][j].fp, slot_arr[i][j].kv_len, slot_arr[i][j].server_id, HashIndexConvert40To64Bits(slot_arr[i][j].pointer));
                if (remote_addr == target_addr) {
                    if (r == 0) {
                        ctx->committed_need_cas_pr = false;
                        return;
                    }
                    else {
                        num_bk_match ++;
                        ctx->recover_match_idx_list.push_back(std::pair<int, int>(i, j));
                        ctx->has_modified_bk_idx = true;
                    }
                }
            }
        }
    }
    // assert(num_bk_match < num_replication_);
    // pr not match
    if (num_bk_match == num_replication_ - 1) {
        ctx->committed_need_cas_pr = true;
        return;
    }
    ctx->committed_need_cas_pr = false;
    return;
}

void ClientCR::kv_log_commit(KVReqCtx * ctx) {
    // kv_assert(ctx->req_type == KV_REQ_UPDATE || ctx->req_type == KV_REQ_INSERT);
    int ret = 0;

    // 1. generate log commit sr
    // print_log(DEBUG, "\t  [%s fb%d %ld] generate log commit wr", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t log_commit_sr_list_num = 0;
    void * local_addr = malloc(sizeof(uint64_t) + sizeof(uint8_t));
    *(uint64_t *)local_addr = ctx->kv_modify_pr_cas_list[0].orig_value;
    *(uint64_t *)((uint64_t)local_addr + sizeof(uint64_t)) = 0xF;
    IbvSrList * log_commit_sr_list = gen_log_commit_sr_lists(ctx->coro_id, 
        local_addr, sizeof(uint64_t) + sizeof(uint8_t), 
        ctx->log_commit_addr_list, &log_commit_sr_list_num);
    
    // 2. post request and wait for completion
    ret = post_sr_lists_and_yield_wait(log_commit_sr_list, log_commit_sr_list_num);
    // kv_assert(ret == 0);
    free_log_commit_sr_lists(log_commit_sr_list, log_commit_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_log_commit_sync(KVReqCtx * ctx) {
    // assert(ctx->req_type == KV_REQ_UPDATE || ctx->req_type == KV_REQ_INSERT || ctx->req_type == KV_REQ_RECOVER_UNCOMMITTED);
    int ret = 0;

    // 1. generate log commit sr
    // print_log(DEBUG, "\t  [%s fb%d %ld] generate log commit wr", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t log_commit_sr_list_num = 0;
    void * local_addr = malloc(sizeof(uint64_t) + sizeof(uint8_t));
    *(uint64_t *)local_addr = ctx->kv_modify_pr_cas_list[0].orig_value;
    *(uint64_t *)((uint64_t)local_addr + sizeof(uint64_t)) = 0xF;
    IbvSrList * log_commit_sr_list = gen_log_commit_sr_lists(ctx->coro_id, 
        local_addr, sizeof(uint64_t) + sizeof(uint8_t), 
        ctx->log_commit_addr_list, &log_commit_sr_list_num);
    
    // 2. post request and wait for completion
    struct ibv_wc log_commit_wc;
    ret = nm_->rdma_post_sr_lists_sync(log_commit_sr_list, log_commit_sr_list_num, &log_commit_wc);
    // assert(ret == 0);
    free_log_commit_sr_lists(log_commit_sr_list, log_commit_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_search_read_buckets(KVReqCtx * ctx) {
    int ret = 0;
    // print_log(DEBUG, "\t[%s fb%d %lld] 1. generate sr for reading buckets", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t read_bucket_sr_list_num;
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);
    // kv_assert(read_bucket_sr_list != NULL);

    IbvSrList * read_cache_kv_sr_list = NULL;
    if (ctx->use_cache) {
        // print_log(DEBUG, "\t[%s fb%d %lld] 1.1. check cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        LocalCacheEntry * local_cache_entry = check_cache(ctx->key_str);
        uint64_t cache_kv_local_addr = (uint64_t)ctx->local_cache_addr;
        ctx->is_local_cache_hit = !!(uint64_t)local_cache_entry;
        if (ctx->is_local_cache_hit) {
            ctx->cache_entry = local_cache_entry;
            read_cache_kv_sr_list = gen_read_cache_kv_sr_lists(ctx->coro_id, &local_cache_entry->l_slot_ptr, cache_kv_local_addr);
        }
    }

    // print_log(DEBUG, "\t[%s fb%d %lld] 2. post requests and wait for completion", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    std::vector<IbvSrList *> kv_search_sr_list_batch;
    std::vector<uint32_t> kv_search_sr_list_num_batch;
    kv_search_sr_list_batch.push_back(read_bucket_sr_list);
    kv_search_sr_list_num_batch.push_back(read_bucket_sr_list_num);

    if (ctx->use_cache && ctx->is_local_cache_hit) {
        kv_search_sr_list_batch.push_back(read_cache_kv_sr_list);
        kv_search_sr_list_num_batch.push_back(1);
    }

    ret = post_sr_list_batch_and_yield_wait(kv_search_sr_list_batch, kv_search_sr_list_num_batch);
    // kv_assert(ret == 0);
    free_read_bucket_sr_lists(read_bucket_sr_list);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        free_read_cache_kv_sr_lists(read_cache_kv_sr_list);
    }

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_search_read_buckets_sync(KVReqCtx * ctx) {
    int ret = 0;
    // print_log(DEBUG, "\t[%s fb%d %lld] 1. generate sr for reading buckets", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t read_bucket_sr_list_num;
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);
    kv_assert(read_bucket_sr_list != NULL);

    IbvSrList * read_cache_kv_sr_list = NULL;
    if (ctx->use_cache) {
        // print_log(DEBUG, "\t[%s fb%d %lld] 1.1. check cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        LocalCacheEntry * local_cache_entry = check_cache(ctx->key_str);
        uint64_t cache_kv_local_addr = (uint64_t)ctx->local_cache_addr;
        ctx->is_local_cache_hit = !!(uint64_t)local_cache_entry;
        if (ctx->is_local_cache_hit) {
            ctx->cache_entry = local_cache_entry;
            read_cache_kv_sr_list = gen_read_cache_kv_sr_lists(ctx->coro_id, &local_cache_entry->l_slot_ptr, cache_kv_local_addr);
        }
    }

    // print_log(DEBUG, "\t[%s fb%d %lld] 2. post requests and wait for completion", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    std::vector<IbvSrList *> kv_search_sr_list_batch;
    std::vector<uint32_t> kv_search_sr_list_num_batch;
    kv_search_sr_list_batch.push_back(read_bucket_sr_list);
    kv_search_sr_list_num_batch.push_back(read_bucket_sr_list_num);

    if (ctx->use_cache && ctx->is_local_cache_hit) {
        kv_search_sr_list_batch.push_back(read_cache_kv_sr_list);
        kv_search_sr_list_num_batch.push_back(1);
    }

    // ret = post_sr_list_batch_and_yield_wait(kv_search_sr_list_batch, kv_search_sr_list_num_batch);
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_list_batch_sync(kv_search_sr_list_batch, kv_search_sr_list_num_batch, &wc);
    // kv_assert(ret == 0);
    free_read_bucket_sr_lists(read_bucket_sr_list);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        free_read_cache_kv_sr_lists(read_cache_kv_sr_list);
    }

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_search_read_kv(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->is_finished) {
        return;
    }
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // print_log(DEBUG, "\t[%s fb%d %lld] check kv in cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        KVLogHeader * cache_header = (KVLogHeader *)ctx->local_cache_addr;
        // print_log(DEBUG, "\t  [%s fb%d %lld] check cached value: valid(%d) key_len(%d) value_len(%d)", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id(), 
        //     log_is_valid(cache_header), cache_header->key_length, cache_header->value_length);
        if (log_is_valid(cache_header)) {
            // print_log(DEBUG, "\t[%s]  return from cache", __FUNCTION__);
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            KVLogHeader * header = (KVLogHeader *)ctx->local_cache_addr;
            if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                ctx->ret_val.value_addr = (void *)((uint64_t)cache_header + sizeof(KVLogHeader) + cache_header->key_length);
                ctx->is_finished = true;
                ctx->cache_entry->acc_cnt ++; // update cache counter
                return;
            }
        }
    }

    // print_log(DEBUG, "\t[%s fb%d %lld] 1. search for target kv", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    find_kv_in_buckets(ctx);

    // print_log(DEBUG, "\t[%s fb%d %lld] 2. generate SrLists", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t read_kv_sr_list_num;
    IbvSrList * read_kv_sr_lists = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &read_kv_sr_list_num);

    // print_log(DEBUG, "\t[%s fb%d %lld] 3. read kv and wait for reply", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    ret = post_sr_lists_and_yield_wait(read_kv_sr_lists, read_kv_sr_list_num);
    // kv_assert(ret == 0);
    free_read_kv_sr_lists(read_kv_sr_lists, read_kv_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_search_read_kv_sync(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // print_log(DEBUG, "\t[%s fb%d %lld] check kv in cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        KVLogHeader * cache_header = (KVLogHeader *)ctx->local_cache_addr;
        // print_log(DEBUG, "\t  [%s fb%d %lld] check cached value: valid(%d) key_len(%d) value_len(%d)", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id(), 
        //     log_is_valid(cache_header), cache_header->key_length, cache_header->value_length);
        if (log_is_valid(cache_header)) {
            // print_log(DEBUG, "\t[%s]  return from cache", __FUNCTION__);
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            KVLogHeader * header = (KVLogHeader *)ctx->local_cache_addr;
            if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                ctx->ret_val.value_addr = (void *)((uint64_t)cache_header + sizeof(KVLogHeader) + cache_header->key_length);
                ctx->is_finished = true;
                ctx->cache_entry->acc_cnt ++; // update cache counter
                return;
            }
        }
    }

    // print_log(DEBUG, "\t[%s fb%d %lld] 1. search for target kv", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    find_kv_in_buckets(ctx);

    // print_log(DEBUG, "\t[%s fb%d %lld] 2. generate SrLists", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t read_kv_sr_list_num;
    IbvSrList * read_kv_sr_lists = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &read_kv_sr_list_num);

    // print_log(DEBUG, "\t[%s fb%d %lld] 3. read kv and wait for reply", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    // ret = post_sr_lists_and_yield_wait(read_kv_sr_lists, read_kv_sr_list_num);
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_lists_sync(read_kv_sr_lists, read_kv_sr_list_num, &wc);
    // kv_assert(ret == 0);
    free_read_kv_sr_lists(read_kv_sr_lists, read_kv_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_search_check_kv(KVReqCtx * ctx) {
    int ret = 0;

    if (ctx->is_finished) {
        // print_log(DEBUG, "\t[%s fb%d %lld] 0. cache hit finish", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        return;
    }

    // print_log(DEBUG, "\t[%s] 1. check key match", __FUNCTION__);

    int32_t match_idx = find_match_kv_idx(ctx);
    if (match_idx != -1) {
        // print_log(DEBUG, "\t[%s] 2. key found finish", __FUNCTION__);
        uint64_t read_key_addr = ctx->kv_read_addr_list[match_idx].l_kv_addr + sizeof(KVLogHeader);
        KVLogHeader * header = (KVLogHeader *)ctx->kv_read_addr_list[match_idx].l_kv_addr;
        ctx->ret_val.value_addr = (void *)(read_key_addr + header->key_length);
        ctx->is_finished = true;

        if (ctx->use_cache) {
            int bucket_id = ctx->kv_idx_list[match_idx].first;
            int slot_id   = ctx->kv_idx_list[match_idx].second;
            uint64_t remote_slot_addr_list[num_replication_];
            uint64_t old_local_slot_addr;
            get_local_bucket_info(ctx);
            if (bucket_id < 2) {
                uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
                old_local_slot_addr   = (uint64_t)&(ctx->f_com_bucket[bucket_id].slots[slot_id]);
                for (int i = 0; i < num_replication_; i ++) {
                    remote_slot_addr_list[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
                }
            } else {
                uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
                old_local_slot_addr   = (uint64_t)&(ctx->s_com_bucket[bucket_id - 2].slots[slot_id]);
                for (int i = 0; i < num_replication_; i ++) {
                    remote_slot_addr_list[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
                }
            }
            uint64_t r_slot_addr_list[num_replication_];
            update_cache(ctx->key_str, (RaceHashSlot *)old_local_slot_addr, remote_slot_addr_list);
        }
        return;
    } else {
        // print_log(DEBUG, "\t[%s] 2. key not found finish", __FUNCTION__);
        ctx->ret_val.value_addr = NULL;
        ctx->is_finished = true;
        return;
    }
    
}

void ClientCR::kv_search_read_all_healthy_index(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %lld] 1. generate sr for reading all buckets", __FUNCTION__,
    //     ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    uint32_t read_bucket_sr_list_num;
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists_on_crash(ctx, &read_bucket_sr_list_num);
    
    IbvSrList * read_cache_kv_sr_list = NULL;
    if (ctx->use_cache) {
        // print_log(DEBUG, "\t[%s fb%d %lld] 1.1. check cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        LocalCacheEntry * local_cache_entry = check_cache(ctx->key_str);
        uint64_t cache_kv_local_addr = (uint64_t)ctx->local_cache_addr;
        ctx->is_local_cache_hit = !!(uint64_t)local_cache_entry;
        if (ctx->is_local_cache_hit) {
            RaceHashSlot * healthy_slot = check_failed_cache(local_cache_entry);
            memcpy(&local_cache_entry->l_slot_ptr, healthy_slot, sizeof(RaceHashSlot));
            read_cache_kv_sr_list = gen_read_cache_kv_sr_lists(ctx->coro_id, healthy_slot, cache_kv_local_addr);
            ctx->cache_entry = local_cache_entry;
        }
    }

    // print_log(DEBUG, "\t[%s fb%d %lld] 2. post requests and wait for completion", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    std::vector<IbvSrList *> kv_search_sr_list_batch;
    std::vector<uint32_t> kv_search_sr_list_num_batch;
    kv_search_sr_list_batch.push_back(read_bucket_sr_list);
    kv_search_sr_list_num_batch.push_back(read_bucket_sr_list_num);

    if (ctx->use_cache && ctx->is_local_cache_hit) {
        kv_search_sr_list_batch.push_back(read_cache_kv_sr_list);
        kv_search_sr_list_num_batch.push_back(1);
    }

    ret = post_sr_list_batch_and_yield_wait(kv_search_sr_list_batch, kv_search_sr_list_num_batch);
    // kv_assert(ret == 0);
    free_read_bucket_sr_lists_on_crash(read_bucket_sr_list, read_bucket_sr_list_num);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        free_read_cache_kv_sr_lists(read_cache_kv_sr_list);
    }

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_search_read_failed_kv(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // print_log(DEBUG, "\t[%s fb%d %lld] check kv in cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        KVLogHeader * cache_header = (KVLogHeader *)ctx->local_cache_addr;
        // print_log(DEBUG, "\t  [%s fb%d %lld] check cached value: valid(%d) key_len(%d) value_len(%d)", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id(), 
        //     log_is_valid(cache_header), cache_header->key_length, cache_header->value_length);
        if (log_is_valid(cache_header)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            KVLogHeader * header = (KVLogHeader *)ctx->local_cache_addr;
            if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                ctx->ret_val.value_addr = (void *)((uint64_t)cache_header + sizeof(KVLogHeader) + cache_header->key_length);
                ctx->is_finished = true;
                ctx->cache_entry->acc_cnt ++; // update cache counter
                return;
            }
        }
    }

    // print_log(DEBUG, "\t[%s fb%d %lld] 1. search for target kv", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    if (ctx->failed_pr_index == true) {
        find_kv_in_buckets_on_crash(ctx);
    }
    else {
        find_kv_in_buckets(ctx);
    }
    check_failed_data(ctx);

    // print_log(DEBUG, "\t[%s fb%d %lld] 2. generate SrLists", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t read_kv_sr_list_num;
    IbvSrList * read_kv_sr_lists = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &read_kv_sr_list_num);

    // print_log(DEBUG, "\t[%s fb%d %lld] 3. read kv and wait for reply", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    ret = post_sr_lists_and_yield_wait(read_kv_sr_lists, read_kv_sr_list_num);
    free_read_kv_sr_lists(read_kv_sr_lists, read_kv_sr_list_num);
    // kv_assert(ret == 0);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr 
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    header->is_valid = true;
    tail->op = KV_OP_INSERT;

    // 1. allocate remote memory
    // print_log(DEBUG, "\t[%s fb%d %ld]   1. Allocate remote memory", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t kv_block_size = header->key_length + header->value_length + sizeof(KVLogHeader);
    mm_->mm_alloc(kv_block_size, nm_, ctx->key_str, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }

    // 2. update kv header and prepare log commit addr
    update_log_tail(tail, &ctx->mm_alloc_ctx);
    prepare_log_commit_addrs(ctx);

    // 2. generate send requests (write_kv and read_bucket)
    // print_log(DEBUG, "\t[%s fb%d %ld]   2. generate send request", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t write_kv_sr_list_num;
    uint32_t read_bucket_sr_list_num;
    uint32_t update_prev_sr_list_num = 0;
    // 2.2 generate read bucket sr
    IbvSrList * write_kv_sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &write_kv_sr_list_num);
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);

    // 2.3 merge these requests
    std::vector<IbvSrList *> kv_insert_p1_sr_list_batch;
    std::vector<uint32_t>    kv_insert_p1_sr_list_num_batch;
    kv_insert_p1_sr_list_batch.push_back(write_kv_sr_list);
    kv_insert_p1_sr_list_batch.push_back(read_bucket_sr_list);
    kv_insert_p1_sr_list_num_batch.push_back(write_kv_sr_list_num);
    kv_insert_p1_sr_list_num_batch.push_back(read_bucket_sr_list_num);

    // 3. post requests and wait for completion
    // print_log(DEBUG, "\t[%s fb%d %ld]   3. post requests and wait for completion", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    ret = post_sr_list_batch_and_yield_wait(kv_insert_p1_sr_list_batch, kv_insert_p1_sr_list_num_batch);
    // kv_assert(ret == 0);
    free_write_kv_sr_lists(write_kv_sr_list);
    free_read_bucket_sr_lists(read_bucket_sr_list);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_insert_read_buckets_and_write_kv_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    header->is_valid = true;
    tail->op = KV_OP_INSERT;


    // 1. allocate remote memory
    // print_log(DEBUG, "\t[%s fb%d %ld]   1. Allocate remote memory", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t kv_block_size = header->key_length + header->value_length + sizeof(KVLogHeader) + sizeof(KVLogTail);
    mm_->mm_alloc(kv_block_size, nm_, ctx->key_str, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }

    // 2. update kv header and prepare log commit addr
    update_log_tail(tail, &ctx->mm_alloc_ctx);
    prepare_log_commit_addrs(ctx);

    // 2. generate send requests (write_kv and read_bucket)
    // print_log(DEBUG, "\t[%s fb%d %ld]   2. generate send request", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t write_kv_sr_list_num;
    uint32_t read_bucket_sr_list_num;
    uint32_t update_prev_sr_list_num = 0;
    // 2.2 generate read bucket sr
    IbvSrList * write_kv_sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &write_kv_sr_list_num);
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);

    // 2.3 merge these requests
    std::vector<IbvSrList *> kv_insert_p1_sr_list_batch;
    std::vector<uint32_t>    kv_insert_p1_sr_list_num_batch;
    kv_insert_p1_sr_list_batch.push_back(write_kv_sr_list);
    kv_insert_p1_sr_list_batch.push_back(read_bucket_sr_list);
    kv_insert_p1_sr_list_num_batch.push_back(write_kv_sr_list_num);
    kv_insert_p1_sr_list_num_batch.push_back(read_bucket_sr_list_num);

    // 3. post requests and wait for completion
    // print_log(DEBUG, "\t[%s fb%d %ld]   3. post requests and wait for completion", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    // ret = post_sr_list_batch_and_yield_wait(kv_insert_p1_sr_list_batch, kv_insert_p1_sr_list_num_batch);
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_list_batch_sync(kv_insert_p1_sr_list_batch, kv_insert_p1_sr_list_num_batch, &wc);
    // assert(ret == 0);
    free_write_kv_sr_lists(write_kv_sr_list);
    free_read_bucket_sr_lists(read_bucket_sr_list);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_insert_backup_consensus_0(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    find_empty_slot(ctx);
    if (ctx->bucket_idx == -1) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        return;
    }

    // 1. calculate cas offset
    // print_log(DEBUG, "\t[%s fb%d %ld]   1. calculate cas offset", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    if (ctx->bucket_idx < 2) {
        uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
        uint64_t old_local_slot_addr = (uint64_t)&(ctx->f_com_bucket[ctx->bucket_idx].slots[ctx->slot_idx]);
        uint64_t remote_slot_addr[num_idx_rep_];
        for (int i = 0; i < num_idx_rep_; i ++) {
            remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
        }
        fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, (RaceHashSlot *)new_local_slot_ptr);
    } else {
        uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
        uint64_t old_local_slot_addr = (uint64_t)&(ctx->s_com_bucket[ctx->bucket_idx - 2].slots[ctx->slot_idx]);
        uint64_t remote_slot_addr[num_idx_rep_];
        for (int i = 0; i < num_idx_rep_; i ++) {
            remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
        }
        fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, (RaceHashSlot *)new_local_slot_ptr);
    }

    if (num_idx_rep_ == 1) {
        ctx->consensus_state = KV_CONSENSUS_WIN_ALL;
        return;
    }

    // 2. cas all backup
    // print_log(DEBUG, "\t[%s fb%d %ld]   2. update backup index", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t bk_cas_sr_list_0_num;
    IbvSrList * bk_cas_sr_list_0 = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_bk_0_cas_list, &bk_cas_sr_list_0_num);

    // assert(bk_cas_sr_list_0_num == num_replication_ - 1);
    iteratively_cas_async(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
    free_cas_sr_lists(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
}

void ClientCR::kv_insert_backup_consensus_0_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    if (ctx->is_finished == true) {
        return;
    }

    int ret = 0;
    find_empty_slot(ctx);
    if (ctx->bucket_idx == -1) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        return;
    }

    // 1. calculate cas offset
    // print_log(DEBUG, "\t[%s fb%d %ld]   1. calculate cas offset", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
    if (ctx->bucket_idx < 2) {
        uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
        uint64_t old_local_slot_addr = (uint64_t)&(ctx->f_com_bucket[ctx->bucket_idx].slots[ctx->slot_idx]);
        uint64_t remote_slot_addr[num_idx_rep_];
        for (int i = 0; i < num_idx_rep_; i ++) {
            remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
        }
        fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, (RaceHashSlot *)new_local_slot_ptr);
    } else {
        uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
        uint64_t old_local_slot_addr = (uint64_t)&(ctx->s_com_bucket[ctx->bucket_idx - 2].slots[ctx->slot_idx]);
        uint64_t remote_slot_addr[num_idx_rep_];
        for (int i = 0; i < num_idx_rep_; i ++) {
            remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
        }
        fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, (RaceHashSlot *)new_local_slot_ptr);
    }

    if (num_idx_rep_ == 1) {
        ctx->consensus_state = KV_CONSENSUS_WIN_ALL;
        return;
    }

    // 2. cas all backup
    // print_log(DEBUG, "\t[%s fb%d %ld]   2. update backup index", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t bk_cas_sr_list_0_num;
    IbvSrList * bk_cas_sr_list_0 = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_bk_0_cas_list, &bk_cas_sr_list_0_num);

    // iteratively cas
    // assert(bk_cas_sr_list_0_num == num_replication_ - 1);
    iteratively_cas_sync(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
    free_cas_sr_lists(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
}

void ClientCR::kv_insert_commit_log(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    if (ctx->is_finished == true) {
        return;
    }
    kv_log_commit(ctx);
    return;
}

void ClientCR::kv_insert_commit_log_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    if (ctx->is_finished || *(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }
    kv_log_commit_sync(ctx);
    return;
}

void ClientCR::kv_insert_cas_primary(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    if (ctx->is_finished) {
        return;
    }
    modify_primary_idx(ctx);
    return;
}

void ClientCR::kv_insert_cas_primary_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    if (ctx->is_finished) {
        return;
    }
    modify_primary_idx_sync(ctx);
    return;
}

void ClientCR::kv_update_read_buckets_and_write_kv(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[2. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;

    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    header->is_valid = true;
    tail->op = KV_OP_UPDATE;

    // 1. allocate memory
    // print_log(DEBUG, "\t[2. %s fb%d %ld]   1. Allocate remote memory", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t kv_block_size = header->key_length + header->value_length + sizeof(KVLogHeader);
    mm_->mm_alloc(kv_block_size, nm_, ctx->key_str, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    
    // 2. update kv header and generate commit addrs
    update_log_tail(tail, &ctx->mm_alloc_ctx);
    prepare_log_commit_addrs(ctx);

    IbvSrList * read_cache_kv_sr_list;
    if (ctx->use_cache) {
        // print_log(DEBUG, "\t[2. %s fb%d %ld]   check cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        LocalCacheEntry * local_cache_entry = check_cache(ctx->key_str);
        uint64_t cache_kv_local_addr = (uint64_t)ctx->local_cache_addr;
        ctx->is_local_cache_hit = !!(uint64_t)local_cache_entry;
        if (ctx->is_local_cache_hit) {
            ctx->cache_entry = local_cache_entry;
            read_cache_kv_sr_list = gen_read_cache_kv_sr_lists(ctx->coro_id, &local_cache_entry->l_slot_ptr, cache_kv_local_addr);
        }
    }
    
    // 2. generate send requests
    // print_log(DEBUG, "\t[2. %s fb%d %ld]   2. generate send requests", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t write_kv_sr_list_num;
    uint32_t read_bucket_sr_list_num;
    uint32_t update_prev_sr_list_num = 0;
    IbvSrList * write_kv_sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &write_kv_sr_list_num);
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);

    // 2.1 merge requests
    std::vector<IbvSrList *> kv_update_p1_sr_list_batch;
    std::vector<uint32_t>    kv_update_p1_sr_list_num_batch;
    kv_update_p1_sr_list_batch.push_back(write_kv_sr_list);
    kv_update_p1_sr_list_batch.push_back(read_bucket_sr_list);
    kv_update_p1_sr_list_num_batch.push_back(write_kv_sr_list_num);
    kv_update_p1_sr_list_num_batch.push_back(read_bucket_sr_list_num);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // push the read cache sr list to the batch if use cache
        kv_update_p1_sr_list_batch.push_back(read_cache_kv_sr_list);
        kv_update_p1_sr_list_num_batch.push_back(1);
    }

    // print_log(DEBUG, "\t[2. %s fb%d %ld]   3. post requests and wait for completion", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    ret = post_sr_list_batch_and_yield_wait(kv_update_p1_sr_list_batch, kv_update_p1_sr_list_num_batch);
    // kv_assert(ret == 0);
    free_write_kv_sr_lists(write_kv_sr_list);
    free_read_bucket_sr_lists(read_bucket_sr_list);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        free_read_cache_kv_sr_lists(read_cache_kv_sr_list);
    }

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_update_read_buckets_and_write_kv_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[2. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;

    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    header->is_valid = true;
    tail->op = KV_OP_UPDATE;

    // 1. allocate memory
    // print_log(DEBUG, "\t[2. %s fb%d %ld]   1. Allocate remote memory", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t kv_block_size = header->key_length + header->value_length + sizeof(KVLogHeader);
    mm_->mm_alloc(kv_block_size, nm_, ctx->key_str, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }
    
    // 2. update kv header and generate commit addrs
    update_log_tail(tail, &ctx->mm_alloc_ctx);
    prepare_log_commit_addrs(ctx);

    IbvSrList * read_cache_kv_sr_list;
    if (ctx->use_cache) {
        // print_log(DEBUG, "\t[2. %s fb%d %ld]   check cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        LocalCacheEntry * local_cache_entry = check_cache(ctx->key_str);
        uint64_t cache_kv_local_addr = (uint64_t)ctx->local_cache_addr;
        ctx->is_local_cache_hit = !!(uint64_t)local_cache_entry;
        if (ctx->is_local_cache_hit) {
            ctx->cache_entry = local_cache_entry;
            read_cache_kv_sr_list = gen_read_cache_kv_sr_lists(ctx->coro_id, &local_cache_entry->l_slot_ptr, cache_kv_local_addr);
        }
    }
    
    // 2. generate send requests
    // print_log(DEBUG, "\t[2. %s fb%d %ld]   2. generate send requests", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t write_kv_sr_list_num;
    uint32_t read_bucket_sr_list_num;
    uint32_t update_prev_sr_list_num = 0;
    IbvSrList * write_kv_sr_list = gen_write_kv_sr_lists(ctx->coro_id, ctx->kv_info, &ctx->mm_alloc_ctx, &write_kv_sr_list_num);
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);

    // 2.1 merge requests
    std::vector<IbvSrList *> kv_update_p1_sr_list_batch;
    std::vector<uint32_t>    kv_update_p1_sr_list_num_batch;
    kv_update_p1_sr_list_batch.push_back(write_kv_sr_list);
    kv_update_p1_sr_list_batch.push_back(read_bucket_sr_list);
    kv_update_p1_sr_list_num_batch.push_back(write_kv_sr_list_num);
    kv_update_p1_sr_list_num_batch.push_back(read_bucket_sr_list_num);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // push the read cache sr list to the batch if use cache
        kv_update_p1_sr_list_batch.push_back(read_cache_kv_sr_list);
        kv_update_p1_sr_list_num_batch.push_back(1);
    }

    // print_log(DEBUG, "\t[2. %s fb%d %ld]   3. post requests and wait for completion", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    // ret = post_sr_list_batch_and_yield_wait(kv_update_p1_sr_list_batch, kv_update_p1_sr_list_num_batch);
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_list_batch_sync(kv_update_p1_sr_list_batch, kv_update_p1_sr_list_num_batch, &wc);
    kv_assert(ret == 0);
    free_write_kv_sr_lists(write_kv_sr_list);
    free_read_bucket_sr_lists(read_bucket_sr_list);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        free_read_cache_kv_sr_lists(read_cache_kv_sr_list);
    }

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_update_read_kv(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[3. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    if (*(ctx->should_stop) || ctx->is_finished) {
        ctx->is_finished = true;
        return;
    }
    
    // 0. check for cache kv object
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // print_log(DEBUG, "\t[3. %s fb%d %ld]   0. check cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        KVLogHeader * cached_kv_header = (KVLogHeader *)ctx->local_cache_addr;
        // print_log(DEBUG, "\t[3. %s fb%d %ld]   check cached value: valid(%d) key_len(%d) value_len(%d)", 
        //     __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id(), cached_kv_header->ctl_bits, 
        //     cached_kv_header->key_length, cached_kv_header->value_length);
        if (log_is_valid(cached_kv_header)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            KVLogHeader * header = cached_kv_header;
            if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                ctx->cache_entry->acc_cnt ++; // update cache counter

                LocalCacheEntry * local_cache_entry = ctx->cache_entry;
                RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
                fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
                uint64_t remote_slot_addr[num_idx_rep_];
                fill_cas_addr(ctx, local_cache_entry->r_slot_addr, &local_cache_entry->l_slot_ptr, new_local_slot_ptr);
                fill_invalid_addr(ctx, &local_cache_entry->l_slot_ptr);
                ctx->is_cache_hit = true;
                return;
            }
        }
    }

    ctx->is_cache_hit = false;
    // print_log(DEBUG, "\t[3. %s fb%d %ld]   1. search for target kv", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    find_kv_in_buckets(ctx);

    // print_log(DEBUG, "\t[3. %s fb%d %ld]   2. generate SrLists", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t read_kv_sr_list_num;
    IbvSrList * read_kv_sr_lists = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &read_kv_sr_list_num);

    // print_log(DEBUG, "\t[3. %s fb%d %ld]   3. read kv and wait for reply", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    ret = post_sr_lists_and_yield_wait(read_kv_sr_lists, read_kv_sr_list_num);
    // kv_assert(ret == 0);
    free_read_kv_sr_lists(read_kv_sr_lists, read_kv_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_update_read_kv_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[3. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    
    // 0. check for cache kv object
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // print_log(DEBUG, "\t[3. %s fb%d %ld]   0. check cache", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        KVLogHeader * cached_kv_header = (KVLogHeader *)ctx->local_cache_addr;
        // print_log(DEBUG, "\t[3. %s fb%d %ld]   check cached value: valid(%d) key_len(%d) value_len(%d)", 
        //     __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id(), cached_kv_header->ctl_bits, 
        //     cached_kv_header->key_length, cached_kv_header->value_length);
        if (log_is_valid(cached_kv_header)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            KVLogHeader * header = cached_kv_header;
            if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                ctx->cache_entry->acc_cnt ++; // update cache counter

                LocalCacheEntry * local_cache_entry = ctx->cache_entry;
                RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
                fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
                uint64_t remote_slot_addr[num_idx_rep_];
                fill_cas_addr(ctx, local_cache_entry->r_slot_addr, &local_cache_entry->l_slot_ptr, new_local_slot_ptr);
                fill_invalid_addr(ctx, &local_cache_entry->l_slot_ptr);
                ctx->is_cache_hit = true;
                return;
            }
        }
    }

    ctx->is_cache_hit = false;
    // print_log(DEBUG, "\t[3. %s fb%d %ld]   1. search for target kv", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    find_kv_in_buckets(ctx);

    // print_log(DEBUG, "\t[3. %s fb%d %ld]   2. generate SrLists", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t read_kv_sr_list_num;
    IbvSrList * read_kv_sr_lists = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &read_kv_sr_list_num);

    // print_log(DEBUG, "\t[3. %s fb%d %ld]   3. read kv and wait for reply", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    // ret = post_sr_lists_and_yield_wait(read_kv_sr_lists, read_kv_sr_list_num);
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_lists_sync(read_kv_sr_lists, read_kv_sr_list_num, &wc);
    kv_assert(ret == 0);
    free_read_kv_sr_lists(read_kv_sr_lists, read_kv_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_update_backup_consensus_0(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[4. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    if (*(ctx->should_stop) || ctx->is_finished) {
        ctx->is_finished = true;
        return;
    }
    if (ctx->use_cache && ctx->is_cache_hit) {
        // cache is hit do nothing
        // print_log(DEBUG, "\t[4. %s fb%d %ld]   0. cache hit", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    } else {
        // print_log(DEBUG, "\t[4. %s fb%d %ld]   1. find match key idx", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        int32_t match_idx = find_match_kv_idx(ctx);
        if (match_idx == -1) {
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
            return;
        }

        std::pair<int32_t, int32_t> idx_pair = ctx->kv_idx_list[match_idx];
        int32_t bucket_idx = idx_pair.first;
        int32_t slot_idx   = idx_pair.second;

        // print_log(DEBUG, "\t[4. %s fb%d %ld]   2. calculate cas offset", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
        fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
        if (bucket_idx < 2) {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->f_com_bucket[bucket_idx].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        } else {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->s_com_bucket[bucket_idx - 2].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        }
    }

    if (num_idx_rep_ == 1) {
        ctx->consensus_state = KV_CONSENSUS_WIN_ALL;
        return;
    }

    // 2. cas all backup
    // print_log(DEBUG, "\t[4. %s fb%d %ld]   2. update backup index", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t bk_cas_sr_list_0_num;
    IbvSrList * bk_cas_sr_list_0 = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_bk_0_cas_list, &bk_cas_sr_list_0_num);

    // assert(bk_cas_sr_list_0_num == num_replication_ - 1);
    iteratively_cas_async(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
    free_cas_sr_lists(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
}

void ClientCR::kv_update_backup_consensus_0_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[4. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    int ret = 0;
    if (ctx->use_cache && ctx->is_cache_hit) {
        // cache is hit do nothing
        // print_log(DEBUG, "\t[4. %s fb%d %ld]   0. cache hit", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    } else {
        // print_log(DEBUG, "\t[4. %s fb%d %ld]   1. find match key idx", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        int32_t match_idx = find_match_kv_idx(ctx);
        if (match_idx == -1) {
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
            return;
        }

        std::pair<int32_t, int32_t> idx_pair = ctx->kv_idx_list[match_idx];
        int32_t bucket_idx = idx_pair.first;
        int32_t slot_idx   = idx_pair.second;

        // print_log(DEBUG, "\t[4. %s fb%d %ld]   2. calculate cas offset", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
        RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
        fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, new_local_slot_ptr);
        if (bucket_idx < 2) {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->f_com_bucket[bucket_idx].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        } else {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->s_com_bucket[bucket_idx - 2].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        }
    }

    if (num_idx_rep_ == 1) {
        ctx->consensus_state = KV_CONSENSUS_WIN_ALL;
        return;
    }

    // 2. cas all backup
    // print_log(DEBUG, "\t[4. %s fb%d %ld]   2. update backup index", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    uint32_t bk_cas_sr_list_0_num;
    IbvSrList * bk_cas_sr_list_0 = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_bk_0_cas_list, &bk_cas_sr_list_0_num);


    // assert(bk_cas_sr_list_0_num == num_replication_ - 1);
    iteratively_cas_sync(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
    free_cas_sr_lists(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
}

void ClientCR::kv_update_commit_log(KVReqCtx * ctx) {
    if (*(ctx->should_stop) || ctx->is_finished) {
        ctx->is_finished = true;
        return;
    }
    kv_log_commit(ctx);
    return;
}

void ClientCR::kv_update_commit_log_sync(KVReqCtx * ctx) {
    kv_log_commit_sync(ctx);
    return;
}

void ClientCR::kv_update_cas_primary(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[7. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    if (*(ctx->should_stop) || ctx->is_finished) {
        ctx->is_finished = true;
        return;
    }
    modify_primary_idx(ctx);
    return;
}

void ClientCR::kv_update_cas_primary_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[7. %s fb%d %ld] start", __FUNCTION__, ctx->coro_id, boost::this_fiber::get_id());
    modify_primary_idx_sync(ctx);
    return;
}

void ClientCR::kv_update_bg_operations(KVReqCtx * ctx) {
    // TODO: conduct background operations
    return;
}

void ClientCR::kv_delete_read_buckets_write_log(KVReqCtx * ctx) {
    int ret = 0;
    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length);
    memset(tail, NULL, sizeof(KVLogTail));
    tail->op = KV_OP_DELETE;
    header->is_valid = true;

    // 0. allocate log memory
    uint32_t kv_block_size = sizeof(KVLogHeader) + header->key_length + sizeof(KVLogTail);
    mm_->mm_alloc(kv_block_size, nm_, ctx->key_str, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }

    // 1. update kv header and generate commit addrs
    update_log_tail(tail, &ctx->mm_alloc_ctx);
    prepare_log_commit_addrs(ctx);

    // 2. generate send requests
    uint32_t read_bucket_sr_list_num;
    uint32_t write_log_sr_list_num;
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);
    IbvSrList * write_log_sr_list = gen_write_del_log_sr_lists(ctx->coro_id, ctx->kv_info, 
        &ctx->mm_alloc_ctx, &write_log_sr_list_num);

    // check cache
    IbvSrList * read_cache_kv_sr_list;
    if (ctx->use_cache) {
        // print_log(DEBUG, "\t[%s] check cache", __FUNCTION__);
        LocalCacheEntry * local_cache_entry = check_cache(ctx->key_str);
        uint64_t cache_kv_local_addr = (uint64_t)ctx->local_cache_addr;
        ctx->is_local_cache_hit = !!(uint64_t)local_cache_entry;
        if (ctx->is_local_cache_hit) {
            ctx->cache_entry = local_cache_entry;
            read_cache_kv_sr_list = gen_read_cache_kv_sr_lists(ctx->coro_id, &local_cache_entry->l_slot_ptr, cache_kv_local_addr);
        }
    }

    // 3. merge send requests
    std::vector<IbvSrList *> phase1_sr_list_batch;
    std::vector<uint32_t> phase1_sr_list_num_batch;
    phase1_sr_list_batch.push_back(read_bucket_sr_list);
    phase1_sr_list_batch.push_back(write_log_sr_list);
    phase1_sr_list_num_batch.push_back(read_bucket_sr_list_num);
    phase1_sr_list_num_batch.push_back(write_log_sr_list_num);

    if (ctx->use_cache && ctx->is_local_cache_hit) {
        phase1_sr_list_batch.push_back(read_cache_kv_sr_list);
        phase1_sr_list_num_batch.push_back(1);
    }

    // 3. post requests
    ret = post_sr_list_batch_and_yield_wait(phase1_sr_list_batch, phase1_sr_list_num_batch);
    // kv_assert(ret == 0);
    free_read_bucket_sr_lists(read_bucket_sr_list);
    free_write_del_log_sr_lists(write_log_sr_list);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        free_read_cache_kv_sr_lists(read_cache_kv_sr_list);
    }

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_delete_read_buckets_write_log_sync(KVReqCtx * ctx) {
    int ret = 0;
    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length);
    memset(tail, NULL, sizeof(KVLogTail));
    tail->op = KV_OP_DELETE;
    header->is_valid = true;

    // 0. allocate log memory
    uint32_t kv_block_size = sizeof(KVLogHeader) + header->key_length + sizeof(KVLogTail);
    mm_->mm_alloc(kv_block_size, nm_, ctx->key_str, &ctx->mm_alloc_ctx);
    if (ctx->mm_alloc_ctx.addr_list[0] < server_st_addr_ || ctx->mm_alloc_ctx.addr_list[0] >= server_st_addr_ + server_data_len_) {
        ctx->is_finished = true;
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
        return;
    }

    // 1. update kv header and generate commit addrs
    update_log_tail(tail, &ctx->mm_alloc_ctx);
    prepare_log_commit_addrs(ctx);

    // 2. generate send requests
    uint32_t read_bucket_sr_list_num;
    uint32_t write_log_sr_list_num;
    IbvSrList * read_bucket_sr_list = gen_read_bucket_sr_lists(ctx, &read_bucket_sr_list_num);
    IbvSrList * write_log_sr_list = gen_write_del_log_sr_lists(ctx->coro_id, ctx->kv_info, 
        &ctx->mm_alloc_ctx, &write_log_sr_list_num);

    // check cache
    IbvSrList * read_cache_kv_sr_list;
    if (ctx->use_cache) {
        // print_log(DEBUG, "\t[%s] check cache", __FUNCTION__);
        LocalCacheEntry * local_cache_entry = check_cache(ctx->key_str);
        uint64_t cache_kv_local_addr = (uint64_t)ctx->local_cache_addr;
        ctx->is_local_cache_hit = !!(uint64_t)local_cache_entry;
        if (ctx->is_local_cache_hit) {
            ctx->cache_entry = local_cache_entry;
            read_cache_kv_sr_list = gen_read_cache_kv_sr_lists(ctx->coro_id, &local_cache_entry->l_slot_ptr, cache_kv_local_addr);
        }
    }

    // 3. merge send requests
    std::vector<IbvSrList *> phase1_sr_list_batch;
    std::vector<uint32_t> phase1_sr_list_num_batch;
    phase1_sr_list_batch.push_back(read_bucket_sr_list);
    phase1_sr_list_batch.push_back(write_log_sr_list);
    phase1_sr_list_num_batch.push_back(read_bucket_sr_list_num);
    phase1_sr_list_num_batch.push_back(write_log_sr_list_num);

    if (ctx->use_cache && ctx->is_local_cache_hit) {
        phase1_sr_list_batch.push_back(read_cache_kv_sr_list);
        phase1_sr_list_num_batch.push_back(1);
    }

    // 3. post requests
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_list_batch_sync(phase1_sr_list_batch, phase1_sr_list_num_batch, &wc);
    kv_assert(ret == 0);
    free_read_bucket_sr_lists(read_bucket_sr_list);
    free_write_del_log_sr_lists(write_log_sr_list);
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        free_read_cache_kv_sr_lists(read_cache_kv_sr_list);
    }

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_delete_read_kv(KVReqCtx * ctx) {
    int ret = 0;

    // 0. check cache
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // print_log(DEBUG, "\t[%s] 0. check cache", __FUNCTION__);
        KVLogHeader * cached_kv_header = (KVLogHeader *)ctx->local_cache_addr;
        // print_log(DEBUG, "\t  [%s] check cached value: valid(%d) key_len(%d) value_len(%d)", 
        //     __FUNCTION__, cached_kv_header->ctl_bits, cached_kv_header->key_length, cached_kv_header->value_length);
        if (log_is_valid(cached_kv_header)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            KVLogHeader * header = cached_kv_header;
            if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                ctx->cache_entry->acc_cnt ++; // update cache counter

                LocalCacheEntry * local_cache_entry = ctx->cache_entry;
                RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
                memset(new_local_slot_ptr, 0, sizeof(RaceHashSlot));
                fill_cas_addr(ctx, local_cache_entry->r_slot_addr, &local_cache_entry->l_slot_ptr, new_local_slot_ptr);
                fill_invalid_addr(ctx, &local_cache_entry->l_slot_ptr);
                ctx->is_cache_hit = true;
                return;
            }
        }
    }

    ctx->is_cache_hit = false;
    // 1. find kv in buckets key idx
    // print_log(DEBUG, "\t[%s] 1. search for target kv", __FUNCTION__);
    find_kv_in_buckets(ctx);

    // print_log(DEBUG, "\t[%s] 2. generate SrLists", __FUNCTION__);
    uint32_t read_kv_sr_list_num;
    IbvSrList * read_kv_sr_lists = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &read_kv_sr_list_num);

    // print_log(DEBUG, "\t[%s] 3. read kv and wait for reply", __FUNCTION__);
    ret = post_sr_lists_and_yield_wait(read_kv_sr_lists, read_kv_sr_list_num);
    // kv_assert(ret == 0);
    // ib_free_sr_lists(read_kv_sr_lists, read_kv_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_delete_read_kv_sync(KVReqCtx * ctx) {
    int ret = 0;

    // 0. check cache
    if (ctx->use_cache && ctx->is_local_cache_hit) {
        // print_log(DEBUG, "\t[%s] 0. check cache", __FUNCTION__);
        KVLogHeader * cached_kv_header = (KVLogHeader *)ctx->local_cache_addr;
        // print_log(DEBUG, "\t  [%s] check cached value: valid(%d) key_len(%d) value_len(%d)", 
        //     __FUNCTION__, cached_kv_header->ctl_bits, cached_kv_header->key_length, cached_kv_header->value_length);
        if (log_is_valid(cached_kv_header)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            KVLogHeader * header = cached_kv_header;
            if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                ctx->cache_entry->acc_cnt ++; // update cache counter

                LocalCacheEntry * local_cache_entry = ctx->cache_entry;
                RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
                memset(new_local_slot_ptr, 0, sizeof(RaceHashSlot));
                fill_cas_addr(ctx, local_cache_entry->r_slot_addr, &local_cache_entry->l_slot_ptr, new_local_slot_ptr);
                fill_invalid_addr(ctx, &local_cache_entry->l_slot_ptr);
                ctx->is_cache_hit = true;
                return;
            }
        }
    }

    ctx->is_cache_hit = false;
    // 1. find kv in buckets key idx
    // print_log(DEBUG, "\t[%s] 1. search for target kv", __FUNCTION__);
    find_kv_in_buckets(ctx);

    // print_log(DEBUG, "\t[%s] 2. generate SrLists", __FUNCTION__);
    uint32_t read_kv_sr_list_num;
    IbvSrList * read_kv_sr_lists = gen_read_kv_sr_lists(ctx->coro_id, ctx->kv_read_addr_list, &read_kv_sr_list_num);

    // print_log(DEBUG, "\t[%s] 3. read kv and wait for reply", __FUNCTION__);
    struct ibv_wc wc;
    ret = nm_->rdma_post_sr_lists_sync(read_kv_sr_lists, read_kv_sr_list_num, &wc);
    kv_assert(ret == 0);
    // ib_free_sr_lists(read_kv_sr_lists, read_kv_sr_list_num);

    ctx->is_finished = false;
    return;
}

void ClientCR::kv_delete_backup_consensus_0(KVReqCtx * ctx) {
    int ret = 0;

    if (ctx->use_cache && ctx->is_cache_hit) {
        // do nothing
        // print_log(DEBUG, "\t[%s] 0. cache hit", __FUNCTION__);
    } else {
        // print_log(DEBUG, "\t[%s] 1. find match key idx", __FUNCTION__);
        int32_t match_idx = find_match_kv_idx(ctx);
        if (match_idx == -1) {
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_SUCCESS;
            return;
        }

        std::pair<int32_t, int32_t> idx_pair = ctx->kv_idx_list[match_idx];
        int32_t bucket_idx = idx_pair.first;
        int32_t slot_idx = idx_pair.second;

        // print_log(DEBUG, "\t[%s] 2. calculate cas offset", __FUNCTION__);
        RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
        memset(new_local_slot_ptr, 0, sizeof(RaceHashSlot));
        if (bucket_idx < 2) {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->f_com_bucket[bucket_idx].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++){
                remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        } else {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->s_com_bucket[bucket_idx - 2].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        }
    }

    if (num_idx_rep_ == 1) {
        ctx->consensus_state = KV_CONSENSUS_WIN_ALL;
        return;
    }

    // 2. cas all backups
    // print_log(DEBUG, "\t[%s] 3. update backup index", __FUNCTION__);
    uint32_t bk_cas_sr_list_0_num;
    IbvSrList * bk_cas_sr_list_0 = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_bk_0_cas_list, &bk_cas_sr_list_0_num);

    // assert(bk_cas_sr_list_0_num == num_replication_ - 1);
    iteratively_cas_async(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
    free_cas_sr_lists(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
}

void ClientCR::kv_delete_backup_consensus_0_sync(KVReqCtx * ctx) {
    int ret = 0;

    if (ctx->use_cache && ctx->is_cache_hit) {
        // do nothing
        // print_log(DEBUG, "\t[%s] 0. cache hit", __FUNCTION__);
    } else {
        // print_log(DEBUG, "\t[%s] 1. find match key idx", __FUNCTION__);
        int32_t match_idx = find_match_kv_idx(ctx);
        if (match_idx == -1) {
            ctx->is_finished = true;
            ctx->ret_val.ret_code = KV_OPS_SUCCESS;
            return;
        }

        std::pair<int32_t, int32_t> idx_pair = ctx->kv_idx_list[match_idx];
        int32_t bucket_idx = idx_pair.first;
        int32_t slot_idx = idx_pair.second;

        // print_log(DEBUG, "\t[%s] 2. calculate cas offset", __FUNCTION__);
        RaceHashSlot * new_local_slot_ptr = (RaceHashSlot *)ctx->local_cas_target_value_addr;
        memset(new_local_slot_ptr, 0, sizeof(RaceHashSlot));
        if (bucket_idx < 2) {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->f_com_bucket[bucket_idx].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++){
                remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        } else {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
            uint64_t old_local_slot_addr   = (uint64_t)&(ctx->s_com_bucket[bucket_idx - 2].slots[slot_idx]);
            uint64_t remote_slot_addr[num_idx_rep_];
            for (int i = 0; i < num_idx_rep_; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            fill_cas_addr(ctx, remote_slot_addr, (RaceHashSlot *)old_local_slot_addr, 
                (RaceHashSlot *)new_local_slot_ptr);
            fill_invalid_addr(ctx, (RaceHashSlot *)old_local_slot_addr);
        }
    }

    if (num_idx_rep_ == 1) {
        ctx->consensus_state = KV_CONSENSUS_WIN_ALL;
        return;
    }

    // 2. cas all backups
    // print_log(DEBUG, "\t[%s] 3. update backup index", __FUNCTION__);
    uint32_t bk_cas_sr_list_0_num;
    IbvSrList * bk_cas_sr_list_0 = gen_cas_sr_lists(ctx->coro_id, ctx->kv_modify_bk_0_cas_list, &bk_cas_sr_list_0_num);

    // assert(bk_cas_sr_list_0_num == num_replication_ - 1);
    iteratively_cas_sync(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
    free_cas_sr_lists(bk_cas_sr_list_0, bk_cas_sr_list_0_num);
}

void ClientCR::kv_delete_commit_log(KVReqCtx * ctx) {
    if (*(ctx->should_stop) || ctx->is_finished) {
        ctx->is_finished = true;
        return;
    }
    kv_log_commit(ctx);
    return;
}

void ClientCR::kv_delete_commit_log_sync(KVReqCtx * ctx) {
    kv_log_commit_sync(ctx);
    return;
}

void ClientCR::kv_delete_cas_primary(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s] start", __FUNCTION__);
    modify_primary_idx(ctx);
    return;
}

void ClientCR::kv_delete_cas_primary_sync(KVReqCtx * ctx) {
    // print_log(DEBUG, "\t[%s] start", __FUNCTION__);
    modify_primary_idx_sync(ctx);
    return;
}

void ClientCR::kv_delete_bg_operations(KVReqCtx * ctx) {
    // TODO: conduct background operations
    return;
}

int ClientCR::post_sr_lists_and_yield_wait(IbvSrList * sr_lists, uint32_t sr_lists_num) {
    int ret = 0;
    std::map<uint64_t, struct ibv_wc *> wait_wrid_wc_map;
    ret = nm_->rdma_post_sr_lists_async(sr_lists, sr_lists_num, wait_wrid_wc_map);
    // kv_assert(ret == 0);
    ret = poll_completion(wait_wrid_wc_map);
    // kv_assert(ret == 0);
    return ret;
}

int ClientCR::post_sr_list_batch_and_yield_wait(std::vector<IbvSrList *> sr_list_batch, std::vector<uint32_t> sr_list_num_batch) {
    int ret = 0;
    std::map<uint64_t, struct ibv_wc *> wait_wrid_wc_map;
    ret = nm_->rdma_post_sr_list_batch_async(sr_list_batch, sr_list_num_batch, wait_wrid_wc_map);
    // kv_assert(ret == 0);
    ret = poll_completion(wait_wrid_wc_map);
    // kv_assert(ret == 0);
    return ret;
}

pthread_t ClientCR::start_polling_thread() {
    NMPollingThreadArgs * args = (NMPollingThreadArgs *)malloc(sizeof(NMPollingThreadArgs));
    args->nm = nm_;
    args->core_id = poll_core_id_;

    pthread_t polling_tid;
    pthread_create(&polling_tid, NULL, nm_polling_thread, (void *)args);
    return polling_tid;
}

boost::fibers::fiber ClientCR::start_polling_fiber() {
    boost::fibers::fiber fb(nm_polling_fiber, (void *)nm_);
    return fb;
}

void ClientCR::stop_polling_thread() {
    nm_->stop_polling();
}

int ClientCR::load_kv_requests(const char * fname, uint32_t st_idx, int32_t num_ops) {
    int ret = 0;
    FILE * workload_file = fopen(fname, "r");
    if (workload_file == NULL) {
        printf("failed to open: %s\n", fname);
        return -1;
    }

    if (num_total_operations_ != 0) {
        free(kv_info_list_);
        free(kv_req_ctx_list_);
        num_total_operations_ = 0;
        num_local_operations_ = 0;
    }

    char operation_buf[16];
    char table_buf[16];
    char key_buf[64];
    char value_buf[128];
    while (fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf) != EOF) {
        num_total_operations_ ++;
    }

    if (num_ops == -1) {
        num_local_operations_ = num_total_operations_;
    } else {
        num_local_operations_ = (st_idx + num_ops > num_total_operations_) ? num_total_operations_ - st_idx : num_ops;
    }
    // print_log(DEBUG, "[%s] local_ops=%d", __FUNCTION__, num_local_operations_);

    kv_info_list_    = (KVInfo *)malloc(sizeof(KVInfo) * num_local_operations_);
    kv_req_ctx_list_ = (KVReqCtx *)malloc(sizeof(KVReqCtx) * num_local_operations_);
    memset(kv_info_list_, 0, sizeof(KVInfo) * num_local_operations_);
    memset(kv_req_ctx_list_, 0, sizeof(KVReqCtx) * num_local_operations_);
    uint64_t input_buf_ptr = (uint64_t)input_buf_;

    rewind(workload_file);
    for (int i = 0; i < st_idx + num_local_operations_; i ++) {
        ret = fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf);
        sprintf(value_buf, "initial-value-%d", i);
        if (i < st_idx) {
            continue;
        }

        // record the key and value
        uint32_t all_len = sizeof(KVLogHeader) + strlen(key_buf) + strlen(value_buf) + sizeof(KVLogTail);
        void * key_st_addr = (void *)(input_buf_ptr + sizeof(KVLogHeader));
        void * value_st_addr = (void *)((uint64_t)key_st_addr + strlen(key_buf));
        memcpy(key_st_addr, key_buf, strlen(key_buf));
        memcpy(value_st_addr, value_buf, strlen(value_buf));
        kv_info_list_[i].key_len = strlen(key_buf);
        kv_info_list_[i].value_len = strlen(value_buf);
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;

        // manage kv log header
        KVLogHeader * kv_log_header = (KVLogHeader *)input_buf_ptr;
        kv_log_header->is_valid = true;
        kv_log_header->key_length = strlen(key_buf);
        kv_log_header->value_length = strlen(value_buf);

        KVLogTail * kv_log_tail = (KVLogTail *)((uint64_t)input_buf_ptr
            + sizeof(KVLogHeader) + kv_log_header->key_length + kv_log_header->value_length);
        kv_log_tail->op = KV_OP_INSERT;
        input_buf_ptr += all_len;

        // record operation
        init_kv_req_ctx(&kv_req_ctx_list_[i], &kv_info_list_[i], operation_buf);
    }

    // assert(input_buf_ptr < (uint64_t)input_buf_ + 512 * 1024 * 1024);

    return 0;
}

void ClientCR::update_log_tail(KVLogTail * tail, ClientMMAllocCtx * mm_alloc_ctx) {
    HashIndexConvert64To40Bits(mm_alloc_ctx->next_addr_list[0], tail->next_addr);
    HashIndexConvert64To40Bits(mm_alloc_ctx->prev_addr_list[0], tail->prev_addr);
}

void ClientCR::init_kv_req_ctx(KVReqCtx * req_ctx, KVInfo * kv_info, char * operation) {
    req_ctx->kv_info = kv_info;
    req_ctx->lkey = local_buf_mr_->lkey;
    req_ctx->kv_modify_pr_cas_list.resize(1);
    req_ctx->kv_modify_bk_0_cas_list.resize(num_idx_rep_ - 1);
    req_ctx->kv_modify_bk_1_cas_list.resize(num_idx_rep_ - 1);
    req_ctx->log_commit_addr_list.resize(num_replication_);
    char key_buf[128] = {0};
    memcpy(key_buf, (void *)((uint64_t)(req_ctx->kv_info->l_addr) + sizeof(KVLogHeader)), req_ctx->kv_info->key_len);
    req_ctx->key_str = std::string(key_buf);
    if (strcmp(operation, "INSERT") == 0) {
        req_ctx->req_type = KV_REQ_INSERT;
    } else if (strcmp(operation, "DELETE") == 0) {
        req_ctx->req_type = KV_REQ_DELETE;
    } else if (strcmp(operation, "UPDATE") == 0) {
        req_ctx->req_type = KV_REQ_UPDATE;
    } else if (strcmp(operation, "READ") == 0) {
        req_ctx->req_type = KV_REQ_SEARCH;
    } else {
        req_ctx->req_type = KV_REQ_SEARCH;
    }
}

void ClientCR::init_kvreq_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops) {
    void * coro_local_addr = (void *)coro_local_addr_list_[coro_id];
    for (uint32_t i = 0; i < num_ops; i ++) {
        uint32_t kv_req_idx = kv_req_st_idx + i;

        kv_req_ctx_list_[kv_req_idx].coro_id = coro_id;
        switch (kv_req_ctx_list_[kv_req_idx].req_type) {
        case KV_REQ_INSERT:
            init_kv_insert_space(coro_local_addr, kv_req_idx);
            break;
        case KV_REQ_SEARCH:
            init_kv_search_space(coro_local_addr, kv_req_idx);
            break;
        case KV_REQ_UPDATE:
            init_kv_update_space(coro_local_addr, kv_req_idx);
            break;
        case KV_REQ_DELETE:
            init_kv_delete_space(coro_local_addr, kv_req_idx);
            break;
        default:
            kv_req_ctx_list_[kv_req_idx].req_type = KV_REQ_SEARCH;
            init_kv_search_space(coro_local_addr, kv_req_idx);
            break;
        }
    }
}

void ClientCR::init_kv_insert_space(void * coro_local_addr, KVReqCtx * kv_req_ctx) {
    kv_req_ctx->use_cache = true;
    kv_req_ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    kv_req_ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    kv_req_ctx->local_cas_return_value_addr = (void *)((uint64_t)kv_req_ctx->local_cas_target_value_addr + sizeof(uint64_t));
}

void ClientCR::init_kv_insert_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_insert_space(coro_local_addr, ctx);
}

void ClientCR::init_kv_search_space(void * coro_local_addr, KVReqCtx * kv_req_ctx) {
    kv_req_ctx->use_cache = true;
    kv_req_ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    kv_req_ctx->local_cache_addr  = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket) * num_replication_);
    kv_req_ctx->local_kv_addr     = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket) * num_replication_);
}

void ClientCR::init_kv_search_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_search_space(coro_local_addr, ctx);
}

void ClientCR::init_kv_update_space(void * coro_local_addr, KVReqCtx * kv_req_ctx) {
    kv_req_ctx->use_cache = true;
    kv_req_ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    kv_req_ctx->local_kv_addr = (void *)((uint64_t)coro_local_addr + 16 * sizeof(RaceHashBucket));
    kv_req_ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 16 * sizeof(RaceHashBucket));
    kv_req_ctx->local_cas_return_value_addr = (void *)((uint64_t)kv_req_ctx->local_cas_target_value_addr + sizeof(uint64_t));
    kv_req_ctx->local_cache_addr = (void *)((uint64_t)kv_req_ctx->local_cas_target_value_addr + sizeof(uint64_t) * num_replication_);
}

void ClientCR::init_kv_update_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_update_space(coro_local_addr, ctx);
}

void ClientCR::init_kv_delete_space(void * coro_local_addr, KVReqCtx * kv_req_ctx) {
    kv_req_ctx->use_cache = true;
    kv_req_ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
    kv_req_ctx->local_cache_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    kv_req_ctx->local_kv_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    kv_req_ctx->local_cas_target_value_addr = (void *)((uint64_t)coro_local_addr + 4 * sizeof(RaceHashBucket));
    kv_req_ctx->local_cas_return_value_addr = (void *)((uint64_t)kv_req_ctx->local_cas_target_value_addr + sizeof(uint64_t));
}

void ClientCR::init_kv_delete_space(void * coro_local_addr, uint32_t kv_req_idx) {
    KVReqCtx * ctx = &kv_req_ctx_list_[kv_req_idx];
    init_kv_delete_space(coro_local_addr, ctx);
}

void * client_cr_ops_fb_cnt_time(void * arg) {
    boost::this_fiber::yield();
    ClientFiberArgs * fiber_args = (ClientFiberArgs *)arg;
    fiber_args->client_cr->init_kvreq_space(fiber_args->coro_id, fiber_args->ops_st_idx, fiber_args->ops_num);
    uint32_t num_failed = 0;
    int ret = 0;
    void * search_addr = NULL;
    
    gettimeofday(fiber_args->st, NULL);
    for (int i = 0; i < fiber_args->ops_num; i ++) {
        KVReqCtx * ctx = &fiber_args->client_cr->kv_req_ctx_list_[i + fiber_args->ops_st_idx];
        ctx->coro_id = fiber_args->coro_id;
        ctx->should_stop = fiber_args->should_stop;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            search_addr = fiber_args->client_cr->kv_search(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            ret = fiber_args->client_cr->kv_insert(ctx);
            if (ret == KV_OPS_FAIL_REDO || ret == KV_OPS_FAIL_RETURN) {
                num_failed++;
            }
            break;
        case KV_REQ_UPDATE:
            fiber_args->client_cr->kv_update(ctx);
            break;
        case KV_REQ_DELETE:
            fiber_args->client_cr->kv_delete(ctx);
            break;
        default:
            fiber_args->client_cr->kv_search(ctx);
            break;
        }
    }
    gettimeofday(fiber_args->et, NULL);
    fiber_args->num_failed = num_failed;
    return NULL;
}

void * client_cr_ops_fb_cnt_ops(void * arg) {
    boost::this_fiber::yield();
    ClientFiberArgs * fiber_args = (ClientFiberArgs *)arg;
    fiber_args->client_cr->init_kvreq_space(fiber_args->coro_id, fiber_args->ops_st_idx, fiber_args->ops_num);
    uint32_t num_failed = 0;
    int ret = 0;
    void * search_addr = NULL;
    
    fiber_args->b->wait();
    boost::this_fiber::yield();
    uint32_t cnt = 0;
    std::unordered_map<std::string, bool> inserted_key_map;
    while (*fiber_args->should_stop == false && fiber_args->ops_num != 0) {
        uint32_t idx = cnt % fiber_args->ops_num;
        KVReqCtx * ctx = &fiber_args->client_cr->kv_req_ctx_list_[idx + fiber_args->ops_st_idx];
        ctx->coro_id = fiber_args->coro_id;
        ctx->should_stop = fiber_args->should_stop;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            search_addr = fiber_args->client_cr->kv_search(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            if (inserted_key_map[ctx->key_str] == true) {
                char * modify = (char *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KVLogHeader));
                modify[4] ++;
                ctx->key_str[4] ++;
            }
            ret = fiber_args->client_cr->kv_insert(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            inserted_key_map[ctx->key_str] = true;
            break;
        case KV_REQ_UPDATE:
            fiber_args->client_cr->kv_update(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_DELETE:
            fiber_args->client_cr->kv_delete(ctx);
            break;
        default:
            fiber_args->client_cr->kv_search(ctx);
            break;
        }
        if (ret == KV_OPS_FAIL_REDO) {
            cnt --;
        }
        cnt ++;
    }

    fiber_args->ops_cnt = cnt;
    fiber_args->num_failed = num_failed;
    return NULL;
}

void * client_cr_ops_fb_cnt_ops_micro(void * arg) {
    boost::this_fiber::yield();
    ClientFiberArgs * fiber_args = (ClientFiberArgs *)arg;
    fiber_args->client_cr->init_kvreq_space(fiber_args->coro_id, fiber_args->ops_st_idx, fiber_args->ops_num);
    uint32_t num_failed = 0;
    int ret = 0;
    void * search_addr = NULL;
    
    fiber_args->b->wait();
    uint32_t cnt = 0;
    bool is_finished = false;
    std::unordered_map<std::string, bool> inserted_key_map;
    std::unordered_map<std::string, bool> deleted_key_map;
    while (*fiber_args->should_stop == false && fiber_args->ops_num != 0) {
        uint32_t idx = cnt % fiber_args->ops_num;
        KVReqCtx * ctx = &fiber_args->client_cr->kv_req_ctx_list_[idx + fiber_args->ops_st_idx];
        ctx->coro_id = fiber_args->coro_id;
        ctx->should_stop = fiber_args->should_stop;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            search_addr = fiber_args->client_cr->kv_search(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            if (inserted_key_map[ctx->key_str] == true) {
                char * modify = (char *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KVLogHeader));
                modify[2] ++;
                ctx->key_str[2] ++;
            }
            do {
                ret = fiber_args->client_cr->kv_insert(ctx);
            } while (ret == KV_OPS_FAIL_REDO);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            if (ret != KV_OPS_FAIL_RETURN) {
                inserted_key_map[ctx->key_str] = true;
            }
            break;
        case KV_REQ_UPDATE:
            ret = fiber_args->client_cr->kv_update(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_DELETE:
            if (deleted_key_map[ctx->key_str] == true) {
                char * modify = (char *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KVLogHeader));
                modify[2] ++;
                ctx->key_str[2] ++;
            }
            ret = fiber_args->client_cr->kv_delete(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            deleted_key_map[ctx->key_str] = true;
            break;
        default:
            fiber_args->client_cr->kv_search(ctx);
            break;
        }
        cnt ++;
        if (cnt > fiber_args->ops_num && is_finished == false) {
            is_finished = true;
            printf("finished!\n");
        }
    }
    fiber_args->ops_cnt = cnt;
    fiber_args->num_failed = num_failed;

    bool should_stop = false;
    while (cnt < fiber_args->ops_num) {
        uint32_t idx = cnt % fiber_args->ops_num;
        KVReqCtx * ctx = &fiber_args->client_cr->kv_req_ctx_list_[idx + fiber_args->ops_st_idx];
        ctx->coro_id = fiber_args->coro_id;
        ctx->should_stop = &should_stop;
        if (ctx->req_type != KV_REQ_INSERT) {
            break;
        }
        
        if (inserted_key_map[ctx->key_str] == true) {

        }
        do {
            ret = fiber_args->client_cr->kv_insert(ctx);
        } while (ret == KV_OPS_FAIL_REDO);
        if (ret == KV_OPS_FAIL_RETURN) {
            num_failed ++;
        }
        if (ret != KV_OPS_FAIL_RETURN) {
            inserted_key_map[ctx->key_str] = true;
        }
        cnt ++;
    }
    return NULL;
}

void * client_cr_ops_fb_cnt_ops_on_crash(void * arg) {
    boost::this_fiber::yield();
    ClientFiberArgs * fiber_args = (ClientFiberArgs *)arg;
    if (fiber_args->ops_cnt == 0) {
        fiber_args->client_cr->init_kvreq_space(fiber_args->coro_id, fiber_args->ops_st_idx, fiber_args->ops_num);
    }
    int ret = 0;
    void * search_addr = NULL; 

    fiber_args->b->wait();
    boost::this_fiber::yield();
    uint32_t num_failed = fiber_args->num_failed;
    uint32_t cnt = fiber_args->ops_cnt;
    std::unordered_map<std::string, bool> inserted_key_map;
    while (*fiber_args->should_stop == false && fiber_args->ops_num != 0) {
        uint32_t idx = cnt % fiber_args->ops_num;
        KVReqCtx * ctx = &fiber_args->client_cr->kv_req_ctx_list_[idx + fiber_args->ops_st_idx];
        ctx->coro_id = fiber_args->coro_id;
        ctx->should_stop = fiber_args->should_stop;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            search_addr = fiber_args->client_cr->kv_search_on_crash(ctx);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            if (inserted_key_map[ctx->key_str] == true) {
                char * modify = (char *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KVLogHeader));
                modify[4] ++;
                ctx->key_str[4] ++;
            }
            ret = fiber_args->client_cr->kv_insert(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            inserted_key_map[ctx->key_str] = true;
            break;
        case KV_REQ_UPDATE:
            fiber_args->client_cr->kv_update(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
            break;
        case KV_REQ_DELETE:
            fiber_args->client_cr->kv_delete(ctx);
            break;
        default:
            fiber_args->client_cr->kv_search_on_crash(ctx);
            break;
        }
        cnt ++;
    }

    fiber_args->ops_cnt = cnt;
    fiber_args->num_failed = num_failed;
    return NULL;
}

int ClientCR::client_recovery() {
    // print_log(DEBUG, "[%s] start", __FUNCTION__);
    int ret = 0;

    // 1. get last operation log entry
    RecoverLogInfo recover_log_info;
    ret = mm_->get_last_log_recover_info(&recover_log_info);
    assert(ret == 0);

    KVLogTail * tail = recover_log_info.local_tail_addr;
    // print_log(DEBUG, "[%s]   1. get last op log: committed(%d) valid(%d) gc(%d) insert(%d) klen(%d) vlen(%d)",
    //     __FUNCTION__, log_is_committed(header), log_is_valid(header), log_is_gc(header), log_is_insert(header),
    //     header->key_length, header->value_length);

    // 2. retry last operation
    // print_log(DEBUG, "[%s]   2. retry last operation", __FUNCTION__);
    // 2.1. get the completed kv
    // print_log(DEBUG, "[%s]   2.1. read complete kv", __FUNCTION__);
    uint32_t total_len = sizeof(KVLogHeader) + recover_log_info.key_len 
        + recover_log_info.val_len;
    uint8_t  server_id = recover_log_info.server_id;
    uint64_t r_addr = recover_log_info.remote_addr;
    ret = nm_->nm_rdma_read_from_sid(input_buf_, input_buf_mr_->lkey, total_len, 
        r_addr, server_mr_info_map_[server_id]->rkey, server_id);
    // assert(ret == 0);

    // 2.2. retry the operation
    // construct recovery ctx
    // print_log(DEBUG, "[%s]   2.2. retry the operation", __FUNCTION__);
    KVReqCtx rec_ctx;
    KVInfo   rec_kv_info;
    rec_kv_info.l_addr = input_buf_;
    rec_kv_info.lkey   = input_buf_mr_->lkey;
    rec_kv_info.key_len   = recover_log_info.key_len;
    rec_kv_info.value_len = recover_log_info.val_len;

    mm_->mm_alloc_log_info(&recover_log_info, &rec_ctx.mm_alloc_ctx);
    init_recover_req_ctx(&rec_kv_info, &rec_ctx);

    mm_->free_recover_buf();
    // print_log(DEBUG, "[%s] finished", __FUNCTION__);
    return 0;
}

void ClientCR::init_recover_req_ctx(KVInfo * kv_info, __OUT KVReqCtx * req_ctx) {
    KVLogHeader * header = (KVLogHeader *)kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    if (log_is_committed(tail)) {
        req_ctx->req_type = KV_REQ_RECOVER_COMMITTED;
    } else {
        req_ctx->req_type = KV_REQ_RECOVER_UNCOMMITTED;
    }

    if (log_is_insert(tail)) {
        init_kv_insert_space(local_buf_, req_ctx);
    } else {
        init_kv_update_space(local_buf_, req_ctx);
    }
    req_ctx->kv_info = kv_info;
    req_ctx->lkey = local_buf_mr_->lkey;
    req_ctx->is_cache_hit = false;
}

void ClientCR::crash_server(const std::vector<uint8_t> & fail_server_list) {
    for (size_t i = 0; i < fail_server_list.size(); i ++) {
        server_crash_map_[fail_server_list[i]] = true;
        // print_log(DEBUG, "[%s] crashing server %d", __FUNCTION__, fail_server_list[i]);
    }
}

void ClientCR::get_addr_translate_table(const std::vector<uint8_t> & server_id_list) {
    int ret = 0;
    for (size_t i = 0; i < server_id_list.size(); i ++) {
        uint64_t local_addr = (uint64_t)local_buf_ + i * CLIENT_META_LEN;
        uint32_t lkey = local_buf_mr_->lkey;
        uint64_t remote_addr = remote_global_meta_addr_ + CLIENT_META_LEN * (server_id_list[i] - num_memory_ + 1);
        uint8_t  healthy_meta_server = 0;
        for (healthy_meta_server = 0; healthy_meta_server < num_replication_ && server_crash_map_[healthy_meta_server] == true; healthy_meta_server ++);
        // print_log(DEBUG, "found healthy: %d", healthy_meta_server);
        ret = nm_->nm_rdma_read_from_sid_sync((void *)local_addr, lkey, CLIENT_META_LEN, 
            remote_addr, server_mr_info_map_[healthy_meta_server]->rkey, healthy_meta_server);
        // assert(ret == 0);

        // push back address translation info
        ClientMetaAddrInfo * addr_info_list = (ClientMetaAddrInfo *)(local_addr + sizeof(ClientLogMetaInfo));
        for (int k = 0; addr_info_list[k].meta_info_type != 0; k ++) {
            if (addr_info_list[k].meta_info_type == TYPE_KVBLOCK) {
                meta_addr_info_.push_back(addr_info_list[k]);
                // print_log(DEBUG, "[%s] translate table get %d: %lx", __FUNCTION__, 
                //     addr_info_list[k].server_id_list[0], addr_info_list[k].addr_list[0]);
                for (int j = 1; j < num_replication_; j ++) {
                    // print_log(DEBUG, "[%s]\t -> %d: %lx", __FUNCTION__,
                    //     addr_info_list[k].server_id_list[j], addr_info_list[k].addr_list[j]);
                }
            }
        }
    }
}

// save all cached item to remote server
void ClientCR::dump_cache() {
    size_t cache_entry_num = addr_cache_.size();
    CacheSaveSlot * save_space = (CacheSaveSlot *)malloc(sizeof(CacheSaveSlot) * cache_entry_num);
    uint32_t space_len = sizeof(CacheSaveSlot) * cache_entry_num;
    // assert(cache_entry_num == 100000);
    printf("cached %ld items\n", cache_entry_num);
    printf("cache: %d MB\n", space_len / 1024 / 1024);
    
    std::map<std::string, LocalCacheEntry *>::iterator it;
    int i = 0;
    for (it = addr_cache_.begin(); it != addr_cache_.end(); it ++) {
        if (it->first.size() > 31) {
            continue;
        }
        strcpy(save_space[i].key, it->first.c_str());
        memcpy(&save_space[i].entry, it->second, sizeof(LocalCacheEntry));
        // printf("%s -> %d:%lx\n", save_space[i].key, save_space[i].entry.l_slot_ptr.server_id, 
        //     HashIndexConvert40To64Bits(save_space[i].entry.l_slot_ptr.pointer));
        i ++;
    }

    FILE * cache_dump = fopen("cache.dump", "wb");
    int ret = fwrite(save_space, space_len, 1, cache_dump);
    assert(ret == 1);
    fclose(cache_dump);
}

void ClientCR::load_cache() {
    int ret = 0;
    int cache_dump_fd = open("./cache.dump", O_RDONLY);
    uint32_t space_len = sizeof(CacheSaveSlot) * 100000;
    CacheSaveSlot * save_space = (CacheSaveSlot *)malloc(space_len);
    assert(save_space != NULL);

    ret = read(cache_dump_fd, save_space, space_len);
    printf("%d\n", ret);

    for (int i = 0; i < 100000; i ++) {
        LocalCacheEntry * tmp_entry = (LocalCacheEntry *)malloc(sizeof(LocalCacheEntry));
        memcpy(tmp_entry, &save_space[i].entry, sizeof(LocalCacheEntry));
        addr_cache_[std::string(save_space[i].key)] = tmp_entry;
        // printf("%s -> %d:%lx\n", save_space[i].key, save_space[i].entry.l_slot_ptr.server_id, 
        //     HashIndexConvert40To64Bits(save_space[i].entry.l_slot_ptr.pointer));
    }
    free(save_space);
    close(cache_dump_fd);
}

void ClientCR::iteratively_cas_sync(IbvSrList * sr_lists, uint32_t num_sr_lists) {
    struct ibv_wc wc;
    uint64_t target_value;
    uint64_t expected_value;
    uint64_t * swap_back;
    int ret = 0;
    for (int i = 0; i < num_sr_lists; i ++) {   
        assert(sr_lists[i].num_sr == 1);
        assert(sr_lists[i].sr_list[0].num_sge == 1);
        target_value   = sr_lists[i].sr_list[0].wr.atomic.swap;
        expected_value = sr_lists[i].sr_list[0].wr.atomic.compare_add;
        swap_back      = (uint64_t *)sr_lists[i].sr_list[0].sg_list[0].addr;
        while (true) {
            ret = nm_->rdma_post_sr_lists_sync(&sr_lists[i], 1, &wc);
            assert(ret == 0);
            if (*swap_back == expected_value) {
                break;
            } else {
                expected_value = (*swap_back);
                sr_lists[i].sr_list[0].wr.atomic.compare_add = expected_value;
            }
        }
    }
}

void ClientCR::iteratively_cas_async(IbvSrList * sr_lists, uint32_t num_sr_lists) {
    struct ibv_wc wc;
    uint64_t target_value;
    uint64_t expected_value;
    uint64_t * swap_back;
    int ret = 0;
    for (int i = 0; i < num_sr_lists; i ++) {
        assert(sr_lists[i].num_sr == 1);
        assert(sr_lists[i].sr_list[0].num_sge == 1);
        target_value   = sr_lists[i].sr_list[0].wr.atomic.swap;
        expected_value = sr_lists[i].sr_list[0].wr.atomic.compare_add;
        swap_back      = (uint64_t *)sr_lists[0].sr_list[0].sg_list[0].addr;
        while (true) {
            ret = post_sr_lists_and_yield_wait(&sr_lists[i], 1);
            assert(ret == 0);
            if (*swap_back == expected_value) {
                break;
            } else {
                expected_value = (*swap_back);
                sr_lists[i].sr_list[0].wr.atomic.compare_add = expected_value;
            }
        }
    }
}

int ClientCR::load_seq_kv_requests(uint32_t num_keys, char * op_type) {
    num_total_operations_ = num_keys;
    num_local_operations_ = num_keys;

    kv_info_list_ = (KVInfo *)malloc(sizeof(KVInfo) * num_local_operations_);
    kv_req_ctx_list_ = new KVReqCtx[num_local_operations_];
    memset(kv_info_list_, 0, sizeof(KVInfo) * num_local_operations_);

    uint64_t input_buf_ptr = (uint64_t)input_buf_;
    char key_buf[256] = {0};
    char value_buf[256] = {0};
    for (int i = 0; i < num_local_operations_; i ++) {
        sprintf(key_buf, "%3d-%d", my_server_id_, i);
        sprintf(value_buf, "initial-value-%d", i);

        uint32_t all_len = sizeof(KVLogHeader) + strlen(key_buf) 
            + strlen(value_buf) + sizeof(KVLogTail);
        void * key_st_addr = (void *)(input_buf_ptr + sizeof(KVLogHeader));
        void * value_st_addr = (void *)((uint64_t)key_st_addr + strlen(key_buf));
        memcpy(key_st_addr, key_buf, strlen(key_buf));
        memcpy(value_st_addr, value_buf, strlen(value_buf));
        kv_info_list_[i].key_len = strlen(key_buf);
        kv_info_list_[i].value_len = strlen(value_buf);
        kv_info_list_[i].l_addr  = (void *)input_buf_ptr;
        kv_info_list_[i].lkey = input_buf_mr_->lkey;

        KVLogHeader * kv_log_header = (KVLogHeader *)input_buf_ptr;
        kv_log_header->key_length = strlen(key_buf);
        kv_log_header->value_length = strlen(value_buf);
        kv_log_header->is_valid = true;

        KVLogTail * tail = (KVLogTail *)((uint64_t)input_buf_ptr
            + sizeof(KVLogHeader) + kv_log_header->key_length + kv_log_header->value_length);
        input_buf_ptr += all_len;

        init_kv_req_ctx(&kv_req_ctx_list_[i], &kv_info_list_[i], op_type);
    }
    return 0;
}

int ClientCR::get_num_rep() {
    return num_replication_;
}