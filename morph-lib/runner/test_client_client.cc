#include "test_client.h"

#include "kv_utils.h"
#include <vector>

void ClientTest::SetUp() {
    int ret = load_config("./client_config.json", &client_conf_);
    ASSERT_TRUE(ret == 0);

    client_ = new Client(&client_conf_);
}

void ClientTest::TearDown() {
    delete client_;
}

TEST_F(ClientTest, initialization) {
    ASSERT_TRUE(1);
}

TEST_F(ClientTest, get_root) {
    RaceHashRoot race_root;
    int ret = client_->test_get_root(&race_root);
    ASSERT_TRUE(ret == 0);

    uint64_t subtable_offset = race_root.subtable_offset + client_conf_.server_base_addr;
    for (int i = 0; i < RACE_HASH_INIT_SUBTABLE_NUM; i ++) {
        RaceHashSubtableEntry target_entry;
        target_entry.lock = 0;
        target_entry.local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
        target_entry.server_id   = 0;
        HashIndexConvert64To40Bits(subtable_offset, target_entry.pointer);
        ASSERT_TRUE(memcmp(&race_root.subtable_entry[i], &target_entry, sizeof(RaceHashSubtableEntry)) == 0);
        subtable_offset += roundup_256(SUBTABLE_LEN);
    }
}

TEST_F(ClientTest, get_log_meta) {
    pthread_t polling_tid = client_->start_polling_thread();
    ClientLogMetaInfo remote_log_meta_info_list[client_conf_.num_replication];
    ClientLogMetaInfo local_log_meta_info;
    int ret = client_->test_get_log_meta_info(remote_log_meta_info_list, &local_log_meta_info);
    ASSERT_TRUE(ret == 0);

    printf("local: sid(%d) log_head(%lx) log_tail(%lx)\n", local_log_meta_info.pr_server_id, 
        local_log_meta_info.pr_log_head, local_log_meta_info.pr_log_tail);
    for (int i = 0; i < client_conf_.num_replication; i ++) {
        printf("remote: sid(%d) log_head(%lx) log_tail(%lx)\n", remote_log_meta_info_list[i].pr_server_id, 
            remote_log_meta_info_list[i].pr_log_head, remote_log_meta_info_list[i].pr_log_tail);
        ASSERT_TRUE(remote_log_meta_info_list[i].pr_server_id == local_log_meta_info.pr_server_id);
        ASSERT_TRUE(remote_log_meta_info_list[i].pr_log_head == local_log_meta_info.pr_log_head);
        ASSERT_TRUE(remote_log_meta_info_list[i].pr_log_tail == local_log_meta_info.pr_log_tail);
    }
    client_->stop_polling_thread();
    pthread_join(polling_tid, NULL);
}

TEST_F(ClientTest, get_meta_addr) {
    int ret = 0;
    uint64_t remote_addr_info_list_len;
    ClientMetaAddrInfo ** remote_addr_info_list = client_->test_get_meta_addr_info(&remote_addr_info_list_len);

    ClientMMBlock local_mm_block;
    uint64_t local_mm_block_num;
    ret = client_->test_get_local_mm_blocks(&local_mm_block, &local_mm_block_num);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(local_mm_block_num == 1);

    for (int i = 0; i < client_conf_.num_replication; i ++) {
        for (int j = 0; j < remote_addr_info_list_len; j ++) {
            ASSERT_TRUE(memcmp(&remote_addr_info_list[i][j], &remote_addr_info_list[0][j], sizeof(ClientMetaAddrInfo)) == 0);
        }
    }
}

// TEST_F(ClientTest, kv_test) {
//     void * client_input_buf = client_->get_input_buf();
//     uint32_t client_input_buf_lkey = client_->get_input_buf_lkey();

//     const char * key_template = "test-%d-k";
//     const char * value_template = "test-%d-v-%s";

//     pthread_t polling_tid = client_->start_polling_thread();

//     for (int i = 0; i < 1000; i ++) {
//         char key[128];
//         char value[128];

//         sprintf(key, key_template, i);
//         sprintf(value, value_template, i, "before");

//         uint64_t l_st_addr = (uint64_t)client_input_buf;

//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader)), key, strlen(key));
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));
//         KVLogHeader * header = (KVLogHeader *)client_input_buf;
//         // header->is_valid = true;
//         header->ctl_bits = KV_LOG_VALID;
//         header->key_length = strlen(key);
//         header->value_length = strlen(value);
//         header->next_addr = 0;

//         KVInfo insert_info_0;
//         insert_info_0.l_addr = client_input_buf;
//         insert_info_0.key_len = strlen(key);
//         insert_info_0.value_len = strlen(value);
//         insert_info_0.lkey = client_input_buf_lkey;

//         int ret = 0;
//         ret = client_->kv_insert(&insert_info_0);
//         ASSERT_TRUE(ret == 0);

//         void * search_addr = client_->kv_search(&insert_info_0);
//         ASSERT_TRUE(memcmp(search_addr, value, strlen(value)) == 0);

//         sprintf(value, value_template, i, "after");
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader)), key, strlen(key));
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));
//         // header->is_valid = true;
//         header->value_length = KV_LOG_VALID;
//         header->key_length = strlen(key);
//         header->value_length = strlen(value);
//         header->next_addr = 0;
//         KVInfo update_info;
//         update_info.l_addr = client_input_buf;
//         update_info.key_len = strlen(key);
//         update_info.value_len = strlen(value);
//         update_info.lkey = client_input_buf_lkey;

//         ret = client_->kv_update(&update_info);
//         ASSERT_TRUE(ret == 0);

//         search_addr = client_->kv_search(&update_info);
//         ASSERT_TRUE(memcmp(search_addr, value, strlen(value)) == 0);

//         ret = client_->kv_delete(&update_info);
//         ASSERT_TRUE(ret == 0);

//         search_addr = client_->kv_search(&update_info);
//         ASSERT_TRUE(search_addr == NULL);
//     }

//     client_->stop_polling_thread();
//     pthread_join(polling_tid, NULL);
// }

// TEST_F(ClientTest, kv_test_cache) {
//     void * client_input_buf = client_->get_input_buf();
//     uint32_t client_input_buf_lkey = client_->get_input_buf_lkey();

//     const char * key_template = "test-%d-k";
//     const char * value_template = "test-%d-v-%s";

//     pthread_t polling_tid = client_->start_polling_thread();

//     for (int i = 0; i < 1000; i ++) {
//         char key[128];
//         char value[128];

//         sprintf(key, key_template, i);
//         sprintf(value, value_template, i, "before");

//         uint64_t l_st_addr = (uint64_t)client_input_buf;

//         KVLogHeader * header = (KVLogHeader *)l_st_addr;
//         // header->is_valid = true;
//         header->ctl_bits = KV_LOG_VALID;
//         header->key_length = strlen(key);
//         header->value_length = strlen(value);
//         header->next_addr = 0;

//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader)), key, strlen(key));
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));

//         KVInfo insert_info;
//         insert_info.l_addr = client_input_buf;
//         insert_info.key_len = strlen(key);
//         insert_info.value_len = strlen(value);
//         insert_info.lkey = client_input_buf_lkey;

//         int ret = 0;
//         ret = client_->kv_insert_w_cache(&insert_info);
//         ASSERT_TRUE(ret == 0);

//         void * search_addr = client_->kv_search_w_cache(&insert_info);
//         ASSERT_TRUE(memcmp(search_addr, value, strlen(value)) == 0);

//         sprintf(value, value_template, i, "after");
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));
//         header->value_length = strlen(value);
        
//         KVInfo update_info;
//         update_info.l_addr = client_input_buf;
//         update_info.key_len = strlen(key);
//         update_info.value_len = strlen(value);
//         update_info.lkey = client_input_buf_lkey;

//         ret = client_->kv_update_w_cache(&update_info);
//         ASSERT_TRUE(ret == 0);

//         search_addr = client_->kv_search_w_cache(&update_info);
//         ASSERT_TRUE(memcmp(search_addr, value, strlen(value)) == 0);
        
//         // ret = client_->kv_delete_w_cache(&update_info);
//         ret = client_->kv_delete_w_cache(&update_info);
//         ASSERT_TRUE(ret == 0);

//         search_addr = client_->kv_search_w_cache(&update_info);
//         ASSERT_TRUE(search_addr == NULL);
//     }
//     client_->stop_polling_thread();
//     pthread_join(polling_tid, NULL);
// }

// TEST_F(ClientTest, test_kvreq) {
//     void * client_input_buf = client_->get_input_buf();
//     uint32_t client_input_buf_lkey = client_->get_input_buf_lkey();
//     void * client_local_buf = client_->get_local_buf_mr()->addr;
//     uint32_t client_local_buf_lkey = client_->get_local_buf_mr()->lkey;

//     const char * key_template = "test-%d-k";
//     const char * value_template = "test-%d-v-%s";

//     // pthread_t polling_tid = client_->start_polling_thread();
//     boost::fibers::fiber fb = client_->start_polling_fiber();

//     for (int i = 0; i < 1000; i ++) {
//         char key[128];
//         char value[128];

//         sprintf(key, key_template, i);
//         sprintf(value, value_template, i, "before");

//         uint64_t l_st_addr = (uint64_t)client_input_buf;

//         KVLogHeader * header = (KVLogHeader *)l_st_addr;
//         // header->is_valid = true;
//         header->ctl_bits = KV_LOG_VALID;
//         header->key_length = strlen(key);
//         header->value_length = strlen(value);
//         header->next_addr = 0;

//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader)), key, strlen(key));
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));

//         KVInfo insert_info;
//         insert_info.l_addr = client_input_buf;
//         insert_info.key_len = strlen(key);
//         insert_info.value_len = strlen(value);
//         insert_info.lkey = client_input_buf_lkey;

//         KVReqCtx insert_ctx;
//         insert_ctx.req_type = KV_REQ_INSERT;
//         insert_ctx.use_cache = true;
//         insert_ctx.kv_info = &insert_info;
//         insert_ctx.local_bucket_addr = (RaceHashBucket *)client_local_buf;
//         insert_ctx.local_cas_target_value_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         insert_ctx.local_cas_return_value_addr = (void *)((uint64_t)insert_ctx.local_cas_target_value_addr + sizeof(uint64_t));
//         insert_ctx.lkey = client_local_buf_lkey;

//         int ret = 0;
//         ret = client_->kv_insert(&insert_ctx);
//         ASSERT_TRUE(ret == 0);

//         KVReqCtx search_ctx;
//         memset(&search_ctx, 0, sizeof(KVReqCtx));
//         search_ctx.req_type = KV_REQ_SEARCH;
//         search_ctx.use_cache = true;
//         search_ctx.kv_info  = &insert_info;
//         search_ctx.local_bucket_addr = (RaceHashBucket *)client_local_buf;
//         search_ctx.local_cache_addr  = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         search_ctx.local_kv_addr     = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         search_ctx.lkey = client_local_buf_lkey;

//         void * search_addr = client_->kv_search(&search_ctx);
//         ASSERT_TRUE(memcmp(search_addr, value, strlen(value)) == 0);

//         sprintf(value, value_template, i, "after");
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));
//         header->value_length = strlen(value);
        
//         KVInfo update_info;
//         update_info.l_addr = client_input_buf;
//         update_info.key_len = strlen(key);
//         update_info.value_len = strlen(value);
//         update_info.lkey = client_input_buf_lkey;

//         KVReqCtx update_ctx;
//         update_ctx.req_type = KV_REQ_UPDATE;
//         update_ctx.use_cache = true;
//         update_ctx.kv_info = &update_info;
//         update_ctx.local_bucket_addr = (RaceHashBucket *)client_local_buf;
//         update_ctx.local_kv_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         update_ctx.local_cas_target_value_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         update_ctx.local_cas_return_value_addr = (void *)((uint64_t)update_ctx.local_cas_target_value_addr + sizeof(uint64_t));
//         update_ctx.local_cache_addr = (void *)((uint64_t)update_ctx.local_cas_target_value_addr + sizeof(uint64_t) * client_conf_.num_replication);
//         update_ctx.lkey = client_local_buf_lkey;

//         ret = client_->kv_update(&update_ctx);
//         ASSERT_TRUE(ret == 0);

//         search_ctx.kv_info = &update_info;
//         search_addr = client_->kv_search(&search_ctx);
//         ASSERT_TRUE(memcmp(search_addr, value, strlen(value)) == 0);
        
//         KVReqCtx delete_ctx;
//         delete_ctx.req_type = KV_REQ_DELETE;
//         delete_ctx.use_cache = true;
//         delete_ctx.kv_info = &update_info;
//         delete_ctx.local_bucket_addr = (RaceHashBucket *)client_local_buf;
//         delete_ctx.local_cache_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         delete_ctx.local_kv_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         delete_ctx.local_cas_target_value_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         delete_ctx.local_cas_return_value_addr = (void *)((uint64_t)delete_ctx.local_cas_target_value_addr + sizeof(uint64_t));
//         delete_ctx.lkey = client_local_buf_lkey;
//         ret = client_->kv_delete(&delete_ctx);
//         ASSERT_TRUE(ret == 0);

//         search_ctx.kv_info = &update_info;
//         search_addr = client_->kv_search(&search_ctx);
//         ASSERT_TRUE(search_addr == NULL);
//     }

//     client_->stop_polling_thread();
//     // pthread_join(polling_tid, NULL);
//     fb.join();
// }

TEST_F(ClientTest, test_load_workload) {
    client_->load_kv_requests("../ycsb-test/workloads/workloada.spec_load", 0, -1);
    ASSERT_TRUE(client_->num_total_operations_ == 100000);
    ASSERT_TRUE(client_->num_local_operations_ == 100000);
    for (int i = 0; i < client_->num_local_operations_; i ++) {
        ASSERT_TRUE(client_->kv_req_ctx_list_[i].req_type == KV_REQ_INSERT);
    }
}

TEST_F(ClientTest, test_ycsb_load) {
    client_->load_kv_requests("../ycsb-test/workloads/workloada.spec_load", 0, -1);
    ASSERT_TRUE(client_->num_total_operations_ == 100000);
    ASSERT_TRUE(client_->num_local_operations_ == 100000);
    // ASSERT_TRUE(client_conf_.num_coroutines == 8);

    pthread_t polling_tid = client_->start_polling_thread();
    // boost::fibers::fiber polling_fb = client_->start_polling_fiber();


    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client_conf_.num_coroutines);
    uint32_t coro_num_ops = client_->num_local_operations_ / client_conf_.num_coroutines;
    for (int i = 0; i < client_conf_.num_coroutines; i ++) {
        fb_args_list[i].client = client_;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
    }
    fb_args_list[client_conf_.num_coroutines - 1].ops_num += client_->num_local_operations_ % client_conf_.num_coroutines;

    boost::fibers::fiber fb_list[client_conf_.num_coroutines];
    for (int i = 0; i < client_conf_.num_coroutines; i ++) {
        printf("%d\n", i);
        boost::fibers::fiber fb(client_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    for (int i = 0; i < client_conf_.num_coroutines; i ++) {
        fb_list[i].join();
    }
    free(fb_args_list);
    client_->stop_polling_thread();
    pthread_join(polling_tid, NULL);
    // polling_fb.join();

    for (int i = 0; i < client_conf_.num_coroutines; i ++) {
        std::cout << fb_args_list[i].num_failed << std::endl;
    }
}

// TEST_F(ClientTest, test_client_mm) {
//     int ret = 0;
//     void * client_input_buf = client_->get_input_buf();
//     uint32_t client_input_buf_lkey = client_->get_input_buf_lkey();
//     void * client_local_buf = client_->get_local_buf_mr()->addr;
//     uint32_t client_local_buf_lkey = client_->get_local_buf_mr()->lkey;

//     const char * key_template = "test-%d-k";
//     const char * value_template = "test-%d-v-%s";

//     pthread_t polling_tid = client_->start_polling_thread();

//     uint32_t fake_value_size = 8 * 1024 * 1024;
//     std::vector<std::string> key_list;
//     for (int i = 0; i < 100; i ++) {
//         char key[128];
//         char value[128];

//         sprintf(key, key_template, i);
//         sprintf(value, value_template, i, "before");
//         key_list.push_back(std::string(key));

//         uint64_t l_st_addr = (uint64_t)client_input_buf;

//         KVLogHeader * header = (KVLogHeader *)l_st_addr;
//         // header->is_valid = true;
//         header->ctl_bits = KV_LOG_VALID;
//         header->key_length = strlen(key);
//         header->value_length = fake_value_size;
//         header->next_addr = 0;

//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader)), key, strlen(key));
//         memcpy((void *)(l_st_addr + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));

//         KVInfo insert_info;
//         insert_info.l_addr    = client_input_buf;
//         insert_info.key_len   = strlen(key);
//         insert_info.value_len = fake_value_size;
//         insert_info.lkey      = client_input_buf_lkey;

//         KVReqCtx insert_ctx;
//         insert_ctx.req_type  = KV_REQ_INSERT;
//         insert_ctx.use_cache = true;
//         insert_ctx.kv_info   = &insert_info;
//         insert_ctx.local_bucket_addr = (RaceHashBucket *)client_local_buf;
//         insert_ctx.local_cas_target_value_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
//         insert_ctx.local_cas_return_value_addr = (void *)((uint64_t)insert_ctx.local_cas_target_value_addr + sizeof(uint64_t));
//         insert_ctx.lkey = client_local_buf_lkey;
//         insert_ctx.coro_id = 0;

//         int ret = 0;
//         KVLogHeader * d_header = (KVLogHeader *)insert_ctx.kv_info->l_addr;
//         printf("%d %d %ld\n", d_header->key_length, d_header->value_length, sizeof(KVLogHeader));
//         ret = client_->kv_insert(&insert_ctx);
//         ASSERT_TRUE(ret == 0);
//     }

//     // get log header
//     ClientLogMetaInfo pr_log_meta;
//     ret = client_->test_get_pr_log_meta_info(&pr_log_meta);
//     ASSERT_TRUE(ret == 0);

//     uint64_t ptr_addr = pr_log_meta.pr_log_head;
//     uint8_t  ptr_sid  = pr_log_meta.pr_server_id;
//     int cnt = 0;
//     while (ptr_addr != 0) {
//         printf("server: %d addr: %lx\n", ptr_sid, ptr_addr);
//         char buf[1024];
//         ret = client_->test_get_remote_log_header(ptr_sid, ptr_addr, 1024, (void *)buf);

//         // parse return data
//         KVLogHeader * log_header = (KVLogHeader *)buf;
//         uint64_t key_st_addr = (uint64_t)buf + sizeof(KVLogHeader);
//         uint32_t key_len = log_header->key_length;
//         ASSERT_TRUE(key_len == key_list[cnt].size());

//         printf("remote key: ");
//         for (int i = 0; i < key_len; i ++) {
//             printf("%c", ((char *)key_st_addr)[i]);
//         }
//         printf("\n");
//         std::cout << "cached key: " << key_list[cnt] << std::endl;
//         ASSERT_TRUE(memcmp((void *)key_st_addr, (void *)key_list[cnt].c_str(), key_len) == 0);

//         // update next pointer
//         ptr_sid = log_header->next_addr & 0xFF;
//         ptr_addr = log_header->next_addr ^ (uint64_t)ptr_sid;
//         cnt++;
//     }
//     ASSERT_TRUE(cnt - 1 == key_list.size()) << cnt << " " << key_list.size() << std::endl;
//     client_->stop_polling_thread();
//     pthread_join(polling_tid, NULL);
// }