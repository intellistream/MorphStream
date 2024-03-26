#include "test_server.h"

void ServerTest::SetUp() {
    setup_server_conf();
    setup_client_conf();
    server_ = new Server(&server_conf_);
    client_nm_ = new UDPNetworkManager(&client_conf_);
}

void ServerTest::TearDown() {
    delete server_;
}

TEST_F(ServerTest, ib_connect) {
    pthread_t server_tid;
    int ret;
    ret = pthread_create(&server_tid, NULL, server_main, server_);
    ASSERT_TRUE(ret == 0);

    struct MrInfo mr_info;
    ret = client_nm_->client_connect_one_rc_qp(0, &mr_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mr_info.addr == server_conf_.server_base_addr);

    uint64_t client_msg[2];
    client_msg[0] = 10086;
    client_msg[1] = 9527;
    ret = client_nm_->nm_rdma_write_inl_to_sid(client_msg, sizeof(uint64_t) * 2, 
        mr_info.addr, mr_info.rkey, 0);
    ASSERT_TRUE(ret == 0);

    ASSERT_TRUE(((uint64_t *)(mr_info.addr))[0] == client_msg[0]);
    ASSERT_TRUE(((uint64_t *)(mr_info.addr))[1] == client_msg[1]);

    server_->stop();
    pthread_join(server_tid, NULL);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(1);
}

TEST_F(ServerTest, rdma_connect) {
    pthread_t server_tid;
    int ret;
    ret = pthread_create(&server_tid, NULL, server_main, server_);
    ASSERT_TRUE(ret == 0);

    struct MrInfo gc_info;
    ret = client_nm_->client_connect_one_rc_qp(0, &gc_info);
    ASSERT_TRUE(ret == 0);

    uint64_t msg = 10086;
    struct ibv_send_wr test_wr;
    struct ibv_sge     test_sge;
    memset(&test_wr, 0, sizeof(struct ibv_send_wr));
    memset(&test_sge, 0, sizeof(struct ibv_sge));
    test_sge.addr = (uint64_t)&msg;
    test_sge.length = sizeof(uint64_t);
    test_sge.lkey = 0;
    test_wr.sg_list = &test_sge;
    test_wr.num_sge = 1;
    test_wr.next = NULL;
    test_wr.opcode = IBV_WR_RDMA_WRITE;
    test_wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
    test_wr.wr.rdma.remote_addr = gc_info.addr;
    test_wr.wr.rdma.rkey = gc_info.rkey;
    test_wr.wr_id = 10000;
    ret = client_nm_->rdma_post_send_batch_async(0, &test_wr);
    ASSERT_TRUE(ret == 0);
    
    struct ibv_wc wc;
    ret = client_nm_->rdma_poll_one_completion(&wc);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(wc.status == IBV_WC_SUCCESS);
    ASSERT_TRUE(wc.wr_id == 10000);
    
    msg = *(uint64_t *)(gc_info.addr);
    ASSERT_TRUE(msg == 10086);

    server_->stop();
    pthread_join(server_tid, NULL);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(1);
}

TEST_F(ServerTest, alloc) {
    pthread_t server_tid;
    int ret;
    ret = pthread_create(&server_tid, NULL, server_main, server_);
    ASSERT_TRUE(ret == 0);

    struct MrInfo addr_info;
    ret = client_nm_->client_connect_one_rc_qp(0, &addr_info);
    ASSERT_TRUE(ret == 0);

    for (int i = 0; i < 10; i ++) {
        struct KVMsg alloc_req;
        memset(&alloc_req, 0, sizeof(struct KVMsg));
        alloc_req.type = REQ_ALLOC;
        alloc_req.id   = client_nm_->get_server_id();
        serialize_kvmsg(&alloc_req);
        ret = client_nm_->nm_send_udp_msg_to_server(&alloc_req, 0);
        ASSERT_TRUE(ret == 0);

        struct KVMsg alloc_rep;
        ret = client_nm_->nm_recv_udp_msg(&alloc_rep, NULL, NULL);
        ASSERT_TRUE(ret == 0);
        deserialize_kvmsg(&alloc_rep);

        ASSERT_TRUE(alloc_rep.body.mr_info.addr == server_->get_kv_area_addr() + i * server_conf_.block_size) << "ret_addr: 0x" << std::hex << alloc_rep.body.mr_info.addr
            << " kv_area_off: 0x" << std::hex << server_->get_kv_area_addr() + i * server_conf_.block_size;
    }
    
    server_->stop();
    pthread_join(server_tid, NULL);
}

TEST_F(ServerTest, alloc_subtable) {
    pthread_t server_tid;
    int ret;
    ret = pthread_create(&server_tid, NULL, server_main, server_);
    ASSERT_TRUE(ret == 0);

    struct MrInfo addr_info;
    ret = client_nm_->client_connect_one_rc_qp(0, &addr_info);
    ASSERT_TRUE(ret == 0);

    for (int i = 0; i < 32; i ++) {
        struct KVMsg alloc_req;
        memset(&alloc_req, 0, sizeof(struct KVMsg));
        alloc_req.type = REQ_ALLOC_SUBTABLE;
        alloc_req.id   = client_nm_->get_server_id();
        serialize_kvmsg(&alloc_req);
        ret = client_nm_->nm_send_udp_msg_to_server(&alloc_req, 0);
        ASSERT_TRUE(ret == 0);

        struct KVMsg alloc_rep;
        ret = client_nm_->nm_recv_udp_msg(&alloc_rep, NULL, NULL);
        ASSERT_TRUE(ret == 0);
        deserialize_kvmsg(&alloc_rep);

        ASSERT_TRUE(alloc_rep.body.mr_info.addr == server_->get_subtable_st_addr() + i * roundup_256(SUBTABLE_LEN)) << "ret_addr: 0x" << std::hex << alloc_rep.body.mr_info.addr
            << " expected: 0x" << std::hex << server_->get_subtable_st_addr() + i * roundup_256(SUBTABLE_LEN);
    }
    server_->stop();
    pthread_join(server_tid, NULL);
}