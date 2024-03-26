#include "test_nm.h"

#include <pthread.h>
#include <infiniband/verbs.h>

#include <cstring>
#include <map>

#include "nm.h"
#include "server.h"
#include "kv_utils.h"

void NMTest::SetUp() {
    setup_server_conf();
    setup_client_conf();
    server_nm_ = new UDPNetworkManager(&server_conf_);
    client_nm_ = new UDPNetworkManager(&client_conf_);
}

void NMTest::TearDown() {
    delete server_nm_;
    delete client_nm_;
}

int NMTest::ib_connect(struct MrInfo * mr_info) {
    pthread_t server_tid;
    int ret;
    pthread_create(&server_tid, NULL, ib_connect_server, server_nm_);

    ret = client_nm_->client_connect_one_rc_qp(0, mr_info);
    assert(ret == 0);
    
    pthread_join(server_tid, NULL);
    return 0;
}

SrReqCtx * NMTest::gen_sr_reqs(struct MrInfo * mr_info) {
    SrReqCtx * ret_ctx = (SrReqCtx *)malloc(sizeof(SrReqCtx));
    for (int i = 0; i < 4; i ++) {
        test_source_data_[i] = (123 * i) ^ i;
    }
    
    for (int i = 0; i < 4; i ++) {
        if (i < 2) {
            ret_ctx->sg_list_1[i].addr   = (uint64_t)&test_source_data_[i];
            ret_ctx->sg_list_1[i].length = 8;
            ret_ctx->sg_list_1[i].lkey   = 0;
        } else {
            ret_ctx->sg_list_2[i - 2].addr   = (uint64_t)&test_source_data_[i];
            ret_ctx->sg_list_2[i - 2].length = 8;
            ret_ctx->sg_list_2[i - 2].lkey   = 0;
        }
    }

    for (int i = 0; i < 4; i ++) {
        if (i < 2) {
            ret_ctx->sr_list_1[i].wr_id = i;
            ret_ctx->sr_list_1[i].sg_list = &ret_ctx->sg_list_1[i];
            ret_ctx->sr_list_1[i].num_sge = 1;
            ret_ctx->sr_list_1[i].opcode  = IBV_WR_RDMA_WRITE;
            ret_ctx->sr_list_1[i].send_flags = IBV_SEND_INLINE;
            ret_ctx->sr_list_1[i].wr.rdma.remote_addr = mr_info->addr + i * sizeof(uint64_t);
            ret_ctx->sr_list_1[i].wr.rdma.rkey        = mr_info->rkey;
            ret_ctx->sr_list_1[i].next = NULL;
        } else {
            ret_ctx->sr_list_2[i - 2].wr_id = i;
            ret_ctx->sr_list_2[i - 2].sg_list = &ret_ctx->sg_list_2[i - 2];
            ret_ctx->sr_list_2[i - 2].num_sge = 1;
            ret_ctx->sr_list_2[i - 2].opcode  = IBV_WR_RDMA_WRITE;
            ret_ctx->sr_list_2[i - 2].send_flags = IBV_SEND_INLINE;
            ret_ctx->sr_list_2[i - 2].wr.rdma.remote_addr = mr_info->addr + i * sizeof(uint64_t);
            ret_ctx->sr_list_2[i - 2].wr.rdma.rkey        = mr_info->rkey;
            ret_ctx->sr_list_2[i - 2].next = NULL;
        }
    }
    ret_ctx->sr_list_1[0].next = &ret_ctx->sr_list_1[1];
    ret_ctx->sr_list_2[0].next = &ret_ctx->sr_list_2[1];

    ret_ctx->m_srl[0].num_sr = 2;
    ret_ctx->m_srl[0].server_id = 0;
    ret_ctx->m_srl[0].sr_list = ret_ctx->sr_list_1;
    ret_ctx->m_srl[1].num_sr = 2;
    ret_ctx->m_srl[1].server_id = 0;
    ret_ctx->m_srl[1].sr_list = ret_ctx->sr_list_2;

    ret_ctx->srl1.num_sr = 2;
    ret_ctx->srl1.server_id = 0;
    ret_ctx->srl1.sr_list = ret_ctx->sr_list_1;
    ret_ctx->srl2.num_sr = 2;
    ret_ctx->srl2.server_id = 0;
    ret_ctx->srl2.sr_list = ret_ctx->sr_list_2;

    return ret_ctx;
}

void * udp_send_recv_server(void * args) {
    UDPNetworkManager * nm = (UDPNetworkManager *)args;
    KVMsg request;
    struct sockaddr_in src_addr;
    socklen_t  src_addr_len = sizeof(struct sockaddr_in);
    int ret = nm->nm_recv_udp_msg(&request, &src_addr, &src_addr_len);
    assert(ret == 0);
    deserialize_kvmsg(&request);
    assert(request.type == REQ_ALLOC);
    assert(request.id == 1);
    KVMsg reply;
    reply.type = REP_ALLOC;
    reply.id   = nm->get_server_id();
    serialize_kvmsg(&reply);
    ret = nm->nm_send_udp_msg(&reply, &src_addr, src_addr_len);
    assert(ret == 0);
    return NULL;
}

void * udp_send_recv_client(void * args) {
    UDPNetworkManager * nm = (UDPNetworkManager *)args;
    struct KVMsg request;
    request.type = REQ_ALLOC;
    request.id   = nm->get_server_id();
    serialize_kvmsg(&request);
    int ret = nm->nm_send_udp_msg_to_server(&request, 0);
    assert(ret == 0);
    struct KVMsg reply;
    ret = nm->nm_recv_udp_msg(&reply, NULL, NULL);
    assert(ret == 0);
    deserialize_kvmsg(&reply);
    assert(reply.id == 0);
    assert(reply.type == REP_ALLOC);
    return NULL;
}

void * ib_connect_server(void * args) {
    UDPNetworkManager * nm = (UDPNetworkManager *)args;
    struct KVMsg request;
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(struct sockaddr_in);
    int rc = nm->nm_recv_udp_msg(&request, &client_addr, &client_addr_len);
    assert(rc == 0);
    deserialize_kvmsg(&request);
    
    assert(request.type == REQ_CONNECT);
    assert(request.id == 1);
    struct KVMsg reply;
    reply.id = nm->get_server_id();
    reply.type = REP_CONNECT;
    rc = nm->nm_on_connect_new_qp(&request, &reply.body.conn_info.qp_info);
    assert(rc == 0);

    struct IbInfo ib_info;
    nm->get_ib_info(&ib_info);
    void * buf = malloc(1024);
    struct ibv_mr * mr = ibv_reg_mr(ib_info.ib_pd, buf, 128,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    reply.body.conn_info.gc_info.addr = (uint64_t)buf;
    reply.body.conn_info.gc_info.rkey = mr->rkey;
    serialize_kvmsg(&reply);

    rc = nm->nm_send_udp_msg(&reply, &client_addr, client_addr_len);
    assert(rc == 0);
    deserialize_kvmsg(&reply);
    rc = nm->nm_on_connect_connect_qp(request.id, &reply.body.conn_info.qp_info,
        &request.body.conn_info.qp_info);
    assert(rc == 0);
    return NULL;
}

TEST_F(NMTest, udp_send_recv) {
    pthread_t server_tid, client_tid;
    pthread_create(&server_tid, NULL, 
        udp_send_recv_server, (void *)server_nm_);
    pthread_create(&client_tid, NULL, 
        udp_send_recv_client, (void *)client_nm_);
    pthread_join(server_tid, NULL);
    pthread_join(client_tid, NULL);
}

TEST_F(NMTest, ib_connect) {
    // create server process
    pthread_t server_tid;
    int ret;
    pthread_create(&server_tid, NULL, ib_connect_server, (void *)server_nm_);

    ret = client_nm_->client_connect_all_rc_qp();
    ASSERT_TRUE(ret == 0);

    pthread_join(server_tid, NULL);

    server_nm_->close_udp_sock();
    client_nm_->close_udp_sock();
}

TEST_F(NMTest, nm_utils) {
    uint32_t server_id = server_nm_->get_server_id();
    uint32_t client_id = client_nm_->get_server_id();
    ASSERT_TRUE(server_id == server_conf_.server_id);
    ASSERT_TRUE(client_id == client_conf_.server_id);
}

TEST_F(NMTest, ib_write_read) {
    struct MrInfo mr_info;
    int ret = ib_connect(&mr_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mr_info.addr != 0);
    memset((void *)mr_info.addr, 0, sizeof(uint64_t) * 4);

    uint64_t test_data = 100;
    ret = client_nm_->nm_rdma_write_inl_to_sid(&test_data, sizeof(uint64_t), 
        mr_info.addr, mr_info.rkey, 0);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(test_data == *(uint64_t *)mr_info.addr);

    test_data = 10101;
    struct IbInfo client_ib_info;
    client_nm_->get_ib_info(&client_ib_info);
    struct ibv_mr * tmp_mr = ibv_reg_mr(client_ib_info.ib_pd, 
        &test_data, sizeof(uint64_t), IBV_ACCESS_LOCAL_WRITE);
    ASSERT_TRUE(tmp_mr != NULL);
    ret = client_nm_->nm_rdma_write_to_sid(&test_data, tmp_mr->lkey, sizeof(uint64_t), mr_info.addr, mr_info.rkey, 0);


    uint64_t read_data = 0;
    tmp_mr = ibv_reg_mr(client_ib_info.ib_pd, &read_data, sizeof(uint64_t), IBV_ACCESS_LOCAL_WRITE);
    ASSERT_TRUE(tmp_mr != NULL);
    ASSERT_EQ((uint64_t)&read_data, (uint64_t)tmp_mr->addr);

    ret = client_nm_->nm_rdma_read_from_sid(&read_data, 
        tmp_mr->lkey, sizeof(uint64_t), mr_info.addr, mr_info.rkey, 0);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(test_data == read_data);
}

TEST_F(NMTest, rdma_post_sr_lists_sync_0) {
    struct MrInfo mr_info;
    int ret = ib_connect(&mr_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mr_info.addr != 0);
    memset((void *)mr_info.addr, 0, sizeof(uint64_t) * 4);

    SrReqCtx * sr_ctx = gen_sr_reqs(&mr_info);
    ASSERT_TRUE(sr_ctx != NULL);

    ret = client_nm_->rdma_post_sr_lists_sync(&sr_ctx->srl1, 1, NULL);
    ASSERT_TRUE(ret == 0);
    ret = client_nm_->rdma_post_sr_lists_sync(&sr_ctx->srl2, 1, NULL);
    ASSERT_TRUE(ret == 0);

    uint64_t * tar_addr = (uint64_t *)mr_info.addr;
    for (int i = 0; i < 4; i ++) {
        ASSERT_TRUE(tar_addr[i] == test_source_data_[i]) << "tar: " << tar_addr[i] << "  src: " 
            << test_source_data_[i] << std::endl;
    }
}

TEST_F(NMTest, rdma_post_sr_lists_sync_1) {
    struct MrInfo mr_info;
    int ret = ib_connect(&mr_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mr_info.addr != 0);
    memset((void *)mr_info.addr, 0, sizeof(uint64_t) * 4);

    SrReqCtx * sr_ctx = gen_sr_reqs(&mr_info);
    ASSERT_TRUE(sr_ctx != NULL);

    ret = client_nm_->rdma_post_sr_lists_sync(sr_ctx->m_srl, 2, NULL);
    ASSERT_TRUE(ret == 0);
    
    uint64_t * tar_addr = (uint64_t *)mr_info.addr;
    for (int i = 0; i < 4; i ++) {
        ASSERT_TRUE(tar_addr[i] == test_source_data_[i]) << "tar: " << tar_addr[i] << "  src: " 
            << test_source_data_[i] << std::endl;
    }
}

TEST_F(NMTest, poll_local) {
    struct MrInfo mr_info;
    int ret = ib_connect(&mr_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mr_info.addr != 0);
    memset((void *)mr_info.addr, 0, sizeof(uint64_t) * 4);
    
    pthread_t polling_tid;
    pthread_create(&polling_tid, NULL, nm_polling_thread, client_nm_);

    SrReqCtx * sr_ctx = gen_sr_reqs(&mr_info);
    ASSERT_TRUE(sr_ctx != NULL);

    sr_ctx->sr_list_1[1].send_flags |= IBV_SEND_SIGNALED;
    sr_ctx->sr_list_2[1].send_flags |= IBV_SEND_SIGNALED;

    ret = client_nm_->rdma_post_send_batch_async(0, sr_ctx->sr_list_1);
    ASSERT_TRUE(ret == 0);
    ret = client_nm_->rdma_post_send_batch_async(0, sr_ctx->sr_list_2);
    ASSERT_TRUE(ret == 0);

    std::map<uint64_t, struct ibv_wc *> l_wait_wc_map;
    l_wait_wc_map[1] = NULL;
    l_wait_wc_map[3] = NULL;
    while (1) {
        ret = client_nm_->nm_check_completion(l_wait_wc_map);
        ASSERT_TRUE(ret == 0);
        if (ib_is_all_wrid_finished(l_wait_wc_map)) {
            break;
        }
    }
    
    uint64_t * tar_addr = (uint64_t *)mr_info.addr;
    for (int i = 0; i < 4; i ++) {
        ASSERT_TRUE(tar_addr[i] == test_source_data_[i]) << "tar: " << tar_addr[i] << "  src: " 
            << test_source_data_[i] << std::endl;
    }

    client_nm_->stop_polling();
    pthread_join(polling_tid, NULL);
}

TEST_F(NMTest, rdma_post_sr_list_batch_sync_0) {
    struct MrInfo mr_info;
    int ret = ib_connect(&mr_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mr_info.addr != 0);
    memset((void *)mr_info.addr, 0, sizeof(uint64_t) * 4);

    SrReqCtx * sr_ctx = gen_sr_reqs(&mr_info);
    ASSERT_TRUE(sr_ctx != NULL);

    std::vector<IbvSrList *> test_batch;
    std::vector<uint32_t> test_num_batch;
    test_batch.push_back(&sr_ctx->srl1);
    test_batch.push_back(&sr_ctx->srl2);
    test_num_batch.push_back(1);
    test_num_batch.push_back(1);

    ret = client_nm_->rdma_post_sr_list_batch_sync(test_batch, test_num_batch, NULL);
    ASSERT_TRUE(ret == 0);

    uint64_t * tar_addr = (uint64_t *)mr_info.addr;
    for (int i = 0; i < 4; i ++) {
        ASSERT_TRUE(tar_addr[i] == test_source_data_[i]) << "tar: " << tar_addr[i] << "  src: " 
            << test_source_data_[i] << std::endl;
    }
}

TEST_F(NMTest, rdma_post_sr_list_batch_sync_1) {
    struct MrInfo mr_info;
    int ret = ib_connect(&mr_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mr_info.addr != 0);
    memset((void *)mr_info.addr, 0, sizeof(uint64_t) * 4);

    SrReqCtx * sr_ctx = gen_sr_reqs(&mr_info);
    ASSERT_TRUE(sr_ctx != NULL);

    std::vector<IbvSrList *> test_batch;
    std::vector<uint32_t> test_num_batch;
    test_batch.push_back(sr_ctx->m_srl);
    test_num_batch.push_back(2);

    ret = client_nm_->rdma_post_sr_list_batch_sync(test_batch, test_num_batch, NULL);
    ASSERT_TRUE(ret == 0);

    uint64_t * tar_addr = (uint64_t *)mr_info.addr;
    for (int i = 0; i < 4; i ++) {
        ASSERT_TRUE(tar_addr[i] == test_source_data_[i]) << "tar: " << tar_addr[i] << "  src: " 
            << test_source_data_[i] << std::endl;
    }
}