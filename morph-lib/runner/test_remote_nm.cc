#include "test_remote_nm.h"

#include <assert.h>

#include "nm.h"
#include "kv_utils.h"

void NMRemoteTest::SetUp() {
    int ret = 0;
    ret = load_config("./client_config.json", &global_conf_);
    ASSERT_TRUE(ret == 0);

    client_nm_ = new UDPNetworkManager(&global_conf_);

    for (int i = 0; i < 2; i ++) {
        ret = client_nm_->client_connect_one_rc_qp(i, &mr_info_[i]);
        ASSERT_TRUE(ret == 0);
    }
}

void NMRemoteTest::TearDown() {
    delete client_nm_;
}

SrReqCtx * NMRemoteTest::gen_sr_reqs() {
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
            ret_ctx->sr_list_1[i].wr.rdma.remote_addr = mr_info_[0].addr + i * sizeof(uint64_t);
            ret_ctx->sr_list_1[i].wr.rdma.rkey        = mr_info_[0].rkey;
            ret_ctx->sr_list_1[i].next = NULL;
        } else {
            ret_ctx->sr_list_2[i - 2].wr_id = i;
            ret_ctx->sr_list_2[i - 2].sg_list = &ret_ctx->sg_list_2[i - 2];
            ret_ctx->sr_list_2[i - 2].num_sge = 1;
            ret_ctx->sr_list_2[i - 2].opcode  = IBV_WR_RDMA_WRITE;
            ret_ctx->sr_list_2[i - 2].send_flags = IBV_SEND_INLINE;
            ret_ctx->sr_list_2[i - 2].wr.rdma.remote_addr = mr_info_[1].addr + i * sizeof(uint64_t);
            ret_ctx->sr_list_2[i - 2].wr.rdma.rkey        = mr_info_[1].rkey;
            ret_ctx->sr_list_2[i - 2].next = NULL;
        }
    }
    ret_ctx->sr_list_1[0].next = &ret_ctx->sr_list_1[1];
    ret_ctx->sr_list_2[0].next = &ret_ctx->sr_list_2[1];

    ret_ctx->m_srl[0].num_sr = 2;
    ret_ctx->m_srl[0].server_id = 0;
    ret_ctx->m_srl[0].sr_list = ret_ctx->sr_list_1;
    ret_ctx->m_srl[1].num_sr = 2;
    ret_ctx->m_srl[1].server_id = 1;
    ret_ctx->m_srl[1].sr_list = ret_ctx->sr_list_2;

    ret_ctx->srl1.num_sr = 2;
    ret_ctx->srl1.server_id = 0;
    ret_ctx->srl1.sr_list = ret_ctx->sr_list_1;
    ret_ctx->srl2.num_sr = 2;
    ret_ctx->srl2.server_id = 1;
    ret_ctx->srl2.sr_list = ret_ctx->sr_list_2;

    return ret_ctx;
}

TEST_F(NMRemoteTest, remote_basic) {
    int ret = 0;
    uint64_t test_data = 1231241;
    ret = client_nm_->nm_rdma_write_inl_to_sid(&test_data, sizeof(uint64_t), 
        mr_info_[0].addr, mr_info_[0].rkey, 0);
    ASSERT_TRUE(ret == 0);
    
    uint64_t read_data = 0;
    struct IbInfo client_ib_info;
    client_nm_->get_ib_info(&client_ib_info);
    struct ibv_mr * tmp_mr = ibv_reg_mr(client_ib_info.ib_pd, &read_data, 
        sizeof(uint64_t), IBV_ACCESS_LOCAL_WRITE);
    ASSERT_TRUE(tmp_mr != NULL);
    
    ret = client_nm_->nm_rdma_read_from_sid(&read_data, tmp_mr->lkey, 
        sizeof(uint64_t), mr_info_[0].addr, mr_info_[0].rkey, 0);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(test_data == read_data);
}

TEST_F(NMRemoteTest, rdma_post_sr_lists_async) {
    int ret = 0;
    SrReqCtx * sr_ctx = gen_sr_reqs();
    ASSERT_TRUE(sr_ctx != NULL);

    pthread_t polling_tid;
    pthread_create(&polling_tid, NULL, nm_polling_thread, client_nm_);

    std::map<uint64_t, struct ibv_wc *> l_wait_wc_map;
    ret = client_nm_->rdma_post_sr_lists_async(sr_ctx->m_srl, 2, l_wait_wc_map);
    ASSERT_TRUE(ret == 0);

    std::map<uint64_t, struct ibv_wc *>::iterator it = l_wait_wc_map.begin();
    ASSERT_TRUE(l_wait_wc_map.size() == 2);

    while (1) {
        ret = client_nm_->nm_check_completion(l_wait_wc_map);
        ASSERT_TRUE(ret == 0);
        if (ib_is_all_wrid_finished(l_wait_wc_map)) {
            break;
        }
    }

    client_nm_->stop_polling();
    pthread_join(polling_tid, NULL);
}

TEST_F(NMRemoteTest, rdma_post_sr_list_batch_async) {
    int ret = 0;
    SrReqCtx * sr_ctx = gen_sr_reqs();
    ASSERT_TRUE(sr_ctx != NULL);

    pthread_t polling_tid;
    pthread_create(&polling_tid, NULL, nm_polling_thread, client_nm_);

    std::vector<IbvSrList *> test_batch;
    std::vector<uint32_t> test_num_batch;
    test_batch.push_back(&sr_ctx->srl1);
    test_batch.push_back(&sr_ctx->srl2);
    test_num_batch.push_back(1);
    test_num_batch.push_back(1);

    std::map<uint64_t, struct ibv_wc *> l_wait_wc_map;
    ret = client_nm_->rdma_post_sr_list_batch_async(test_batch, test_num_batch, l_wait_wc_map);
    ASSERT_TRUE(ret == 0);

    std::map<uint64_t, struct ibv_wc *>::iterator it = l_wait_wc_map.begin();
    ASSERT_TRUE(l_wait_wc_map.size() == 2);

    while (1) {
        ret = client_nm_->nm_check_completion(l_wait_wc_map);
        ASSERT_TRUE(ret == 0);
        if (ib_is_all_wrid_finished(l_wait_wc_map)) {
            break;
        }
    }

    client_nm_->stop_polling();
    pthread_join(polling_tid, NULL);
}