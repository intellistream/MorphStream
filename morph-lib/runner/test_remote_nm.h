#ifndef DDCKV_TEST_REMOTE_NM_H_
#define DDCKV_TEST_REMOTE_NM_H_

#include <gtest/gtest.h>

#include "ddckv_test.h"
#include "kv_utils.h"
#include "nm.h"

typedef struct TagSrReqCtx {
    struct ibv_send_wr sr_list_1[2];
    struct ibv_send_wr sr_list_2[2];
    struct ibv_sge     sg_list_1[2];
    struct ibv_sge     sg_list_2[2];

    IbvSrList srl1;
    IbvSrList srl2;
    IbvSrList m_srl[2];
} SrReqCtx;

class NMRemoteTest : public DDCKVTest {
protected:
    void SetUp() override;
    void TearDown() override;

public:
    struct GlobalConfig global_conf_;
    struct MrInfo mr_info_[2];
    UDPNetworkManager * client_nm_;

    uint64_t test_source_data_[4];

    SrReqCtx * gen_sr_reqs();
};

void * udp_send_recv_server(void * args);
void * udp_send_recv_client(void * args);

void * ib_connect_server(void * args);

#endif