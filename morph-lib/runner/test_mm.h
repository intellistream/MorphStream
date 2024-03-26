#ifndef DDCKV_TEST_MM_H_
#define DDCKV_TEST_MM_H_

#include <gtest/gtest.h>

#include <pthread.h>

#include "ddckv_test.h"
#include "kv_utils.h"
#include "client.h"
#include "server.h"
#include "nm.h"

class MMTest : public DDCKVTest {
protected:
    void SetUp() override;
    void TearDown() override;

public:
    Server            * server_;
    Client            * client_;
    ClientMM          * client_mm_;
    UDPNetworkManager * client_nm_;

    pthread_t server_tid_;
    pthread_t polling_tid_;

    int ib_connect(struct MrInfo * mr_info);
};

#endif