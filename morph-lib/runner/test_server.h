#ifndef DDCKV_TEST_SERVER_H_
#define DDCKV_TEST_SERVER_H_

#include <gtest/gtest.h>

#include "ddckv_test.h"
#include "server.h"
#include "nm.h"

class ServerTest : public DDCKVTest {
protected:
    void SetUp() override;
    void TearDown() override;

public:
    Server * server_;
    UDPNetworkManager * client_nm_;
};

#endif