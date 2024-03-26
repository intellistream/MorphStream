#ifndef DDCKV_TEST_CLIENT_H_
#define DDCKV_TEST_CLIENT_H_

#include <gtest/gtest.h>

#include "client.h"
#include "kv_utils.h"

class ClientTest : public ::testing::Test {
protected:
    void SetUp() override;
    void TearDown() override;

public:
    struct GlobalConfig client_conf_;
    Client * client_;
};

#endif