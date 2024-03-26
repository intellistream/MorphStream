#ifndef DDCKV_TEST_H_
#define DDCKV_TEST_H_

#include <gtest/gtest.h>

#include "kv_utils.h"

#define GB (1024ll * 1024 * 1024)
#define MB (1024ll * 1024)
#define KB (1024ll)

class DDCKVTest : public ::testing::Test {
protected:
    void setup_server_conf();
    void setup_client_conf();
    
public:
    struct GlobalConfig server_conf_;
    struct GlobalConfig client_conf_;
};

#endif