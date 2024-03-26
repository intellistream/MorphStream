#include <gtest/gtest.h>

#include <random>
#include <sys/mman.h>

#include "kv_utils.h"

TEST(test_kv_utils, kv_msg_conn_info) {
    struct KVMsg orig_kvmsg;
    struct KVMsg sent_kvmsg;
    
    std::default_random_engine e;

    orig_kvmsg.id = (uint16_t)e();
    orig_kvmsg.type = REQ_CONNECT;

    for (int i = 0; i < 16; i ++) {
        orig_kvmsg.body.conn_info.qp_info.gid[i] = (uint8_t)e();
    }

    orig_kvmsg.body.conn_info.qp_info.gid_idx = 0;
    orig_kvmsg.body.conn_info.qp_info.lid = (uint16_t)e();
    orig_kvmsg.body.conn_info.qp_info.port_num = (uint8_t)e();
    orig_kvmsg.body.conn_info.qp_info.qp_num = (uint32_t)e();
    orig_kvmsg.body.conn_info.gc_info.addr = (uint64_t)e();
    orig_kvmsg.body.conn_info.gc_info.rkey = (uint32_t)e();

    memcpy(&sent_kvmsg, &orig_kvmsg, sizeof(struct KVMsg));

    serialize_kvmsg(&sent_kvmsg);
    deserialize_kvmsg(&sent_kvmsg);

    int rc = memcmp(&sent_kvmsg, &orig_kvmsg, sizeof(struct KVMsg));
    ASSERT_EQ(rc, 0);
}

TEST(test_kv_utils, kv_msg_alloc_info) {
    struct KVMsg orig_kvmsg;
    struct KVMsg sent_kvmsg;

    std::default_random_engine e;
    orig_kvmsg.id = e();
    orig_kvmsg.type = REQ_ALLOC;
    orig_kvmsg.body.mr_info.addr = (uint64_t)e();
    orig_kvmsg.body.mr_info.rkey = (uint32_t)e();

    memcpy(&sent_kvmsg, &orig_kvmsg, sizeof(struct KVMsg));

    serialize_kvmsg(&sent_kvmsg);
    deserialize_kvmsg(&sent_kvmsg);

    int rc = memcmp(&sent_kvmsg, &orig_kvmsg, sizeof(struct KVMsg));
    ASSERT_EQ(rc, 0);
}

TEST(test_kv_utils, kv_msg_alloc_subtable_info) {
    struct KVMsg orig_kvmsg;
    struct KVMsg sent_kvmsg;

    std::default_random_engine e;
    orig_kvmsg.id = e();
    orig_kvmsg.type = REQ_ALLOC_SUBTABLE;
    orig_kvmsg.body.mr_info.addr = (uint64_t)e();
    orig_kvmsg.body.mr_info.rkey = (uint32_t)e();

    memcpy(&sent_kvmsg, &orig_kvmsg, sizeof(struct KVMsg));

    serialize_kvmsg(&sent_kvmsg);
    deserialize_kvmsg(&sent_kvmsg);

    int rc = memcmp(&sent_kvmsg, &orig_kvmsg, sizeof(struct KVMsg));
    ASSERT_EQ(rc, 0);
}

TEST(test_kv_utils, load_config) {
    const char * config_file_name = "./test_conf.json";
    struct GlobalConfig conf;
    int ret = load_config(config_file_name, &conf);
    ASSERT_TRUE(ret == 0);

    ASSERT_TRUE(conf.role == SERVER);
    ASSERT_TRUE(conf.conn_type == IB);
    ASSERT_TRUE(conf.server_id == 0);
    ASSERT_TRUE(conf.udp_port == 2333);
    ASSERT_TRUE(conf.memory_num == 2);
    ASSERT_EQ(strcmp(conf.memory_ips[0], "10.10.10.1"), 0);
    ASSERT_EQ(strcmp(conf.memory_ips[1], "10.10.10.2"), 0);

    ASSERT_TRUE(conf.ib_dev_id == 0);
    ASSERT_TRUE(conf.ib_port_id == 1);
    ASSERT_TRUE(conf.ib_gid_idx == -1);

    ASSERT_TRUE(conf.server_base_addr == 0x100000);
    ASSERT_TRUE(conf.server_data_len == 2147483648);
    ASSERT_TRUE(conf.block_size == 64 * 1024 * 1024);
    ASSERT_TRUE(conf.subblock_size == 256);
    ASSERT_TRUE(conf.client_local_size == 1024 * 1024 * 1024);

    ASSERT_TRUE(conf.num_replication == 3);
}

TEST(test_kv_utils, encode_gc_slot) {
    size_t buf_sz = 64 * 1024 * 1024;
    void * buf_pr = mmap((void *)0x10000000, buf_sz, PROT_READ | PROT_WRITE, 
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB, -1, 0);
    void * buf_bk = mmap((void *)0x20000000, buf_sz, PROT_READ | PROT_WRITE, 
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB, -1, 0);

    ASSERT_TRUE((uint64_t)buf_pr == 0x10000000);
    ASSERT_TRUE((uint64_t)buf_bk == 0x20000000);

    DecodedClientGCSlot orig;
    uint64_t blockoff = 5 * 256;
    orig.bk_addr = (uint64_t)buf_bk + blockoff;
    orig.pr_addr = (uint64_t)buf_pr + blockoff;
    orig.num_subblocks = 5;

    EncodedClientGCSlot e;
    encode_gc_slot(&orig, &e.meta_gc_addr);

    DecodedClientGCSlot d;
    decode_gc_slot(e.meta_gc_addr, &d);
    ASSERT_TRUE(d.pr_addr == orig.pr_addr);
    ASSERT_TRUE(d.bk_addr == orig.bk_addr);
    ASSERT_TRUE(d.num_subblocks == orig.num_subblocks);
}