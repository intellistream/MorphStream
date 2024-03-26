#include "ddckv_test.h"

void DDCKVTest::setup_server_conf() {
    strcpy(server_conf_.memory_ips[0], "127.0.0.1");
    server_conf_.role       = SERVER;
    server_conf_.conn_type  = IB;
    server_conf_.server_id  = 0;
    server_conf_.udp_port   = 2333;
    server_conf_.memory_num = 1;
    server_conf_.ib_dev_id  = 0;
    server_conf_.ib_port_id = 1;
    server_conf_.ib_gid_idx = -1;
    server_conf_.server_base_addr = 0x10000000;
    server_conf_.server_data_len  = 2ll * GB;
    server_conf_.block_size = 64ll * MB;
}

void DDCKVTest::setup_client_conf() {
    strcpy(client_conf_.memory_ips[0], "127.0.0.1");
    client_conf_.role       = CLIENT;
    client_conf_.conn_type  = IB;
    client_conf_.server_id  = 1;
    client_conf_.udp_port   = 2333;
    client_conf_.memory_num = 1;
    client_conf_.ib_dev_id  = 0;
    client_conf_.ib_port_id = 1;
    client_conf_.ib_gid_idx = -1;
    client_conf_.num_replication = 2;
}