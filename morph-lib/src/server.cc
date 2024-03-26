#include "server.h"

#include <assert.h>

#include "kv_utils.h"
#include "kv_debug.h"

void * server_main(void * server_main_args) {
    ServerMainArgs * args = (ServerMainArgs *)server_main_args;
    Server * server_instance = args->server;
    
    // stick to a core
    int ret = stick_this_thread_to_core(args->core_id);
    // assert(ret == 0);
    // print_log(DEBUG, "server is running on core: %d", args->core_id);

    // start working
    return server_instance->thread_main();   
}

Server::Server(const struct GlobalConfig * conf) {
    server_id_ = conf->server_id;
    need_stop_ = 0;

    nm_ = new UDPNetworkManager(conf);

    struct IbInfo ib_info;
    nm_->get_ib_info(&ib_info);
    mm_ = new ServerMM(conf->server_base_addr, conf->server_data_len,
        conf->block_size, &ib_info, conf);
}

Server::~Server() {
    delete mm_;
    delete nm_;
}

int Server::server_on_connect(const struct KVMsg * request, 
    struct sockaddr_in * src_addr, 
    socklen_t src_addr_len) {
    int rc = 0;
    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));

    reply.id   = server_id_;
    reply.type = REP_CONNECT;
    rc = nm_->nm_on_connect_new_qp(request, &reply.body.conn_info.qp_info);
    // assert(rc == 0);

    rc = mm_->get_mr_info(&reply.body.conn_info.gc_info);
    // assert(rc == 0);

    serialize_kvmsg(&reply);

    rc = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    // assert(rc == 0);
    
    deserialize_kvmsg(&reply);
    rc = nm_->nm_on_connect_connect_qp(request->id, &reply.body.conn_info.qp_info, &request->body.conn_info.qp_info);
    // assert(rc == 0);
    return 0;
}

int Server::server_on_alloc(const struct KVMsg * request, struct sockaddr_in * src_addr, 
        socklen_t src_addr_len) {
    uint64_t alloc_addr = mm_->mm_alloc();
    // assert(mmblock != NULL);
    // print_log(DEBUG, "allocated addr: %lx", mmblock->addr);
    // assert((mmblock->addr & 0x3FFFFFF) == 0);

    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));
    reply.type = REP_ALLOC;
    reply.id   = nm_->get_server_id();
    reply.body.mr_info.rkey = mm_->get_rkey();
    if (alloc_addr != 0) {
        reply.body.mr_info.addr = alloc_addr;
    } else {
        printf("server no space\n");
        reply.body.mr_info.addr = 0;
    }
    serialize_kvmsg(&reply);

    int ret = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    // assert(ret == 0);

    return 0;
}

int Server::server_on_alloc_subtable(const struct KVMsg * request, struct sockaddr_in * src_addr,
        socklen_t src_addr_len) {
    uint64_t subtable_addr = mm_->mm_alloc_subtable();
    // assert(subtable_addr != 0);
    // print_log(DEBUG, "alloc subtable: %lx", subtable_addr);
    // assert((subtable_addr & 0xFF)  == 0);

    struct KVMsg reply;
    memset(&reply, 0, sizeof(struct KVMsg));
    reply.type = REP_ALLOC_SUBTABLE;
    reply.id   = nm_->get_server_id();
    reply.body.mr_info.addr = subtable_addr;
    reply.body.mr_info.rkey = mm_->get_rkey();
    serialize_kvmsg(&reply);
    int ret = nm_->nm_send_udp_msg(&reply, src_addr, src_addr_len);
    // assert(ret == 0);
    return 0;
}

void * Server::thread_main() {
    struct sockaddr_in client_addr;
    socklen_t          client_addr_len = sizeof(struct sockaddr_in);
    struct KVMsg request;
    int rc = 0;
    while (!need_stop_) {
        rc = nm_->nm_recv_udp_msg(&request, &client_addr, &client_addr_len);
        if (rc && need_stop_) {
            break;
        } else if (rc) {
            continue;
        }
        // assert(rc == 0);
        deserialize_kvmsg(&request);

        if (request.type == REQ_CONNECT) {
            rc = server_on_connect(&request, &client_addr, client_addr_len);
            // assert(rc == 0);
        } else if (request.type == REQ_ALLOC_SUBTABLE) {
            rc = server_on_alloc_subtable(&request, &client_addr, client_addr_len);
            // assert(rc == 0);
        } else {
            // assert(request.type == REQ_ALLOC);
            rc = server_on_alloc(&request, &client_addr, client_addr_len);
            // assert(rc == 0);
        }
    }
    return NULL;
}

void Server::stop() {
    need_stop_ = 1;
}

uint64_t Server::get_kv_area_addr() {
    return mm_->get_kv_area_addr();
}

uint64_t Server::get_subtable_st_addr() {
    return mm_->get_subtable_st_addr();
}