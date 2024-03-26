#ifndef DDCKV_SERVER_H_
#define DDCKV_SERVER_H_

#include "nm.h"
#include "server_mm.h"
#include "kv_utils.h"

class Server {
    uint32_t server_id_;
    volatile uint8_t need_stop_;
    UDPNetworkManager * nm_;
    ServerMM          * mm_;
    
public:
    Server(const struct GlobalConfig * conf);
    ~Server();

    int server_on_connect(const struct KVMsg * request, 
        struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc(const struct KVMsg * request, 
        struct sockaddr_in * src_addr, socklen_t src_addr_len);
    int server_on_alloc_subtable(const struct KVMsg * request,
        struct sockaddr_in * src_addr, socklen_t src_addr_len);

    void * thread_main();

    void stop();

    // for testing
    uint64_t get_kv_area_addr();
    uint64_t get_subtable_st_addr();
};

typedef struct TagServerMainArgs {
    Server * server;
    int      core_id;
} ServerMainArgs;

void * server_main(void * server_main_args);

#endif