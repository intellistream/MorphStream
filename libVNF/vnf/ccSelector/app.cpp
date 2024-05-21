// #include <core.hpp>
#include <libvnf/core.hpp>
#include <utility>
#include <unordered_map>
#include <cassert>
#include <iostream>

using namespace vnf;

// State to hold through out a request.
struct BState {
    int ID;
    int host;
    int CC;
};

int max_backend = 10;

using namespace DB4NFV;

int CCRequest(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    spdlog::debug("[DEBUG] CC Request UDF.");
	auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    // Failure abortion
    assert( threadLocal->CC != 0);
    // Balance insufficient abortion
    int* lastCnt = ctx.get_value(raw, length, 0);

    // Remind python script to send next.
    char ret[8];
    memcpy(ret, (char *)&threadLocal->ID, sizeof(int));
    assert(threadLocal->ID > 10000);
    memcpy(ret + 4, ";", 1);
    connId.sendData(ret, sizeof(int) + 1);

    // Calculate result.
    return *lastCnt + 1; 
};


// Perform load balancing.
void app_accept_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Connection");
    return;
};

// Perform routing. Expected content: ID,Host,IsFirstPacket,Protocol,ValueLength,Abortion
void app_read_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Packet accepted");
    auto pkt = string(ctx.packet(), ctx.packet_len());

    // Parse packet.
    auto fields = parse_args(pkt);
    // Invalid pkt.
    if (fields.size() != 7) {
        std::cout << (pkt) << std::endl;
        return;
    }

    // std::cout << content.substr(comma_pos[0] + 1, comma_pos[1] - comma_pos[0] - 1) << std::endl;
    auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    assert(threadLocal != NULL);

    threadLocal->ID = atoi(fields[0].c_str());
    threadLocal->host = atoi(fields[1].c_str());
    threadLocal->CC = atoi(fields[6].c_str());
    __debug__change_tuple_strategy(threadLocal->host, threadLocal->CC);

    ctx.Transaction(0).Trigger(connId, ctx, (char *) &threadLocal->host, false); // Write to host entry about the destination.

    return;
};

auto CCApp = DB4NFV::App{
    "CCApp",
    {
        Transaction{
            "CCSwitching",
            {
                StateAccess{
                    "RoutePacket",
                    0, 1, 9999,
                    None, 
                    CCRequest, nullptr, nullptr, RWType::READ
                }
            }
        }
    },
    app_accept_packet_handler,
    app_read_packet_handler,
    nullptr,
    sizeof(BState),
};

int VNFMain(int argc, char *argv[]){
    std::string path;
    if (argc <= 2){
        // Use defaut.
        perror("VNF Config not provided.");
        // assert(false);
        path = "/home/kailian/DB4NFV/libVNF/vnf/ccSelector/config.csv";
    } else {
        path = std::string(argv[1]);
    }
    // Parse the first parameter as the path to config.
    SetConfig(path);

    // Get the main SFC to construct.
    auto& SFC = GetSFC();
    SFC.Entry(CCApp);

    // No nextApp
    // SFC.Add(SLApp, SomeNextApp);

    return 0;
}
