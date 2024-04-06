// #include <core.hpp>
#include <libvnf/core.hpp>
#include <utility>
#include <unordered_map>
#include <cassert>
#include <iostream>

using namespace vnf;

// State to hold through out a request.
struct BState {
    int host;
    int assigned_backend;
    bool abortion;
};

int max_backend = 10;

using namespace DB4NFV;

int IncrementPacketCountUDF(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    spdlog::debug("[DEBUG] IncrementPacketCount triggered");
	auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    // Failure abortion
    assert( threadLocal->assigned_backend == 0);
    if (threadLocal->abortion || length < 1){
        ctx.Abort();
        return -1;
    }
    // Balance insufficient abortion
    int* lastCnt = ctx.get_value(raw, length, 0);
    // Calculate result.
    return *lastCnt + 1; 
};

int RoutePacketUDF(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    spdlog::debug("[DEBUG] route packet udf triggered");
	auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    if (threadLocal->abortion || length < 1){
        ctx.Abort();
        return -1;
    }
    int* route_destination = ctx.get_value(raw, length, 0);
    spdlog::debug("[DEBUG] packet routed to: %d", *route_destination);
    return -1; // Read only.
};

int AssignStreamUDF(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    spdlog::debug("[DEBUG] assignment udf triggered");
	auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    // Failure abortion
    if (threadLocal->abortion || length < 1){
        ctx.Abort();
        return -1;
    }
    // Fetch State and decide assignment.
    int* lastAssigned = ctx.get_value(raw, length, 0);
    threadLocal->assigned_backend = (*lastAssigned + 1) % max_backend;
    return threadLocal->assigned_backend; 
};


// Perform load balancing.
void app_accept_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Connection");
    return;
};

// Perform routing. Expected content: Host,IsFirstPacket,Protocol,Value,Abortion
void app_read_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Packet accepted");
    auto content = string(ctx.packet());

    // Parse content from comma.
    int comma_pos[3] = {0,0,0};
    int com = 0;
    for (int i = 0; i < content.length() && com < 3; i++)
    {
        if (content[i] == ','){
            comma_pos[com] = i;
            com++;
        } 
    }

    // std::cout << content.substr(0, comma_pos[0]) << std::endl;
    auto host = content.substr(0, comma_pos[0]).c_str(); 
	auto isFirst = content.substr(comma_pos[2] + 1) == "false"? true: false;
    // std::cout << "key" << key << std::endl;

    // std::cout << content.substr(comma_pos[0] + 1, comma_pos[1] - comma_pos[0] - 1) << std::endl;
    auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    assert(threadLocal != NULL);

    // Make assignment if is new connection.
    if (isFirst){
        ctx.Transaction(1).Trigger(connId, ctx, host, false); // Write to host entry about the destination.
    } else {
        ctx.Transaction(0).Trigger(connId, ctx, (const char*)&threadLocal->assigned_backend, false); // Rount and count. Record count to the earlier assigned backend count.
    }
    // Route and increment count if not.
    threadLocal->abortion = content.substr(comma_pos[2] + 1) == "false"? true: false;
    return;
};

auto LBApp = DB4NFV::App{
    "LBApp",
    {
        Transaction{
            "Route Packet",
            {
                StateAccess{
                    "IncrementCount",
                    0, 1, None, 
                    IncrementPacketCountUDF, nullptr, nullptr, WRITE
                },
                StateAccess{
                    "RoutePacket",
                    0, 1, None, 
                    RoutePacketUDF, nullptr, nullptr, RWType::READ
                }
            }
        },
        Transaction{
            "AssignStream",
            {
                StateAccess{
                    // TODO. Add definition of field to write result.
                    "assign stream",
                    0, 1, None, 
                    AssignStreamUDF, nullptr, nullptr, WRITE
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
        path = "/home/kailian/MorphStream/libVNF/vnf/lb/config.csv";
    } else {
        path = std::string(argv[1]);
    }
    // Parse the first parameter as the path to config.
    SetConfig(path);

    // Get the main SFC to construct.
    auto& SFC = GetSFC();
    SFC.Entry(LBApp);

    // No nextApp
    // SFC.Add(SLApp, SomeNextApp);

    return 0;
}
