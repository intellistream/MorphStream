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

// Perform routing. Expected content: ID,Host,IsFirstPacket,Protocol,ValueLength,Abortion
void app_read_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Packet accepted");
    auto pkt = string(ctx.packet());

    // Parse packet.
    auto fields = parse_args(pkt);

    // std::cout << content.substr(comma_pos[0] + 1, comma_pos[1] - comma_pos[0] - 1) << std::endl;
    auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    assert(threadLocal != NULL);

    threadLocal->host = atoi(fields[1].c_str());
	bool isFirst = fields[2] == "true"? true: false;
    threadLocal->abortion = fields[5] == "true"? true: false;

    // Make assignment if is new connection.
    if (isFirst){
        ctx.Transaction(1).Trigger(connId, ctx, (char *) &threadLocal->host, false); // Write to host entry about the destination.
    } else {
        ctx.Transaction(0).Trigger(connId, ctx, (const char*)&threadLocal->assigned_backend, false); // Rount and count. Record count to the earlier assigned backend count.
    }

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
                    0, 1, 9999,
                    None, 
                    IncrementPacketCountUDF, nullptr, nullptr, WRITE
                },
                StateAccess{
                    "RoutePacket",
                    0, 1, 9999,
                    None, 
                    RoutePacketUDF, nullptr, nullptr, RWType::READ
                }
            }
        },
        Transaction{
            "AssignStream",
            {
                StateAccess{
                    // TODO. Add definition of field to write result.
                    "ConnectionAssign",
                    0, 1, 9999,
                    None, 
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
        path = "/home/kailian/DB4NFV/libVNF/vnf/lb/config.csv";
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
