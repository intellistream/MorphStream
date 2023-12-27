// #include <core.hpp>
#include <libvnf/core.hpp>
#include <utility>
#include <unordered_map>
#include <cassert>
#include <iostream>

using namespace vnf;

// State to hold through out a request.
struct BState {
    int amount;
    bool abortion;
};

Config config("/home/kailian/libVNF/vnf/SL/config.csv");

using namespace DB4NFV;

// Handler function.
int src_transfer_sa_udf(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    spdlog::debug("[DEBUG] src_transfer_sa_udf triggered");
	auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    // Failure abortion
    if (threadLocal->abortion || length < 4){
        ctx.Abort();
        return -1;
    }
    // Balance insufficient abortion
    int* srcBalance = ctx.get_value(raw, length, 0);
    if (*srcBalance < threadLocal->amount) {
        ctx.Abort();
        return -1;
    }
    // Calculate result.
    return *srcBalance - threadLocal->amount; 
};

int dest_transfer_sa_udf(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    spdlog::debug("[DEBUG] dest_transfer_sa_udf triggered");
	auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    if (threadLocal->abortion || length < 4){
        ctx.Abort();
        return -1;
    }
    int* dst_balance = ctx.get_value(raw, length, 0);
    return *dst_balance + threadLocal->amount;
};

int deposit_sa_udf(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    spdlog::debug("[DEBUG] deposit_sa_udf triggered");
	auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    if (threadLocal->abortion || length < 4){
        ctx.Abort();
        return -1;
    }
    int* srcBalance = ctx.get_value(raw, length, 0);
    return *srcBalance - threadLocal->amount;
}

void sl_app_accept_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Connection");
    return;
};

void sl_app_read_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Packet accepted");
    auto content = string(ctx.packet());

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
    auto key = content.substr(0, comma_pos[0]).c_str();
    // std::cout << "key" << key << std::endl;

    // std::cout << content.substr(comma_pos[0] + 1, comma_pos[1] - comma_pos[0] - 1) << std::endl;
    auto threadLocal = reinterpret_cast<BState *>(ctx.reqObj());
    assert(threadLocal != NULL);
    threadLocal->amount = atoi(content.substr(comma_pos[0] + 1, comma_pos[1] - comma_pos[0] - 1).c_str());
    // std::cout << "amount" << threadLocal->amount << std::endl;

    // std::cout << content.substr(comma_pos[1] + 1, comma_pos[2] - comma_pos[1] - 1) << std::endl;
    std::string txn = content.substr(comma_pos[1] + 1, comma_pos[2] - comma_pos[1] - 1);
    // int res = (content.substr(comma_pos[1] + 1, comma_pos[2] - comma_pos[1] - 1) == string("transfer")) || 
    //     (content.substr(comma_pos[1] + 1, comma_pos[2] - comma_pos[1] - 1) == string("deposit")) ? 1 : 0;

    // std::cout << content.substr(comma_pos[2] + 1) << std::endl;
    threadLocal->abortion = content.substr(comma_pos[2] + 1) == "false"? true: false;
    // std::cout << "abortion" << threadLocal->abortion << std::endl;

    if (txn == string("transfer")){
        // Set next app here if needed. Before Transaction triggered. Or you can place them in sa handler.
        // ctx.NextApp(1, vnf::READ);
        ctx.Transaction(1).Trigger(connId, ctx, content.substr(0, comma_pos[0]).c_str() , false);
    } else if (txn == string("deposit")){
        // Set next app here if needed. Before Transaction triggered. Or you can place them in sa handler.
        // ctx.NextApp(1, vnf::READ);
        ctx.Transaction(0).Trigger(connId, ctx, content.substr(0, comma_pos[0]).c_str(), false);
    } else {
        std::cout << boost::stacktrace::stacktrace();
        std::cout << "Invalid txn name: " << txn << std::endl;
        assert(false);
    }
    return;
};

auto SLApp = DB4NFV::App{
    {
        Transaction{
            // "transfer_transaction".
            {
                StateAccess{
                    {0}, {1}, None, 
                    deposit_sa_udf, nullptr, nullptr, WRITE
                }
            }
        },
        Transaction{
            // "deposit_transaction",
            {
                StateAccess{
                    // TODO. Add definition of field to write result.
                    // "src_transfer_sa",
                    {0}, {1}, None, 
                    src_transfer_sa_udf, nullptr, nullptr, WRITE
                },
                StateAccess{
                    // "dst_transfer_sa",
                    {1}, {1}, None,
                    dest_transfer_sa_udf, nullptr, nullptr, WRITE
                }
            }
        }
    },
    sl_app_accept_packet_handler,
    sl_app_read_packet_handler,
    nullptr,
    sizeof(BState),
};

int VNFMain(int argc, char *argv[]){
    // Get the main SFC to construct.
    auto& SFC = GetSFC();
    SFC.Entry(SLApp);

    // No nextApp
    // SFC.Add(SLApp, SomeNextApp);

    // TODO. Call Json to formalize.
    return 0;
}
