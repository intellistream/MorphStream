// #include <core.hpp>
#include <libvnf/core.hpp>
#include <utility>
#include <unordered_map>
#include <cassert>
#include <iostream>

using namespace vnf;

// State to hold through out a request.
struct BState {};

Config config("/home/kailian/libVNF/vnf/SL/config.csv");

using namespace DB4NFV;

// Handler function.
int src_transfer_sa_udf(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    int* srcBalance = ctx.get_value(raw, length, 0);
    if (*srcBalance > double(100)) {
        return *srcBalance - 100; 
        // ctx.WriteBack(&res, sizeof(double));
    } else {
        // Forgot how to dispose abortion.. TODO.
        ctx.Abort();
        return -1;
    }   
};

int dest_transfer_sa_udf(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    int* src_balance = ctx.get_value(raw, length, 0);
    int* dst_balance = ctx.get_value(raw, length, 1);
    if (* src_balance >= 100) {
        return *dst_balance - 100;
    } else {
        ctx.Abort();
        return -1;
    }   
};

int deposit_sa_udf(vnf::ConnId& connId, Context &ctx, char * raw, int length){
    int* srcBalance = ctx.get_value(raw, length, 0);
    return *srcBalance - 100;
    return -1;
}

void sl_app_accept_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Connection");
    return;
};

void sl_app_read_packet_handler(vnf::ConnId& connId, Context &ctx){
    spdlog::debug("[DEBUG] New Packet accepted");
    auto content = string(ctx.packet());
    // TODO. Clear timeStamping out of the user code.
    if (content == "transfer"){
        // Set next app here if needed. Before Transaction triggered. Or you can place them in sa handler.
        // ctx.NextApp(1, vnf::READ);
        ctx.Transaction(1).Trigger(connId, ctx, "0000:0001", false);
    } else if (content == "deposit") {
        // Set next app here if needed. Before Transaction triggered. Or you can place them in sa handler.
        // ctx.NextApp(1, vnf::READ);
        ctx.Transaction(0).Trigger(connId, ctx, "0000:0001", false);
    } else {
        assert(false);
    }
    return;
};

auto SLApp = DB4NFV::App{
    {
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
        },
        Transaction{
            // "transfer_transaction".
            {
                StateAccess{
                    {0}, {1}, None, 
                    deposit_sa_udf, nullptr, nullptr, WRITE
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
