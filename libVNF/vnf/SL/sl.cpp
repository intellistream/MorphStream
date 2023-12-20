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
void src_transfer_sa_udf(vnf::ConnId& connId, Context &ctx){
    double* srcBalance = static_cast<double *>(ctx.value());
    if (*srcBalance > double(100)) {
        double res = *srcBalance - 100; 
        ctx.WriteBack(&res, sizeof(double));
    } else {
        // Forgot how to dispose abortion.. TODO.
        ctx.Abort();
    }   
};

void dest_transfer_sa_udf(vnf::ConnId& connId, Context &ctx){
    // balance0 is the src balance
    // balance1 is the dst balance
    if (*(double*)(ctx.value()) > double(100)) {
        // Double1 = *(double*)(ctx.value() + sizeof(double));
        double res = *(double*)(ctx.value() + sizeof(double)) - 100;
        ctx.WriteBack(&res, sizeof(double));
    } else {
        ctx.Abort();
    }   
};

void deposit_sa_udf(vnf::ConnId& connId, Context &ctx){
    double* srcBalance = static_cast<double *> (ctx.value());
    double res = *srcBalance - 100;
    ctx.WriteBack(&res, sizeof(double));
}

void sl_app_accept_packet_handler(vnf::ConnId& connId, Context &ctx){
    std::cout << "[DEBUG] New Connection" << std::endl;
    return;
};

void sl_app_read_packet_handler(vnf::ConnId& connId, Context &ctx){
    std::cout << "[DEBUG] New Packet accepted" << std::endl;
    auto content = string(ctx.packet());
    // TODO. Clear timeStamping out of the user code.
    if (content == "transfer\n"){
        ctx.Transaction(1).Trigger(connId, ctx, "0000:0001", false);
    } else if (content == "deposit\n") {
        ctx.Transaction(0).Trigger(connId, ctx, "0000:0001", false);
        std::cout << GetSFC().SFC_chain[0]->Txns.size() << std::endl;
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
