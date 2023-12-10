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
void src_transfer_sa_udf(vnf::ConnId& connId, const std::vector<Transaction>& txns, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, void* value, int valueLen, int errorCode){
    double* srcBalance = static_cast<double *> (value);
    if (*srcBalance > double(100)) {
        *RESULT_PTR(reqObj) = *srcBalance - 100;
    } else {
        // Forgot how to dispose abortion.. TODO.
        registerNextApp(nullptr, -1, ABORT);
    }   
};

void dest_transfer_sa_udf(vnf::ConnId& connId, const std::vector<Transaction>& txns, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, void* value, int valueLen, int errorCode){
    double** balances = static_cast<double **> (value);
    // balance0 is the src balance
    // balance1 is the dst balance
    if (*(balances[0]) > double(100)) {
        *RESULT_PTR(reqObj) = *(balances[1]) - 100;
    } else {
        registerNextApp(nullptr, -1, ABORT);
    }   
};

void deposit_sa_udf(vnf::ConnId& connId, const std::vector<Transaction>& txns, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, void* value, int valueLen, int errorCode){
    double* srcBalance = static_cast<double *> (value);
    *RESULT_PTR(reqObj) = *srcBalance - 100;
}

void sl_app_accept_packet_handler(vnf::ConnId& connId, const std::vector<Transaction>& txns, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, int errorCode){
    std::cout << "Connection accepted" << std::endl;
    return;
};

void sl_app_read_packet_handler(vnf::ConnId& connId, const std::vector<Transaction>& txns, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, int errorCode){
    std::cout << "Connection accepted" << std::endl;
    auto content = string(static_cast<char *>(packet));
    if (content == "transfer\n"){
        txns[1].Trigger(connId, packet, packetLen, packetId, reqObj, reqObjId);
    } else if (content == "deposit\n") {
        txns[0].Trigger(connId, packet, packetLen, packetId, reqObj, reqObjId);
    } else {
        assert(false);
    }
    return;
};

auto SLApp = DB4NFV::App{
    // TODO: ADD: Transactions.
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
                    src_transfer_sa_udf, nullptr, nullptr, WRITE
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
