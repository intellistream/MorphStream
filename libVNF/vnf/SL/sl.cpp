#include <libvnf/core.hpp>
#include <utility>
#include <unordered_map>
#include <cassert>

using namespace vnf;

// State to hold through out a request.
struct BState {};

Config config("/home/kailian/libVNF/vnf/SL/config.csv");

using namespace DB4NFV;

// Handler function.
void src_transfer_sa_udf(vnf::ConnId& connId, const std::vector<Transaction*>& availableTransactions, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, void* value, int valueLen, int errorCode){
    double* srcBalance = static_cast<double *> (value);
    if (*srcBalance > double(100)) {
        *srcBalance -= 100;
    } else {
        // Forgot how to dispose abortion.. TODO.
        abortTxn(saData); //an example of abort txn at application-level
    }   
};

// TODO. Add definition of keyIndexInEvent.
// TODO. Rename definition of fieldTableIndex.
const std::vector<std::string> src_transfer_sa_fields = {"0"};

// TODO. Add definition of field to write....
auto srcTransfer_sa = DB4NFV::StateAccess{
    src_transfer_sa_fields,
    // TODO. We dont need types here. User handle. delete it.
    src_transfer_sa_fields,
    None,
    src_transfer_sa_udf,
    nullptr,
    // TODO. Fix this. Move PostTxnHandler to Txns.
    nullptr,
    WRITE
};

void dest_transfer_sa_udf(vnf::ConnId& connId, const std::vector<Transaction*>& availableTransactions, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, void* value, int valueLen, int errorCode){
    double** balances = static_cast<double **> (value);
    // balance0 is the src balance
    // balance1 is the dst balance
    if (*(balances[0]) > double(100)) {
        *(balances[1]) -= 100;
    } else {
        abortTxn(saData); //an example of abort txn at application-level
    }   
};

void deposit_sa_udf(vnf::ConnId& connId, const std::vector<Transaction*>& availableTransactions, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, void* value, int valueLen, int errorCode){
    double* srcBalance = static_cast<double *> (value);
    *srcBalance += 100;
}

auto transfer_txn = DB4NFV::Transaction{

};

auto deposit_txn = DB4NFV::Transaction{

};

const std::vector<DB4NFV::Transaction*> Txns = {
    
};

void sl_app_accept_packet_handler(vnf::ConnId& connId, const std::vector<Transaction*>& availableTransactions, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, int errorCode){

};

void sl_app_read_packet_handler(vnf::ConnId& connId, const std::vector<Transaction*>& availableTransactions, int reqObjId, void* reqObj, char * packet, int packetLen, int packetId, int errorCode){

};

auto SLApp = DB4NFV::App{
    // TODO: ADD: Transactions.
    sl_app_accept_packet_handler,
    sl_app_read_packet_handler,
    nullptr,
    sizeof(BState),
};

std::string __init_SFC(int argc, char *argv[]){
    // Get the main SFC to construct.
    auto SFC = DB4NFV::GetSFC();
    SFC.Entry(SLApp);

    // No nextApp
    // SFC.Add(SLApp, SomeNextApp);

    // TODO. Call Json to formalize.
    return nullptr;
}
