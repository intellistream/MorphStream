#ifndef __DB4NFV_UTILS__
#define __DB4NFV_UTILS__

#include "core.hpp"

#define MAX_REQUEST_OBJECT_TYPES 4
#define FIRST_TIME_CONN_ID ConnId(0, 0)

#define CONTEXT(REQOBJ) ((REQOBJ)-sizeof(Context))
#define COREID(TXNID) (static_cast<int>((TXNID) >> sizeof(int)))
#define PACKETID(TXNID) (static_cast<int>(TXNID))
#define TXNID(COREID, PACKETID) ((COREID) << sizeof(int) | (PACKETID))

using namespace vnf;
using namespace std;

struct alignas(16) PendingData {
    char *data;
    int dataLen;
    int streamNum;

    PendingData() : data(NULL), dataLen(0), streamNum(-1) {}

    PendingData(char* data, int dataLen, int streamNum = -1) : data(data), dataLen(dataLen), streamNum(streamNum) {}
};
typedef queue<PendingData> PendingDataQueue;

struct ServerPThreadArgument {
    int coreId;
    string ip;
    int port;
    CallbackFn onAcceptByServerCallback[NUM_CALLBACK_EVENTS];
    // DSInitFn InitFn;
    DSCallbackFn onAcceptByServerDSCallback;
    ReqObjExtractorFn onAcceptByServerReqObjIdExtractor;
    PacketBoundaryDisambiguatorFn onAcceptByServerPBD;

    ServerPThreadArgument() : coreId(-1), ip(""), port(-1),
                              onAcceptByServerCallback(),
                              onAcceptByServerDSCallback(nullptr),
                              onAcceptByServerReqObjIdExtractor(nullptr),
                              onAcceptByServerPBD(nullptr) {}

    void set(int id, string ip, int portNo,
             CallbackFn* onAcceptByServerCallback,
             DSCallbackFn onAcceptByServerDSCallback,
             ReqObjExtractorFn reqObjExtractorFn,
             PacketBoundaryDisambiguatorFn disambiguatorFn) {
            //  DSInitFn dataStoreInitfn) {
        this->coreId = id;
        this->ip = ip;
        this->port = portNo;
        for (int eventType = 0; eventType < NUM_CALLBACK_EVENTS; ++eventType)
        {
            this->onAcceptByServerCallback[eventType] = onAcceptByServerCallback[eventType];
        }
        this->onAcceptByServerDSCallback = onAcceptByServerDSCallback;
        this->onAcceptByServerReqObjIdExtractor = reqObjExtractorFn;
        this->onAcceptByServerPBD = disambiguatorFn;
        // this->InitFn = dataStoreInitfn;
    }
};

struct PerCoreState {
    bool isJobDone;
    // callback maps
    unordered_map<int, CallbackFn> socketIdCallbackMap[NUM_CALLBACK_EVENTS];
    unordered_map<int, CallbackFn> socketIdErrorCallbackMap;

    // ds callback maps
    unordered_map<int, DSCallbackFn> socketIdDSCallbackMap;
    unordered_map<int, DSCallbackFn> socketIdDSErrorCallbackMap;

    // // just registered postTransacrtion maps.
    // unordered_map<int, DB4NFV::PostTxnHandler> socketIdPostTxnHandlerMap;
    // unordered_map<int, DB4NFV::PostTxnHandler> socketIdPostTxnErrorHandlerMap;

    // Increasing packet number. Used to mark each packet.
    int packetNumber;
    unordered_map<int, Context *> packetNumberContextMap;

    // request object memory management
    int reqObjSizesInPowersOf2[MAX_REQUEST_OBJECT_TYPES];
    vector<vector<char>> reqObjMemPoolBlocks;
    boost::simple_segregated_storage<size_t> reqObjMemPoolManagers[MAX_REQUEST_OBJECT_TYPES];
    // request object memory management
    unordered_map<int, unordered_map<int, void *> > socketIdReqObjIdToReqObjMap;
    unordered_map<int, unordered_map<int, int> > reqObjAllocatorSocketIdReqObjIdToReqObjIdMap;
    unordered_map<int, ReqObjExtractorFn> socketIdReqObjIdExtractorMap;
    unordered_map<int, PacketBoundaryDisambiguatorFn> socketIdPBDMap;
    map<int, class timer*> fdToObjectMap;

    // packet memory management
    vector<char> packetMemPoolBlock;
    boost::simple_segregated_storage<size_t> packetsMemPoolManager;
    unordered_map<void *, bool> doNotEvictPacketBoolMap;
    unordered_map<int, string> socketIdLeftOverPacketFragmentMap;

    // datastore socket ids and id-loopers
    int dsSocketId1, dsSocketId2;
    string dsSocketProtocol;
    int dsSockIdSetLooper, dsSockIdGetLooper;

    // The epFD used for notifications.
    int epollFd = -1;

    // FD used to transmit the morphMessage.
    int txnSocket = -1;

    // stat counters
    int connCounter;
    int numSends, numRecvs;
    int numPacketsSentToDs, numPacketsRecvFromDs;

    //moved from globals
    string serverProtocol;
    unordered_map<int, string> socketProtocolMap;
    unordered_map<int, sockaddr_in> udpSocketAddrMap;

    unordered_map<int, PendingDataQueue> socketIdPendingDataQueueMap;

    PerCoreState() : isJobDone(false),
                     epollFd(0),
                     txnSocket(0),
                     dsSocketId1(0), dsSocketId2(0),
                     dsSockIdSetLooper(0), dsSockIdGetLooper(0),
                     connCounter(0),
                     numSends(0), numRecvs(0),
                     numPacketsSentToDs(0), numPacketsRecvFromDs(0) {
        for (int &reqObjSizeInPowerOf2 : reqObjSizesInPowersOf2) {
            reqObjSizeInPowerOf2 = 0;
        }
        reqObjMemPoolBlocks.resize(MAX_REQUEST_OBJECT_TYPES);
        // packet vector size. may have to increase it for very high I/O application
        // assuming pkt size 1024 TODO
        packetMemPoolBlock.resize(1500 * 2048); //1024
    }

    /**
     * Expects reqObjType to start from 0
     * */
    void initMemPoolOfRequestObject(int reqObjType) {
        if (reqObjSizesInPowersOf2[reqObjType] != 0) {
            reqObjMemPoolBlocks[reqObjType].resize(reqObjSizesInPowersOf2[reqObjType] * 2097152);
            spdlog::info("Request object type {} has memory pool size of {} bytes", reqObjType, reqObjMemPoolBlocks[reqObjType].size());
            reqObjMemPoolManagers[reqObjType].add_block(&reqObjMemPoolBlocks[reqObjType].front(),
                                                        reqObjMemPoolBlocks[reqObjType].size(),
                                                        reqObjSizesInPowersOf2[reqObjType]);
        }
    }

    /**
     * Expects reqObjIndex to start from 0
     * */
    int mallocReqObj(int socketId, int reqObjType, int reqObjId) {
        reqObjAllocatorSocketIdReqObjIdToReqObjIdMap[socketId][reqObjId] = socketId;
        socketIdReqObjIdToReqObjMap[socketId][reqObjId] = reqObjMemPoolManagers[reqObjType].malloc(); // TODO lock
        return socketIdReqObjIdToReqObjMap[socketId][reqObjId] == nullptr ? -1 : 0;
    }

    /**
     * Expects reqObjIndex to start from 0
     * */
    void freeReqObj(int socketId, int reqObjType, int reqObjId) {
        reqObjMemPoolManagers[reqObjType].free(socketIdReqObjIdToReqObjMap[socketId][reqObjId]);
        socketIdReqObjIdToReqObjMap[socketId].erase(reqObjId);
        if (socketIdReqObjIdToReqObjMap[socketId].empty()) {
            socketIdReqObjIdToReqObjMap.erase(socketId);
        }
        reqObjAllocatorSocketIdReqObjIdToReqObjIdMap[socketId].erase(reqObjId);
        if (reqObjAllocatorSocketIdReqObjIdToReqObjIdMap[socketId].empty()) {
            reqObjAllocatorSocketIdReqObjIdToReqObjIdMap.erase(socketId);
        }
    }

    bool isARequestObjectAllocator(int socketId, int reqObjId) {
        auto socketIdIter = reqObjAllocatorSocketIdReqObjIdToReqObjIdMap.find(socketId);
        if (socketIdIter != reqObjAllocatorSocketIdReqObjIdToReqObjIdMap.end()) {
            auto reqObjIdIter = socketIdIter->second.find(reqObjId);
            return reqObjIdIter != socketIdIter->second.end();
        }
        return false;
    }

    void tagPacketNonEvictable(void *packet) {
        doNotEvictPacketBoolMap[packet] = true;
    }

    void tagPacketEvictable(void *packet) {
        doNotEvictPacketBoolMap.erase(packet);
    }

    bool canEvictPacket(void *packet) {
        unordered_map<void *, bool>::const_iterator iterator = doNotEvictPacketBoolMap.find(packet);
        return iterator == doNotEvictPacketBoolMap.end();
    }

    bool isDatastoreSocket(int socketId) {
        return socketId == dsSocketId1 || socketId == dsSocketId2;
    }

    string getLeftOverPacketFragment(int socketId) {
        auto it = socketIdLeftOverPacketFragmentMap.find(socketId);
        if (it == socketIdLeftOverPacketFragmentMap.end()) {
            socketIdLeftOverPacketFragmentMap[socketId] = "";
        }
        return socketIdLeftOverPacketFragmentMap[socketId];
    }

    void setLeftOverPacketFragment(int socketId, string leftOver) {
        socketIdLeftOverPacketFragmentMap[socketId] = leftOver;
    }

    void delLeftOverPacketFragment(int socketId) {
        auto it = socketIdLeftOverPacketFragmentMap.find(socketId);
        if (it != socketIdLeftOverPacketFragmentMap.end()) {
            socketIdLeftOverPacketFragmentMap.erase(it);
        }
    }

    bool isPendingDataQueueEmpty(int socketId) {
        auto it = socketIdPendingDataQueueMap.find(socketId);
        if (it == socketIdPendingDataQueueMap.end()) {
           return true;
        }

        return it->second.empty();
    }
};


// cache list of addr for clearing cache..cache remove
unordered_map<void *, int> cache_void_list; // TODO put this into Globals struct appropriately
/**
 * Container for globals
 * */
struct Globals {
    // server
    int listeningSocketFd;
    string serverIp;
    int serverPort;
    string serverProtocol;
    bool hasInitialized = false;
    CallbackFn onAcceptByServerCallback[NUM_CALLBACK_EVENTS];
    DSCallbackFn onAcceptByServerDSCallback;
    ReqObjExtractorFn onAcceptByServerReqObjIdExtractor;
    PacketBoundaryDisambiguatorFn onAcceptByServerPBD;

    // DB4NFV globals.
    DB4NFV::SFC sfc;
	int txnNotifyFd = -1;

    // data store management
    // may have to increase this at high load
    vector<char> dsMemPoolBlock;
    boost::simple_segregated_storage<size_t> dsMemPoolManager;
    unordered_map<int, void *> localDatastore;
    unordered_map<int, int> localDatastoreLens;
    unordered_map<int, void *> cachedRemoteDatastore;
    unordered_map<void *, int> doNotEvictCachedDSValueKeyMap;
  //  unordered_map<int, string> socketProtocolMap;
    int dsSize;

    // locks
    mutex epollArrayLock, dataStoreLock, listenLock;

    Globals() : listeningSocketFd(-1),
                onAcceptByServerCallback(),
                onAcceptByServerDSCallback(nullptr),
                onAcceptByServerReqObjIdExtractor(nullptr),
                onAcceptByServerPBD(nullptr),
                serverIp(""), serverPort(0),
                serverProtocol(""), dsSize(0) {
        dsMemPoolBlock.resize(1024 * 32768);
    }

    bool keyExistsInLocalDatastore(int key) {
        unordered_map<int, void *>::const_iterator iterator = localDatastore.find(key);
        return iterator != localDatastore.end();
    }

    bool keyExistsInCachedRemoteDatastore(int key) {
        unordered_map<int, void *>::const_iterator iterator = cachedRemoteDatastore.find(key);
        return iterator != cachedRemoteDatastore.end();
    }

    bool canEvictCachedDSKey(void *value) {
        return doNotEvictCachedDSValueKeyMap.find(value) == doNotEvictCachedDSValueKeyMap.end();
    }
};

int __event_fd() {
    return globals.txnNotifyFd;
}

/**
 * Container for constants user passes in initLibvnf() function
 * */
struct UserConfig {
    const int MAX_CORES;
    const int BUFFER_SIZE;
    const string DATASTORE_IP;
    const vector<int> DATASTORE_PORTS;
    const int DATASTORE_THRESHOLD;
    const enum DataLocation USE_REMOTE_DATASTORE;

    UserConfig(int maxCores, int bufferSize,
               string &dataStoreIP,
               vector<int> &dataStorePorts,
               int dataStoreThreshold,
               enum DataLocation dsType) : MAX_CORES(maxCores),
                                          BUFFER_SIZE(bufferSize),
                                        //   DATASTORE_TYPE(dataStoreType),
                                          DATASTORE_IP(dataStoreIP),
                                          DATASTORE_PORTS(dataStorePorts),
                                          DATASTORE_THRESHOLD(dataStoreThreshold),
                                          USE_REMOTE_DATASTORE(dsType) {
        spdlog::info("UserConfig: [ MAX_CORES={}, BUFFER_SIZE={},  DATASTORE_THRESHOLD={}, DS_TYPE={} ]",
               MAX_CORES, BUFFER_SIZE, DATASTORE_THRESHOLD, USE_REMOTE_DATASTORE);
    }
};

int readFromStream(int connFd, uint8_t *buf, int len) {
    if (connFd < 0 || len <= 0) {
        return -1;
    }

    int remainingBytes = len;
    int ptr = 0;
    while (true) {
        int readBytes = static_cast<int>(read(connFd, buf + ptr, remainingBytes));
        if (readBytes <= 0) {
            return readBytes;
        }
        ptr += readBytes;
        remainingBytes -= readBytes;
        if (remainingBytes == 0) {
            return len;
        }
    }
}

int makeSocketNonBlocking(int socketFd) {
    int flags = fcntl(socketFd, F_GETFL, 0);
    if (flags == -1) {
        spdlog::error("NBS fcntl");
        return -1;
    }

    flags |= O_NONBLOCK;
    int s = fcntl(socketFd, F_SETFL, flags);
    if (s == -1) {
        spdlog::error("NBS fcntl flags");
        return -1;
    }

    return 0;
}


namespace DB4NFV{

enum RWType {
	READ = 1,
	WRITE = 2,
};

enum ConsistencyRequirement {
    None = 0,
};

class App{
friend SFC;
public:
	App(std::vector<Transaction*> txns, 
        AcceptHandler accept, 
        ReadHandler read, 
        ErrorHandler error,
        int reqObjSize):
		Txns(txns), 
        acceptHandler(accept), errorHandler(error), readHandler(read),
        reqObjSize(reqObjSize)
	{};
	
	const AcceptHandler acceptHandler = nullptr;
	const ReadHandler   readHandler = nullptr;
	const ErrorHandler  errorHandler = nullptr;

	// The callbacks router.
	// int _callBack(int TxnIdx, int SAIdx, void* value, int length, int errCode);

    // Point the request obj
    void *reqObjClip(void * reqObjClip) { return reqObjClip + sizeof(Context) + globals.sfc.objSizes.at(globals.sfc.AppIdxMap[this]);}
	int reqObjSize = 0; // Register the size of per-thread user defined structure.

    const std::vector<Transaction*> Txns;

    int appIndex;
	App* next = nullptr;
	App* MoveNext() { return this->next;}
};


class Transaction {
friend SFC;
public:
    Transaction(std::vector<StateAccess*> sas):sas(sas){}
    void TxnStart() { perror("javaProvide.");}
    void TxnDone()  { perror("javaProvide.");}
	const std::vector<StateAccess*> sas;	// The State Access event to be used for transaction handlers. Including transaction handlers.
	const App* app;
	int txnIndex;
};

class StateAccess {
friend SFC;
public:
    // What is a state scope?
    StateAccess(const std::vector<std::string> &fields,
                const std::vector<std::string> &types,
                ConsistencyRequirement cr,
                TxnCallBackHandler txnHandler,
                TxnCallBackHandler errTxnHandler,
                PostTxnHandler postHandler,
                RWType rw)
        : fields_(fields), types_(types), cr_(cr),
          txnHandler_(txnHandler), rw_(rw), errorTxnHandler_(errTxnHandler),
          postTxnHandler_(postHandler) {}

	const enum RWType rw_;	

    const std::vector<std::string> &fields_;
    const std::vector<std::string> &types_;
    const ConsistencyRequirement cr_;

	// The binded transaction handler for this State Access.
	const TxnCallBackHandler txnHandler_ = nullptr;
	const TxnCallBackHandler errorTxnHandler_ = nullptr;
	const PostTxnHandler postTxnHandler_ = nullptr;

	// Called by user to start request, send the transaction request to schedule.
	int Request(vnf::ConnId& connId, char * packet, int packetLen, int packetId, void * reqObj);

	App* app;
	Transaction* txn;
	int txnIndex;
    int saIndex;
};

/**
 * @brief Define the whole SFC topology. Collect meta data for the whole SFC and communicate with txnEngine.
 * 	
 */
class SFC{
public:
	SFC() {}

	~SFC(){};

	// Setting cores of this SFC apps.
	int cores = 1;	

	// Construct SFC topology.
	void Entry(App & entry);
	void Add(App& last, App& app);

	// Report SFC apps to txnEngine.
	int NFs();

	void Init(int maxCores){
        // Fix the backlink.
        int reqObjStart = 0;
        int reqObjIndex = 0;
        for (int i = 0 ; i < SFC_chain.size(); i += 1)
        {
            auto app = SFC_chain.at(i);
            app.appIndex = i;
            // Sort the reqObj. TODO.
            this->objSizes.push_back(app.reqObjSize);
            this->objSizesStarting.push_back(reqObjStart);
            reqObjStart += app.reqObjSize;
            this->AppIdxMap[&app] = reqObjIndex;
            reqObjIndex += 1;
            
            for (int j = 0 ; j < app.Txns.size(); j += 1){
                auto Txn = app.Txns.at(j);
                Txn->txnIndex = j;
                Txn->app = &app;
                for (int k = 0; k < (Txn->sas).size(); k += 1){
                    auto sa = Txn->sas.at(k);
                    sa->app = &app;
                    sa->txn = Txn;
                    sa->txnIndex = j;
                    sa->saIndex = k;
                }
            }
        }
        this->reqObjTotalSize = reqObjStart;
        // Config and init libVNF.
        this->cores = maxCores;
        // Create back links for transactions and state access.
        vnf::initLibvnf(cores, 128, "127.0.0.1", std::vector<int>(), 131072, vnf::LOCAL);
    }

    // The Callbacks router for the whole SFC.
    int _callBack(vnf::ConnId& connId, int AppIdx, int TxnIdx, int SAIdx, int reqObjId, void* reqobj, char * packet, int packetlen, void * value, int length, int errCode);

    void * AppObj(App *app, void * reqObj) {
        int idx = AppIdxMap[app];
        return reqObj + objSizesStarting[idx];
    }

// private:
    int reqObjTotalSize = 0;
	std::vector<App&> SFC_chain = {};
	std::unordered_map<App*, int> AppIdxMap = {};
	std::vector<int> objSizes;
    std::vector<int> objSizesStarting;
};

typedef void (*TxnCallBackHandler)(vnf::ConnId& connId, const std::vector<Transaction*>& availableTransactions, int reqObjId, void* reqObj, char * packet, int packetLen, void* value, int valueLen, int errorCode);

typedef void (*PacketHandler)(vnf::ConnId& connId, const std::vector<Transaction*>& availableTransactions, int reqObjId, void* reqObj, char * packet, int packetLen, int errorCode);

typedef PacketHandler AcceptHandler;
typedef PacketHandler ReadHandler;
typedef PacketHandler ErrorHandler;
typedef PacketHandler PostTxnHandler;

// User get SFC single instance.
DB4NFV::SFC& GetSFC() { 
    return globals.sfc; 
};

}

struct Context{
    // Execution flow.
    DB4NFV::PacketHandler next;
    // Data fetching to be saved.
    char * packet_record;
    int packet_len;
    // Packet to be saved.
    void * value;
    int value_len;

    // Running status.
    bool waiting_for_transaction_back = false;
    int next_app = 0;

    // The socketId for the last request.
    int old_socket = -1;
    vnf::EventType ret;

    // Construct Empty.
    Context(DB4NFV::PacketHandler next, char * packet_record, int packet_len):
        next(next), packet_record(packet_record), packet_len(packet_len), value(nullptr), value_len(0)
    {}

    void SetValue(void * value, int value_len) {this->value = value; this->value_len = value_len;};
};

// Define the next app target.
int registerNextApp(void * reqObj, int AppIdx, vnf::EventType type) {
    auto o = static_cast<Context *>(reqObj - sizeof(Context));
    o->next_app = AppIdx;
    o->ret = type;
    return;
}

// Main loop for traversal of the apps.
void _AppsDisposalAccept(vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum);
void _AppsDisposalRead  (vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum);
void _AppsDisposalError (vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum);

void _disposalBody(vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode);

// The entry of the SFC thread from java.
int __VNFThread(int argc, char *argv[]);
int __init_SFC(int argc, char *argv[]);

// The entry for sending txn execution request to txnEngine.
// TODO. Extend the other information.
int __request(uint64_t txnId);

// call backs router for txnEngine. Could be renamed according to the generated func signature.
// int _callBack(vnf::ConnId& connId, int AppIdx, int TxnIdx, int SAIdx, int reqObjId, void* reqobj, char * packet, int packetlen, void * value, int length, int errCode);
int _callBack(uint64_t txnId, void * value, int length);


#endif