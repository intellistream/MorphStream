#ifndef __DB4NFV_UTILS__
#define __DB4NFV_UTILS__

#include "core.hpp"

uint64_t GetCurrentTime()
{
	auto currentTimePoint = std::chrono::high_resolution_clock::now();
	auto duration = currentTimePoint.time_since_epoch();
	return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}

#define MAX_REQUEST_OBJECT_TYPES 4
#define FIRST_TIME_CONN_ID ConnId(0, 0)

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

struct Monitor{
    uint64_t average_latency_in_ns[10] = {0,0,0,0,0,0,0,0,0,0};
    uint64_t allocated_packet = 0;
    uint64_t conn_num = 0;
    uint64_t deallocated_packet = 0;
    uint64_t packets_done = 0;

    inline int packet_waiting(int coreId);
    inline void update_latency(int idx, uint64_t time) {
        time = getDelay(time); 
        if (average_latency_in_ns[idx] == 0){
            average_latency_in_ns[idx] = time;
        } else {
            average_latency_in_ns[idx] = uint64_t(average_latency_in_ns[idx] * 0.99 + time * 0.01); 
        }
    }
    inline void packet_done() {packets_done++;}
    inline void allocate_packet() { allocated_packet++; }
    inline void deallocate_packet() { deallocated_packet++;}
    inline void conn_accepted() { conn_num++;}
    inline void report_header() {
        spdlog::info(
            "this_is_a_report,ts,CoreId,ConnNum,L0,L1,L2,L3,num_pkts_done,num_alloc_pkts,num_deallocated_pkts,num_wip_packets"
        );
    }
    inline void report(int coreId) {
        spdlog::info(
            "this_is_a_report,{},{},{},{},{},{},{},{},{},{},{}",
            getNow(),
            coreId,
            conn_num,
            average_latency_in_ns[0],
            average_latency_in_ns[1],
            average_latency_in_ns[2],
            average_latency_in_ns[3],
            packets_done,
            allocated_packet,
            deallocated_packet,
            packet_waiting(coreId)
        );
    }
};

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
    JavaVM * __jvm;
    JNIEnv* __env;
    std::mutex boost_malloc_mutex;

    // DB4NFV globals.
    DB4NFV::SFC sfc;
	int txnNotifyFd = -1;
    Config config;

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

// Get JVM and jenv related. FIXME. Optimize.
bool GetJniEnv(JNIEnv **env);

Globals& GetGlobal();

struct PerCoreState {
    // For joining threads.
    bool isJobDone;

    // callback maps
    unordered_map<int, CallbackFn> socketIdCallbackMap[NUM_CALLBACK_EVENTS];
    unordered_map<int, CallbackFn> socketIdErrorCallbackMap;

    // ds callback maps
    unordered_map<int, DSCallbackFn> socketIdDSCallbackMap;
    unordered_map<int, DSCallbackFn> socketIdDSErrorCallbackMap;

    // Increasing packet number. Used to mark each packet.
    int packetNumber;
    unordered_map<int, Context *> packetNumberContextMap;

    // request object memory management
    int reqObjSizesInPowersOf2[MAX_REQUEST_OBJECT_TYPES];
    // FIXME. Check if it requires to be optimized.
    vector<vector<char>> reqObjMemPoolBlocks;
    boost::simple_segregated_storage<size_t> reqObjMemPoolManagers[MAX_REQUEST_OBJECT_TYPES];
    std::mutex reqObjMemPoolAllocatorLock;

    // request object memory management
    unordered_map<int, unordered_map<int, void *> > socketIdReqObjIdToReqObjMap;
    unordered_map<int, unordered_map<int, int> > reqObjAllocatorSocketIdReqObjIdToReqObjIdMap;
    
    // ?? We still don't know what does ReqObjIdExtractor do.
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

    // Handing txn handle_done.
    int txnSocket = -1;
    std::queue<uint64_t> txnDoneQueue;
    std::mutex txnDoneQueueLock;

    // stat counters
    int connCounter;
    int numSends, numRecvs;
    int numPacketsSentToDs, numPacketsRecvFromDs;
    Monitor monitor;

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
    void * myMallocReqObj(int socketId, int reqObjType, int reqObjId) {
        reqObjMemPoolAllocatorLock.lock(); 
        assert(reqObjMemPoolManagers[reqObjType].empty() == false);
        auto ret = reqObjMemPoolManagers[reqObjType].malloc();
        reqObjMemPoolAllocatorLock.unlock(); 
        return ret;
    }

    // int mallocReqObj(int socketId, int reqObjType, int reqObjId) {
    //     assert(false);
    //     reqObjAllocatorSocketIdReqObjIdToReqObjIdMap[socketId][reqObjId] = socketId;
    //     assert(reqObjMemPoolManagers[reqObjType].empty() == false);
    //     socketIdReqObjIdToReqObjMap[socketId][reqObjId] = reqObjMemPoolManagers[reqObjType].malloc(); // TODO lock
    //     return socketIdReqObjIdToReqObjMap[socketId][reqObjId] == nullptr ? -1 : 0;
    // }

    // It's another release. Incompatible with libVNF orginal design.
    void myFreeReqObj(int type, void * ptr){
        reqObjMemPoolManagers[type].free(ptr);
    }

    /**
     * Expects reqObjIndex to start from 0
     * */
    // void freeReqObj(int socketId, int reqObjType, int reqObjId) {
    //     assert(false);
    //     reqObjMemPoolManagers[reqObjType].free(socketIdReqObjIdToReqObjMap[socketId][reqObjId]);
    //     socketIdReqObjIdToReqObjMap[socketId].erase(reqObjId);
    //     if (socketIdReqObjIdToReqObjMap[socketId].empty()) {
    //         socketIdReqObjIdToReqObjMap.erase(socketId);
    //     }
    //     reqObjAllocatorSocketIdReqObjIdToReqObjIdMap[socketId].erase(reqObjId);
    //     if (reqObjAllocatorSocketIdReqObjIdToReqObjIdMap[socketId].empty()) {
    //         reqObjAllocatorSocketIdReqObjIdToReqObjIdMap.erase(socketId);
    //     }
    // }

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

#endif