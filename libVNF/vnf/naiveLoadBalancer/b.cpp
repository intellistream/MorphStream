#include <libvnf/core.hpp>
#include <utility>
#include <unordered_map>
#include <cassert>

using namespace vnf;

struct upstreamServer {
    int id;
    std::string ip;
    int port;
    int count;
    // std::unordered_map<ConnId*, ConnId*> connects;
    // Custom constructor to initialize ip and port
    upstreamServer(const std::string &ip_, int port_)
    : ip(ip_), port(port_), count(0){
        // Temporarily use port as id.
        id = port;
        // std::ostringstream t;
        // t << ip << ':' << port;
        // this.id = oss.str();
    }
};

// State to hold through out a request.
struct BState {
    int cCoreId;
    int cSocketId;
    int aCoreId;    // Core number and socket number to form Key later.
    int aSocketId;
};

Config config("/home/kailian/libVNF/vnf/naiveLoadBalancer/config.csv");

string mmeIp;
int mmePort;

int getKeyId(int coreId, int socketId) {
    int keyId;
    if (coreId >= 1 && coreId <= 7) {
        keyId = coreId * 100000 + socketId;
    } else {
        keyId = 800000 + socketId;
    }
    return keyId;
}

/*
    These two are doing the same things. 
        Transfer the packet to the other side.
*/

void onPacketReceivedFromTheOtherSide(ConnId& tConnId, 
    int reqObjId, void *requestObject,  // ??
    char *packet, int packetLen,    // Content of the packet.
    int errCode, int streamNum      // ??
) {
    // Construct the ConnId to the other side.
    BState *state = static_cast<BState *>(requestObject);
    int otherCoreId = state->aCoreId;
    int otherSocketId = state->aSocketId;
    if (state->aCoreId == tConnId.coreId && state->aSocketId == tConnId.socketId){
        otherCoreId = state->cCoreId;
        otherSocketId = state->cSocketId;
    }

    auto otherConn = ConnId(otherCoreId, otherSocketId);

    // Send from this Conn to the other Conn.
    char *buffer = otherConn.getPktBuf();

    std::string message = (char *) tConnId.setPktDNE((void *) packet);
    memcpy((void *) buffer, (void *) message.c_str(), message.size());
	tConnId.unsetPktDNE((void *) packet);

    otherConn.sendData(buffer, message.size());
    // TODO. Does buffer released?
}


// Guess. Connection close would cause ERROR, leading to this.
void ErrorCallback(ConnId& aConnId, 
    int reqObjId, void *requestObject,  // ??
    char *packet, int packetLen,    // Content of the packet.
    int errCode, int streamNum      // ??
) {
    // Possible. Not handled yet.
    assert(false);
    // aConnId.freeReqObj(1).delData("", keyId, LOCAL).closeConn();
}

auto onConnectionClose = ErrorCallback;

// Choose which one to send back to.
void onDatastoreReply(ConnId& aConnId, 
    int reqObjId, void *requestObject,  // The requested object is recorded to ConnId. Hereby it's fetched.
    void *value, int valueLen, int errCode) {
    /*
        Deciding which one to choose. 
        Considerations:
        1. Cache allowed. Slightly out-of dated does not really matters. 
        Problems:
        1. May use something like min Heap for efficient fetching minimum server. Simplified here.
        2. Write back counter may cause problems. Needing a more complex solution. Just omit here.
    */
    if (errCode) std::cout << errCode << std::endl;

    // Use the return value to decide the minimum server.
    /*
        This part finished the table fetching. Decide which server to send.
    */
    upstreamServer* min_server = (upstreamServer*)value;

    // 
    BState *state = static_cast<BState *>(requestObject);

    assert(min_server->ip != "" || "min_server error.");

    // New Connection To C.
    auto cConnId = aConnId \
        .createClient(mmeIp, min_server->ip, min_server->port, "tcp") \
        .registerCallback(ERROR, onConnectionClose) \
        .registerCallback(READ, onPacketReceivedFromTheOtherSide) \
        .linkReqObj(requestObject); 

    // Start a new connection just at the aConnId at. Since reqObj is core local.
    state->aSocketId = aConnId.socketId;
    state->aCoreId = aConnId.coreId;
    state->cSocketId = cConnId.socketId;
    state->cCoreId = cConnId.coreId;
    
    // Establish 2-way pipeline.
    registerCallback(aConnId, READ, onPacketReceivedFromTheOtherSide);
    registerCallback(aConnId, ERROR, onConnectionClose);
    // registerCallback(cConnId, READ, onPacketReceivedFromTheOtherSide);

    /*
        This part update the connection load for the backend server.
    */
    // Construct key, 
    int key = 0;

    // Data from datastore is versatile. Each time the datastore is full, it would be cleaned. So we need to DNE the cache.
    auto content = (int *)setCachedDSKeyDNE(key); 
    if (content == (int *)0x0) {
        assert(false);
        // Not in datastore.
        int newc = 1;
        aConnId.storeData("min_server", min_server->id, MORPH, 
            static_cast<void *> (&newc), 
            sizeof(int),
        nullptr );

        unsetCachedDSKeyDNE(0);
    } else {
        min_server->count += 1;
        aConnId.storeData("servers", min_server->id, MORPH, 
            static_cast<void *> (min_server), 
            sizeof(int),
        nullptr );

        unsetCachedDSKeyDNE(0);
    }

    // auto content = (std::vector<upstreamServer> *)setCachedDSKeyDNE(0); 
    // // Count for the minimum.
    // upstreamServer * choose = &(*content)[0];
    // int mC = choose->count;
    // // Traverse through each std::vector<upstreamServer>*
    // for (auto& server : *servers) {
    //     // Compare and update the minimum count
    //     if (server.count < mC) {
    //         mC = server.count;
    //         choose = &server;
    //     }
    // }

    // Setup Connection to C.
    // auto cConnId = aConnId \
    //     .createClient(mmeIp, choose->ip, choose->port, "tcp") \
    //     .registerCallback(ERROR, onConnectionClose) \
    //     .linkReqObj(requestObject); // Since there can be multiple connections on a single core, single ReqObj can be linked to multiple ConnIds.
    // Since there can be multiple connections on a single core, single ReqObj can be linked to multiple ConnIds.


    // // Update & write back new C.
    // choose->count++;
    // choose->connects[&aConnId] = &cConnId;

    // No need to check write in?
    // aConnId.storeData("", 0, LOCAL, servers, sizeof((void *)servers), nullptr);
    
	// Handle done. No need to preserve the data anymore.
    // aConnId.unsetPktDNE((void *) state->req);
}

// Accept new request. Construct the reqObj and fetch from the data store for proper C.
void onNewRequestFromA(ConnId& aConnId, 
    int reqObjId, void *requestObject,  // ?? What can I do with reqId? reqobj life cycle is managed with slab. No need to manually control.
    char *packet, int packetLen,    // Content of the packet.
    int errCode, int streamNum      // ??
    ) {
    requestObject = aConnId.allocReqObj(1);
    // Logic of fetching data from datastore and decide the backend.

    // Fetch data from Remote DataStore and decide the minimum data.
    aConnId.retrieveData("min_server", 1, MORPH, onDatastoreReply);
}

int LoadBalancer(int argc, char *argv[]) {
    // These are the IP:Ports to visit remote DS. For local DS, it's not needed.

    // Init.
    initLibvnf(config.coreNumbers,         // Core numbers. 1 by default.
                config.bufferSize,                                     // Buffer size.
                config.dataStoreSocketPath, 
                std::vector<int>{1,2,3,4}, // Datastore IP: Ports
                config.dataStoreThreshold,                                  // Data store threshold.
                MORPH);

    /*
        Do some other init related to morphStream here.
        TODO.
    */

    ConnId serverId = initServer("", config.serverIP, config.serverPort, "tcp");

    // Register the backend.
    registerCallback(serverId, ACCEPT, onNewRequestFromA);
    // LB: just transfer to C.
    registerCallback(serverId, READ, onPacketReceivedFromTheOtherSide);
    // LB: just transfer to C.
    registerCallback(serverId, ERROR, onConnectionClose);
    // TODO; Question: if the ACCEPT event both trigger these two?

    // request object declaration
    int requestObjectSizes[1] = {sizeof(struct BState)};
    initReqPool(requestObjectSizes, 1);

    // start vnf
    startEventLoop();

    return 0;
}

int Main(int argc, char *argv[]){
    if (argc < 5) {
        exit(0);
    } 
    return LoadBalancer(argc, argv);
}
