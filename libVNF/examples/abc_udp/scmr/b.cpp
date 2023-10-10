#include <libvnf/core.hpp>

using namespace vnf;

struct BState {
    char *req;
    void *dsreq;
    int val;
    int aSocketId;
    int aCoreId;
};

string mmeIp;
int mmePort;
string neighbour1Ip;
int neighbour1Port;

int uniqueId=1;
int uniqueKey=1;

unordered_map<int,unordered_map<int,unordered_map<int,int> > > keyMap;
int getKey(int coreId, int socketId, int reqObjId) {
    if(keyMap[coreId][socketId][reqObjId]==0){
        keyMap[coreId][socketId][reqObjId]= uniqueKey;
        uniqueKey++;
    }
    int keyId = keyMap[coreId][socketId][reqObjId];
    return keyId;
}

vector<int> pbdAtoB(char* buffer, int bufLen) {
    int pktLen = 50;
    vector<int> pktLengths(bufLen/pktLen,pktLen);
    return pktLengths;
}

void onDatastoreReply(ConnId& aConnId, int reqObjId, void *requestObject, void *packet, int packetLen, int errCode) {
    cout.flush();

    // prepare key id
    int dsKey = getKey(aConnId.coreId, aConnId.socketId, reqObjId);

    // some weird stuff going on
    BState *state = static_cast<BState *>(requestObject);
    state->dsreq = setCachedDSKeyDNE(dsKey);
    unsetCachedDSKeyDNE(dsKey);
    aConnId.unsetPktDNE((void *) state->req);
    
    // send data to A
    char *buffer = aConnId.getPktBuf();
    memcpy((void *) buffer, packet, 50);
    aConnId.sendData(buffer, 50).freeReqObj(1, reqObjId).delData("", dsKey, LOCAL);
}

void onPacketReceivedFromC(ConnId& cConnId, int reqObjId, void *requestObject, char *packet, int packetLen, int errCode, int streamNum) {
    // get values from request object [created at B when a packet is received from A]
    BState *state = static_cast<BState *>(requestObject);
    
    cConnId.freeReqObj(1).closeConn();

    // prepare A's connection id and key id
    ConnId aConnId = ConnId(state->aCoreId, state->aSocketId);

    int aReqObjId = 0;
    packetLen = 20;
    for (int i = 0; i < packetLen; ++i)
    {
        if(packet[i] < '0' || packet[i] > '9') {
            aReqObjId = -1;
            break;
        }
        aReqObjId *= 10;
        aReqObjId += packet[i] - '0';
    }
    // get key value pair stored in data store [when a packet is received from A]
    int dsKey = getKey(state->aCoreId, state->aSocketId, aReqObjId);
    aConnId.retrieveData("", dsKey, LOCAL, onDatastoreReply, aReqObjId);
}

void onPacketReceivedFromA(ConnId& aConnId, int reqObjId, void *requestObject, char *packet, int packetLen, int errCode, int streamNum) {
    // allocate request object and bind it to A's connection
    reqObjId = uniqueId;
    uniqueId++;
    requestObject = aConnId.allocReqObj(1, reqObjId);
    // decode core and socket ids from connection id
    int dsKey = getKey(aConnId.coreId, aConnId.socketId, reqObjId);

    // connect to C as a client
    ConnId cConnId = aConnId.createClient(mmeIp, neighbour1Ip, neighbour1Port, "udp");

    // set values in request object
    BState *state = static_cast<BState *>(requestObject);
    int val = aConnId.socketId;
    for (int i = 1; i < 20000000; i++) {
        val = val + i;
    }
    state->val = val;
    state->req = (char *) aConnId.setPktDNE((void *) packet);

    state->aSocketId = aConnId.socketId;
    state->aCoreId = aConnId.coreId;

    // store key value pair in data store
    aConnId.storeData("", dsKey, LOCAL, (void *) "B: Hey A, I am fine. Thank you!", 31, NULL);

    // send data to C
    int pktBtoCLen = 20;
    char pktBtoC[pktBtoCLen + 1];
    pktBtoC[pktBtoCLen] = '\0';
    for (int i = 0; i < pktBtoCLen; ++i)
    {
        pktBtoC[pktBtoCLen - i - 1] = '0' + reqObjId % 10;
        reqObjId /= 10;
    }
    char *buffer = cConnId.getPktBuf();
    memcpy((void *) buffer, (void *) pktBtoC, pktBtoCLen);
    cConnId.linkReqObj(requestObject).registerCallback(READ, onPacketReceivedFromC).sendData(buffer, pktBtoCLen);
}

int main(int argc, char *argv[]) {
    // init libvnf
    spdlog::set_level(spdlog::level::info);
    vector<int> dataStorePorts;
    dataStorePorts.push_back(7000);
    dataStorePorts.push_back(7001);
    dataStorePorts.push_back(7002);
    dataStorePorts.push_back(7003);

    if (argc < 5) {
        spdlog::critical("Usage: {} <b-ip> <b-port> <c-ip> <c-port>", argv[0]);
        exit(1);
    }
    initLibvnf((argc == 6 ? atoi(argv[5]) : 1), 1024, "127.0.0.1", dataStorePorts, 131072, false);

    // init network parameters
    mmeIp = argv[1];
    mmePort = atoi(argv[2]);
    neighbour1Ip = argv[3];
    neighbour1Port = atoi(argv[4]);

    // create server
    ConnId serverId = initServer("", mmeIp, mmePort, "udp");
    // register callback to handle packets coming from A
    /* registerReqObjIdExtractor(serverId, roidExtractorAtoB); */
    registerPacketBoundaryDisambiguator(serverId, pbdAtoB);
    registerCallback(serverId, READ, onPacketReceivedFromA);

    // request object declaration
    int requestObjectSizes[1] = {sizeof(struct BState)};
    initReqPool(requestObjectSizes, 1);

    // start vnf
    startEventLoop();

    return 0;
}
