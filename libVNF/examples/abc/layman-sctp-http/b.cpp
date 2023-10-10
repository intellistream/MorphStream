#include <libvnf/core.hpp>
#include <spdlog/spdlog.h>

using namespace vnf;

struct BState {
    char *req;
    void *dsreq;
    int val;
    int aSocketId;
    int aCoreId;
    int streamNum;
};

string mmeIp;
int mmePort;
string neighbour1Ip;
int neighbour1Port;

int getKeyId(int coreId, int socketId) {
    int keyId;
    if (coreId >= 1 && coreId <= 7) {
        keyId = coreId * 100000 + socketId;
    } else {
        keyId = 800000 + socketId;
    }
    return keyId;
}

void onDatastoreReply(ConnId& aConnId, int reqObjId, void *requestObject, void *packet, int packetLen, int errCode) {
    // prepare key id
    int keyId = getKeyId(aConnId.coreId, aConnId.socketId);

    // some weird stuff going on
    BState *state = static_cast<BState *>(requestObject);
    state->dsreq = setCachedDSKeyDNE(keyId);
    unsetCachedDSKeyDNE(keyId);
    aConnId.unsetPktDNE((void *) state->req);

    // send data to A
    char *buffer = aConnId.getPktBuf();
    memcpy((void *) buffer, packet, 50);
    aConnId.sendData(buffer, 50, state->streamNum);

    aConnId.freeReqObj(1).delData("", keyId, LOCAL).closeConn();
}

int sendCounter = 0;
int recvCounter = 0;

void onPacketReceivedFromC(ConnId& cConnId, int reqObjId, void *requestObject, char *packet, int packetLen, int errCode, int streamNum) {
    if (packet[packetLen - 1] != '`') {
        // the http packet has not fully arrived yet
        return;
    }
    recvCounter++;

    // get values from request object [created at B when a packet is received from A]
    BState *state = static_cast<BState *>(requestObject);

    cConnId.freeReqObj(1).closeConn();

    // prepare A's connection id and key id
    int keyId = getKeyId(state->aCoreId, state->aSocketId);
    ConnId aConnId = ConnId(state->aCoreId, state->aSocketId);

    // get key value pair stored in data store [when a packet is received from A]
    aConnId.retrieveData("", keyId, LOCAL, onDatastoreReply);
}

void onPacketReceivedFromA(ConnId& aConnId, int reqObjId, void *requestObject, char *packet, int packetLen, int errCode, int streamNum) {
    cout.flush();

    // allocate request object and bind it to A's connection
    requestObject = aConnId.allocReqObj(1);

    // connect to C as a client
    ConnId cConnId = aConnId.createClient(mmeIp, neighbour1Ip, neighbour1Port, "tcp");

    // set values in request object
    BState *state = static_cast<BState *>(requestObject);
    int val = aConnId.socketId;
    for (int i = 1; i < 20000000; i++) {
        val = val + i;
    }
    state->val = val;
    state->req = (char *) aConnId.setPktDNE((void *) packet);
    state->streamNum = streamNum;

    state->aSocketId = aConnId.socketId;
    state->aCoreId = aConnId.coreId;

    // store key value pair in data store
    int keyId = getKeyId(aConnId.coreId, aConnId.socketId);
    aConnId.storeData("", keyId, LOCAL, (void *) "_______________________B->A_______________________", 50, NULL);

    // send data to C
    char *buffer = cConnId.getPktBuf();
    string httpPacket = "GET / HTTP/1.1\r\n"
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n"
            "Accept-Language: en-us\r\n"
            "Content-Length: 50\r\n"
            "\r\n"
            "This is a very good example of a 50 length messag`"
    ;
    memcpy((void *) buffer, (void *) httpPacket.c_str(), httpPacket.size());
    cConnId.linkReqObj(requestObject).registerCallback(READ, onPacketReceivedFromC).sendData(buffer, httpPacket.size());

    sendCounter++;
}

int main(int argc, char *argv[]) {
    // init libvnf
    vector<int> dataStorePorts;
    dataStorePorts.push_back(7000);
    dataStorePorts.push_back(7001);
    dataStorePorts.push_back(7002);
    dataStorePorts.push_back(7003);
    initLibvnf(1, 1024, "127.0.0.1", dataStorePorts, 131072, false);

    if (argc != 5) {
      spdlog::error("Run : ./b <client-ip-addr> <client-port> <server-ip-addr> <server-port>\n");
      exit(0);
    }

    // init network parameters
    mmeIp = argv[1];
    mmePort = atoi(argv[2]);
    neighbour1Ip = argv[3];
    neighbour1Port = atoi(argv[4]);

    // create server
    ConnId serverId = initServer("", mmeIp, mmePort, "sctp");
    // register callback to handle packets coming from A
    registerCallback(serverId, READ, onPacketReceivedFromA);

    // request object declaration
    int requestObjectSizes[1] = {sizeof(struct BState)};
    initReqPool(requestObjectSizes, 1);

    // start vnf
    startEventLoop();

    return 0;
}
