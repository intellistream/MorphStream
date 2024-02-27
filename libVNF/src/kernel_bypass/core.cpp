//#include "json.hpp"
// #include "core.hpp" // Included in utils.hpp
#include "utils.hpp"

using namespace vnf;
using namespace std;

/*
    Data structure to maintain Cores. Hierarchy: Core[CoreId] -> Conn[ConnId] -> Callbacks [event]
*/

PerCoreState *perCoreStates;

/*
    Globals includes all the global information. Including:
    1. A lock based local datastore. 
        Featuring partition (Multiple tables) / Key-Value pairs. Naive version of redis.
        Strategy includes: Free after reaching threshold.
    2. 
*/
Globals globals;

int pinThreadToCore(int core_id)
{
    int numCores = sysconf(_SC_NPROCESSORS_ONLN);
    if (core_id < 0 || core_id >= numCores)
        return -1;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t currentThread = pthread_self();
    return pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &cpuset);
}

UserConfig *userConfig = nullptr;

int vnf::initLibvnf(int maxCores, int bufferSize, string dataStoreIPOrPath, vector<int> dataStorePortsOrFile, int dataStoreThreshold,
               enum DataLocation dsType) {
    userConfig = new UserConfig(maxCores, bufferSize,
                                dataStoreIPOrPath, dataStorePortsOrFile,
                                dataStoreThreshold, dsType);
    perCoreStates = new PerCoreState[userConfig->MAX_CORES];
    return 0;
}

/*
int vnf::initLibvnf(const string &jsonFilePath) {
    std::ifstream jsonFileInputStream(jsonFilePath);
    nlohmann::json json;
    jsonFileInputStream >> json;

    return initLibvnf(json["maxCores"].get<int>(),
                      json["bufferSize"].get<int>(),
                      json["dataStoreIP"].get<string>(),
                      json["dataStorePorts"].get<vector<int> >(),
                      json["dataStoreThreshold"].get<int>(),
                      json["useRemoteDataStore"].get<bool>());
}
*/
void vnf::initReqPool(int requestObjectSizes[], int numRequestObjectSizes) {
    if (numRequestObjectSizes > MAX_REQUEST_OBJECT_TYPES) {
        // todo: error handling
        spdlog::error("Maximum {} types of request objects are allowed", MAX_REQUEST_OBJECT_TYPES);
        return;
    }

    spdlog::info("Number of request object types requested: {}", numRequestObjectSizes);

    int sizesInPowersOf2[numRequestObjectSizes];
    for (int i = 0; i < numRequestObjectSizes; i++) {
        if (requestObjectSizes[i] && !(requestObjectSizes[i] & (requestObjectSizes[i] - 1))) {
            // requestObjectSizes[i] is a non-negative power of 2
            sizesInPowersOf2[i] = requestObjectSizes[i];
        } else {
            int sizeInPowersOf2 = 1; // starting from 2^0
            while (sizeInPowersOf2 < requestObjectSizes[i]) {
                sizeInPowersOf2 <<= 1;
            }
            sizesInPowersOf2[i] = sizeInPowersOf2;
        }
        spdlog::info("Request object type {} is of {} bytes", i + 1, sizesInPowersOf2[i]);
    }

    for (int coreId = 0; coreId < userConfig->MAX_CORES; coreId++) {
        for (int reqObjType = 0; reqObjType < numRequestObjectSizes; reqObjType++) {
            perCoreStates[coreId].reqObjSizesInPowersOf2[reqObjType] = sizesInPowersOf2[reqObjType];
        }
    }
}

// free memory from data store pool when threshold reached. remove cached entry without the dne bit set
void freeDSPool() {
    for (auto &it : cache_void_list) {
        if (globals.canEvictCachedDSKey(it.first)) {
            globals.cachedRemoteDatastore.erase(it.second);
            globals.localDatastore.erase(it.second);
            globals.localDatastoreLens.erase(it.second);
            globals.dsMemPoolManager.free(it.first);
        }
    }
    cache_void_list.clear();
    globals.dsSize = 0;
}

int createClientToDS(int coreId, string remoteIP, int remotePort, enum DataLocation type){
    int socketId = -1;
    int ret = -1;
    mctx_t mctx = perCoreStates[coreId].mctxFd;
    socketId = mtcp_socket(mctx, AF_INET, SOCK_STREAM, 0);
    if (socketId < 0)
    {
        spdlog::error("Failed to create listening socket!");
        return -1;
    }
    mtcp_setsock_nonblock(mctx, socketId);

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(remoteIP.c_str());
    address.sin_port = htons(remotePort);

    ret = mtcp_connect(mctx, socketId, (struct sockaddr *)&address, sizeof(struct sockaddr_in));
    perCoreStates[coreId].dsSocketProtocol = globals.serverProtocol;
    if (ret < 0 && errno != EINPROGRESS)
    {
        spdlog::error("Connect issue {}", errno);
        mtcp_close(mctx, socketId);
        return -1;
    }
    struct mtcp_epoll_event ev;
    ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
    ev.data.sockid = socketId;
    int epFd = perCoreStates[coreId].epollFd;
    mtcp_epoll_ctl(mctx, epFd, MTCP_EPOLL_CTL_ADD, socketId, &ev);

    return socketId;
    }

    void *serverThread(void *args)
    {
        struct ServerPThreadArgument argument = *((struct ServerPThreadArgument *)args);
        int coreId = argument.coreId;
        spdlog::info("Server thread started on core {}", coreId);

        mtcp_core_affinitize(coreId);
        mctx_t mctx = mtcp_create_context(coreId);
        if (!mctx)
        {
            spdlog::error("Failed to create mtcp context!");
            return nullptr;
        }
        globals.mctxLock.lock();
        perCoreStates[coreId].mctxFd = mctx;
        globals.mctxLock.unlock();

        // memory pool initialization for request objects
        for (int reqObjType = 0; reqObjType < MAX_REQUEST_OBJECT_TYPES; ++reqObjType)
        {
            perCoreStates[coreId].initMemPoolOfRequestObject(reqObjType);
        }

        // memory pool initialization for packets
        spdlog::info("Packets Memory Pool Size: {}", perCoreStates[coreId].packetMemPoolBlock.size());
        perCoreStates[coreId].packetsMemPoolManager.add_block(
            &perCoreStates[coreId].packetMemPoolBlock.front(),
            perCoreStates[coreId].packetMemPoolBlock.size(),
            1500);

        int epFd = mtcp_epoll_create(mctx, MAX_EVENTS + 5);
        if (epFd < 0)
        {
            spdlog::error("Failed to create epoll descriptor!");
            return nullptr;
        }
        globals.epollArrayLock.lock();
        perCoreStates[coreId].epollFd = epFd;
        globals.epollArrayLock.unlock();

        // listener socket creation
        int listeningSocketFd;
        listeningSocketFd = mtcp_socket(mctx, AF_INET, SOCK_STREAM, 0);
        if (listeningSocketFd < 0)
        {
            spdlog::error("Failed to create listening socket!");
            return nullptr;
        }
        int ret = mtcp_setsock_nonblock(mctx, listeningSocketFd);
        if (ret < 0)
        {
            spdlog::error("Failed to set socket in nonblocking mode.");
            return nullptr;
        }
        struct sockaddr_in saddr;

        saddr.sin_family = AF_INET;
        saddr.sin_addr.s_addr = inet_addr((argument.ip).c_str()); // inet_addr("192.168.100.2");//INADDR_ANY;
        saddr.sin_port = htons(argument.port);

        ret = mtcp_bind(mctx, listeningSocketFd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
        if (ret < 0)
        {
            spdlog::error("Failed to bind to the listening socket!");
            return nullptr;
        }

        // step 5. mtcp_listen, mtcp_epoll_ctl
        /* listen (backlog: 4K) */
        ret = mtcp_listen(mctx, listeningSocketFd, 4096); // 4096
        if (ret < 0)
        {
            spdlog::error("mtcp_listen() failed!");
            return nullptr;
        }

        struct mtcp_epoll_event ev;
        ev.events = MTCP_EPOLLIN | MTCP_EPOLLET;
        ev.data.sockid = listeningSocketFd;
        mtcp_epoll_ctl(mctx, epFd, MTCP_EPOLL_CTL_ADD, listeningSocketFd, &ev);

        // Apply txnSocket.
        perCoreStates[coreId].txnSocket = eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK); // TODO. May not compatible.
        if (perCoreStates[coreId].txnSocket == -1)
        {
            perror("fatal: failed to create event fd.");
            assert(false);
        }
        struct mtcp_epoll_event ev1;
        // TODO. set mode.
        ev.events = MTCP_EPOLLIN | MTCP_EPOLLET;
        ev1.data.sockid = perCoreStates[coreId].txnSocket;

        if (mtcp_epoll_ctl(mctx, epFd, MTCP_EPOLL_CTL_ADD, perCoreStates[coreId].txnSocket, &ev1) == -1)
        {
            spdlog::error("epoll_ctl failed, txn socket fd {},  Error {}",
                          perCoreStates[coreId].txnSocket, strerror(errno));
        }

        struct mtcp_epoll_event *epollEvents;
        epollEvents = (struct mtcp_epoll_event *)calloc(MAX_EVENTS, sizeof(struct mtcp_epoll_event));
        if (!epollEvents)
        {
            spdlog::error("Failed to create epoll event struct");
            exit(-1);
        }
        spdlog::info("Waiting for epollEvents");

        enum DataLocation _useRemoteDataStore = userConfig->USE_REMOTE_DATASTORE;
        while (!perCoreStates[coreId].isJobDone)
        {
            // connect to remote data store for first time
            if (_useRemoteDataStore == REMOTE || _useRemoteDataStore == UDS)
            {
                // Load balancing through different ports.
                if (coreId == 0)
                {
                    perCoreStates[coreId].dsSocketId1 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[0], _useRemoteDataStore);
                    perCoreStates[coreId].dsSocketId2 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[1], _useRemoteDataStore);
                }
                else
                {
                    perCoreStates[coreId].dsSocketId1 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[2], _useRemoteDataStore);
                    perCoreStates[coreId].dsSocketId2 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[3], _useRemoteDataStore);
                }
                _useRemoteDataStore = LOCAL;
            }

            // wait for epoll events
            int numEventsCaptured = mtcp_epoll_wait(mctx, epFd, epollEvents, MAX_EVENTS, -1);
            if (numEventsCaptured < 0)
            {
                if (errno != EINTR)
                {
                    perror("epoll_wait");
                }
                break;
            }
            spdlog::debug("Caught {} events\n", numEventsCaptured);

            for (int i = 0; i < numEventsCaptured; i++)
            {
                int currentSocketId = epollEvents[i].data.sockid;
                uint32_t currentEvents = epollEvents[i].events;

                /* EPOLLERR: Error  condition  happened  on  the associated file descriptor.  This event is also reported for the write end of a pipe when the read end has been closed. EPOLLRDHUP (since Linux 2.6.17): Stream socket peer closed connection, or shut down writing half of connection. (This flag is especially useful for writing simple code to detect peer shutdown when using Edge Triggered monitoring.) */
                if ((currentEvents & MTCP_EPOLLERR) || (currentEvents & MTCP_EPOLLRDHUP))
                {
                    spdlog::error("EPOLLERR or EPOLLRDHUP. Clearing pending data queue and closing socket fd");
                    if (currentSocketId == listeningSocketFd)
                    {
                        spdlog::error("Oh Oh, lsfd it is");
                        exit(-1);
                    }
                    while (!perCoreStates[coreId].isPendingDataQueueEmpty(currentSocketId))
                    {
                        PendingData dataToSend = perCoreStates[coreId].socketIdPendingDataQueueMap[currentSocketId].front();
                        perCoreStates[coreId].packetsMemPoolManager.free((void *)dataToSend.data);
                        perCoreStates[coreId].socketIdPendingDataQueueMap[currentSocketId].pop();
                    }
                    mtcp_close(mctx, currentSocketId);
                    mtcp_epoll_ctl(mctx, epFd, MTCP_EPOLL_CTL_DEL, currentSocketId, NULL);
                    continue;
                }

                /* Event occured on global listening socket. Therefore accept the connection */
                if (currentSocketId == listeningSocketFd)
                {

                    while (true)
                    {
                        int socketId = mtcp_accept(mctx, listeningSocketFd, NULL, NULL);
                        if (socketId < 0)
                        {
                            // Need lsfd non blocking to run this!!!!!!
                            if (errno != EAGAIN && errno != EWOULDBLOCK)
                            {
                                spdlog::error("Error on accept");
                            }
                            break;
                        }
#ifdef DEBUG
                        perCoreStates[coreId].monitor.conn_accepted();
#endif
                        mtcp_setsock_nonblock(mctx, socketId);
                        ev.events = MTCP_EPOLLIN;
                        ev.data.sockid = socketId;
                        mtcp_epoll_ctl(mctx, epFd, MTCP_EPOLL_CTL_ADD, socketId, &ev);

                        perCoreStates[coreId].socketProtocolMap[socketId] = globals.serverProtocol;
                        perCoreStates[coreId].connCounter++;
                        for (int eventType = 0; eventType < NUM_CALLBACK_EVENTS; ++eventType)
                        {
                            perCoreStates[coreId].socketIdCallbackMap[eventType][socketId] = argument.onAcceptByServerCallback[eventType];
                        }
                        perCoreStates[coreId].socketIdDSCallbackMap[socketId] = argument.onAcceptByServerDSCallback;
                        perCoreStates[coreId].socketIdReqObjIdExtractorMap[socketId] = argument.onAcceptByServerReqObjIdExtractor;
                        perCoreStates[coreId].socketIdPBDMap[socketId] = argument.onAcceptByServerPBD;
                        CallbackFn callback = perCoreStates[coreId].socketIdCallbackMap[ACCEPT][socketId];
                        ConnId connId = ConnId(coreId, socketId);
                        if (callback)
                        {
                            callback(connId, 0, nullptr, nullptr, perCoreStates[coreId].packetNumber++, 0, 0, 0);
                        }
                    }

                    continue;
                }

                /* EPOLLIN: The associated file is available for read(2) operations. */
                if (currentEvents & MTCP_EPOLLIN)
                {
                    // Some handler done. Now we are going to trigger the continue handling.
                    if (perCoreStates[coreId].txnSocket == currentSocketId)
                    {

                        // FIXME. To delete. Read to decrease from somaphore here.
                        uint64_t wasted = 0;
                        if (read(currentSocketId, &wasted, sizeof(uint64_t)) < 0)
                        {
                            perror("serverThread.transactions.read");
                        }

                        perCoreStates[coreId].txnDoneQueueLock.lock();
                        uint64_t txnReqId = perCoreStates[coreId].txnDoneQueue.front();
                        perCoreStates[coreId].txnDoneQueue.pop();
                        perCoreStates[coreId].txnDoneQueueLock.unlock();
                        assert(COREID(txnReqId) == coreId);

                        // Prepare resources here.
                        auto ctx = perCoreStates[coreId].packetNumberContextMap[PACKETID(txnReqId)];

                        ConnId connId = ConnId(coreId, ctx->_old_socket());
                        ctx->_move_next();

                        // TODO. Debug. Set the cache.
                        // No need to modify to use the cache. Since it's totally inside.
                        // void *dsMalloc;
                        // globals.dataStoreLock.lock();
                        // if (globals.dsSize == userConfig->DATASTORE_THRESHOLD) {
                        //     freeDSPool();
                        // }
                        // // cache the state
                        // dsMalloc = globals.dsMemPoolManager.malloc();
                        // globals.dsSize++;
                        // memcpy(dsMalloc, buffer.c_str(), buffer.length());
                        // // The local datastore is used as cache for reading.
                        // globals.localDatastore[bufKey] = dsMalloc;
                        // globals.localDatastoreLens[bufKey] = buffer.length();
                        // globals.cachedRemoteDatastore[bufKey] = dsMalloc;
                        // cache_void_list[dsMalloc] = bufKey;
                        // globals.dataStoreLock.unlock();

                        // TODO. Can receive error here.

                        // _disposalBody(connId, o->reqObjId,
                        //             perCoreStates[coreId].socketIdReqObjIdToReqObjMap[o->old_socket][o->reqObjId],
                        //             o->packet_record, o->packet_len, 0);

#ifdef DEBUG
                        perCoreStates[coreId].monitor.update_latency(3, ctx->_full_ts());
#endif

                        _disposalBody(connId, *ctx);

                        // This is allocated when txn triggered. So erase in handle done.
                        perCoreStates[coreId].packetNumberContextMap.erase(
                            perCoreStates[coreId].packetNumberContextMap.find(PACKETID(txnReqId)));

#ifdef DEBUG
                        perCoreStates[coreId].monitor.packet_done();
#endif
                        // TODO. How to recover packets here.
                        continue;
                    }
                    /* Current socketId points to remote datastore
                        Each time we communicate with the data store, we are using the specified socket (Special IP and ports).
                        So we can decide this is the message from the database.
                    *
                        /* Current socketId points to remote datastore */
                    if (perCoreStates[coreId].isDatastoreSocket(currentSocketId))
                    {
                        /* TODO: fix this shit */
                        while (true)
                        {
                            DSPacketHandler pkt;
                            int pkt_len, retval;
                            pkt.clear_pkt();
                            retval = readFromStream(mctx, currentSocketId, pkt.data, sizeof(int));
                            if (retval < 0)
                            {
                                if (errno == EAGAIN)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                perCoreStates[coreId].numPacketsRecvFromDs++;
                                memmove(&pkt_len, pkt.data, sizeof(int) * sizeof(uint8_t));
                                pkt.clear_pkt();
                                retval = readFromStream(mctx, currentSocketId, pkt.data, pkt_len);
                                pkt.data_ptr = 0;
                                pkt.len = retval;
                                if (retval < 0)
                                {
                                    spdlog::error("Error: Packet from HSS Corrupt, break");
                                    break;
                                }
                            }
                            int socketId, bufKey;
                            string buffer;
                            pkt.extract_item(socketId);
                            pkt.extract_item(bufKey);
                            pkt.extract_item(buffer);
                            buffer += '\0';

                            void *dsMalloc;
                            globals.dataStoreLock.lock();
                            if (globals.dsSize == userConfig->DATASTORE_THRESHOLD)
                            {
                                freeDSPool();
                            }
                            // cache the state
                            dsMalloc = globals.dsMemPoolManager.malloc();
                            globals.dsSize++;
                            memcpy(dsMalloc, buffer.c_str(), buffer.length());
                            globals.localDatastore[bufKey] = dsMalloc;
                            globals.localDatastoreLens[bufKey] = buffer.length();
                            globals.cachedRemoteDatastore[bufKey] = dsMalloc;
                            cache_void_list[dsMalloc] = bufKey;
                            globals.dataStoreLock.unlock();

                            ConnId connId = ConnId(coreId, socketId);

                            // request object id extractor from packet
                            ReqObjExtractorFn extractor = perCoreStates[coreId].socketIdReqObjIdExtractorMap[socketId];
                            int reqObjId = extractor == nullptr ? 0 : extractor(const_cast<char *>(buffer.c_str()), 0); // fix me : replace 0 packetlen with actual packet len
                            cout.flush();

                            // callback
                            DSCallbackFn callback = perCoreStates[coreId].socketIdDSCallbackMap[socketId];
                            callback(connId, reqObjId,
                                     perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId],
                                     (void *)buffer.c_str(),
                                     perCoreStates[coreId].packetNumber++,
                                     // The buffer size needs to be resigned before..
                                     userConfig->BUFFER_SIZE, 0);
                        }

                        continue;
                    }

                    /* Current socketId points to non-remote datastore network function */
                    int socketId = currentSocketId;
                    char buffer[userConfig->BUFFER_SIZE];
                    int numBytesRead;
                    string currentProtocol = perCoreStates[coreId].socketProtocolMap[socketId];
                    int streamNum = -1;
                    /* read from socket as per the protocol specified */
                    numBytesRead = (int) mtcp_read(mctx, socketId, buffer, (size_t) userConfig->BUFFER_SIZE);

                /* numBytesRead == -1 for errors & numBytesRead == 0 for EOF */
                if (numBytesRead <= 0) {
                    //spdlog::error("Read error on non-remote datastore socket. Trying to close the socket");
                    if (mtcp_close(mctx, socketId) < 0) {
                        spdlog::error("Connection could not be closed properly");
                    }
                    continue;
                }

                perCoreStates[coreId].numRecvs++;

                /* packet boundary disambiguation */
                string prependedBuffer = perCoreStates[coreId].getLeftOverPacketFragment(socketId);
                for (int j = 0; j < numBytesRead; j++) {
                    prependedBuffer.append(1, buffer[j]);
                }
                vector<int> packetLengths(1, numBytesRead);
                PacketBoundaryDisambiguatorFn pbd = perCoreStates[coreId].socketIdPBDMap[socketId];
                if (pbd != nullptr) {
                    packetLengths = pbd((char *) prependedBuffer.c_str(), prependedBuffer.size());
                }

                int packetStart = 0;
                ConnId connId = ConnId(coreId, socketId);
                for (int packetLength : packetLengths) {
                    /* allocate heap and put current packet copy there for user to use */
                    assert(perCoreStates[coreId].packetsMemPoolManager.empty() == false);
                    void *packet = perCoreStates[coreId].packetsMemPoolManager.malloc();
                    perCoreStates[connId.coreId].monitor.allocate_packet();
                    assert(packet != NULL);
                    string currentPacket = prependedBuffer.substr((uint) packetStart, (uint) packetLength);
                    memcpy(packet, currentPacket.c_str(), (size_t) (packetLength));

                    /* request object id extraction from packet */
                    ReqObjExtractorFn extractor = perCoreStates[coreId].socketIdReqObjIdExtractorMap[socketId];
                    int reqObjId = extractor == nullptr ? 0 : extractor((char *) packet, packetLength);
                    cout.flush();

                    /* user-defined on read callback function */
                    CallbackFn callback = perCoreStates[coreId].socketIdCallbackMap[READ][socketId];
                    void *reqObj = perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId];
                    callback(connId, 0, nullptr, (char *)packet, packetLength, perCoreStates[coreId].packetNumber++, 0, streamNum);
                    // callback(connId, 0, nullptr, nullptr, perCoreStates[coreId].packetNumber++, 0, 0, 0);
                    packetStart += packetLength;

                    /* free heap previously allocated for packet if evicatable */
                    if (perCoreStates[coreId].canEvictPacket(packet)) {
                        perCoreStates[coreId].packetsMemPoolManager.free(packet);
                    }
                }

                // finally store the left over part in buffer
                perCoreStates[coreId].setLeftOverPacketFragment(socketId, prependedBuffer.substr((uint) packetStart, prependedBuffer.size() - packetStart));

                continue;
            }

            /* EPOLLOUT: The associated file is available for write(2) operations. */
            if (currentEvents & MTCP_EPOLLOUT) {
                int socketId = currentSocketId;
                while (!perCoreStates[coreId].isPendingDataQueueEmpty(socketId)) {
                    PendingData dataToSend = perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].front();
                    int ret;
                    ret = mtcp_write(mctx, socketId, dataToSend.data, dataToSend.dataLen);
                    perCoreStates[coreId].packetsMemPoolManager.free((void *) dataToSend.data);
                    perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].pop();
                    if (ret < 0) {
                        spdlog::error("Connection closed with client");
			mtcp_epoll_ctl(mctx, epFd, MTCP_EPOLL_CTL_DEL, socketId, NULL);
                        mtcp_close(mctx, socketId);
                        break;
                    }
                }

                continue;
            }
        }
    }

    return NULL;
}

void* vnf::ConnId::allocReqObj(int reqObjType, int reqObjId) {
    int coreId = this->coreId;
    int socketId = this->socketId;
    return perCoreStates[coreId].myMallocReqObj(socketId, reqObjType - 1, reqObjId);

    // int ret = perCoreStates[coreId].mallocReqObj(socketId, reqObjType - 1, reqObjId);
    // if (ret) {
    //     spdlog::error("Could not allocate request object on ConnId(coreId, socketId): ({}, {})", this->coreId, this->socketId);
    // }
    // return perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId];
}

ConnId& vnf::ConnId::freeReqObj(int reqObjType, int reqObjId) {
    int coreId = this->coreId;
    int socketId = this->socketId;
    if (perCoreStates[coreId].isARequestObjectAllocator(socketId, reqObjId)) {
        assert(false);
        // perCoreStates[coreId].freeReqObj(socketId, reqObjType - 1, reqObjId);
    } else {
        perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId].erase(reqObjId);
        if (perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId].empty()) {
            perCoreStates[coreId].socketIdReqObjIdToReqObjMap.erase(socketId);
        }
    }

    return *this;
}

ConnId& vnf::ConnId::linkReqObj(void *requestObj, int reqObjId) {
    int coreId = this->coreId;
    int socketId = this->socketId;
    perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId] = requestObj;

    return *this;
}

void* vnf::ConnId::setPktDNE(void *packet) {
    int coreId = this->coreId;
    perCoreStates[coreId].tagPacketNonEvictable(packet);
    return packet; // fixme
}

ConnId& vnf::ConnId::unsetPktDNE(void *packet) {
    int coreId = this->coreId;
    perCoreStates[coreId].packetsMemPoolManager.free(packet);
    perCoreStates[coreId].tagPacketEvictable(packet);

    return *this;
}

char* vnf::ConnId::getPktBuf() {
    int coreId = this->coreId;
    char *buffer = static_cast<char *>(perCoreStates[coreId].packetsMemPoolManager.malloc());
    return buffer;
}

void* vnf::setCachedDSKeyDNE(int dsKey) {
    /* TODO: fix this */
    void *value;

    globals.dataStoreLock.lock();
    globals.doNotEvictCachedDSValueKeyMap[globals.localDatastore[dsKey]] = dsKey;
    value = globals.localDatastore[dsKey];
    globals.dataStoreLock.unlock();

    return value; // fixme
}

void vnf::unsetCachedDSKeyDNE(int dsKey) {
    /* TODO: fix this */
    globals.dataStoreLock.lock();
    globals.doNotEvictCachedDSValueKeyMap.erase(globals.localDatastore[dsKey]);
    globals.dataStoreLock.unlock();
}

ConnId vnf::initServer(string iface, string serverIp, int serverPort, string protocol) {
    assert(userConfig != nullptr);
    signal(SIGPIPE, SIG_IGN);
    globals.serverIp = serverIp;
    globals.serverPort = serverPort;
    globals.serverProtocol = protocol;
    spdlog::info("Server initialized with IP: {} and port {}", serverIp, serverPort);
    return FIRST_TIME_CONN_ID;
}

// Use nested hashmaps as callback routers.
ConnId &vnf::ConnId::registerCallback(enum EventType eventType, vnf::CallbackFn callback)
{
    if (*this == FIRST_TIME_CONN_ID) {
        globals.onAcceptByServerCallback[eventType] = callback;
    } else {
        int coreId = this->coreId;
        int socketId = this->socketId;
        if (eventType == ERROR) {
            perCoreStates[coreId].socketIdErrorCallbackMap[socketId] = callback;
        } else {
            perCoreStates[coreId].socketIdCallbackMap[eventType][socketId] = callback;
        }
    }

    return *this;
}

ConnId& vnf::ConnId::registerReqObjIdExtractor(int roidExtractor(char * packet, int packetLen)) {
    if (*this == FIRST_TIME_CONN_ID) {
        globals.onAcceptByServerReqObjIdExtractor = roidExtractor;
    } else {
        int coreId = this->coreId;
        int socketId = this->socketId;
        perCoreStates[coreId].socketIdReqObjIdExtractorMap[socketId] = roidExtractor;
    }

    return *this;
}

ConnId& vnf::ConnId::registerPacketBoundaryDisambiguator(vector<int> pbd(char *buffer, int bufLen)) {
    if (*this == FIRST_TIME_CONN_ID) {
        globals.onAcceptByServerPBD = pbd;
    } else {
        int coreId = this->coreId;
        int socketId = this->socketId;
        perCoreStates[coreId].socketIdPBDMap[socketId] = pbd;
    }

    return *this;
}

void sigINTHandler(int signalCode) {
    for (int i = 0; i < userConfig->MAX_CORES; i++) {
        spdlog::critical("\nOn Core: {}\n\tNo. Accepted Connections: {}\n\tNo. Packets Recv: {}\n\tNo. Packets Sent: {}\n", i, perCoreStates[i].connCounter, perCoreStates[i].numRecvs, perCoreStates[i].numSends);
	mctx_t mctx = perCoreStates[i].mctxFd;
        mtcp_close(mctx, perCoreStates[i].dsSocketId1);
        mtcp_close(mctx, perCoreStates[i].dsSocketId2);
        perCoreStates[i].isJobDone = true;
    }
    fflush(stdout);
    exit(signalCode);
}

void *monitorThread(void *args)
{
    struct ServerPThreadArgument argument = *((struct ServerPThreadArgument *)args);
    int coreId = argument.coreId;
    pinThreadToCore(coreId);
    spdlog::info("Monitor thread started on core {}", coreId);

    while (true)
    {
        for (int i = 0; i < globals.config.coreNumbers; i += 1)
        {
            perCoreStates[i].monitor.report(i);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(globals.config.monitorInterval));
    }
}

void vnf::startEventLoop() {
    spdlog::info("Event loop is started");
    /* initialize mtcp */
    char *conf_file = "server.conf";
    if (conf_file == NULL) {
	spdlog::error("You forgot to pass the mTCP startup config file!");
        exit(-1);
    } else {
        spdlog::info("Reading configuration from: {}", conf_file);
    }
    int ret = -1;
    ret = mtcp_init(conf_file);
    if (ret) {
        spdlog::error("Failed to initialize mtcp");
        exit(-1);
    }

    /* register signal handler to mtcp */
    mtcp_register_signal(SIGINT, sigINTHandler);


    spdlog::info("Datastore Memory Pool Size: {}", globals.dsMemPoolBlock.size());
    globals.dataStoreLock.lock();
    globals.dsMemPoolManager.add_block(&globals.dsMemPoolBlock.front(), globals.dsMemPoolBlock.size(), 256);
    globals.dataStoreLock.unlock();

    auto *servers = new pthread_t[userConfig->MAX_CORES];
    auto *arguments = new struct ServerPThreadArgument[userConfig->MAX_CORES + 1];
  //  auto  mctx_arr = new mctx_t[userConfig->MAX_CORES];
//    auto  ep_arr = new int[userConfig->MAX_CORES];
    for (int ithCore = 0; ithCore < userConfig->MAX_CORES; ithCore++) {
        arguments[ithCore].set(ithCore, globals.serverIp, globals.serverPort, globals.onAcceptByServerCallback,
                               globals.onAcceptByServerDSCallback,
                               globals.onAcceptByServerReqObjIdExtractor,
                               globals.onAcceptByServerPBD);

        // todo start client only after all threads have started or it would clear actual sockid mappings
        pthread_create(&servers[ithCore], NULL, serverThread, &arguments[ithCore]);
    }

#if DEBUG
    arguments[userConfig->MAX_CORES].set(userConfig->MAX_CORES, globals.serverIp, globals.serverPort, globals.onAcceptByServerCallback,
                                         globals.onAcceptByServerDSCallback,
                                         globals.onAcceptByServerReqObjIdExtractor,
                                         globals.onAcceptByServerPBD);
    pthread_create(&servers[userConfig->MAX_CORES], NULL, monitorThread, &arguments[userConfig->MAX_CORES]);
#endif

    for (int i = 0; i < userConfig->MAX_CORES; i++) {
        pthread_join(servers[i], NULL);
    }
}

ConnId vnf::ConnId::createClient(string localIP, string remoteIP, int remotePort, string protocol) {
    int sockId;
    int coreId = this->coreId;
    mctx_t mctx = perCoreStates[coreId].mctxFd;
    sockId = mtcp_socket(mctx, AF_INET, SOCK_STREAM, 0);
    if (sockId < 0) {
        spdlog::error("Failed to create listening socket! {}", errno);
        return ConnId(-1);
    }

    int ret = mtcp_setsock_nonblock(mctx,sockId);
    perCoreStates[coreId].socketProtocolMap[sockId] = protocol;
    if (ret < 0) {
        spdlog::error("Failed to set socket in nonblocking mode.");
        return ConnId(-1);
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(remoteIP.c_str());
    address.sin_port = htons(remotePort);

    ret = mtcp_connect(mctx, sockId, (struct sockaddr *) &address, sizeof(struct sockaddr_in));
    if (ret < 0 && errno != EINPROGRESS) {
        spdlog::error("connect issue {}", errno);
        mtcp_close(mctx, sockId);
        return ConnId(-1);
    }

    struct mtcp_epoll_event epollEvent;
    epollEvent.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
    epollEvent.data.sockid = sockId;
//    int coreId = this->coreId;
    int epFd = perCoreStates[coreId].epollFd;
    mtcp_epoll_ctl(mctx, epFd, MTCP_EPOLL_CTL_ADD, sockId, &epollEvent);

    return ConnId(coreId, sockId);
}

ConnId& vnf::ConnId::sendData(char *data, int dataLen, int streamNum) {
    int coreId = this->coreId;
    int socketId = this->socketId;

    // TODO: learn why this check is made
    if (socketId == -1) {
        spdlog::error("SocketId issue");
        exit(-1);
    }

    if (!perCoreStates[coreId].isPendingDataQueueEmpty(socketId)) {
        PendingData dataToSend(data, dataLen);
        perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].push(dataToSend);
        spdlog::warn("sendData: adding packet to queue as queue is already not empty");
        return *this;
    }

    int ret;
    string currentProtocol;
    if (perCoreStates[coreId].isDatastoreSocket(socketId))
    {
        currentProtocol = perCoreStates[coreId].dsSocketProtocol;
    }
    else
    {
        currentProtocol = perCoreStates[coreId].socketProtocolMap[socketId];
    }
    // string currentProtocol = perCoreStates[coreId].socketProtocolMap[socketId];
    mctx_t mctx = perCoreStates[coreId].mctxFd;
    ret = mtcp_write(mctx,socketId, data, dataLen);
    int errnoLocal = errno;

    if (ret < 0) {
        if (errnoLocal == ENOTCONN) {
            PendingData dataToSend(data, dataLen, streamNum);
            perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].push(dataToSend);
            spdlog::warn("sendData: adding packet to queue, not sent successfully");
        } else {
            spdlog::error("sendData: error on write {}", errnoLocal);
            perCoreStates[coreId].packetsMemPoolManager.free((void *) data);
        }
        return *this;
    }

    if (ret != dataLen) {
        PendingData dataToSend(data, dataLen, streamNum);
        perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].push(dataToSend);
        spdlog::warn("sendData: packet complete not sent, added it to pendingDataQueue");
        return *this;
    }

    if (perCoreStates[coreId].isDatastoreSocket(socketId)) {
        perCoreStates[coreId].numPacketsSentToDs++;
    } else {
        perCoreStates[coreId].packetsMemPoolManager.free((void *) data);
        perCoreStates[coreId].numSends++;
    }


    return *this;
}

void registerDSCallback(ConnId &connId, enum EventType eventType, DSCallbackFn callback)
{
    if (connId == FIRST_TIME_CONN_ID) {
        globals.onAcceptByServerDSCallback = callback;
    } else {
        int coreId = connId.coreId;
        int socketId = connId.socketId;
        if (eventType == ERROR) {
            perCoreStates[coreId].socketIdDSErrorCallbackMap[socketId] = callback;
        } else {
            perCoreStates[coreId].socketIdDSCallbackMap[socketId] = callback;
        }
    }
}

ConnId &vnf::ConnId::storeData(string tableName, int key, enum DataLocation location, void *value, int valueLen, DSCallbackFn callback)
{
    if (callback != nullptr)
    {
        // todo call this somewhere
        registerDSCallback(*this, ERROR, callback);
    }

    if (location == REMOTE || location == UDS)
    {
        /* TODO: fix this shit */
        int coreId = this->coreId;
        int socketId = this->socketId;
        char *s2;

        globals.dataStoreLock.lock();

        if (globals.dsSize == userConfig->DATASTORE_THRESHOLD) {
            freeDSPool();
        }

        /* TODO: Check in cache before allocating */
        /* TODO: Why same dsMemPoolManager for both cache and localDs */
        void *dsMalloc = globals.dsMemPoolManager.malloc();
        globals.dsSize++;
        memcpy(dsMalloc, value, static_cast<size_t>(valueLen) + 1);
        /* TODO: check if these lines can be commented */
        /* globals.localDatastore[key] = dsMalloc; */
        /* globals.localDatastoreLens[key] = valueLen; */
        globals.cachedRemoteDatastore[key] = dsMalloc;
        cache_void_list[dsMalloc] = key;

        globals.dataStoreLock.unlock();

        string snd_cmd = "set", snd_table = "abc", snd_value = "xyz";
        DSPacketHandler packetHandler;
        packetHandler.clear_pkt();
        packetHandler.append_item(socketId);
        packetHandler.append_item(snd_cmd);
        packetHandler.append_item(snd_table);
        packetHandler.append_item(key);
        s2 = (char *) (value);
        string s3(s2);
        packetHandler.append_item(s3);
        packetHandler.prepend_len();

        if (perCoreStates[coreId].dsSockIdSetLooper == 0) {
            ConnId dsConnId = ConnId(coreId, perCoreStates[coreId].dsSocketId1);
            dsConnId.sendData((char *) packetHandler.data, packetHandler.len);
            perCoreStates[coreId].dsSockIdSetLooper = 1;
        } else {
            ConnId dsConnId = ConnId(coreId, perCoreStates[coreId].dsSocketId2);
            dsConnId.sendData((char *) packetHandler.data, packetHandler.len);
            perCoreStates[coreId].dsSockIdSetLooper = 0;
        }

        return *this;
    }

    if (location == LOCAL) {
        globals.dataStoreLock.lock();

        if (globals.dsSize == userConfig->DATASTORE_THRESHOLD) {
            freeDSPool();
        }

        void *dsMalloc;
        if (globals.keyExistsInLocalDatastore(key)) {
            dsMalloc = globals.localDatastore[key];
        } else {
            dsMalloc = globals.dsMemPoolManager.malloc();
            globals.dsSize++;
        }
        memcpy(dsMalloc, value, static_cast<size_t>(valueLen) + 1);
        globals.localDatastore[key] = dsMalloc;
        globals.localDatastoreLens[key] = valueLen;

        globals.dataStoreLock.unlock();

        return *this;
    }

    spdlog::warn("storeData: Unknown location used {}", location);

    return *this;
}

ConnId &vnf::ConnId::retrieveData(string tableName, int key, enum DataLocation location, vnf::DSCallbackFn callback, int reqObjId)
{
    int coreId = this->coreId;
    int socketId = this->socketId;

    /* TODO: fix this shit */
    // if checkcache option retrieve from cache if entry exists else fetch from remote store
    // store the callback to be called after value retrieved
    if (location == CHECKCACHE) {
        // todo check this
        registerDSCallback(*this, READ, callback);
        bool isInCache = false;

        globals.dataStoreLock.lock();
        isInCache = globals.keyExistsInCachedRemoteDatastore(key);
        globals.dataStoreLock.unlock();

        if (isInCache) {
            char *packet = static_cast<char *>(globals.cachedRemoteDatastore[key]);
            // callback
            callback(*this, reqObjId,
                     perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId],
                     packet, userConfig->BUFFER_SIZE, 0, 0);
        } else {
            string sndCmd = "get";
            DSPacketHandler packetHandler;
            packetHandler.clear_pkt();
            sndCmd = "get";
            packetHandler.append_item(socketId);
            packetHandler.append_item(sndCmd);
            packetHandler.append_item(tableName);
            packetHandler.append_item(key);
            packetHandler.prepend_len();

            if (perCoreStates[coreId].dsSockIdGetLooper == 0) {
                ConnId dsConnId = ConnId(coreId, perCoreStates[coreId].dsSocketId1);
                dsConnId.sendData((char *) packetHandler.data, packetHandler.len);
                perCoreStates[coreId].dsSockIdGetLooper = 1;
            } else {
                ConnId dsConnId = ConnId(coreId, perCoreStates[coreId].dsSocketId2);
                dsConnId.sendData((char *) packetHandler.data, packetHandler.len);
                perCoreStates[coreId].dsSockIdGetLooper = 0;
            }
            // todo check this
        }

        return *this;
    }

    if (location == REMOTE) {
        // todo check this
        registerDSCallback(*this, READ, callback);

        string sndCmd = "get";
        DSPacketHandler packetHandler;
        packetHandler.clear_pkt();
        packetHandler.append_item(socketId);
        packetHandler.append_item(sndCmd);
        packetHandler.append_item(tableName);
        packetHandler.append_item(key);
        packetHandler.prepend_len();

        if (perCoreStates[coreId].dsSockIdGetLooper == 0) {
            ConnId dsConnId = ConnId(coreId, perCoreStates[coreId].dsSocketId1);
            dsConnId.sendData((char *) packetHandler.data, packetHandler.len);
            perCoreStates[coreId].dsSockIdGetLooper = 1;
        } else {
            ConnId dsConnId = ConnId(coreId, perCoreStates[coreId].dsSocketId2);
            dsConnId.sendData((char *) packetHandler.data, packetHandler.len);
            perCoreStates[coreId].dsSockIdGetLooper = 0;
        }
        // todo check this

        return *this;
    }

    if (location == LOCAL) {
        globals.dataStoreLock.lock();

        int packetLen = globals.localDatastoreLens[key];
        char *value = (char *) (globals.localDatastore[key]);

        char valueCopy[packetLen + 1];
        for (int i = 0; i < packetLen; ++i) {
            valueCopy[i] = value[i];
        }
        valueCopy[packetLen] = 0;

        globals.dataStoreLock.unlock();

        callback(*this, reqObjId, perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId], valueCopy, userConfig->BUFFER_SIZE, 0, 0);
        return *this;
    }

    spdlog::warn("retrieveData: Unknown location used {}", location);

    return *this;
}

ConnId& vnf::ConnId::delData(string tableName, int key, enum DataLocation location) {
    if (location == REMOTE) {
        /* TODO: fix this shit */
        if (globals.keyExistsInCachedRemoteDatastore(key)) {
            globals.dataStoreLock.lock();
            cache_void_list.erase(globals.cachedRemoteDatastore[key]);
            globals.dsMemPoolManager.free(globals.cachedRemoteDatastore[key]);
            globals.cachedRemoteDatastore.erase(key);
            globals.localDatastore.erase(key);
            globals.dataStoreLock.unlock();
        }
        // todo : complete this
        return *this;
    }

    if (location == LOCAL) {
        globals.dataStoreLock.lock();

        globals.dsMemPoolManager.free(globals.localDatastore[key]);
        globals.localDatastore.erase(key);

        globals.dataStoreLock.unlock();
        return *this;
    }

    spdlog::warn("delData: Unknown location used {}", location);

    return *this;
}

void vnf::ConnId::closeConn() {
    int socketId = this->socketId;
    int coreId = this->coreId;
    mctx_t mctx = perCoreStates[coreId].mctxFd;
    while (!perCoreStates[coreId].isPendingDataQueueEmpty(socketId)) {
        PendingData dataToSend = perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].front();
        int ret;
        ret = mtcp_write(mctx, socketId, dataToSend.data, dataToSend.dataLen);
        perCoreStates[coreId].packetsMemPoolManager.free((void *) dataToSend.data);
        perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].pop();
        if (ret < 0) {
            spdlog::error("Connection closed with client");
            mtcp_close(mctx, socketId);
            break;
        }
    }
    perCoreStates[coreId].delLeftOverPacketFragment(socketId);
    // todo free callback registrations
    perCoreStates[coreId].socketProtocolMap.erase(socketId);
    mtcp_close(mctx, socketId);
}

void * vnf::allocReqObj(ConnId& connId, int reqObjType, int reqObjId) {
  return connId.allocReqObj(reqObjType, reqObjId);
}

ConnId& vnf::freeReqObj(ConnId connId, int reqObjType, int reqObjId) {
  return connId.freeReqObj(reqObjType, reqObjId);
}

ConnId& vnf::linkReqObj(ConnId connId, void *requestObj, int reqObjId) {
  return connId.linkReqObj(requestObj, reqObjId);
}

void * vnf::setPktDNE(ConnId& connId, void *packet) {
  return connId.setPktDNE(packet);
}

ConnId& vnf::unsetPktDNE(ConnId connId, void *packet) {
  return connId.unsetPktDNE(packet);
}

char * vnf::getPktBuf(ConnId& connId) {
  return connId.getPktBuf();
}

ConnId &vnf::registerCallback(ConnId &connId, enum EventType event, vnf::CallbackFn callback)
{
    return connId.registerCallback(event, callback);
}

ConnId& vnf::registerReqObjIdExtractor(ConnId& connId, int roidExtractor(char *packet, int packetLen)) {
  return connId.registerReqObjIdExtractor(roidExtractor);
}

ConnId& vnf::registerPacketBoundaryDisambiguator(ConnId& connId, vector<int> pbd(char *buffer, int bufLen)) {
  return connId.registerPacketBoundaryDisambiguator(pbd);
}

ConnId vnf::createClient(ConnId& connId, string localIP, string remoteIP, int remotePort, string protocol) {
  return connId.createClient(localIP, remoteIP, remotePort, protocol);
}

ConnId& vnf::sendData(ConnId& connId, char *packetToSend, int packetSize, int streamNum) {
  return connId.sendData(packetToSend, packetSize, streamNum);
}

ConnId &vnf::storeData(ConnId &connId, string tableName, int key, enum DataLocation location, void *value, int valueLen, vnf::DSCallbackFn errorCallback)
{
    return connId.storeData(tableName, key, location, value, valueLen, errorCallback);
}

ConnId &vnf::retrieveData(ConnId &connId, string tableName, int key, enum DataLocation location, vnf::DSCallbackFn callback, int reqObjId)
{
    return connId.retrieveData(tableName, key, location, callback, reqObjId);
}

ConnId& vnf::delData(ConnId& connId, string tableName, int key, enum DataLocation location) {
  return connId.delData(tableName, key, location);
}

void vnf::closeConn(ConnId& connId) {
  return connId.closeConn();
}

void defaultTimeOutFunction(timer *t){
	spdlog::debug("Default Timeout Function for fd {}, Timer Attempt Remaining : {}",
		t->getFd(),t->retries);
	t->retries-=1;
	if(t->retries<=0)
	{
		//De-register the timer
		t->stopTimer();
		return;
	}
	/*Following Code is necessity for reloading Timer,Reason Unknown....*/
	struct itimerspec temp;
	int rc=timerfd_gettime(t->getFd(),&temp);
	/*timerfd_gettime return 0 on success and -1 on failure*/
	if(rc==-1)
	{
		spdlog::debug("Failed in reloading the timer and extracting the current bufferes from fd {}",t->getFd());
	}
}

timer::timer(int coreId){
	/*Initially default duration set to 6 Sec*/
	this->duration=TIMER_DEFAULT_DURATION;
	this->retries=TIMER_DEFAULT_RETRIES;
	this->fd=-1;
    this->coreId = coreId;
    this->timeOutFunction = defaultTimeOutFunction;
}

int timer::getFd(){
	return this->fd;
}

timer::~timer(){
    // LOG_ENTRY;
	this->stopTimer();
    // LOG_EXIT;
}

void timer::startTimer(int duration,int retries)
{
    // LOG_ENTRY;

    mctx_t mctx = perCoreStates[coreId].mctxFd;
	struct mtcp_epoll_event ev = {};
	struct itimerspec iterationDetails = {};
	if(this->fd==-1){
		/*New fd is created for timer,required while executing for first time*/
		this->fd=timerfd_create(CLOCK_REALTIME, 0);
        if(this->fd==-1){
            spdlog::error("Error in creating fd for Timer : {} ",strerror(errno));
            return;
        }
		/*this fd is added in the map to retrieve back entire object 
		when it will get triggered from*/ 
		perCoreStates[coreId].fdToObjectMap[this->fd]=this;
	}
	this->duration=(duration<0)?TIMER_DEFAULT_DURATION:duration;
	this->retries=(retries<0)?TIMER_DEFAULT_RETRIES:retries;
	struct timespec now = {};
	//Get Current time
	if (clock_gettime(CLOCK_REALTIME, &(now)) == -1)
		spdlog::debug("Error in getting Clock");/*Has not occured yet*/
	//Configuring Event for Fd created
	ev.events=MTCP_EPOLLIN;
	ev.data.sockid=this->fd;
	//Configuring Duration of timer
	iterationDetails.it_value.tv_sec = now.tv_sec + duration;
	iterationDetails.it_value.tv_nsec = now.tv_nsec;

	/*Following values will get autofilled after timeout
	1 is subtracted because time includes 0 as the last cout 
	i.e. 4:4,3,2,1,0 but our intention was 4 not 5*/
	iterationDetails.it_interval.tv_sec = duration-1;
	iterationDetails.it_interval.tv_nsec = now.tv_nsec;
	//start/update the timer
	if (timerfd_settime(this->fd, TFD_TIMER_ABSTIME, &iterationDetails,
		NULL) == -1)
	{
		spdlog::error("Error in settingup timer : {}",strerror(errno));
	}
	//Registered fd to epollFd for monitoring
    int epollFd = perCoreStates[coreId].epollFd;
    if(epollFd == -1) {
        spdlog::debug("Epoll fd has not been created. fd {} will be added later",
                this->fd);
    }
    // else if(mtcp_epoll_ctl(mctx, epollFd,MTCP_EPOLL_CTL_ADD,this->fd,&ev)==-1)
	// {   
	// 	spdlog::error("Error in settingup epoll event for Timer: {}",strerror(errno));
	// }
	spdlog::debug("Successfully starting Timer with fd {} in epfd {}",
		this->fd, epollFd);
    // LOG_EXIT;
}

void timer::startTimer(){
	startTimer(this->duration,this->retries);	
}

void timer::stopTimer(){
	spdlog::debug("Default stopTimer for fd {}",this->fd);
    if(this->fd!=-1)
    {
        mctx_t mctx = perCoreStates[coreId].mctxFd;
        //De-Register the timer
        // mtcp_epoll_ctl(mctx, perCoreStates[coreId].epollFd,MTCP_EPOLL_CTL_DEL,this->getFd(),NULL);
        /*Remove the entry from the map*/
        perCoreStates[coreId].fdToObjectMap.erase(this->fd);
        /*Free the fd resource/closing timer*/
        close(this->fd);
        this->fd=-1;
    }
    /* Further Actions for stopTimer should be implemented by derived class. */
}

timer* vnf::registerTimer(void timeOutFunction(timer *), ConnId& connId){
    timer *t = new timer(connId.coreId);
    t->timeOutFunction = timeOutFunction;
    return t;
}

void vnf::deleteTimer(timer *t){
    delete t;
}

void tokenize(const std::string& s, const char* delim,
			std::vector<std::string>& out, unsigned n=INT_MAX, bool trim=false)
{
	string::size_type beg = 0;
	for (string::size_type end = 0, i=0; (end = s.find(delim, end)) != std::string::npos and i<n; ++end, ++i)
	{
        string s1 = s.substr(beg, end - beg);
        if(trim)
            boost::trim(s1);
		out.push_back(s1);
		beg = end + strlen(delim);
	}
    string s1 = s.substr(beg);
    if(trim)
        boost::trim(s1);
    out.push_back(s1);
}

string vnf::http::createHTTPRequest(string reqType, string host, string url, nlohmann::json reqBody, string contentType){
    return vnf::http::createHTTPRequest1(reqType, host, url, reqBody.dump(), contentType);
}

string vnf::http::createHTTPRequest1(string reqType, string host, string url, string reqBody, string contentType){
    string httpRequest;
    
    if(reqType == "GET")
        httpRequest =
            "GET " + url + " HTTP/1.1\r\n"
            "Host: " + host + "\r\n"
            "User-Agent:libvnf\r\n"
            "Connection: Keep-Alive\r\n"
            "\r\n";
    else
        httpRequest =
            reqType + " " + url + " HTTP/1.1\r\n"
            "Host: " + host + "\r\n"
            "Content-Length:" + to_string(reqBody.size()) + "\r\n"
            "Content-Type:" + contentType + "\r\n"
            "User-Agent:libvnf\r\n"
            "Connection: Keep-Alive\r\n"
            "\r\n"
            + reqBody;

    return httpRequest;
}

void vnf::http::extractHTTPResponse(int &status, extractResponseArg &arg){

    string sPacket(arg.rawPacket, arg.packetLen);
    map<string,string> &headers = arg.headers;

    if(!status){

        vector<string> v;
        tokenize(sPacket, "\r\n\r\n", v, 1);

        vector<string> sHeaders;
        tokenize(v[0], "\r\n", sHeaders);

        vector<string> meta;
        tokenize(sHeaders[0], " ", meta);
        status = stoi(meta[1]);

        for(uint i=1; i<sHeaders.size(); i++){
            vector<string> t;
            tokenize(sHeaders[i], ":", t, 1, true);
            transform(t[0].begin(), t[0].end(), t[0].begin(), ::tolower);
            headers[t[0]] = t[1];
        }
        auto it = headers.find("content-length");
        if(it != headers.end()){
            int contentLength = stoi(it->second);
            if(contentLength != v[1].length()){
                arg.errCode = INCOMPLETE_PACKET;
                return;
            }
        }

        arg.packet = v[1];
    
    }
    else if(sPacket.length() > 0){
        arg.packet = sPacket;
    }
    else{
        spdlog::warn("Unknown Packet");
        arg.errCode = NO_PACKET;
        return;
    }

    arg.errCode = DONE;
    return;
    
}

void vnf::http::splitUrl(string baseUrl, string &host, string &ipAddr, int &port){
    host = baseUrl.substr(7,baseUrl.length()-8);
    int delim = baseUrl.find(":", 7);
    ipAddr = baseUrl.substr(7,delim-7);
    port = stoi(baseUrl.substr(delim+1,baseUrl.length()-delim-2));
}

string urlEncode(string str){
    string new_str = "";
    char c;
    int ic;
    const char* chars = str.c_str();
    char bufHex[10];
    int len = strlen(chars);

    for(int i=0;i<len;i++){
        c = chars[i];
        ic = c;
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') new_str += c;
        else {
            sprintf(bufHex,"%X",c);
            if(ic < 16) 
                new_str += "%0"; 
            else
                new_str += "%";
            new_str += bufHex;
        }
    }
    return new_str;
}

string vnf::http::encodeQuery(string name, string value){
    return urlEncode(name) + "=" + urlEncode(value);
}

string vnf::http::createHTTPResponse(int status_code, nlohmann::json resBody, string contentType){
    if(resBody == NULL)
        return createHTTPResponse1(status_code, "", contentType);
    else
        return createHTTPResponse1(status_code, resBody.dump(), contentType);
}

string vnf::http::createHTTPResponse1(int status_code, string resBody, string contentType){
    string status;
    if(status_code == 200)
        status = "200 OK";
    else if(status_code == 201)
        status = "201 Created";
    else if(status_code == 204)
        status = "204 No Content";
    else if(status_code == 400)
        status = "400 Bad Request";
    else if(status_code == 403)
        status = "403 Forbidden";
    else if(status_code == 404)
        status = "404 Not Found";
    else if(status_code == 500)
        status = "500 Internal Error";
    else
        status = "500 Internal Error";


    string httpResponse = "HTTP/1.1 "+status+"\r\n";
    if(resBody == ""){
        httpResponse += "Content-Length:0\r\n"
                        "\r\n";
    }
    else{
        httpResponse += "Content-Length:"+ to_string(resBody.size()) + "\r\n"
                        "Content-Type:"+contentType+"\r\n"
                        "\r\n"
                        + resBody;
    }
    return httpResponse;
}

void vnf::http::extractHTTPRequest(bool &status, extractRequestArg &arg){
    string sPacket(arg.rawPacket, arg.packetLen);

    if(!status){

        vector<string> v;
        tokenize(sPacket, "\r\n\r\n", v, 1);

        vector<string> sHeaders;
        tokenize(v[0], "\r\n", sHeaders);

        vector<string> meta;
        tokenize(sHeaders[0], " ", meta);
        arg.reqType = meta[0];

        tokenize(meta[1].substr(1), "/", arg.path);

        for(uint i=1; i<sHeaders.size(); i++){
            vector<string> t;
            tokenize(sHeaders[i], ":", t, 1, true);
            transform(t[0].begin(), t[0].end(), t[0].begin(), ::tolower);
            arg.headers[t[0]] = t[1];
        }

        status = 1;
        
        if(v.size() == 1 || (v.size()==2 && v[1].length() == 0)){
            arg.errCode = INCOMPLETE_PACKET;
            return;
        }

        arg.packet = v[1];
    
    }
    else if(sPacket.length() > 0){
        arg.packet = sPacket;
    }
    else{
        spdlog::warn("Unknown Packet");
        arg.errCode = NO_PACKET;
        return;
    }

    arg.errCode = DONE;
    return;
}


vnf::ConnId vnf::getObjConnId(uint32_t connId) { 
    return vnf::ConnId(connId / 10000000, connId % 10000000); 
}

uint32_t vnf::getIntConnId(vnf::ConnId& connId) { 
    return connId.coreId * 10000000 + connId.socketId;
}

uint64_t getNow()
{
    auto currentTime = std::chrono::high_resolution_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime).count();
}

inline uint64_t getDelay(uint64_t start)
{
    uint64_t delay = getNow() - start;
    return delay;
}

// Entry for Java calling this thread.
int __VNFThread(int argc, char *argv[])
{
    vnf::startEventLoop();
    return 0;
}

/*
    There's only 2 possibilities exiting this function:
    1. Packet handle done. With return value.
        So you should do packet clean up.
    2. Packet waiting for txns.
        So you should save context for next handing.
*/
void _disposalBody(vnf::ConnId &connId, Context &ctx)
{
    while (ctx.AppIdx() != -1)
    {
        auto app = globals.sfc.SFC_chain[ctx.AppIdx()];
        ctx.NextApp(-1, ctx.ReturnValue());
        switch (ctx.ReturnValue())
        {
        case vnf::ERROR:
            (*app->errorHandler)(connId, ctx);
            ctx._move_next();
            break;
        case vnf::ACCEPT:
            (*app->acceptHandler)(connId, ctx);
            ctx._move_next();
            break;
        case vnf::READ:
            (*app->readHandler)(connId, ctx);
            ctx._move_next();
            break;
        case vnf::ABORT:
            // Execute when txn_finished. But the transaction sets ABORT. Then just stop.
            ctx._move_next();
            break;
        default:
            assert(false);
        }
        // If transactions handler being called, the mark would be set.
        if (ctx.IsWaitingForTxnBack())
        {
            // If the packet is waiting for txn handling, it should return here.
            return;
        }
    }
    // If the packet has been handled done, it should return here. So do clean ups here.

    /* free heap previously allocated for packet if evicatable */
    if (ctx.packet() != NULL && perCoreStates[connId.coreId].canEvictPacket(static_cast<void *>(ctx.packet())))
    {
        perCoreStates[connId.coreId].packetsMemPoolManager.free(static_cast<void *>(ctx.packet()));
    }
    // Free reqObj. And reqObj ptr is just context ptr.
    perCoreStates[connId.coreId].myFreeReqObj(1, static_cast<void *>(&ctx));
    perCoreStates[connId.coreId].monitor.deallocate_packet();

    spdlog::debug("Packet Handled done. ");
    return;
}

// These three are executed when packet first arrives.
void _AppsDisposalAccept(vnf::ConnId &connId, int reqObjId, void *requestObject, char *packet, int packetLen, int packetId, int errorCode, int streamNum)
{
    assert(requestObject == nullptr);
    requestObject = connId.allocReqObj(1);
    auto ctx = Context::Init(requestObject, nullptr, 0);
    ctx->_set_status(ACCEPT);
    _disposalBody(connId, *ctx);
}

void _AppsDisposalRead(vnf::ConnId &connId, int reqObjId, void *requestObject, char *packet, int packetLen, int packetId, int errorCode, int streamNum)
{
    requestObject = connId.allocReqObj(1);
    assert(packet != NULL);
    auto ctx = Context::Init(requestObject, packet, packetLen);
    ctx->_set_status(READ);
    _disposalBody(connId, *ctx);
}

void _AppsDisposalError(vnf::ConnId &connId, int reqObjId, void *requestObject, char *packet, int packetLen, int packetId, int errorCode, int streamNum)
{
    requestObject = connId.allocReqObj(1);
    assert(packet != NULL);
    auto ctx = Context::Init(requestObject, packet, packetLen);
    ctx->_set_status(ERROR);
    _disposalBody(connId, *ctx);
}

std::string DB4NFV::SFC::NFs()
{
    std::string json = globals.sfc.toJson().toStyledString();
    if (globals.config.Debug)
    {
        std::cout << "[DEBUG] Registered App Definitions:" << std::endl;
        std::cout << json << std::endl;
    }
    return json;
};

void DB4NFV::SFC::Entry(App &app)
{
    if (this->SFC_chain.size() != 0)
    {
        perror("fatal: the SFC has already registered entry.");
    }
    this->SFC_chain.push_back(&app);
}
void DB4NFV::SFC::Add(App &last, App &app)
{
    if (this->SFC_chain.size() == 0)
    {
        perror("fatal: the SFC has no entry.");
    }
    last.next = &app;
    this->SFC_chain.push_back(&app);
}

// The entry for sending txn execution request to txnEngine.
void __request(JNIEnv *env, uint64_t ts, uint64_t txnReqId, const char *key, int flag, ConnId &connId)
{
    // FIXME. To be optimized. Try to Cache JNIEnv in one persistent VNF thread.
    int len = sizeof(uint64_t) * 2 + strlen(key) + sizeof(int) * 2 + 4; // four separators

    // Use SetByteArrayRegion to copy data into the byte array
    jbyte data[len];
    int offset = 0;

    memcpy(data + offset, &ts, sizeof(uint64_t));
    offset += sizeof(uint64_t);
    data[offset++] = jbyte(';');

    memcpy(data + offset, &txnReqId, sizeof(uint64_t));
    assert((txnReqId & 0xfffffff000000000) == 0);
    offset += sizeof(uint64_t);
    data[offset++] = ';';

    memcpy(data + offset, key, strlen(key));
    offset += strlen(key);
    data[offset++] = ';';

    memcpy(data + offset, &flag, sizeof(int));
    offset += sizeof(int);
    data[offset++] = ';';

    // Todo. remove this field.
    int isAbort_i = 0;
    memcpy(data + offset, &isAbort_i, sizeof(int));
    offset += sizeof(int);
    data[offset] = '\0';

    // Copy the data into the msg byte array
    auto msg = env->NewByteArray(len);
    assert(msg != NULL);
    env->SetByteArrayRegion(msg, 0, len, data);

    jclass cls = env->FindClass("intellistream/morphstream/api/input/InputSource");
    assert(cls != NULL);

    // jmethodID methodId = env->GetMethodID(cls, "insertInputData", "(Ljava/lang/String;)V");
    jmethodID methodId = env->GetStaticMethodID(cls, "libVNFInsertInputData", "([B)V");
    assert(methodId != NULL);

#ifdef DEBUG
    perCoreStates[connId.coreId].monitor.update_latency(0, ts);
#endif

    // We can't cache JNIenv and related things according to https://stackoverflow.com/questions/12420463/keeping-a-global-reference-to-the-jnienv-environment.
    env->CallStaticVoidMethod(cls, methodId, msg);
    spdlog::debug("[DEBUG:] txn request sent. ");

    // Clean up resources when done
    env->DeleteLocalRef(msg); // Release the reference to msg
}

vector<int> pbdSeparator(char *buffer, int bufLen)
{
    vector<int> ret = {};
    int size = 0;
    for (int i = 0; i < bufLen; i += 1)
    {
        size += 1;
        // Assume last char is ; or \0.
        if (buffer[i] == ';' || buffer[i] == '\n')
        {
            assert(buffer[i] == ';' || buffer[i] == '\n' || buffer[i] == '\0');
            buffer[i] = '\0';
            ret.push_back(size);
            size = 0;
        }
        else if (i == bufLen - 1)
        {
            // No substitution at the last.
            ret.push_back(size);
            break;
        }
    }
    return ret;
}

// TODO. TO BE DEBUGGED.
JNIEXPORT jstring
    JNICALL
    Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1init_1SFC(JNIEnv *env, jobject obj, jint argc, jobjectArray argv)
{
#if DEBUG
    perCoreStates[0].monitor.report_header();
    spdlog::set_level(spdlog::level::info);
#endif
    // Convert the jobjectArray to a char* array
    char **argvC;
    jsize arrayLength;
    if (argv != nullptr)
    {
        arrayLength = env->GetArrayLength(argv);
        argvC = new char *[arrayLength];
        for (jsize i = 0; i < arrayLength; i++)
        {
            jstring string = (jstring)env->GetObjectArrayElement(argv, i);
            const char *str = env->GetStringUTFChars(string, 0);
            argvC[i] = strdup(str);
            env->ReleaseStringUTFChars(string, str);
        }
    }
    else
    {
        arrayLength = 0;
    }

    // Find the method and save in globals.
    // Find the class that contains the sendTxnRequest method
    // jclass cls = env->FindClass("intellistream/morphstream/api/input/InputSource");
    // assert( cls != NULL);

    // Get a reference to the txn method
    // jmethodID methodId = env->GetStaticMethodID(cls, "libVNFInsertInputData", "(Ljava/lang/String;)V");

    // Create a static object of the InputSource class.
    // jmethodID constructor = env->GetMethodID(cls, "<init>", "()V");
    // assert( constructor != NULL);

    // jobject IS = env->NewObject(cls, constructor);
    // Set Permanenet Ref.

    // jmethodID methodId = env->GetStaticMethodID(cls, "insertInputData", "(Ljava/lang/String;)V");
    // assert( methodId != NULL);

    // Call the __request method
    jint rs = env->GetJavaVM(&globals.__jvm);
    assert(rs == JNI_OK);

    // Call JVM. Errored. TODO.
    // globals.__java_env->CallVoidMethod(
    //     globals.__client_obj, globals.__request,
    //     globals.__java_env->NewStringUTF(msg)
    //     // Other parameters to be added.
    // );

    // TODO. Maybe we can provide command line parameters pass-in.

    // Call the __init_SFC function
    int res = VNFMain(argc, argvC);

    if (res < 0)
    {
        perror("VNFMain returns error");
        return env->NewStringUTF(std::string("").c_str());
    }

    vnf::initLibvnf(globals.config.coreNumbers, 128, "127.0.0.1", std::vector<int>(), 131072, vnf::LOCAL);
    vnf::ConnId serverSocketId = vnf::initServer("", globals.config.serverIP, globals.config.serverPort, "tcp");

    // Clean up the allocated memory
    for (jsize i = 0; i < arrayLength; i++)
    {
        free(argvC[i]);
    }
    delete[] argvC;

    // Fix the backlink.
    int reqObjStart = 0;
    int reqObjIndex = 0;
    for (int i = 0; i < globals.sfc.SFC_chain.size(); i += 1)
    {
        auto app = globals.sfc.SFC_chain.at(i);
        app->appIndex = i;
        // Sort the reqObj. TODO.
        globals.sfc.objSizes.push_back(app->reqObjSize);
        globals.sfc.objSizesStarting.push_back(reqObjStart);
        reqObjStart += app->reqObjSize;
        globals.sfc.AppIdxMap[app] = reqObjIndex;
        reqObjIndex += 1;

        for (int j = 0; j < app->Txns.size(); j += 1)
        {
            auto Txn = &app->Txns.at(j);
            Txn->txnIndex = j;
            Txn->app = app;
            for (int k = 0; k < (Txn->sas).size(); k += 1)
            {
                auto sa = &Txn->sas.at(k);
                sa->app = app;
                sa->txn = Txn;
                sa->appIndex = i;
                sa->txnIndex = j;
                sa->saIndex = k;
                app->SAs.push_back(sa);
            }
        }
    }
    // Config and init libVNF.
    globals.sfc.reqObjTotalSize = reqObjStart + sizeof(Context);
    // Create back links for transactions and state access.

    int size[] = {globals.sfc.reqObjTotalSize};
    vnf::initReqPool(size, 1);

    serverSocketId.registerCallback(vnf::ACCEPT, _AppsDisposalAccept);
    serverSocketId.registerCallback(vnf::READ, _AppsDisposalRead);
    serverSocketId.registerCallback(vnf::ERROR, _AppsDisposalError);
    registerPacketBoundaryDisambiguator(serverSocketId, pbdSeparator);

    // Return the result as a Java string
    return env->NewStringUTF(globals.sfc.NFs().c_str());
}

// Actual vnfs loop.
JNIEXPORT void
    JNICALL
    Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1VNFThread(JNIEnv *env, jobject obj, jint c, jobjectArray v)
{
    jint rs = env->GetJavaVM(&globals.__jvm);
    assert(rs == JNI_OK);
    // globals.__env = env;
    startEventLoop();
}

// The callback handling entrance.
JNIEXPORT jbyteArray
    JNICALL
    Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1execute_1sa_1udf(JNIEnv *env, jclass cls, jlong txnReqId_jni, jint saIdx, jbyteArray value, jint param_count)
{
    // Save the value in ctx.
    uint64_t txnReqId = 0;
    // reverse for switching endianness.
    uint8_t *buf = reinterpret_cast<uint8_t *>(&txnReqId);
    uint8_t *buf_jni = reinterpret_cast<uint8_t *>(&txnReqId_jni);
    for (int i = 0; i < sizeof(jlong); i += 1)
    {
        buf[sizeof(jlong) - i - 1] = buf_jni[i];
    }
    assert((txnReqId & 0xffffff0000000000) == 0);

    int coreId = COREID(txnReqId);
    auto ctx = perCoreStates[coreId].packetNumberContextMap.at(PACKETID(txnReqId));

#ifdef DEBUG
    perCoreStates[coreId].monitor.update_latency(1, ctx->_full_ts());
#endif

    /*
    Value fetched deallocation happens:
    1. Requesting another transaction.
    2. Return from the saUDF.
    */

    // Copy allocation value.
    jbyte *inputBuffer = env->GetByteArrayElements(value, NULL);

    // Length. Here we are getting the length from: field number; type.
    int length = STATE_TYPE_SIZE * param_count;

    auto tmp = new char[length]; // TO be Optimized.
    memcpy(tmp, inputBuffer, length);
    // ctx->_set_value_from_callback(tmp, length);

    ConnId conn = ConnId(COREID(txnReqId), ctx->_old_socket());

    assert(ctx->AppIdx() != -1);
    // Call actual sa udf.
    STATE_TYPE res = (*globals.sfc.SFC_chain[ctx->AppIdx()]->SAs[saIdx]->txnHandler_)(conn, *ctx, tmp, length);
    int abortion = (ctx->ReturnValue()) ? 1 : 0;

    delete tmp;

    // Write back.
    if (env == nullptr)
    {
        perror("core.cpp.Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1execute_1sa_1udf.NewByteArray.nullptr");
    }

    auto _result = env->NewByteArray(sizeof(int) + STATE_TYPE_SIZE);
    // Set Abortion.
    env->SetByteArrayRegion(_result, static_cast<jsize>(0), static_cast<jsize>(sizeof(int)), reinterpret_cast<jbyte *>(&abortion));
    // Set result.
    env->SetByteArrayRegion(_result, static_cast<jsize>(sizeof(int)), static_cast<jsize>(STATE_TYPE_SIZE), reinterpret_cast<jbyte *>(&res));

#ifdef DEBUG
    perCoreStates[coreId].monitor.update_latency(2, ctx->_full_ts());
#endif

    // How to release write back value? We don't need to release. They are managed.
    return _result;
}

// Report done.
JNIEXPORT jint
    JNICALL
    Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1txn_1finished(JNIEnv *env, jclass cls, jlong txnReqId_jni)
{
    // Save the value in ctx.
    uint64_t txnReqId = 0;
    // reverse for switching endianness.
    uint8_t *buf = reinterpret_cast<uint8_t *>(&txnReqId);
    uint8_t *buf_jni = reinterpret_cast<uint8_t *>(&txnReqId_jni);
    for (int i = 0; i < sizeof(jlong); i += 1)
    {
        buf[sizeof(jlong) - i - 1] = buf_jni[i];
    }
    assert((txnReqId & 0xffffff0000000000) == 0);

    int coreId = COREID(txnReqId);

    // Release context transaction state.
    perCoreStates[coreId].packetNumberContextMap[PACKETID(txnReqId)]->_unset_wait_txn_callback();

    // Enqueue the txnReqId.
    // FIXME. To be Optimized. may be the bottleneck.
    perCoreStates[coreId].txnDoneQueueLock.lock();
    perCoreStates[coreId].txnDoneQueue.push(txnReqId);
    perCoreStates[coreId].txnDoneQueueLock.unlock();

    // Write to the fd to suggest done and wake up the txn.
    if (write(perCoreStates[coreId].txnSocket, &txnReqId, sizeof(uint64_t)) < 0)
    {
        perror("jni.handle_done.write");
        return -1;
    }
    return 0;
}

// FIXME. TODO. CONTEXT shall not be done so easily...
void *DB4NFV::App::reqObjClip(void *reqObjClip)
{
    return reqObjClip + sizeof(Context) + globals.sfc.objSizesStarting.at(globals.sfc.AppIdxMap[this]);
}

DB4NFV::SFC &GetSFC()
{
    auto &ref = globals.sfc;
    return ref;
};

void SetConfig(std::string path)
{
    globals.config = Config(path);
}

Globals &GetGlobal()
{
    return globals;
}

/*
    Operations about context.
*/
int Context::packet_len()
{
    if (_packet_len < 0)
    {
        std::cout << boost::stacktrace::stacktrace();
    }
    return _packet_len;
}

char *Context::packet()
{
    if (_packet == NULL && _ret != vnf::ACCEPT)
    {
        spdlog::warn("packet is null.");
        std::cout << boost::stacktrace::stacktrace();
    }
    return _packet;
}

void Context::_set_txn_idx(int txnIdx)
{
    _TxnIdx = txnIdx;
}

// int Context::value_len()
// {
//     assert(_value != NULL);
//     return _value_len;
// }

// void *Context::value()
// {
//     assert(_value != NULL);
//     return _value;
// }

STATE_TYPE *Context::get_value(char *raw, int length, int index)
{
    assert(raw != NULL);
    if (STATE_TYPE_SIZE * (index + 1) > length)
    {
        perror("index out of range.");
        assert(false);
    }
    else
        return static_cast<STATE_TYPE *>(static_cast<void *>(raw + (index * STATE_TYPE_SIZE)));
}

Context *Context::Init(void *reqObj, char *packet, int packet_len)
{
    // Input check
    assert(reqObj != NULL);

    // Get the current time in nanoseconds since the epoch
    auto ctx = static_cast<Context *>(reqObj);
    ctx->_set_time_now();
    ctx->_set_packet(packet, packet_len);
    ctx->NextApp(0, vnf::ABORT);
    ctx->_unset_wait_txn_callback();
    ctx->_set_txn_idx(-1);
    ctx->_set_old_socket(-1);

    return ctx;
}

int Context::AppIdx()
{
    return _AppIdx;
}

int Context::TxnIdx()
{
    return _TxnIdx;
}

void *Context::reqObj()
{
    return GetGlobal().sfc.SFC_chain[_AppIdx]->reqObjClip(this);
}

// int Context::reqObjId()
// {
//     return _reqObjId;
// }

// void Context::WriteBack(void *v, int len)
// {
//     assert(_result == NULL);
//     JNIEnv *env;
//     GetJniEnv(&env);
//     _result = env->NewByteArray(sizeof(bool) + len);
//     env->SetByteArrayRegion(_result, 1, static_cast<jsize>(len), reinterpret_cast<jbyte *>(v));
// }

void Context::Abort()
{
    _ret = ABORT;
}

DB4NFV::Transaction &Context::Transaction(int idx)
{
    assert(idx < GetGlobal().sfc.SFC_chain[_AppIdx]->Txns.size());
    // _clear_value();
    return GetGlobal().sfc.SFC_chain[_AppIdx]->Txns[idx];
}

void Context::_move_next()
{
    _AppIdx = _next_AppIdx;
    _next_AppIdx = -1;
    // // Already end.
    // if (_AppIdx == -1){
    //     return;
    // // Move to end.
    // } else if (globals.sfc.SFC_chain[_AppIdx]->MoveNext() != nullptr){
    //     this->_next_AppIdx = globals.sfc.SFC_chain[_AppIdx]->MoveNext()->appIndex;
    // } else {
    // }
}

void Context::NextApp(int appIdx, vnf::EventType ret)
{
    _next_AppIdx = appIdx;
    _ret = ret;
}

vnf::EventType Context::ReturnValue()
{
    return _ret;
}

bool Context::IsWaitingForTxnBack()
{
    return waiting_for_transaction_back;
}

void Context::_reset_appIdx()
{
    _AppIdx = -1;
}

void Context::_set_status(vnf::EventType status)
{
    _AppIdx = 0;
    _ret = status;
}

// void Context::_set_value_from_callback(void *value, int value_len)
// {
//     _clear_value();
//     _value = value;
//     _value_len = value_len;
// }

// void Context::_clear_value()
// {
//     if (_value)
//     {
//         // delete _value;   // FIXME. LEAK.
//         _value_len = -1;
//     }
// }

int Context::_old_socket()
{
    return old_socket;
}

void Context::_set_old_socket(int s)
{
    old_socket = s;
}

void Context::_set_wait_txn_callback()
{
    waiting_for_transaction_back = true;
}

void Context::_unset_wait_txn_callback()
{
    waiting_for_transaction_back = false;
}

// jbyteArray Context::_res_ptr()
// {
//     return _result;
// }

uint32_t Context::_ts_low_32b()
{
    return static_cast<uint32_t>(ts & 0xFFFFFFFF);
}

uint64_t Context::_full_ts()
{
    return ts;
}

void DB4NFV::Transaction::Trigger(vnf::ConnId &connId, Context &ctx, const char *key, bool isAbort) const
{
    // Reset when transaction handle done.
    ctx._set_wait_txn_callback();

    ctx._set_txn_idx(txnIndex);
    ctx.NextApp(ctx.AppIdx(), ctx.ReturnValue());
    ctx._set_old_socket(connId.socketId);

    // Create a new event blocking fd.
    if (uint64_t(ctx.reqObj()) == 0)
    {
        perror("fatal: reqObj is null.");
    }

    // Register call back parameters in the context.
    perCoreStates[connId.coreId].packetNumberContextMap[ctx._ts_low_32b()] = &ctx;

    JNIEnv *env;
    GetJniEnv(&env);
    __request(env, ctx._full_ts(), TXNREQID(connId.coreId, ctx._ts_low_32b()), key, this->txnIndex, connId);
}

// Get JVM and jenv related. FIXME. Optimize.
// TODO. May not be working.
bool GetJniEnv(JNIEnv **env)
{
    bool did_attach_thread = false;
    *env = nullptr;
    // Check if the current thread is attached to the VM
    auto get_env_result = globals.__jvm->GetEnv((void **)env, JNI_VERSION_1_6);
    if (get_env_result == JNI_EDETACHED)
    {
        if (globals.__jvm->AttachCurrentThread((void **)env, NULL) == JNI_OK)
        {
            did_attach_thread = true;
        }
        else
        {
            assert(false);
        }
    }
    else if (get_env_result == JNI_EVERSION)
    {
        assert(false);
    }
    return did_attach_thread;
    // // jint rs = (*GetGlobal().__jvm).AttachCurrentThread((void **)&env, NULL);
    // // assert(rs == JNI_OK);
    // return true;
}

inline int Monitor::packet_waiting(int coreId)
{
    return perCoreStates[coreId].packetNumberContextMap.size();
}