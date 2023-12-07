// #include "core.hpp"
#include "utils.hpp"

using namespace vnf;
using namespace std;

PerCoreState *perCoreStates;

Globals globals;

UserConfig *userConfig = nullptr;

int pinThreadToCore(int core_id) {
   int numCores = sysconf(_SC_NPROCESSORS_ONLN);
   if (core_id < 0 || core_id >= numCores)
      return -1;

   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t currentThread = pthread_self();
   return pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &cpuset);
}

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

Managing ReqPool with slab: 
1. Allocate "Minimum sufficient space" for object.
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

int createClientToDS(int coreId, string remoteIP, int remotePort, enum DataLocation type) {
    int socketId = -1;
    int ret = -1;
    if (type == REMOTE) {
        socketId = socket(AF_INET, SOCK_STREAM, 0);
        if (socketId < 0) {
            spdlog::error("Failed to create listening socket!");
            return -1;
        }
        makeSocketNonBlocking(socketId);
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = inet_addr(remoteIP.c_str());
        address.sin_port = htons(remotePort);
        ret = connect(socketId, (struct sockaddr *) &address, sizeof(struct sockaddr_in));
        perCoreStates[coreId].dsSocketProtocol = globals.serverProtocol;
    } else if (type == UDS) {
        socketId = socket(AF_UNIX, SOCK_STREAM, 0);
        struct sockaddr_un remote;
        remote.sun_family = AF_UNIX;
        std::ostringstream oss;
        oss << remoteIP << remotePort << ".sock";
        strcpy(remote.sun_path, oss.str().c_str());

        if (socketId < 0)
        {
            spdlog::error("Failed to create listening socket!");
            return -1;
        }
        ret = connect(socketId, (struct sockaddr *) &remote, strlen(remote.sun_path) + sizeof(remote.sun_family));
        std::cout << "[DEBUG] Listen on " << oss.str() << std::endl;
        perCoreStates[coreId].dsSocketProtocol = "uds";
    } else {
        std::cerr << "no such type" << std::endl;
        assert(false);
    }

    if (ret < 0 && errno != EINPROGRESS) {
        spdlog::error("Connect issue {}", errno);
        close(socketId);
        return -1;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.fd = socketId;
    int epFd = perCoreStates[coreId].epollFd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, socketId, &ev);

    return socketId;
}

void *serverThread(void *args) {
    struct ServerPThreadArgument argument = *((struct ServerPThreadArgument *) args);
    int coreId = argument.coreId;
    pinThreadToCore(coreId);
    spdlog::info("Server thread started on core {}", coreId);

    // memory pool initialization for request objects
    for (int reqObjType = 0; reqObjType < MAX_REQUEST_OBJECT_TYPES; ++reqObjType) {
        perCoreStates[coreId].initMemPoolOfRequestObject(reqObjType);
    }

    // memory pool initialization for packets
    spdlog::info("Packets Memory Pool Size: {}", perCoreStates[coreId].packetMemPoolBlock.size());
    perCoreStates[coreId].packetsMemPoolManager.add_block(
            &perCoreStates[coreId].packetMemPoolBlock.front(),
            perCoreStates[coreId].packetMemPoolBlock.size(),
            1500); //1024 //1500

    // set protocol for listening socket
    perCoreStates[coreId].socketProtocolMap[globals.listeningSocketFd] = globals.serverProtocol;

    int epFd = epoll_create(MAX_EVENTS + 5);
    if (epFd < 0) {
        spdlog::error("Failed to create epoll descriptor!");
        return nullptr;
    }
    globals.epollArrayLock.lock();
    perCoreStates[coreId].epollFd = epFd;
    globals.epollArrayLock.unlock();

    struct epoll_event ev;
    globals.listenLock.lock();
    ev.events = EPOLLIN | (globals.serverProtocol == "udp" ? EPOLLEXCLUSIVE : EPOLLET);
    ev.data.fd = globals.listeningSocketFd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, globals.listeningSocketFd, &ev);

    // Apply txnSocket.
    struct epoll_event ev1;
    // TODO. set mode.
    ev1.events = EPOLLIN | (globals.serverProtocol == "udp" ? EPOLLEXCLUSIVE : EPOLLET);
    ev1.data.fd = perCoreStates[coreId].txnSocket;
    perCoreStates[coreId].txnSocket = eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK);
    if ( perCoreStates[coreId].txnSocket == -1) {
        perror("fatal: failed to create event fd.");
    }
    epoll_ctl(epFd, EPOLL_CTL_ADD, perCoreStates[coreId].txnSocket, &ev1);
    globals.listenLock.unlock();

    map <int, class timer*>::iterator timerItr = perCoreStates[coreId].fdToObjectMap.begin();
    while(timerItr != perCoreStates[coreId].fdToObjectMap.end()) {
        memset(&ev,0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.fd = timerItr->second->getFd();
        if(epoll_ctl(epFd, EPOLL_CTL_ADD,
                     timerItr->second->getFd(), &ev)==-1){
            spdlog::error("epoll_ctl failed, eventFD {},  Error {}",
                    timerItr->second->getFd(), strerror(errno));
        }
        spdlog::debug("added timer fd {} into epoll", timerItr->second->getFd());
        timerItr++;
    }

    struct epoll_event *epollEvents;
    epollEvents = (struct epoll_event *) calloc(MAX_EVENTS, sizeof(struct epoll_event));
    if (!epollEvents) {
        spdlog::error("Failed to create epoll event struct");
        exit(-1);
    }
    spdlog::info("Waiting for epollEvents");

    enum DataLocation _useRemoteDataStore = userConfig->USE_REMOTE_DATASTORE;
    while (!perCoreStates[coreId].isJobDone) {
        // connect to remote data store for first time
        if (_useRemoteDataStore == REMOTE || _useRemoteDataStore == UDS) {
            // Load balancing through different ports. 
            if (coreId == 0) {
                perCoreStates[coreId].dsSocketId1 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[0], _useRemoteDataStore);
                perCoreStates[coreId].dsSocketId2 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[1], _useRemoteDataStore);
            } else {
                perCoreStates[coreId].dsSocketId1 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[2], _useRemoteDataStore);
                perCoreStates[coreId].dsSocketId2 = createClientToDS(coreId, userConfig->DATASTORE_IP, userConfig->DATASTORE_PORTS[3], _useRemoteDataStore);
            }
            _useRemoteDataStore = LOCAL;
        }

        int numEventsCaptured = epoll_wait(epFd, epollEvents, MAX_EVENTS, -1);      // Epoll provided by libc. -1: infinite time out.
        if (numEventsCaptured < 0) {
            if (errno != EINTR) {
                perror("epoll_wait");
            }
            break;
        }
        // Epoll would return many events at one time. Here we dispose them one by one.
        // spdlog::debug("Caught {} events\n", numEventsCaptured);
        for (int i = 0; i < numEventsCaptured; i++) {
            uint64_t u64 = epollEvents[i].data.u64;
            int currentSocketId = epollEvents[i].data.fd;
            uint32_t currentEvents = epollEvents[i].events;

            /* EPOLLERR: Error  condition  happened  on  the associated file descriptor.  This event is also reported for the write end of a pipe when the read end has been closed. EPOLLRDHUP (since Linux 2.6.17): Stream socket peer closed connection, or shut down writing half of connection. (This flag is especially useful for writing simple code to detect peer shutdown when using Edge Triggered monitoring.) */
            /*
                Dispose Socket error here.

                C style judging type with &. 
                EPOLLERR: error from epoll. Doc didn't say what would be the situation.
                EPOLLHUP: Randomly shutdown.
            */
            if ((currentEvents & EPOLLERR) || (currentEvents & EPOLLRDHUP)) {
                //spdlog::error("EPOLLERR or EPOLLRDHUP. Clearing pending data queue and closing socket fd");
                if (currentSocketId == globals.listeningSocketFd) {
                    spdlog::error("Oh Oh, lsfd it is");
                    exit(-1);
                }
                while (!perCoreStates[coreId].isPendingDataQueueEmpty(currentSocketId)) {
                    PendingData dataToSend = perCoreStates[coreId].socketIdPendingDataQueueMap[currentSocketId].front();
                    perCoreStates[coreId].packetsMemPoolManager.free((void *) dataToSend.data);
                    perCoreStates[coreId].socketIdPendingDataQueueMap[currentSocketId].pop();
                }
                close(currentSocketId);

                continue;
            }

            /*For Handling Timers : Check if fd belongs to any timer*/
            /*
                Kernel expose timer as function. Seems this part handles the time alarm through fd.
            */
            map <int, class timer*>::iterator timerItr =
                        perCoreStates[coreId].fdToObjectMap.find(currentSocketId);
			if(timerItr != perCoreStates[coreId].fdToObjectMap.end()){
				/*Timeout Function will execute and if all retries are 
				exhausted then timer will be de-registered.What action to be 
				after exhausting all timers is yet to discuss and implement */ 
				spdlog::debug("Timeout Function for fd {} Triggered",
                        currentSocketId);
				(timerItr->second)->timeOutFunction(timerItr->second);
                continue;
			}

            /* Event occured on global listening socket. Therefore accept the connection */
            if (currentSocketId == globals.listeningSocketFd && globals.serverProtocol != "udp") {

                while (true) {
                    int socketId = accept(globals.listeningSocketFd, NULL, NULL);
                    if (socketId < 0) {
                        // Need lsfd non blocking to run this!!!!!!
                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                            spdlog::error("Error on accept");
                        }
                        break;
                    }
                    if(globals.serverProtocol == "sctp"){
                        struct sctp_initmsg s_initmsg;
                        memset(&s_initmsg, 0, sizeof (s_initmsg));
                        s_initmsg.sinit_num_ostreams = 8;
                        s_initmsg.sinit_max_instreams = 8;
                        s_initmsg.sinit_max_attempts = 8;
                        if(setsockopt(socketId, IPPROTO_SCTP, SCTP_INITMSG, &s_initmsg, sizeof (s_initmsg)) < 0){
                            spdlog::error("Socket option for sctp_init error");
                            exit(-1);
                        }
                        struct sctp_event_subscribe events;
                        memset((void *) &events, 0, sizeof(events));
                        events.sctp_data_io_event = 1;
                        if(setsockopt( socketId, SOL_SCTP, SCTP_EVENTS, (const void *)&events, sizeof(events)) < 0){
                            spdlog::error("Socket option for sctp_events error");
                            exit(-1);
                        }
                    }
                    makeSocketNonBlocking(socketId);
                    ev.events = EPOLLIN;
                    ev.data.fd = socketId;
                    epoll_ctl(epFd, EPOLL_CTL_ADD, socketId, &ev);

                    perCoreStates[coreId].socketProtocolMap[socketId] = globals.serverProtocol;
                    perCoreStates[coreId].connCounter++;
                    for (int eventType = 0; eventType < NUM_CALLBACK_EVENTS; ++eventType) {
                        perCoreStates[coreId].socketIdCallbackMap[eventType][socketId] = argument.onAcceptByServerCallback[eventType];
                    }
                    perCoreStates[coreId].socketIdDSCallbackMap[socketId] = argument.onAcceptByServerDSCallback;
                    perCoreStates[coreId].socketIdReqObjIdExtractorMap[socketId] = argument.onAcceptByServerReqObjIdExtractor;
                    perCoreStates[coreId].socketIdPBDMap[socketId] = argument.onAcceptByServerPBD;
                    CallbackFn callback = perCoreStates[coreId].socketIdCallbackMap[ACCEPT][socketId];
                    ConnId connId = ConnId(coreId, socketId);
                    if(callback){
                        callback(connId, 0, nullptr, nullptr, perCoreStates[coreId].packetNumber++, 0, 0, 0);
                    }
                }

                continue;
            }

            /*
                There are new messages come in from the socket. So we decide the type and call the related callbacks.
            */
            if (currentEvents & EPOLLIN) {
                // Some handler done. Now we are going to trigger the continue handling.
                if (perCoreStates[coreId].txnSocket == currentSocketId) {
                    // read from socket.
                    uint64_t txnReqId = 0; 
                    if (read(currentSocketId, &txnReqId, sizeof(uint64_t)) < 0) {
                        perror("serverThread.transactions.read");
                    }
                    // TODO. Make sure only the right thread receives this.
                    if (COREID(txnReqId) == coreId) {

                        auto o = perCoreStates[coreId].packetNumberContextMap[PACKETID(txnReqId)];
                        ConnId connId = ConnId(coreId, o->old_socket);

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

                        _disposalBody(connId, o->reqObjId,
                                    perCoreStates[coreId].socketIdReqObjIdToReqObjMap[o->old_socket][o->reqObjId],
                                    o->packet_record, o->packet_len, 0);
                        // Delete ctx from the hashmap. No need to delete ctx since it belongs to reqObj.
                        perCoreStates[coreId].packetNumberContextMap.erase(
                            perCoreStates[coreId].packetNumberContextMap.find(PACKETID(txnReqId))
                        );
                    // TODO. How to recover packates here.
                    }
                    continue;    
                }
                /* Current socketId points to remote datastore 
                    Each time we communicate with the data store, we are using the specified socket (Special IP and ports).
                    So we can decide this is the message from the database.
                */
                if (perCoreStates[coreId].isDatastoreSocket(currentSocketId)) {
                    while (true) {
                        DSPacketHandler pkt;
                        int pkt_len, retval;
                        pkt.clear_pkt();
                        /*
                            We've predefined the data format here: first byte is an integer suggesting length.
                            And the others are the true data.
                        */
                        retval = readFromStream(currentSocketId, pkt.data, sizeof(int));
                        if (retval < 0) {
                            if (errno == EAGAIN) {
                                break;
                            }
                        } else {
                            perCoreStates[coreId].numPacketsRecvFromDs++;
                            memmove(&pkt_len, pkt.data, sizeof(int) * sizeof(uint8_t));
                            pkt.clear_pkt();
                            retval = readFromStream(currentSocketId, pkt.data, pkt_len);
                            pkt.data_ptr = 0;
                            pkt.len = retval;
                            if (retval < 0) {
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
                        if (globals.dsSize == userConfig->DATASTORE_THRESHOLD) {
                            freeDSPool();
                        }
                        // cache the state
                        dsMalloc = globals.dsMemPoolManager.malloc();
                        globals.dsSize++;
                        memcpy(dsMalloc, buffer.c_str(), buffer.length());
                        // The local datastore is used as cache for reading.
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

                        // callback. This part handles the DataBase return function.
                        DSCallbackFn callback = perCoreStates[coreId].socketIdDSCallbackMap[socketId];
                        callback(connId, reqObjId,
                                 perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId],
                                 (void *) buffer.c_str(),
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
                if(currentProtocol == "sctp"){
                    struct sctp_sndrcvinfo s_sndrcvinfo;
                    numBytesRead = (int) sctp_recvmsg(socketId, buffer, (size_t)userConfig->BUFFER_SIZE, (struct sockaddr *) NULL, 0, &s_sndrcvinfo, NULL);
                    streamNum = s_sndrcvinfo.sinfo_stream;
                }
                else if(currentProtocol == "tcp"){
                    numBytesRead = (int) read(socketId, buffer, (size_t) userConfig->BUFFER_SIZE);
                }
                else if(currentProtocol == "uds"){
                    numBytesRead = (int) read(socketId, buffer, (size_t) userConfig->BUFFER_SIZE);
                } else if(currentProtocol == "udp"){
                    struct sockaddr_in address;
                    int len = sizeof(address);
                    numBytesRead = recvfrom(socketId, buffer, (size_t) userConfig->BUFFER_SIZE, MSG_WAITALL, (struct sockaddr *) &address, (socklen_t *)&len);
                    if(socketId != globals.listeningSocketFd){
                        if(address.sin_port != perCoreStates[coreId].udpSocketAddrMap[socketId].sin_port
                            || address.sin_addr.s_addr != perCoreStates[coreId].udpSocketAddrMap[socketId].sin_addr.s_addr){
                            spdlog::warn("UDP packet from different server\n");
                        }
                    }
                    else if(numBytesRead <= 0){
                        continue;
                    }
                    else{
                        socketId = address.sin_port + SOCK_BOUNDARY;
                        /* check if this is new udp connection */
                        if(perCoreStates[coreId].udpSocketAddrMap.count(socketId) == 0){
                            perCoreStates[coreId].udpSocketAddrMap[socketId] = address;
                            perCoreStates[coreId].socketProtocolMap[socketId] = globals.serverProtocol;
                            perCoreStates[coreId].connCounter++;
                            for (int eventType = 0; eventType < NUM_CALLBACK_EVENTS; ++eventType) {
                                perCoreStates[coreId].socketIdCallbackMap[eventType][socketId] = argument.onAcceptByServerCallback[eventType];
                            }
                            perCoreStates[coreId].socketIdDSCallbackMap[socketId] = argument.onAcceptByServerDSCallback;
                            perCoreStates[coreId].socketIdReqObjIdExtractorMap[socketId] = argument.onAcceptByServerReqObjIdExtractor;
                            perCoreStates[coreId].socketIdPBDMap[socketId] = argument.onAcceptByServerPBD;
                            CallbackFn callback = perCoreStates[coreId].socketIdCallbackMap[ACCEPT][socketId];
                            ConnId connId = ConnId(coreId, socketId);
                            if(callback){
                                callback(connId, 0, nullptr, nullptr, perCoreStates[coreId].packetNumber++, 0, 0, 0);
                            }
                        }
                    }
                }
                
                /* numBytesRead == -1 for errors & numBytesRead == 0 for EOF */
                if (numBytesRead <= 0 && currentProtocol != "udp") {
                    spdlog::error("Read error on non-remote datastore socket. Trying to close the socket");
                    if (close(socketId) < 0) {
                        spdlog::error("Connection could not be closed properly");
                    }
                    continue;
                }
                buffer[numBytesRead] = '\0';
                char *temp = buffer;
                spdlog::debug("Message: {}\n", temp);
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
                    void *packet = perCoreStates[coreId].packetsMemPoolManager.malloc();
                    string currentPacket = prependedBuffer.substr((uint) packetStart, (uint) packetLength);
                    memcpy(packet, currentPacket.c_str(), (size_t) (packetLength));

                    /* request object id extraction from packet */
                    ReqObjExtractorFn extractor = perCoreStates[coreId].socketIdReqObjIdExtractorMap[socketId];
                    int reqObjId = extractor == nullptr ? 0 : extractor((char *) packet, packetLength);
                    cout.flush();

                    /* user-defined on read callback function */
                    CallbackFn callback = perCoreStates[coreId].socketIdCallbackMap[READ][socketId];
                    void *reqObj = perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId];
                    callback(connId, reqObjId, reqObj, (char *) packet, packetLength,  perCoreStates[coreId].packetNumber++, 0, streamNum);
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
            if (currentEvents & EPOLLOUT) {
                int socketId = currentSocketId, ret, dataLen;
                char *data;
                string currentProtocol = perCoreStates[coreId].socketProtocolMap[socketId];
                while (!perCoreStates[coreId].isPendingDataQueueEmpty(socketId)) {
                    PendingData dataToSend = perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].front();
                    dataLen = dataToSend.dataLen;
                    data = dataToSend.data;
                    if(currentProtocol == "sctp"){
                        ret = sctp_sendmsg(socketId, data, dataLen, NULL, 0, 0, 0, dataToSend.streamNum, 0, 0);
                    } else if(currentProtocol == "tcp"){
                        ret = write(socketId, data, dataLen);
                    } else if(currentProtocol == "uds"){
                        ret = write(socketId, data, dataLen);
                    } else if(currentProtocol == "udp"){
                        struct sockaddr_in address = perCoreStates[coreId].udpSocketAddrMap[socketId];
                        if(socketId >= SOCK_BOUNDARY){
                            ret = sendto(globals.listeningSocketFd, data, dataLen, MSG_CONFIRM, (const struct sockaddr *) &address, sizeof(address));
                        }
                        else{
                            ret = sendto(socketId, data, dataLen, MSG_CONFIRM, (const struct sockaddr *) &address, sizeof(address));
                        }
                    }
                    perCoreStates[coreId].packetsMemPoolManager.free((void *) dataToSend.data);
                    perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].pop();
                    if (ret < 0 && socketId != globals.listeningSocketFd) {
                        spdlog::error("Connection closed with client");
                        close(socketId);
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
    int ret = perCoreStates[coreId].mallocReqObj(socketId, reqObjType - 1, reqObjId);
    if (ret) {
        spdlog::error("Could not allocate request object on ConnId(coreId, socketId): ({}, {})", this->coreId, this->socketId);
    }
    return perCoreStates[coreId].socketIdReqObjIdToReqObjMap[socketId][reqObjId];
}

ConnId& vnf::ConnId::freeReqObj(int reqObjType, int reqObjId) {
    int coreId = this->coreId;
    int socketId = this->socketId;
    if (perCoreStates[coreId].isARequestObjectAllocator(socketId, reqObjId)) {
        perCoreStates[coreId].freeReqObj(socketId, reqObjType - 1, reqObjId);
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

ConnId& vnf::ConnId::registerCallback(enum EventType eventType, vnf::CallbackFn callback) {
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
        close(perCoreStates[i].dsSocketId1);
        close(perCoreStates[i].dsSocketId2);
        perCoreStates[i].isJobDone = true;
    }
    fflush(stdout);
    exit(signalCode);
}

void vnf::startEventLoop() {
// void vnf::startEventLoop(int (*init)(vnf::ConnId&) = nullptr) {
    spdlog::info("Event loop is started");

    signal(SIGINT, sigINTHandler);

    if(globals.serverProtocol == "sctp"){
        globals.listeningSocketFd = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    }
    else if(globals.serverProtocol == "tcp"){
        globals.listeningSocketFd = socket(AF_INET, SOCK_STREAM, 0);
    }
    else if(globals.serverProtocol == "uds"){
        globals.listeningSocketFd = socket(AF_UNIX, SOCK_STREAM, 0);
    }
    else if(globals.serverProtocol == "udp"){
        globals.listeningSocketFd = socket(AF_INET, SOCK_DGRAM, 0);
    }
    if (globals.listeningSocketFd < 0) {
        spdlog::error("Failed to create listening socket!");
        return;
    }

    int ret = makeSocketNonBlocking(globals.listeningSocketFd);
    if (ret < 0) {
        spdlog::error("Failed to set socket in non-blocking mode.");
        return;
    }
//    globals.socketProtocolMap[globals.listeningSocketFd] = globals.serverProtocol;

    struct sockaddr_in socketAddrIn;
    socketAddrIn.sin_family = AF_INET;
    socketAddrIn.sin_addr.s_addr = inet_addr(globals.serverIp.c_str());
    socketAddrIn.sin_port = htons(globals.serverPort);

    // Enable SO_REUSEADDR option
    int reuse = 1;
    if (setsockopt(globals.listeningSocketFd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1) {
        std::cerr << "Error setting SO_REUSEADDR: " << strerror(errno) << std::endl;
        close(globals.listeningSocketFd);
        return;
    }

    ret = bind(globals.listeningSocketFd, (struct sockaddr *) &socketAddrIn, sizeof(struct sockaddr_in));
    if (ret < 0) {
        spdlog::error("Failed to bind to the listening socket! ");
        return;
    }

    if(globals.serverProtocol == "sctp"){
        struct sctp_initmsg s_initmsg;
        memset (&s_initmsg, 0, sizeof (s_initmsg));
        s_initmsg.sinit_num_ostreams = 8;
        s_initmsg.sinit_max_instreams = 8;
        s_initmsg.sinit_max_attempts = 8;
        if(setsockopt (globals.listeningSocketFd, IPPROTO_SCTP, SCTP_INITMSG, &s_initmsg, sizeof (s_initmsg))<0){
            spdlog::error("Socket option error sctp");
            return;
        }
        struct sctp_event_subscribe events;
        memset( (void *)&events, 0, sizeof(events) );
        events.sctp_data_io_event = 1;
        if(setsockopt( globals.listeningSocketFd, SOL_SCTP, SCTP_EVENTS, (const void *)&events, sizeof(events))<0){
            spdlog::error("Socket option for sctp_events error");
            exit(-1);
        }
    }

    if(globals.serverProtocol != "udp"){
        ret = listen(globals.listeningSocketFd, 4096);
        if (ret < 0) {
            spdlog::error("Listen failed!");
            return;
        }
        spdlog::info("Listen on global socket successful");
    }

    spdlog::info("Datastore Memory Pool Size: {}", globals.dsMemPoolBlock.size());
    globals.dataStoreLock.lock();
    globals.dsMemPoolManager.add_block(&globals.dsMemPoolBlock.front(), globals.dsMemPoolBlock.size(), 256);
    globals.dataStoreLock.unlock();

    auto *servers = new pthread_t[userConfig->MAX_CORES];
    auto *arguments = new struct ServerPThreadArgument[userConfig->MAX_CORES];
    for (int ithCore = 0; ithCore < userConfig->MAX_CORES; ithCore++) {
        arguments[ithCore].set(ithCore, globals.serverIp, globals.serverPort, globals.onAcceptByServerCallback,
                               globals.onAcceptByServerDSCallback,
                               globals.onAcceptByServerReqObjIdExtractor,
                               globals.onAcceptByServerPBD);

        // todo start client only after all threads have started or it would clear actual sockid mappings
        pthread_create(&servers[ithCore], NULL, serverThread, &arguments[ithCore]);
    }

    for (int i = 0; i < userConfig->MAX_CORES; i++) {
        pthread_join(servers[i], NULL);
    }
}

ConnId vnf::ConnId::createClient(string localIP, string remoteIP, int remotePort, string protocol) {
    int sockId;
    if(protocol == "sctp"){
        sockId = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
        struct sctp_initmsg s_initmsg;
        memset (&s_initmsg, 0, sizeof (s_initmsg));
        s_initmsg.sinit_num_ostreams = 8;
        s_initmsg.sinit_max_instreams = 8;
        s_initmsg.sinit_max_attempts = 8;
        if(setsockopt (sockId, IPPROTO_SCTP, SCTP_INITMSG, &s_initmsg, sizeof (s_initmsg))<0){
            spdlog::error("Socket option error sctp");
            return ConnId(-1);
        }
        struct sctp_event_subscribe events;
        memset( (void *)&events, 0, sizeof(events) );
        events.sctp_data_io_event = 1;
        if(setsockopt( sockId, SOL_SCTP, SCTP_EVENTS, (const void *)&events, sizeof(events))<0){
            spdlog::error("Socket option for sctp_events error");
            exit(-1);
        }
    }
    else if(protocol == "tcp"){
        sockId = socket(AF_INET, SOCK_STREAM, 0);
    }
    else if(protocol == "uds"){
        std::cerr << "uds shall not be created as client." << std::endl;
        assert(false);
        // sockId = socket(AF_UNIX, SOCK_STREAM, 0);
    }
    else if(protocol == "udp"){
        sockId = socket(AF_INET, SOCK_DGRAM, 0);
    }
    if (sockId < 0) {
        spdlog::error("Failed to create listening socket! {}", errno);
        return ConnId(-1);
    }

    int coreId = this->coreId;
    int ret = makeSocketNonBlocking(sockId);
    perCoreStates[coreId].socketProtocolMap[sockId] = protocol;
    if (ret < 0) {
        spdlog::error("Failed to set socket in nonblocking mode.");
        return ConnId(-1);
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(remoteIP.c_str());
    address.sin_port = htons(remotePort);

    if(protocol == "udp"){
        perCoreStates[coreId].udpSocketAddrMap[sockId] = address;
    }
    else{
        ret = connect(sockId, (struct sockaddr *) &address, sizeof(struct sockaddr_in));
        if (ret < 0 && errno != EINPROGRESS) {
            spdlog::error("connect issue {}", errno);
            close(sockId);
            return ConnId(-1);
        }
    }

    struct epoll_event epollEvent;
    epollEvent.events = EPOLLIN | EPOLLOUT;
    epollEvent.data.fd = sockId;
//    int coreId = this->coreId;
    int epFd = perCoreStates[coreId].epollFd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, sockId, &epollEvent);

    return ConnId(coreId, sockId);
}

ConnId& vnf::ConnId::sendData(char *data, int dataLen, int streamNum) {
    int coreId = this->coreId;
    int socketId = this->socketId;

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
    if (perCoreStates[coreId].isDatastoreSocket(socketId)){
        currentProtocol = perCoreStates[coreId].dsSocketProtocol;
    } else {
        currentProtocol = perCoreStates[coreId].socketProtocolMap[socketId];
    }
    if(currentProtocol == "sctp"){
        ret = sctp_sendmsg(socketId, (void *) data, (size_t) dataLen, NULL, 0, 0, 0, streamNum, 0, 0);
    } else if(currentProtocol == "tcp"){
        ret = write(socketId, data, dataLen);
    } else if(currentProtocol == "uds"){    // Unix domain socket for local storage visiting.
    	// Very similar to normal TCP based socket.
        ret = send(socketId, data, dataLen, 0);         // Normally, we can use send, write to send a package from socket. Write is send with flags set to 0.
    } else if(currentProtocol == "udp"){
        struct sockaddr_in address = perCoreStates[coreId].udpSocketAddrMap[socketId];
        if(socketId >= SOCK_BOUNDARY){
            ret = sendto(globals.listeningSocketFd, data, dataLen, MSG_CONFIRM, (const struct sockaddr *) &address, sizeof(address));
        }
        else{
            ret = sendto(socketId, data, dataLen, MSG_CONFIRM, (const struct sockaddr *) &address, sizeof(address));
        }
    }
    int errnoLocal = errno;

    if (ret < 0) {
        if (errnoLocal == EAGAIN || errnoLocal == 32) {
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
    }

    perCoreStates[coreId].numSends++;

    return *this;
}

void registerDSCallback(ConnId& connId, enum EventType eventType, DSCallbackFn callback) {
    if (connId == FIRST_TIME_CONN_ID) {
        // TODO. Why special case the first CONN?
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

// Error call back is of the same format as callback. We don't need to return anything if we don't need callback.
ConnId& vnf::ConnId::storeData(string tableName, int key, enum DataLocation location, void *value, int valueLen, DSCallbackFn callback) {
    if (callback != nullptr) {
        // todo call this somewhere
        registerDSCallback(*this, ERROR, callback);
    }

    if (location == REMOTE || location == UDS) {
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

ConnId& vnf::ConnId::retrieveData(string tableName, int key, enum DataLocation location, vnf::DSCallbackFn callback, int reqObjId) {
    int coreId = this->coreId;
    int socketId = this->socketId;

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

    if (location == REMOTE || location == UDS) {
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
    if (location == REMOTE || location == UDS) {
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
    int ret, dataLen;
    char *data;
    string currentProtocol = perCoreStates[coreId].socketProtocolMap[socketId];
    while (!perCoreStates[coreId].isPendingDataQueueEmpty(socketId)) {
        PendingData dataToSend = perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].front();
        dataLen = dataToSend.dataLen;
        data = dataToSend.data;
        if(currentProtocol == "sctp"){
            ret = sctp_sendmsg(socketId, data, dataLen, NULL, 0, 0, 0, dataToSend.streamNum, 0, 0);
        } else if(currentProtocol == "tcp"){
            ret = write(socketId, data, dataLen);
        } else if(currentProtocol == "uds"){
            ret = send(socketId, data, dataLen, 0);
        } else if(currentProtocol == "udp"){
            struct sockaddr_in address = perCoreStates[coreId].udpSocketAddrMap[socketId];
            ret = sendto(socketId, data, dataLen, MSG_CONFIRM, (const struct sockaddr *) &address, sizeof(address));
        }
        perCoreStates[coreId].packetsMemPoolManager.free((void *) dataToSend.data);
        perCoreStates[coreId].socketIdPendingDataQueueMap[socketId].pop();
        if (ret < 0) {
            spdlog::error("Connection closed with client");
            break;
        }
    }
    perCoreStates[coreId].delLeftOverPacketFragment(socketId);
    // todo free callback registrations
    perCoreStates[coreId].socketProtocolMap.erase(socketId);
    if(currentProtocol == "udp"){
        perCoreStates[coreId].udpSocketAddrMap.erase(socketId);
    }
    if(socketId < SOCK_BOUNDARY){
        close(socketId);
    }
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

ConnId& vnf::registerCallback(ConnId& connId, enum EventType event, vnf::CallbackFn callback) {
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

ConnId& vnf::storeData(ConnId& connId, string tableName, int key, enum DataLocation location, void *value, int valueLen, vnf::DSCallbackFn errorCallback) {
  return connId.storeData(tableName, key, location, value, valueLen, errorCallback);
}

ConnId& vnf::retrieveData(ConnId& connId, string tableName, int key, enum DataLocation location, vnf::DSCallbackFn callback, int reqObjId) {
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

	struct epoll_event ev = {};
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
	ev.events=EPOLLIN | EPOLLET;/*Edge Triggered Behavior*/
	ev.data.fd=this->fd;
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
    if(epollFd == 0) {
        spdlog::debug("Epoll fd has not been created. fd {} will be added later",
                this->fd);
    }else if(epoll_ctl(epollFd,EPOLL_CTL_ADD,this->fd,&ev)==-1)
	{   
		spdlog::error("Error in settingup epoll event for Timer: {}",strerror(errno));
	}
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
        //De-Register the timer
        epoll_ctl(perCoreStates[coreId].epollFd,EPOLL_CTL_DEL,this->getFd(),NULL);
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

// Entry for Java calling this thread.
int __VNFThread(int argc, char *argv[]){
	vnf::startEventLoop();
}

void _disposalBody(vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int packetId, int errorCode){
    auto o = static_cast<Context *> (requestObject);
    while (o->AppIdx != -1){
        auto app = globals.sfc.SFC_chain[o->AppIdx];
        // Only user manually point the next app to send.
        o->AppIdx = -1;
        switch (o->ret)
        {
        case vnf::ERROR:
            if ((*app->errorHandler) == nullptr ){
                // Abort if error but no handler.
                o->ret = ABORT;
                return;
            }
            (*app->errorHandler)(connId, app->Txns, reqObjId, app->reqObjClip(requestObject), packet, packetId, packetLen, 0);
            break;
        case vnf::ACCEPT:
            (*app->acceptHandler)(connId, app->Txns, reqObjId, app->reqObjClip(requestObject), packet, packetId, packetLen, 0);
            break;
        case vnf::READ:
            (*app->readHandler)(connId, app->Txns, reqObjId, app->reqObjClip(requestObject), packet, packetId, packetLen, 0);
            break;
        case vnf::ABORT:
            return;
        } 
        // If transactions handler being called, the mark would be set.
        if ( o->waiting_for_transaction_back == true ){
        // FIXME. Copy to preserver for now.
            // Copy the packet.
            o->packet_record = new char[packetLen];
            memcpy(o->packet_record, packet, packetLen);
            o->packet_len = packetLen;
            // Break out if called request.
            return;
        }
    }
}

void _AppsDisposalAccept(vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int packetId, int errorCode, int streamNum) {
    auto o = static_cast<Context *> (requestObject);
    o->ret = vnf::ACCEPT;
	_disposalBody(connId, reqObjId, requestObject, packet, packetLen, packetId, errorCode);
}

void _AppsDisposalRead(vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int packetId, int errorCode, int streamNum) {
    auto o = static_cast<Context *> (requestObject);
    o->ret = vnf::ACCEPT;
	_disposalBody(connId, reqObjId, requestObject, packet, packetLen, packetId, errorCode);
}

void _AppsDisposalError(vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int packetId, int errorCode, int streamNum) {
    auto o = static_cast<Context *> (requestObject);
    o->ret = vnf::ACCEPT;
	_disposalBody(connId, reqObjId, requestObject, packet, packetLen, packetId, errorCode);
}

int __initSFC(int argc, char *argv[]){
	// User construct Main.
	int ret = VNFMain(argc, argv);

	if (ret != 0){
		return ret;
	}

	std::string ip;
	int port = 0; // Initialize to a default value, or you can set an error value if no valid port is provided.

	if (argc != 3)
	{
		std::cerr << "Usage: " << argv[0] << " <IP address> <port>" << std::endl;
		return -1; // Return an error code
	}

	// Parse port (argv[2])
	try
	{
		port = std::stoi(argv[2]);
	}
	catch (const std::exception &e)
	{
		std::cerr << "Invalid port number: " << e.what() << std::endl;
		return 1; // Return an error code
	}

	// Parse IP address (argv[1])
	ip = argv[1];

	// TODO. Param Init.
    vnf::ConnId serverId = vnf::initServer("", ip, port, "tcp");

    int size[] = {globals.sfc.reqObjTotalSize};
    vnf::initReqPool(size, 1);

	serverId.registerCallback(vnf::ACCEPT, _AppsDisposalAccept);
	serverId.registerCallback(vnf::READ, _AppsDisposalRead);
	serverId.registerCallback(vnf::ERROR, _AppsDisposalError);

	// report to txnEngine.
	return globals.sfc.NFs();
};


// TODO. Define the structure for message convey.
int DB4NFV::SFC::NFs(){};

void DB4NFV::SFC::Entry(App& app){
	if (this->SFC_chain.size() != 0){
		perror("fatal: the SFC has already registered entry.");
	}
	this->SFC_chain.push_back(&app);
}
void DB4NFV::SFC::Add(App& last, App& app){
	if (this->SFC_chain.size() == 0){
		perror("fatal: the SFC has no entry.");
	} 
	last.next = &app;
	this->SFC_chain.push_back(&app);
}

// int DB4NFV::SFC::_callBack(vnf::ConnId& connId, int AppIdx, int TxnIdx, int SAIdx, int reqObjId, void* reqObj, char * packet, int packetlen, void * value, int length, int errCode){
// 	// Seems we need to find out the ConnId from the context queue and continue.
//     auto app = this->SFC_chain.at(AppIdx);
// 	return (*app.Txns.at(TxnIdx)->sas.at(SAIdx)->txnHandler_)(
//         connId,
//         app.Txns,
//         reqObjId, app.reqObjClip(reqObj),
//         packet, packetlen,
//         value, length, errCode);
// }

// The entry for sending txn execution request to txnEngine.
// TODO. Extend the other information.
void __request(uint64_t txnId){
    return globals.__java_env->CallVoidMethod(
        globals.__client_obj, globals.__request,
        txnId
        // Other parameters to be added.
    );
}

int DB4NFV::StateAccess::Request(vnf::ConnId& connId, char * packet, int packetLen,  int packetId, void * reqObj, int reqObjId) {
    // Create a new event blocking fd.
    if (uint64_t(reqObj) == 0){
        perror("fatal: reqObj is null.");
    }
    auto o = reinterpret_cast<Context *>(CONTEXT(reqObj));

    o->AppIdx = this->appIndex;
    o->TxnIdx = this->txnIndex;
    o->SAIdx = this->saIndex;
    o->old_socket = connId.socketId;
    o->reqObjId = reqObjId;

    // Register call back parameters in the context.
    perCoreStates[connId.coreId].packetNumberContextMap[packetId] = o;
    // TODO. Fix parameter passing.
	__request(TXNID(connId.coreId, packetId));
}

// Routing to the context of transaction breakpoint.
int _callBack(uint64_t txnReqId, void * value, int length, void * result){
    // Write the value to the corresponding context.
    auto ctx = perCoreStates[COREID(txnReqId)].packetNumberContextMap[PACKETID(txnReqId)];
    ctx->value = value;
    ctx->value_len = length;

    // FIXME: If callback handled this way, it would need another way to report abortion and write back.
    // Trigger the hook and return.
    // if (write(perCoreStates[COREID(txnId)].epollFd, &txnId, sizeof(uint64_t)) < 0){
    //     perror("_callback.write");
    // }

    ConnId connId = ConnId(COREID(txnReqId), ctx->old_socket);
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

	// Return -1 if abort.
    _disposalBody(connId, ctx->reqObjId,
                perCoreStates[COREID(txnReqId)].socketIdReqObjIdToReqObjMap[ctx->old_socket][ctx->reqObjId],
                ctx->packet_record, ctx->packet_len, 0);
    return ctx->ret == ABORT? -1:0;    
}

// TODO. TO BE DEBUGGED.
JNIEXPORT jstring 
JNICALL Java_libVNFFrontend_NativeInterface__1_1init_1SFC
  (JNIEnv *env , jobject obj, jint argc, jobjectArray argv){

		// Convert the jobjectArray to a char* array
		jsize arrayLength = env->GetArrayLength(argv);
		char **argvC = new char *[arrayLength];

		for (jsize i = 0; i < arrayLength; i++)
		{
			jstring string = (jstring)env->GetObjectArrayElement(argv, i);
			const char *str = env->GetStringUTFChars(string, 0);
			argvC[i] = strdup(str);
			env->ReleaseStringUTFChars(string, str);
		}

        // Find the method and save in globals.
        // Find the class that contains the sendTxnRequest method
        jclass cls = env->FindClass("cli/CliFrontend");
        if (cls == NULL)
        {
            perror("utils.jni.init_SFC:class not found.");
		    return env->NewStringUTF("");
        }

        // Get a reference to the sendTxnRequest method
        jmethodID methodId = env->GetMethodID(cls, "sendTxnRequest", "()V");

        if (methodId == NULL)
        {
            perror("utils.jni.init_SFC:method not found.");
		    return env->NewStringUTF("");
        }

        // Call the sendTxnRequest method
        globals.__request = methodId;
        globals.__client_obj = obj;
        globals.__java_env = env;

        // Call the __init_SFC function
		std::string result = __init_SFC(argc, argvC);

		// Clean up the allocated memory
		for (jsize i = 0; i < arrayLength; i++)
		{
			free(argvC[i]);
		}
		delete[] argvC;

		// Return the result as a Java string
		return env->NewStringUTF(result.c_str());
}

// Actual vnfs loop.
JNIEXPORT void 
JNICALL Java_libVNFFrontend_NativeInterface__1_1VNFThread
  (JNIEnv * env, jobject obj, jint c, jobjectArray v){
	startEventLoop();
  }


// The callback handling entrance.
JNIEXPORT jbyteArray 
JNICALL Java_libVNFFrontend_NativeInterface__1execute_1sa_1udf
  (JNIEnv * env, jobject obj, jlong saReqId_jni, jbyteArray value, jint length){
    // Save the value in ctx.
    uint64_t txnReqId = static_cast<uint64_t>(saReqId_jni); 
    jbyte *inputBuffer = env->GetByteArrayElements(value, NULL);

	int coreId = COREID(txnReqId);
	auto ctx = perCoreStates[COREID(txnReqId)].packetNumberContextMap[PACKETID(txnReqId)];
    // TODO. Check if deallocated.
    auto tmp = new uint8_t[length];
    ctx->value = tmp;
    ctx->value_len = length;
    memcpy(ctx->value, inputBuffer, length);

    ConnId conn = ConnId(COREID(txnReqId), ctx->old_socket);

    // How to recover the Index.
	bool abortion = globals.sfc._callBack(conn, ctx->AppIdx, ctx->TxnIdx, ctx->SAIdx, 
        ctx->reqObjId, 
        perCoreStates[coreId].socketIdReqObjIdToReqObjMap[ctx->old_socket][ctx->reqObjId],
        ctx->packet_record, 
        ctx->packet_len, ctx->value, ctx->value_len, 0) == 1? true: false;

    delete tmp;
    ctx->value_len = -1;

    auto ret = env->NewByteArray(sizeof(bool) + sizeof(ctx->result_len));
    if (env == nullptr) {
        perror("core.cpp.Java_libVNFFrontend_NativeInterface__1execute_1sa_1udf.NewByteArray.nullptr");
    }

    jbyte firstByte = static_cast<jbyte>(abortion);
    env->SetByteArrayRegion(ret, 0, 1, &firstByte);
    env->SetByteArrayRegion(ret, 1, static_cast<jsize>(ctx->result_len), reinterpret_cast<jbyte *>(ctx->result));
    return ret;
  }

// Report done.
JNIEXPORT jint 
JNICALL Java_libVNFFrontend_NativeInterface__1_1txn_1finished
  (JNIEnv * env, jobject obj, jlong saReqId_jni){
    uint64_t txnReqId = static_cast<uint64_t>(saReqId_jni); 
    // Write to the fd to suggest done.
    if (write(perCoreStates[COREID(txnReqId)].txnSocket, &txnReqId, sizeof(uint64_t)) < 0){
        perror("jni.handle_done.write");
        return -1;
    }
    return 0;
  }

void DB4NFV::SFC::Init(int maxCores){
    // Fix the backlink.
    int reqObjStart = 0;
    int reqObjIndex = 0;
    for (int i = 0 ; i < SFC_chain.size(); i += 1)
    {
        auto app = SFC_chain.at(i);
        app->appIndex = i;
        // Sort the reqObj. TODO.
        this->objSizes.push_back(app->reqObjSize);
        this->objSizesStarting.push_back(reqObjStart);
        reqObjStart += app->reqObjSize;
        this->AppIdxMap[app] = reqObjIndex;
        reqObjIndex += 1;
        
        for (int j = 0 ; j < app->Txns.size(); j += 1){
            auto Txn = &app->Txns.at(j);
            Txn->txnIndex = j;
            Txn->app = app;
            for (int k = 0; k < (Txn->sas).size(); k += 1){
                auto sa = Txn->sas.at(k);
                sa.app = app;
                sa.txn = Txn;
                sa.appIndex = i;
                sa.txnIndex = j;
                sa.saIndex = k;
            }
        }
    }
    this->reqObjTotalSize = reqObjStart;
    // Config and init libVNF.
    this->cores = maxCores;
    // Create back links for transactions and state access.
    vnf::initLibvnf(cores, 128, "127.0.0.1", std::vector<int>(), 131072, vnf::LOCAL);
}

void *DB4NFV::App::reqObjClip(void * reqObjClip) { return reqObjClip + sizeof(Context) + globals.sfc.objSizes.at(globals.sfc.AppIdxMap[this]);}

DB4NFV::SFC& GetSFC(){ 
    return globals.sfc; 
};