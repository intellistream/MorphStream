#ifndef LIBVNF_H
#define LIBVNF_H

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <spdlog/spdlog.h>
#include <assert.h>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <cstdint>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h> 
#include <sys/timerfd.h>
#include <netinet/in.h>
#include <netinet/sctp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <dirent.h>
#include <cstring>
#include <ctime>
#include <pthread.h>
#include <csignal>
#include <iostream>
#include <sys/time.h>
#include <sched.h>
#include <map>
#include <unordered_map>
#include <string>
#include <bitset>
#include <boost/pool/simple_segregated_storage.hpp>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp> 
#include <vector>
#include <cstddef>
#include <algorithm>
#include <mutex>
#include <fstream>
#include <chrono>
#include "intellistream_morphstream_util_libVNFFrontend_NativeInterface.h"

#include "datastore/dspackethandler.hpp"
#include "json.hpp"

#define LIBVNF_STACK_KERNEL 1
#define LIBVNF_STACK_KERNEL_BYPASS 2
#define LIBVNF_STACK_KERNEL_L3 3

#if LIBVNF_STACK == LIBVNF_STACK_KERNEL

#include <sys/epoll.h>

#elif LIBVNF_STACK == LIBVNF_STACK_KERNEL_BYPASS

#include "mtcp_api.h"
#include "mtcp_epoll.h"
//#include "dpdk_api.h"
//#include "netmap_api.h"
//#include "cpu.h"
#include "debug.h"
#include "rss.h"

#elif LIBVNF_STACK == LIBVNF_STACK_KERNEL_L3

#define NETMAP_WITH_LIBS
#include <net/netmap_user.h>
#include <net/netmap.h>
#include <sys/poll.h>
#include <net/ethernet.h>
#include <netinet/ip.h>
#include <netinet/udp.h>

#endif

#define MAX_EVENTS 2048
#define ARP_REQUEST 1
#define ARP_REPLY 2
#define ARP_CACHE_LEN 20
#define NUM_CALLBACK_EVENTS 2
#define SOCK_BOUNDARY 65536
#define TIMER_DEFAULT_DURATION (6)
#define TIMER_DEFAULT_RETRIES  (4)

#define COREID(TXNID) (static_cast<int>((TXNID) >> sizeof(int)))
#define PACKETID(TXNID) (static_cast<int>(TXNID))
#define TXNREQID(COREID, PACKETID) ((COREID) << sizeof(int) | (PACKETID))

// User define VNF init.
int VNFMain(int argc, char ** argv);

namespace vnf {

enum EventType {
    READ = 0, ACCEPT = 1, ERROR = 2, WAIT = 3, ABORT = 4
};

enum DataLocation {
#ifdef MORPH
    LOCAL = 1, REMOTE = 2, CHECKCACHE = 3, MORPH = 4, UDS = 5
#else
    LOCAL = 1, REMOTE = 2, CHECKCACHE = 3, UDS = 5
#endif
};

class ConnId;

// ConnId, reqObjId, reqObj, packet, packetId, packetLen, streamNum, errorCode.
typedef void (*CallbackFn)
	(ConnId&, int, void *, char *, int, int, int, int);

// ConnId, reqObjId, reqObj, packet, packetId, packetLen, streamNum, errorCode.
typedef void (*ErrorCallbackFn)
	(ConnId& connId, int reqObjId, void * requestObject, void * value, int valueLen, int errorCode, int streamNum);

// ConnId, reqObjId, reqObj, value, valueLen, streamNum, errorCode.
typedef void (*DSCallbackFn)
	(ConnId& connId, int reqObjId, void * requestObject, void *value, int valuelen, int errorCode, int streamNum);


class ConnId {
public:
  const int coreId;
  const int socketId;
  // Get the current timeStamp.
  uint64_t id;

  ConnId(int coreId, int socketId) : coreId(coreId), socketId(socketId) {
	  auto currentTime = std::chrono::high_resolution_clock::now();
	  auto nanosecondsSinceEpoch = std::chrono::time_point_cast<std::chrono::nanoseconds>(currentTime).time_since_epoch().count();
	  uint64_t timestamp = static_cast<uint64_t>(nanosecondsSinceEpoch);
	  id = timestamp & 0x00000000FFFFFFFFULL | (static_cast<uint64_t>(coreId) << 32);
  }

  ConnId(int value) : coreId(value), socketId(value) {
	  auto currentTime = std::chrono::high_resolution_clock::now();
	  auto nanosecondsSinceEpoch = std::chrono::time_point_cast<std::chrono::nanoseconds>(currentTime).time_since_epoch().count();
	  uint64_t timestamp = static_cast<uint64_t>(nanosecondsSinceEpoch);
	  id = timestamp & 0x00000000FFFFFFFFULL | (static_cast<uint64_t>(coreId) << 32); }

  // Recover ConnId
  ConnId(int coreId, int socketId, int id) : coreId(coreId), socketId(socketId), id(id) {}

  int coreIdFromId(){
	return id >> 32;
  }

  bool isValid() {
    return coreId > -1 || socketId > -1;
  }

  bool operator==(const ConnId& rhs) {
    return this->coreId == rhs.coreId && this->socketId == rhs.socketId;
  }

  void *
  allocReqObj(int reqObjType, int reqObjId = 0);

  ConnId&
  freeReqObj(int reqObjType, int reqObjId = 0);

  ConnId&
  linkReqObj(void *requestObj, int reqObjId = 0);

  void *
  setPktDNE(void *packet);

  ConnId&
  unsetPktDNE(void *packet);

  char *
  getPktBuf();

  ConnId&
  registerCallback(enum EventType event, CallbackFn callback);

  ConnId&
  registerReqObjIdExtractor(int roidExtractor(char *packet, int packetLen));

  ConnId&
  registerPacketBoundaryDisambiguator(vector<int> pbd(char *buffer, int bufLen));

  ConnId
  createClient(string localIP, string remoteIP, int remotePort, string protocol);

  ConnId&
  sendData(char *packetToSend, int packetSize, int streamNum = 0);

  ConnId&
  storeData(string tableName, int key, enum DataLocation location, void *value, int valueLen,
          ErrorCallbackFn errorCallback);

  ConnId&
  retrieveData(string tableName, int key, enum DataLocation location,
          DSCallbackFn callback, int reqObjId = 0);

  ConnId&
  delData(string tableName, int key, enum DataLocation location);

  void
  closeConn();

};

class timer{
private:
  int coreId;
  int fd;
  timer(const class timer& t) = delete;
  class timer & operator=(const timer &t) = delete;
public:
  int duration;
  int retries;
  void* data;

  timer(int coreId=0);
  ~timer();
  void (*timeOutFunction)(timer *);
  void startTimer(int duration, int retries);
  void startTimer();//previous/default time of 6 Sec
  void stopTimer();
  int getFd();
};

timer* registerTimer(void timeOutFunction(timer *), ConnId& ConnId);
void deleteTimer(timer *t);

namespace http {

  enum err {
  DONE = 0,
  INCOMPLETE_PACKET = 1,
  NO_PACKET = 2
  };

  struct extractResponseArg {
    char* rawPacket;
    int packetLen;
    string packet;
    map<string,string> headers;
    enum err errCode;
  };

  struct extractRequestArg {
    char* rawPacket;
    int packetLen;
    string packet;
    string reqType;
    vector<string> path;
    map<string,string> headers;
    enum err errCode;
  };

  string createHTTPRequest(string reqType, string host, string url, nlohmann::json reqBody=NULL, string contentType="application/json");
  string createHTTPRequest1(string reqType, string host, string url, string reqBody="", string contentType="application/json");

  void extractHTTPResponse(int &status_code, extractResponseArg &arg);

  string createHTTPResponse(int status_code, nlohmann::json reqBody=NULL, string contentType="application/json");
  string createHTTPResponse1(int status_code, string resBody="", string contentType="application/json");

  void extractHTTPRequest(bool &status, extractRequestArg &arg);

  void splitUrl(string baseUrl, string &host, string &ipAddr, int &port);

  string encodeQuery(string name, string value);
  
};

typedef void (*fn_ctrl)(string task, string vnf_name, string vnf_ip, string event);

typedef int (*ReqObjExtractorFn)(char *, int);

typedef vector<int> (*PacketBoundaryDisambiguatorFn)(char *, int);

/**
 * @brief Initializes the libvnf config parameters
 * @param maxCores: number of cores of VM
 * @param bufferSize: packet buffer size,
 * @param dataStoreIP: ip of data store,
 * @param dataStorePorts: ports of datastore,
 * @param dataStoreThreshold: local datastore size,
 * @param useRemoteDataStore: use local or remote datastore
 * @return 0 for success, appropriate error code for any failure
 * */
int
initLibvnf(int maxCores, int bufferSize,
           string dataStoreIP,
           vector<int> dataStorePorts,
           int dataStoreThreshold,
           enum DataLocation dsType);

/**
 * @brief Initializes the libvnf config parameters
 * @param jsonFilePath: path to json file containing parameters
 * @return 0 for success, appropriate error code for any failure
 * */
/*
int
initLibvnf(const string &jsonFilePath);
*/
/**
 * @brief Initialize number of request object and their sizes
 * @param requestObjectSizes[]: size of request object of each type
 * @param numRequestObjectSizes: total number of request object types
 */
void
initReqPool(int requestObjectSizes[], int numRequestObjectSizes);

/**
 * @brief Assign a request object to a connection
 * @param connId: connection identifier
 * @param reqObjType: type of request object
 * @param reqObjId: request object identifier in set belonging to a connection id
 * @return pointer to the memory allocated for request object with id as reqObjId
 * */
void *
allocReqObj(ConnId& connId, int reqObjType, int reqObjId = 0);

/**
 * @brief Free the memory of a request object
 * @param connId: connection identifier to which the request object is assigned
 * @param reqObjType: type of request object
 * @param reqObjId: request object identifier in set belonging to a connection id
 * */
ConnId&
freeReqObj(ConnId connId, int reqObjType, int reqObjId = 0);

/**
 * @brief Link an existing request object to a new connection pertaining to the same request
 * @param connId: connection identifier
 * @param *requestObj: existing request object
 * @param reqObjId: request object identifier in set belonging to a connection id
 * */
ConnId&
linkReqObj(ConnId connId, void *requestObj, int reqObjId = 0);

/**
 * @brief Set do not evict flag on packet i.e. it is not freed right after the callback is executed
 * @param connId: connection identifier
 * @param *packet: pointer to packet in packet pool
 * @return same as packet
 * */
void *
setPktDNE(ConnId& connId, void *packet);

/**
 * @brief Remove DNE flag and remove the packet from the packet pool
 * @param connId: connection identifier
 * @param *pktMemPtr: pointer to packet in packet pool
 * */
ConnId&
unsetPktDNE(ConnId connId, void *packet);

/**
 * @brief Get a pointer newly allocated memory where packet can be written and passed to sendData. The deallocation of this memory is automatically done after packet is sent successfully.
 * @param connId: connection for which buffer is needed
 * @return pointer to newly allocated memory where packet to send can be written
 * */
char *
getPktBuf(ConnId& connId);

/**
 * @brief Tag cached key value pair of remote datastore non evictable. By doing this it is not cleared from cache until unsetCachedDSKeyDNE is called.
 * @param dsKey: the key in key value pair
 * @return pointer to the value corresponding to dsKey stored in cache is returned
 * */
void *
setCachedDSKeyDNE(int dsKey);

/**
 * @brief Tag cached key value pair of remote datastore evictable.
 * @param dsKey: the key in key value pair
 * */
void
unsetCachedDSKeyDNE(int dsKey);

/**
 * @brief Initialize the VNF as server with network parameters
 * @param _interface: the interface to send/receive packets (neede for L3 VNF)
 * @param serverIp: IP of VNF (needed for app-layer VNF)
 * @param serverPort: port of VNF (needed for app-layer VNF)
 * @param protocol: communication protocol
 * @return the connection id that can be used to register callbacks when server accepts clients
 * */
ConnId
initServer(string _interface, string serverIp, int serverPort, string protocol);

/**
 * @brief Register a callback for an event type on a connection
 * @param connId: connection identifier
 * @param event: type of event READ or ERROR
 * @param callback(ConnId& connId, void * requestObject, char * packet, int packet_length, int error_code)
 * */
ConnId&
registerCallback(ConnId& connId, enum EventType event,
                 CallbackFn callback);

/**
 * @brief Register a function to extract request object id from a packet on a connection
 * @param connId: connection identifier
 * @param roidExtractor(char * packet, int packetLen): function that extracts request object id from a given packet
 * */
ConnId&
registerReqObjIdExtractor(ConnId& connId, int roidExtractor(char *packet, int packetLen));

/**
 * @brief Register a function to return a list of packets given a network buffer
 * @param connId: connection identifier
 * @param pbd(char *buffer, int bufLen): function to return a list of packets given a network buffer
 * */
ConnId&
registerPacketBoundaryDisambiguator(ConnId& connId, vector<int> pbd(char *buffer, int bufLen));

/**
 * @brief start the network function
 * */
void
startEventLoop();
// startEventLoop(int (*init)(ConnId&) = nullptr);

/**
 * @brief Connect as client to an IP and port using a protocol
 * @param connId: connection identifier
 * @param localIP: IP of current VNF
 * @param remoteIP: IP of VNF to which we want to connect as client
 * @param remotePort: port of the other VNF
 * @param protocol: communication protocol
 * @return the connection id that can be used to register callbacks when read or error occurs on the socket
 * */
ConnId
createClient(ConnId& connId, string localIP, string remoteIP, int remotePort, string protocol);

/**
 * @brief Send data to a network function
 * @param connId: connection identifier
 * @param *packetToSend: data to be sent
 * @param packetSize: size of data
 * @param streamNum: stream number of sctp connection if the connection is on sctp
 * */
ConnId&
sendData(ConnId& connId, char *packetToSend, int packetSize, int streamNum = 0);

/**
 * @brief Store data in datastore
 * @param connId: connection identifier
 * @param table_name: name of table (can be empty string if only one table)
 * @param key: key identifier (currently only int value allowed)
 * @param location: where data to be stored remote or local
 * @param *value: value to be stored corresponding to the key
 * @param _value_len: length of value
 * @param callbackFnPtr(ConnId& connId, void * requestObject, void * value, int packet_length, int error_code) : callback function called when error occurs
 * */
ConnId&
storeData(ConnId& connId, string tableName, int key, enum DataLocation location, void *value, int valueLen,
        ErrorCallbackFn errorCallback);

/**
 * @brief Fetch data from datastore
 * @param connId: connection identifier
 * @param tableName: name of table (can be empty string if only one table)
 * @param key: key identifier (currently only int value allowed)
 * @param location: where data to be stored remote or local
 * @param callback(ConnId& connId, void * requestObject, void * value, int packet_length, int error_code) : callback function called when data is received
 * */
ConnId&
retrieveData(ConnId& connId, string tableName, int key, enum DataLocation location,
        DSCallbackFn callbck, int reqObjId = 0);

/**
 * @brief Delete key-value pair from datastore
 * @param connId: connection identifier
 * @param tableName: name of table (can be empty string if only one table)
 * @param key: key identifier (currently only int value allowed)
 * @param location: where data to be stored remote or local
 * */
ConnId&
delData(ConnId& connId, string tableName, int key, enum DataLocation location);

/**
 * @brief Close a connection
 * @param connId: connection identifier
 * */
void
closeConn(ConnId& connId);

void
registerforNotification(string controller_ip,
                        void callbackFnPtr(string task, string vnf_name, string vnf_ip, string event));

void
handle_arp_packet(char *buffer);


ConnId getObjConnId(uint32_t connId);

uint32_t getIntConnId(ConnId& connId);
}

struct BackendServer
{
	std::string ip;
	int port;
};

class Config
{
public:
	std::string dataLocation = "LOCAL"; // Default value
	std::vector<BackendServer> backendServers;
	int bufferSize = 1024;									 // Default value
	int dataStoreThreshold = 50;							 // Default value
	int coreNumbers = 4;									 // Default value
	std::string dataStoreSocketPath = "/tmp/datastore/"; // Default value
	int dataStoreFileNames = 10;							 // Default value
	std::string dataStoreIP = "127.0.0.1";					 // Default value
	int dataStorePort = 8080;								 // Default value
	std::string serverIP = "0.0.0.0";						 // Default value
	int serverPort = 8888;									 // Default value
	int reuseMode = 0;									 // Default value

	Config() {}
	Config(std::string path)
	{
		try
		{
			readConfigFromCSV(path);

			// Access configuration values
			std::cout << "Data Location: " << dataLocation << std::endl;
			for (const auto &server : backendServers)
			{
				std::cout << "Backend Server IP: " << server.ip << ", Port: " << server.port << std::endl;
			}
			std::cout << "Buffer Size: " << bufferSize << std::endl;
			std::cout << "Data Store Threshold: " << dataStoreThreshold << std::endl;
			std::cout << "Core Numbers: " << coreNumbers << std::endl;
			std::cout << "Data Store Socket Path: " << dataStoreSocketPath << std::endl;
			std::cout << "Data Store File Names: " << dataStoreFileNames << std::endl;
			std::cout << "Data Store IP: " << dataStoreIP << std::endl;
			std::cout << "Data Store Port: " << dataStorePort << std::endl;
			std::cout << "Server IP: " << serverIP << std::endl;
			std::cout << "Server Port: " << serverPort << std::endl;
			std::cout << "Reusing Port: " << reuseMode << std::endl;
		}
		catch (const std::exception &e)
		{
			std::cerr << "Error: " << e.what() << std::endl;
		}
	}

	void readConfigFromCSV(const std::string &csvFilePath)
	{
		std::ifstream inputFile(csvFilePath);
		if (!inputFile.is_open())
		{
			throw std::runtime_error("Failed to open CSV file.");
			exit(-1);
		}

		std::string line;
		while (std::getline(inputFile, line))
		{
			std::istringstream iss(line);
			std::string key, value;
			if (std::getline(iss, key, ',') && std::getline(iss, value, ','))
			{
				parseConfigKeyValue(key, value);
				// assert(false);
			}
		}
	}

private:
	void parseConfigKeyValue(const std::string &key, const std::string &value)
	{
		if (key == "dataLocation")
		{
			setDataLocation(value);
		}
		else if (key == "backendServer")
		{
			parseBackendServer(value);
		}
		else if (key == "bufferSize")
		{
			bufferSize = std::stoi(value);
		}
		else if (key == "dataStoreThreshold")
		{
			dataStoreThreshold = std::stoi(value);
		}
		else if (key == "coreNumbers")
		{
			coreNumbers = std::stoi(value);
		}
		else if (key == "dataStoreSocketPath")
		{
			dataStoreSocketPath = value;
		}
		else if (key == "dataStoreFileNames")
		{
			dataStoreFileNames = std::stoi(value);
		}
		else if (key == "dataStoreIP")
		{
			dataStoreIP = value;
		}
		else if (key == "dataStorePort")
		{
			dataStorePort = std::stoi(value);
		}
		else if (key == "serverIP")
		{
			serverIP = value;
		}
		else if (key == "serverPort")
		{
			serverPort = std::stoi(value);
		}
		else if (key == "reuseMode")
		{
			reuseMode = std::stoi(value);
		}
		else
		{
			throw std::runtime_error("Unknown configuration key: " + key);
		}
	}

	void setDataLocation(const std::string &value)
	{
		if (value == "LOCAL")
		{
			dataLocation = value;
		}
		else if (value == "REMOTE")
		{
			dataLocation = value;
		}
		else if (value == "UDS")
		{
			dataLocation = value;
		}
		else
		{
			throw std::invalid_argument("Invalid dataLocation value: " + value);
		}
	}

	void parseBackendServer(const std::string &value)
	{
		std::istringstream iss(value);
		std::string ipStr, portStr;
		if (std::getline(iss, ipStr, ':') && std::getline(iss, portStr, ':'))
		{
			BackendServer server;
			server.ip = ipStr;
			server.port = std::stoi(portStr);
			backendServers.push_back(server);
		}
		else
		{
			throw std::invalid_argument("Invalid backendServer value: " + value);
		}
	}
};

class Context{

public:
	// Exposing packet.
	int packet_len() { assert( _packet != NULL); return _packet_len; }
	char * packet() { assert( _packet != NULL); return _packet; }
	
	// Fetched Value
	int value_len() { assert( _value != NULL); return _value_len; }
	void * value() { assert( _value != NULL); return _value; }

	// Execution Status.
	int AppIdx() { return _AppIdx; }
	int TxnIdx() { return _TxnIdx; }

	// ReqObj
	void * reqObj(){ return GetGlobal().sfc.SFC_chain[_AppIdx]->reqObjClip(this);}
	int reqObjId(){ return _reqObjId;}

	// Write back.
	void WriteBack(void * v, int len){
		assert(_result == NULL);
		JNIEnv * env;
		GetJniEnv(&env);
		_result = env->NewByteArray(sizeof(bool) + len);
		env->SetByteArrayRegion(_result, 1, static_cast<jsize>(len), reinterpret_cast<jbyte *>(v));
	}

	void Abort() { _ret = ABORT;}

	// Available Transactions.
	DB4NFV::Transaction& const Transaction(int idx) {
		assert(idx < GetGlobal().sfc.SFC_chain[_AppIdx]->Txns.size());
		_clear_value();
		return GetGlobal().sfc.SFC_chain[_AppIdx]->Txns[idx];
	}

	// Register Next App.
	void NextApp(int appIdx, vnf::EventType ret){
		_AppIdx = appIdx;
		_ret = ret;
	}

	vnf::EventType ReturnValue(){
		return _ret;
	}

	// Used to register is returning from a txnRequest?
	bool IsWaitingForTxnBack() { return waiting_for_transaction_back;}

	/*
		These interface shall not be called by user.
	*/
	// Reserved Interface to provide lifecycle manage.
	void _reset_appIdx() { _AppIdx = -1;}

	// Context wont need a malloc type constructor. It's alias of reqObj.
	void _set_status(vnf::EventType status){
		_AppIdx = 0;
		_ret = status;
	}
	void _set_value_from_callback(void * value, int value_len){
		_clear_value();
		_value = value;
		_value_len = value_len;
	}
	void _clear_value(){
		if (_value) {
			delete _value;
			_value_len = -1;
		}
	}
	inline int _old_socket() { return old_socket; }
	inline void _set_old_socket(int s) { old_socket = s; }
	inline void _set_wait_txn_callback() {waiting_for_transaction_back = true;}
	inline jbyteArray _res_ptr() {return _result;}
	inline int _ts_low_32b(){ return ts_low_32b; }
private:
	// Sequence. 
	int ts_low_32b;

    // Breakpoint status.
    int _AppIdx;
    int _TxnIdx;

    // Data fetching to be saved.
    char * _packet;
    int _packet_len;

    // Data fetched.
    void * _value;
    int _value_len;

	// Data ptr to be write back.
	jbyteArray _result;

    // ReqObjId;
    int _reqObjId;

    // Running status.
    bool waiting_for_transaction_back = false;

    // The socketId for the last request.
    int old_socket = -1;
    vnf::EventType _ret;
};

namespace DB4NFV{

class App;
class SFC;
class Transaction;
class StateAccess;

typedef void (*TxnCallBackHandler)(vnf::ConnId& connId, Context &ctx);
typedef void (*PacketHandler)(vnf::ConnId& connId, Context &ctx);

typedef PacketHandler AcceptHandler;
typedef PacketHandler ReadHandler;
typedef PacketHandler ErrorHandler;
typedef PacketHandler PostTxnHandler;

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
	App(std::vector<Transaction> txns, 
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
    void *reqObjClip(void * reqObjClip);
	int reqObjSize = 0; // Register the size of per-thread user defined structure.

    std::vector<Transaction> Txns;

    int appIndex;
	App* next = nullptr;
	App* MoveNext() { return this->next;}
};

class StateAccess {
friend SFC;
public:
    // What is a state scope?
    StateAccess(
		const std::vector<int> keys,
		const std::vector<int> fields,
		// const std::vector<std::string> types,
                ConsistencyRequirement cr,
                TxnCallBackHandler txnHandler,
                TxnCallBackHandler errTxnHandler,
                PostTxnHandler postHandler,
                RWType rw)
        : fields_(fields), keys_(keys), cr_(cr),
        // : fields_(fields), types_(types), cr_(cr),
          txnHandler_(txnHandler), rw_(rw), errorTxnHandler_(errTxnHandler),
          postTxnHandler_(postHandler) {}

	const enum RWType rw_;	

    const std::vector<int> &keys_;
    const std::vector<int> &fields_;
    const ConsistencyRequirement cr_;

	// The binded transaction handler for this State Access.
	const TxnCallBackHandler txnHandler_ = nullptr;
	const TxnCallBackHandler errorTxnHandler_ = nullptr;
	const PostTxnHandler postTxnHandler_ = nullptr;

	// Called by user to start request, send the transaction request to schedule.
	void Request(
    	vnf::ConnId& connId, Context &ctx, const char *key, bool isAbort
	);

	App* app;
	Transaction* txn;
    int appIndex;
	int txnIndex;
    int saIndex;
};

class Transaction {
friend SFC;
public:
    Transaction(std::vector<StateAccess> sas):sas(sas){}
	std::vector<StateAccess> sas = {};	// The State Access event to be used for transaction handlers. Including transaction handlers.
	const App* app;
	int txnIndex;
	void Trigger(vnf::ConnId& connId, Context &ctx, const char *key, bool isAbort ) const
	{ 
		ctx._set_wait_txn_callback();
		for (auto sa: sas){
			sa.Request(connId, ctx, key, isAbort); 
		}
	}
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
	void Entry(App& entry);
	void Add(App& last, App& app);

	// Report SFC apps to txnEngine.
	std::string NFs();

    // The Callbacks router for the whole SFC.
    int _callBack(vnf::ConnId& connId, int AppIdx, int TxnIdx, int SAIdx, int reqObjId, void* reqobj, char * packet, int packetlen, void * value, int length, int errCode);

    void * AppObj(App *app, void * reqObj) {
        int idx = AppIdxMap[app];
        return reqObj + objSizesStarting[idx];
    }

// private:
    int reqObjTotalSize = 0;
	std::vector<App* > SFC_chain = {};
	std::unordered_map<App*, int> AppIdxMap = {};
	std::vector<int> objSizes;
    std::vector<int> objSizesStarting;
};
}

// Main loop for traversal of the apps. Wrapper from libVNF Callback Format to Our disposal Format.
void _AppsDisposalAccept(vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum);
void _AppsDisposalRead  (vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum);
void _AppsDisposalError (vnf::ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum);

void _disposalBody(vnf::ConnId& connId, Context & ctx );

// The entry of the SFC thread from java.
int __VNFThread(int argc, char *argv[]);

// User get SFC single instance.
DB4NFV::SFC& GetSFC();

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
    JavaVM * __jvm;

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

// Get JVM and jenv related. FIXME. Optimize.
// TODO. May not be working.
bool GetJniEnv(JNIEnv **env)
{
	// bool did_attach_thread = false;
	// *env = nullptr;
	// // Check if the current thread is attached to the VM
	// auto get_env_result = globals.__jvm->GetEnv((void **)env, JNI_VERSION_1_6);
	// if (get_env_result == JNI_EDETACHED)
	// {
	// 	if (globals.__jvm->AttachCurrentThread((void **)env, NULL) == JNI_OK)
	// 	{
	// 		did_attach_thread = true;
	// 	}
	// 	else
	// 	{
	// 		assert(false);
	// 	}
	// }
	// else if (get_env_result == JNI_EVERSION)
	// {
	// 	assert(false);
	// }
	// return did_attach_thread;
    jint rs = (*GetGlobal().__jvm).AttachCurrentThread((void **)&env, NULL);
	return true;
}

Globals& GetGlobal();

#endif