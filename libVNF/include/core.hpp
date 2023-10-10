#ifndef LIBVNF_H
#define LIBVNF_H

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

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

namespace vnf {

enum EventType {
    READ = 0, ACCEPT = 1, ERROR = 2
};

enum DataLocation {
    LOCAL = 1, REMOTE = 2, CHECKCACHE = 3, UDS = 4
};

class ConnId {
public:
  const int coreId;
  const int socketId;

  ConnId(int coreId, int socketId) : coreId(coreId), socketId(socketId) {}

  ConnId(int value) : coreId(value), socketId(value) {}

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
  registerCallback(enum EventType event, void callback(ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum));

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
          void errorCallback(ConnId& connId, int reqObjId, void * requestObject, void * value, int valueLen, int errorCode));

  ConnId&
  retrieveData(string tableName, int key, enum DataLocation location,
          void callback(ConnId& connId, int reqObjId, void * requestObject, void * value, int valueLen, int errorCode), int reqObjId = 0);

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

typedef void (*CallbackFn)(ConnId&, int, void *, char *, int, int, int);

typedef void (*DSCallbackFn)(ConnId&, int, void *, void *, int, int);

// typedef int (*DSInitFn) (ConnId&);

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
                 void callback(ConnId& connId, int reqObjId, void * requestObject, char * packet, int packetLen, int errorCode, int streamNum));

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
 * @param value_len: length of value
 * @param callbackFnPtr(ConnId& connId, void * requestObject, void * value, int packet_length, int error_code) : callback function called when error occurs
 * */
ConnId&
storeData(ConnId& connId, string tableName, int key, enum DataLocation location, void *value, int valueLen,
        void errorCallback(ConnId& connId, int reqObjId, void * requestObject, void * value, int valueLen, int errorCode));

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
        void callback(ConnId& connId, int reqObjId, void * requestObject, void * value, int valueLen, int errorCode), int reqObjId = 0);

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

#endif

