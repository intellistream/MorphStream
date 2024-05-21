#include <iostream>
#include <string>
#include <sstream>
#include <stdexcept>
#include <boost/asio.hpp>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fstream> 
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>


#include "stateManager.hpp"
#include "runtime.hpp"

using namespace google::protobuf::io;

StateManager _stateManager;

StateManager& GlobalStateManager(){ return _stateManager; }

void StateManager::ConnectAndHandle(const StateManagerConfig& config){
	const char *server_ip = config.host.c_str();
	const int server_port = config.port;

	// Create socket
	this->_socket = socket(AF_INET, SOCK_STREAM, 0);
	if (this->_socket == -1)
	{
		panic("Error: Could not create socket\n");
	}

	// Connect to server
	sockaddr_in server_addr;
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(server_port);
	inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

	if (connect(this->_socket, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)) == -1)
	{
		close(this->_socket);
		panic("Error: Could not connect to server\n");
	}

	auto s = new FileInputStream(this->_socket);

	while (true){
		MessageFromStateManager wrapper;
		this->rl.lock();
		bool clean_eof;
		if (!google::protobuf::util::ParseDelimitedFromZeroCopyStream(&wrapper, s , &clean_eof)) {
			if (clean_eof) { 
				close(this->_socket);
				stateManagerError("Connection closed by server.");
			} else {
				close(this->_socket);
				stateManagerError("Error reading message from server.");
			}
		} // TODO. Return error handling.
		this->rl.unlock();
		if (wrapper.has_continuemessage()) { 
			StateManagerServiceImpl::Continue();
		} else if (wrapper.has_getccmessage()) {
			StateManagerServiceImpl::GetCC(wrapper.getccmessage());
		} else if (wrapper.has_getdsmessage()) {
			StateManagerServiceImpl::GetValue(wrapper.getdsmessage());
		} else if (wrapper.has_pausemessage()) {
			StateManagerServiceImpl::Pause();
		} else if (wrapper.has_setccmessage()) {
			StateManagerServiceImpl::PostCC(wrapper.setccmessage());
		} else if (wrapper.has_setdsmessage()) {
			StateManagerServiceImpl::PostValue(wrapper.setdsmessage());
		} else if (wrapper.has_txndonemessage()) {
			StateManagerServiceImpl::TxnDone(wrapper.txndonemessage());
		} else if (wrapper.has_udfmessage()) {
			StateManagerServiceImpl::UDFReady(wrapper.udfmessage());
		} else {
			close(this->_socket);
			stateManagerError("Unknown message type received.");
		}
	}
}

void StateManager::MonitorReport(int cc){
	message::MonitorReportMessage message;
	message.set_cc(static_cast<message::CC>(cc));
	message::MessageFromVNFInst wrapper;
	wrapper.mutable_monitorreportmessage()->CopyFrom(message);
	// Serialize message and send to server
	google::protobuf::util::SerializeDelimitedToFileDescriptor(wrapper, this->_socket);
}

void StateManager::TxnRequest(int cc, int saIdx, uint64_t id, int key){
	message::TxnReqMessage message;
	message.set_cc(static_cast<message::CC>(cc));
	message.set_id(id);
	message.set_saidx(saIdx);
	message.set_key(key);	
	message::MessageFromVNFInst wrapper;
	wrapper.mutable_txnreqmessage()->CopyFrom(message);
	// Serialize message and send to server
	google::protobuf::util::SerializeDelimitedToFileDescriptor(wrapper, this->_socket);
}

void StateManager::SFC(const std::string SFCJson){
	message::SFCMessage message;
	message.set_sfcjson(SFCJson);
	message::MessageFromVNFInst wrapper;
	wrapper.mutable_sfcmessage()->CopyFrom(message);
	// Serialize message and send to server
	google::protobuf::util::SerializeDelimitedToFileDescriptor(wrapper, this->_socket);
}

void StateManager::PushCC(const setCCMessage& msg){
	message::MessageFromVNFInst wrapper;
	wrapper.mutable_pushccmessage()->CopyFrom(msg);
	// Serialize message and send to server
	google::protobuf::util::SerializeDelimitedToFileDescriptor(wrapper, this->_socket);
}

void StateManager::PushVal(const setDSMessage& msg){
	message::MessageFromVNFInst wrapper;
	wrapper.mutable_pushdsmessage()->CopyFrom(msg);
	// Serialize message and send to server
	google::protobuf::util::SerializeDelimitedToFileDescriptor(wrapper, this->_socket);
}

void StateManagerServiceImpl::Pause()
{
	if (!GlobalRuntime().is_running()){
		stateManagerError("GlobalRuntime() is already paused.");
	}
	GlobalRuntime().runtime_pause();
}
void StateManagerServiceImpl::Continue(){
	if (GlobalRuntime().is_running()){
		stateManagerError("GlobalRuntime() is already running.");
	}
	GlobalRuntime().runtime_continue();
}
void StateManagerServiceImpl::PostCC(const setCCMessage &request){
	if (request.key() >= 10000 || request.key() < 0){
		stateManagerError("Invalid key value.");
	}
	GlobalRuntime().runtime_update_cc(request.key(), request.cc());
}
void StateManagerServiceImpl::GetCC(const getCCMessage& request){
	if (request.key() >= 10000 || request.key() < 0){
		stateManagerError("Invalid key value.");
	}
	message::setCCMessage message;
	message.set_cc(static_cast<message::CC>(GlobalRuntime().runtime_fetch_cc(request.key())));
	GlobalStateManager().PushCC(message);
}
void StateManagerServiceImpl::PostValue(const setDSMessage& request){
	if (request.key() >= 10000 || request.key() < 0){
		stateManagerError("Invalid key value.");
	}
	GlobalRuntime().runtime_update_localds(request.key(), request.value());
}
void StateManagerServiceImpl::GetValue(const getDSMessage& request){
	if (request.key() >= 10000 || request.key() < 0){
		stateManagerError("Invalid key value.");
	}
	message::setDSMessage message;
	message.set_value(GlobalRuntime().runtime_fetch_localds(request.key()));
	GlobalStateManager().PushVal(message);
}
void StateManagerServiceImpl::UDFReady(const UDFReadyMessage& request){
	GlobalRuntime().ReadyTxnRequest({.saIdx = request.saidx(), .pktId = request.id()});
}
void StateManagerServiceImpl::TxnDone(const TxnDoneMessage& request){
	// Unfinished.
	assert(false);
}
