#pragma once

#include <string>
#include <cstdint>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <spdlog/spdlog.h>

#include "message.pb.h"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
// #include "message.grpc.pb.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include "utils.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ClientContext;

struct StateManagerConfig {
	std::string host;
	int port;
};

using namespace message;
class StateManager {
private:
	int _socket;
	std::mutex rl;
	std::mutex wl;
public:
	void ConnectAndHandle(const StateManagerConfig& config);
	void MonitorReport(int cc);
	void TxnRequest(int cc, int saIdx, uint64_t id, int key);
	void SFC(const std::string SFCJson);

	// Answering Get*.
	void PushCC(const setCCMessage& msg);
	void PushVal(const setDSMessage& msg);
};

class StateManagerServiceImpl {
public:
	static void Pause();
	static void Continue();
	static void PostCC(const setCCMessage&);
	static void GetCC(const getCCMessage&);
	static void PostValue(const setDSMessage&);
	static void GetValue(const getDSMessage&);
	static void UDFReady(const UDFReadyMessage&);
	static void TxnDone(const TxnDoneMessage&);
};

StateManager& GlobalStateManager();