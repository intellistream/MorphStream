#pragma once

#include <vector>
#include <mutex>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <spdlog/spdlog.h>
#include "utils.hpp"

struct packet {
	std::vector<std::string> fields;
};

// Wrapper around packet.
class Context {
private:
	std::unordered_map<std::string, int> packetLocal;
	packet pkt;
	int txnRequestWaitingCnt;
public:
	int appIdx;
	int txnIdx;
	uint64_t id;

	Context(packet pkt): pkt(pkt), txnRequestWaitingCnt(0), appIdx(0), txnIdx(0) { 
		id = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
		packetLocal = {}; 
	} 
	packet& Packet() { return pkt; }
	bool isInTxn() {return txnRequestWaitingCnt != 0;}
	void finishTxnHandlingRequest() { txnRequestWaitingCnt -= 1; if (!isInTxn()) {txnIdx = -1;}}
	void triggerTxnHandlingRequest(int idx) { txnRequestWaitingCnt += 1; if (txnIdx != 0 && txnIdx != idx) {bug("sas from multiple txn triggered.");} txnIdx = idx;}
	auto& LocalParam() { return packetLocal;}
};

// Base class for reader.
class PacketReader{
public:
	virtual Context* read_packet() = 0;
	virtual void parse_args(const std::string&, packet&) = 0;
};

class PacketManager{
private:
	std::unordered_map<uint64_t, Context*> buf = {};

	PacketReader* pr;
	// Called when sending request to state manager. Deposit the packet.
	Context& deposit(Context *ctx) {
		buf.emplace(ctx->id, ctx);
		return *ctx;
	}
public:
	PacketManager(PacketReader *p): pr(p){}
	void packet_finish(Context *ctx) {
		auto pkt = buf.find(ctx->id);
		if (pkt == buf.end()){
			stateManagerError("poping packet not pending.");
		}
		buf.erase(pkt->first);
		delete pkt->second;
		return;
	}
	Context& packet_from_id(uint64_t id) {
		auto pkt = buf.find(id);
		if (pkt == buf.end()){
			stateManagerError("poping packet not exists.");
		}
		return *pkt->second;
	}
	// Read new packet from packet reader.
	Context& read_packet() {
		return deposit(pr->read_packet());
	}
};

class CSVReader : public PacketReader {
private:
	std::string path;
	std::ifstream csv;

public:
	CSVReader(const std::string& file_path) {
		path = file_path;
		csv = std::ifstream(file_path);
		if (!csv.is_open()) {
			throw std::runtime_error("Error: Unable to open file " + file_path);
		}
	}

	// read line from CSV and parse into packet. Wrap up with context and return.
	Context* read_packet() override {
		std::string line;
		// csv = std::ifstream(path);
		if (csv.peek() == EOF){
			throw std::runtime_error("File reaches end.");
		}
		if (std::getline(csv, line)) {
			packet pkt;
			// Populate packet fields from line
			parse_args(line, pkt);
			auto ctx = new Context(pkt);
			return ctx;
		} else {
			// Handle end of file or read error
			// For now, returning an empty packet
			throw std::runtime_error("Failed to fetch csv record");
		}
	}

	void parse_args(const std::string& line, packet& pkt) override {
		pkt.fields.clear(); 
		std::stringstream ss(line);
		std::string arg;
		auto trimChar = ';';
		while (std::getline(ss, arg, ','))
		{
			arg.erase(std::find_if(arg.rbegin(), arg.rend(), [trimChar](char ch)
								   { return ch != trimChar; })
						  .base(),
					  arg.end());
			pkt.fields.push_back(arg);
		}
	}
};