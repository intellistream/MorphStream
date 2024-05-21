#pragma once
#ifndef __SFC__HPP
#define __SFC_HPP__

#include <vector>
#include <string>
#include <queue>
#include <mutex>
#include <jsoncpp/json/json.h>

// #include "json.hpp"
#include "runtime.hpp"

// class Context{}; // forward Context class definition.

typedef int (*TxnUDFHandler)(Context &ctx, std::vector<int> value);
typedef void (*ReadHandler)(Context &ctx);

class SFC;
class App;
class Transaction;
class StateAccess;

enum RWType
{
	READ = 1,
	WRITE = 2,
};

struct stateAccessConfig {
	std::string name;
	int key;
	int field;
	int max_entries;
	TxnUDFHandler txnHandler;
	RWType rw;
};

struct transactionConfig {
	std::string name;
	std::vector<stateAccessConfig> stateAccess;
};

struct appConfig {
	std::string name;
std::vector<transactionConfig> transactions;
	ReadHandler readHandler;
};

class StateAccess
{
friend SFC;

public:
	StateAccess(const stateAccessConfig& config)
		: field_(config.field), key_(config.key), name_(config.name), max_entries(config.max_entries),
		  txnHandler_(config.txnHandler), rw_(config.rw)
	{}

	Json::Value toJson() const
	{
		Json::Value json;
		json["TableName"] = name_;
		json["type"] = rw_ == 1 ? "read" : "write";
		json["max_entries"] = max_entries;
		json["fieldTableIndex"] = field_;
		json["keyIndexInEvent"] = key_;
		return json;
	}

	enum RWType rw_;
	std::string name_;

	int key_;
	int field_;
	int max_entries;

	// The binded transaction handler for this State Access.
	TxnUDFHandler txnHandler_ = nullptr;

	App *app;
	Transaction *txn;
	// int appIndex;
	// int txnIndex;
	int saIndex;
};

class Transaction
{
	friend SFC;

public:
	Transaction(const transactionConfig &config)
		: name(config.name) {
			sas = {};
			for (auto s: config.stateAccess){
				sas.push_back(StateAccess(s));
			}
		}
	std::vector<StateAccess> sas = {}; // The State Access event to be used for transaction handlers. Including transaction handlers.
	App *app;
	std::string name;
	int txnIndex;

	void Trigger(Context& ctx, int key) const;

	Json::Value toJson() const
	{
		Json::Value json;
		// Contain series of sas.
		Json::Value txnArray(Json::arrayValue);
		for (const auto &sa : sas)
		{
			txnArray.append(sa.toJson());
		}
		json["StateAccesses"] = txnArray;
		return json;
	}
};

class App
{
	friend SFC;

public:
	App(const appConfig &config) 
		: name(config.name), readHandler(config.readHandler)
		{
			Txns = {};
			for (auto t: config.transactions){
				Txns.push_back(Transaction(t));
			}
		};

	Json::Value toJson() const
	{
		Json::Value json;
		// Contain series of transactions.
		Json::Value txnArray(Json::arrayValue);
		for (const auto &txn : Txns)
		{
			txnArray.append(txn.toJson());
		}
		json["name"] = name;
		json["transactions"] = txnArray;
		return json;
	}

	ReadHandler readHandler = nullptr;
	std::vector<Transaction> Txns;

	int appIndex;
	std::string name;
	App *next = nullptr;
	// std::vector<StateAccess *> SAs;
};

class SFC{
private:
	Json::Value toJson() const
	{
		Json::Value json;
		// Contain series of transactions.
		Json::Value txnArray(Json::arrayValue);
		for (const auto &app : SFC_chain)
		{
			txnArray.append(app.toJson());
		}
		json["apps"] = txnArray;
		return json;
	}

public:
	SFC(){};
	~SFC(){};

	// Construct SFC topology.
	void Entry(App entry);
	void Add(App last, App app);

	// Report SFC apps to txnEngine.
	std::string NFs();

	// The Callbacks router for the whole SFC.
	int txn_callback_handler(Context &ctx, int saIdx, std::vector<int> value);
	int app_read_handler(Context &ctx);
	int packet_handler(Context& ctx, int saIdx);

	/* Trigger transactions */
	void Trigger(Context& ctx, int txnIdx, int &key) { SFC_chain[ctx.appIdx].Txns[txnIdx].Trigger(ctx, key); }

	// private:
	std::vector<App> SFC_chain = {};
};

SFC& GlobalSFC();
int sfc_packet_handler(Context& ctx, int saIdx);

#endif