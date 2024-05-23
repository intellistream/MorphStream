#include "sfc.hpp"

void SFC::Entry(App entry){
	if (SFC_chain.size() == 0) {
		entry.appIndex = 0;
	} else {
		throw std::runtime_error("Entry has been set.");
	}
	SFC_chain.push_back(entry);
}

void SFC::Add(App last, App next) {
	// TODO.
}

std::string SFC::NFs() {
	// Set App indexes.
	for (int i = 0; i < SFC_chain.size(); ++i) {
		SFC_chain[i].appIndex = i;
		for (int j = 0; j < SFC_chain[i].Txns.size(); ++j) {
			SFC_chain[i].Txns[j].txnIndex = j;
			SFC_chain[i].Txns[j].app = &SFC_chain[i];
			for (int k = 0; k < SFC_chain[i].Txns[j].sas.size(); ++k) {
				SFC_chain[i].Txns[j].sas[k].saIndex = k;
				SFC_chain[i].Txns[j].sas[k].app = &SFC_chain[i];
				SFC_chain[i].Txns[j].sas[k].txn = &SFC_chain[i].Txns[j];
			}
		}
	}
	// TODO. Check integrity.
	Json::StreamWriterBuilder builder;
	builder["indentation"] = ""; // If you want whitespace-less output
	const std::string output = Json::writeString(builder, toJson());
	return output;
}

/*
	Entry for packet handling:
	1. Packet may have passed read handler. Within the process of txn handling. Call Txn handler.
	2. Packet txn not triggered. Go to read_handler first.
*/
int SFC::packet_handler(Context& ctx, int saIdx) {
	int ret;
	if (ctx.isInTxn()) {
		if (saIdx < 0){
			bug("packet handler calling transaction handler when saIdx < 0");
		}
		auto value = GlobalRuntime().runtime_fetch_localds(
			SFC_chain[ctx.appIdx].Txns[ctx.txnIdx].sas[saIdx].key_
		); 
		ret = txn_callback_handler(ctx, saIdx, {value});
	} else {
		if (saIdx != -1){
			bug("packet handler calling app_read_handler when saIdx is not -1");
		}
		ret = app_read_handler(ctx);
	}
	return ret;
}

int SFC::app_read_handler(Context &ctx) {
	if (ctx.appIdx >= SFC_chain.size()) {
		return 0;
	}
	if (ctx.isInTxn()) {
		return 1;
	}
	while (1) {
		// Perform current handler and move to next.
		SFC_chain[ctx.appIdx].readHandler(ctx);
		if (ctx.isInTxn()) {
			return 1; // 1 = ctx break. still in waiting for txn request.
		}
		// Only move to next when current handling done.
		ctx.appIdx += 1;
		if (ctx.appIdx >= SFC_chain.size()) {
			return 0; // 0 = packet has finished all apps.
		}
	}
}

// Perform txn handling. Each sa decreases txn count.
int SFC::txn_callback_handler(Context &ctx, int saIdx, std::vector<int> value){ 
	if (ctx.appIdx >= SFC_chain.size()) {
		bug("txn handler out of range.");
	}
	if (!ctx.isInTxn()) {
		bug("txn handler diposing a context not performing transaction.");
	}
	auto sa = SFC_chain[ctx.appIdx].Txns[ctx.txnIdx].sas[saIdx];
	// Perform current handler.
	int res = sa.txnHandler_(ctx, value);
	if (sa.rw_ == WRITE) {
		GlobalRuntime().runtime_update_localds(
			sa.key_, res
		);
	}
	ctx.finishTxnHandlingRequest();
	if (!ctx.isInTxn()) {
		ctx.appIdx += 1;
		return app_read_handler(ctx);
	}
	return 1;
}

void Transaction::Trigger(Context& ctx, int key) const {
	// Triggers its sa in sequence.
	for (auto sa: sas){
		int sa_key = sa.key_;
		ctx.triggerTxnHandlingRequest(txnIndex);
		switch (GlobalRuntime().runtime_fetch_cc(sa_key)){
			case 0: 
				// Partition.
				GlobalRuntime().HandlePacket(ctx, sa.saIndex);
				break;
			case 1:
				// Cache
				GlobalRuntime().HandlePacket(ctx, sa.saIndex);
				break;
			case 2:
				// Offloading.
				GlobalRuntime().NewTxnRequest(ctx, key, 2, sa.saIndex);
				break;
			case 3:
				// Preemptive.
				GlobalRuntime().NewTxnRequest(ctx, key, 3, sa.saIndex);
			default:
				bug("unknown cc type.");
		}
	}
}

SFC sfc;
SFC& GlobalSFC(){
	return sfc;
}

int sfc_packet_handler(Context& ctx, int saIdx) { return GlobalSFC().packet_handler(ctx, saIdx);}
