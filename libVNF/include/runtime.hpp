#pragma once
#include <queue>
#include <mutex>
#include <vector>

#include "packets.hpp"
#include "dataStore.hpp"

// Entry of user app.
std::string VNFMain(int argc, char* argv[]);

struct TxnReq {
	int saIdx;
	uint64_t pktId;
};

struct RuntimeConfig {
	int stop_limit;
	int start_limit;
	PacketManager *packet_manager;
	int(*packet_handling_entry)(Context&, int);
};

class Runtime{
private: 
	/* Request sending punctuation. */
	std::mutex statusLock;
	bool running;

	/* VNF threads management. */
	std::vector<pthread_t> vnfThreads = {};

	/* App call back function list. */
	int(*packet_handling_callback)(Context&, int);

	/* Data Source and package Management. */
	PacketManager *pktManager;
	std::queue<TxnReq> ready_queue{}; // Initialize using default constructor.
	std::mutex ready_queue_lock;

	/* Flow control. Stop receving from packet Manager once full. */
	int stop_limit = 0;
	int start_limit = 0;
	int in_process_req = 0;
	std::mutex req_cnt_lock;
	bool check_overloaded_pause() { if (in_process_req > stop_limit) {runtime_pause(); return true;} return false; }
	bool check_overloaded_continue() { if (in_process_req > start_limit) {runtime_continue(); return true;} return false; }

public:
	Runtime() = default;
	void Init(RuntimeConfig p){ 
		assert(p.stop_limit > 0); 
		assert(p.start_limit > 0); 
		assert(p.stop_limit > p.start_limit); 
		pktManager = p.packet_manager; stop_limit = p.stop_limit; start_limit = p.start_limit; 
		packet_handling_callback = p.packet_handling_entry;
	};

	/* State logic handling local cache */
	int runtime_fetch_cc(int key);
	int runtime_fetch_localds(int key);
	void runtime_update_cc(int key, int value);
	void runtime_update_localds(int key, int value);

	/* Thread logic managing worker life cycle */
	int new_vnf_worker_thread();
	void join();
	void *VNF(void *);
	// void notify_exit(); TODO.

	/* Scheduling packet handling. */
	bool is_running();
	void runtime_pause();
	void runtime_continue();
	int HandlePacket(Context &ctx, int saIdx);

	/* TxnRequest notification */
	void NewTxnRequest(Context& ctx, int key, int CC, int saIdx);
	void ReadyTxnRequest(TxnReq r);
};

Runtime& GlobalRuntime();

// ccSeletor runtime shortcut.
void __debug__change_tuple_strategy(int key, int cc);