#include "runtime.hpp"

/*
	Runtime status handling logic.
*/

Runtime runtime;
Runtime& GlobalRuntime() {return runtime;}

int Runtime::runtime_fetch_cc(int key){
	return GlobalLocalDS().fetch_cc(key);
}
int Runtime::runtime_fetch_localds(int key){
	return GlobalLocalDS().fetch(key);
}
void Runtime::runtime_pause(){
	statusLock.lock();
	running = false;
	statusLock.unlock();
	spdlog::debug("packet receiving continued for pending packets reaches limit.");
}
void Runtime::runtime_continue(){
	statusLock.lock();
	running = true;
	statusLock.unlock();
	spdlog::debug("packet receiving continued below start limit.");
}
void Runtime::runtime_update_cc(int key, int value){
	return GlobalLocalDS().update_cc(key, value);
}
void Runtime::runtime_update_localds(int key, int value){
	return GlobalLocalDS().update(key, value);
}
bool Runtime::is_running(){
	return running;
}

/*
	VNF threads life cycle management.
*/
void *Runtime::VNF(void * arg){
	auto res = VNFMain(0, NULL);
	GlobalStateManager().SFC(res);

	// Main handling loop.
	while (1) {
		// Check response queue to evict answered requests.
		// Try pop from runtime.ready_queue to get packet Id.
		if (ready_queue.size() > 0) {
			// Pop packet from pending packets.
			ready_queue_lock.lock();
			auto txnReq = ready_queue.front();
			ready_queue.pop();
			ready_queue_lock.unlock();

			auto ctx = pktManager->packet_from_id(txnReq.pktId);
			HandlePacket(ctx, txnReq.saIdx);
			continue;
		}

		// Start to read packet from reader and parse to send request. Check runtime status before disposing packets.
		auto ctx = pktManager->read_packet();
		HandlePacket(ctx, -1);
	}

	return NULL;
}

void *RuntimeVNF(void * arg) {
	return runtime.VNF(arg);
}

int Runtime::new_vnf_worker_thread(){
	pthread_t thread;
	vnfThreads.push_back(thread);

	// Create a new thread and pass a lambda function calling VNF as the argument
	if (pthread_create(&thread, NULL, RuntimeVNF, NULL) != 0)
	{
		perror("Failed to create thread");
		return 1;
	}
	return 0;
}

void Runtime::join(){
	// Wait for the thread to finish
	for (int i = 1; i < vnfThreads.size(); i++){
		pthread_join(vnfThreads[i], NULL);
	}
}

void Runtime::NewTxnRequest(Context& ctx, int key, int cc, int saIdx){
	// Record cnt.
	req_cnt_lock.lock(); 
	in_process_req += 1; 
	req_cnt_lock.unlock();

	check_overloaded_pause();
	GlobalStateManager().TxnRequest(cc, saIdx, ctx.id, key);
}

void Runtime::ReadyTxnRequest(TxnReq r){
	// Dequeue
	ready_queue_lock.lock();
	ready_queue.push(r);
	ready_queue_lock.unlock();

	// Record txnReq count
	req_cnt_lock.lock();
	in_process_req -= 1;
	req_cnt_lock.unlock();

	check_overloaded_continue();
}

int Runtime::HandlePacket(Context &ctx, int saIdx) {
	int ret = (this->packet_handling_callback)(ctx, saIdx);
	if (ret == 0) {
		// Ret == 0. The app has successfully reached its ends.
		pktManager->packet_finish(&ctx);
	}
	// Ret == 1. The packet has other txnRequest to handle. Just return and wait.
	return ret;
}

void __debug__change_tuple_strategy(int key, int cc){ GlobalRuntime().runtime_update_cc(key, cc); }