#include "sfc.hpp"
#include <pthread.h>

int vnf_thread = 0;

int main()
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	GlobalLocalDS().Init({
		.defaultStrategy = 2,
		.defaultValue = 0,
		.cnt = 10000,
	});
	GlobalRuntime().Init({
		.stop_limit = 500,
		.start_limit = 300,
		.packet_manager = new PacketManager(
			new	CSVReader("/home/kailian/VNFRuntime/src/vnf/ccSelector.csv")
		),
		.packet_handling_entry = sfc_packet_handler
	});
	// VNF threads.
	for (int i = 1; i < vnf_thread + 1; i++){
		GlobalRuntime().new_vnf_worker_thread();
	}
	// StateManager pipe.
	GlobalStateManager().ConnectAndHandle({
		.host = "localhost",
		.port = 8080,
	});
	return 0;
}