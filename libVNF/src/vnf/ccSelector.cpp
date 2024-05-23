// #include <core.hpp>
#include <utility>
#include <unordered_map>
#include <cassert>
#include <iostream>
#include "sfc.hpp"

int max_backend = 10;

int CCRequest(Context &ctx, std::vector<int> value)
{
	spdlog::debug("[DEBUG] CC Request UDF.");
	// Failure abortion
	assert(ctx.LocalParam()["CC"] != 0);
	// Balance insufficient abortion
	int lastCnt = value[0];

	// Calculate result.
	return lastCnt + 1;
};

// Perform routing. Expected content: ID,Host,IsFirstPacket,Protocol,ValueLength,Abortion
void app_read_packet_handler(Context &ctx)
{
	spdlog::debug("[DEBUG] New Packet accepted");
	// // Invalid pkt.
	// if (ctx.fields.size() != 7)
	// {
	// 	return;
	// }

	// std::cout << content.substr(comma_pos[0] + 1, comma_pos[1] - comma_pos[0] - 1) << std::endl;
	ctx.LocalParam()["ID"] = atoi(ctx.Packet().fields[0].c_str());
	ctx.LocalParam()["host"] = atoi(ctx.Packet().fields[1].c_str());
	ctx.LocalParam()["CC"] = atoi(ctx.Packet().fields[6].c_str());
	// __debug__change_tuple_strategy(ctx.LocalParam()["host"], ctx.LocalParam()["CC"]);

	GlobalSFC().Trigger(ctx, 0, ctx.LocalParam()["host"]); // Write to host entry about the destination.

	return;
};

auto CCApp = App({
	.name = "CCApp",
	.transactions = 
	{
		{
			.name = "CCSwitching",
			.stateAccess = {
					{
						.name = "RoutePacket",
						.key = 0, 
						.field = 1, 
						.max_entries = 9999,
						.txnHandler = CCRequest, 
						.rw = RWType::READ
					}
			}
		}
	},
	.readHandler = app_read_packet_handler
});

std::string VNFMain(int argc, char *argv[])
{
	// Get the main SFC to construct.
	auto &SFC = GlobalSFC();
	SFC.Entry(CCApp);

	// No nextApp
	// SFC.Add(SLApp, SomeNextApp);

	return SFC.NFs();
}
