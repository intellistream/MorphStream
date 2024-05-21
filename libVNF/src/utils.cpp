#include "utils.hpp"

void panic(const std::string &msg){
	// Fatal level output of spdlog
	spdlog::critical(msg.c_str());
	exit(-1);
}

void bug(const std::string &msg){
	panic(msg);
}

void stateManagerError(const std::string &msg){
	panic(msg);
}

void error(const std::string &msg){
	spdlog::error(msg.c_str());
}