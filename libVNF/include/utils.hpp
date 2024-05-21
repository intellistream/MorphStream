#pragma once

#include "spdlog/spdlog.h"
#include <string>

void panic(const std::string &msg);
void bug(const std::string &msg);
void stateManagerError(const std::string &msg);
void error(const std::string &msg);