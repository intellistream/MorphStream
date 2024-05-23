#pragma once
#ifndef __DATASTORE_HPP__
#define __DATASTORE_HPP__	

#include <unordered_map>
#include <iostream>
#include <unordered_map>
#include <mutex>

#include <unordered_map>
#include <mutex>
#include <string>
#include "utils.hpp"
#include "stateManager.hpp"
#include "spdlog/spdlog.h"

#define MAX_SIZE 10000

struct dsConfig{
	int defaultStrategy;
	int defaultValue;
	int cnt;
};

class DataStore
{
private:
	int dataMap[MAX_SIZE];
	std::mutex dsMapMutex[MAX_SIZE];
	int strategy[MAX_SIZE];

public:
	void Init(dsConfig d);
	int fetch(int key);
	void update(int key, int value);
	void update_cc(int key, int value);
	int fetch_cc(int key);
};

DataStore& GlobalLocalDS();

#endif 