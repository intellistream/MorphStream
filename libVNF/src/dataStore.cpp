#include "dataStore.hpp"

DataStore _localds; 
DataStore& GlobalLocalDS() {return _localds;}

void DataStore::Init(dsConfig d) {
	assert(d.defaultStrategy <= 3);
	assert(d.defaultStrategy >= 0);
	// Populate localds with keys 0-10000 and default value 0, and set mutex for each key.
	for (int i = 0; i <= d.cnt; ++i) {
		dataMap[i] = d.defaultValue;
		strategy[i] = d.defaultStrategy;
	}
}

int DataStore::fetch(int key)
{
	std::lock_guard<std::mutex> lock(dsMapMutex[key]);
	if (key > MAX_SIZE)
	{
		spdlog::error("Key {} not found. Cannot read non-existing key.", key);
		throw std::runtime_error("Key not found");
	}
	return dataMap[key];
}

void DataStore::update(int key, int value)
{
	std::lock_guard<std::mutex> lock(dsMapMutex[key]);
	if (key > MAX_SIZE)
	{
		spdlog::error("Key {} not found. Cannot update non-existing key.", key);
		throw std::runtime_error("Key not found");
	}
	dataMap[key] = value;
}

void DataStore::update_cc(int key, int value)
{
	std::lock_guard<std::mutex> lock(dsMapMutex[key]);
	if (key > MAX_SIZE)
	{
		spdlog::error("Key {} not found. Cannot update non-existing key.", key);
		throw std::runtime_error("Key not found");
	}
	strategy[key] = value;
}

int DataStore::fetch_cc(int key)
{
	std::lock_guard<std::mutex> lock(dsMapMutex[key]);
	if (key > MAX_SIZE)
	{
		spdlog::error("Key {} not found. Cannot update non-existing key.", key);
		throw std::runtime_error("Key not found");
	}
	return strategy[key];
}