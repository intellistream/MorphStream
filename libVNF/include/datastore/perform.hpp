#ifndef PERF_H
#define PERF_H

#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>

class PerfRecord{
private:
	std::chrono::high_resolution_clock::time_point timestamp_;
	std::string message_;

public:	
	PerfRecord::PerfRecord(const std::string& message): 
		message_(message) {
			timestamp_ = std::chrono::high_resolution_clock::now();
		}

	// Dump shall be called from PerMetric class by who, Writing error shall be handled.
	void dump(std::ofstream &file) const {
		auto timestamp = std::chrono::time_point_cast<std::chrono::nanoseconds>(timestamp_);
		auto timestamp_ns = timestamp.time_since_epoch().count();
		file << timestamp_ns << "," << message_ << "\n";
	}
};

class PerfMonitor {
private:
	std::vector<PerfRecord> r_;
	std::string metric;

public:
	PerfMonitor(const std::string metric);
	int Start(const std::string message);
	int End(const std::string message);
	int Dump(const std::string& filePath){
		std::ofstream file(filePath);
		if (!file.is_open())
		{
			std::cerr << "Failed to open file for writing." << std::endl;
			return -1;
		}
		for (auto it = r_.begin(); it != r_.end(); it++) it->dump(file);
		file.close();
	}
	std::string& Metric() {return metric;};
};

#endif 