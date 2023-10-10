#ifndef UTILS_H
#define UTILS_H

/* (C++) chrono: high_resolution_clock, microseconds */
#include <chrono>

/* (C++) cout, endl */
#include <iostream>

/* (C) INT_MAX */
#include <climits>

/* (C) pthread_create, pthread_kill */
#include <pthread.h>

/* (C++) STL: queue */
#include <queue>

/* (C++) default_random_engine, exponential_distribution<T> */
#include <random>

/* (C) signal */
#include <csignal>

/* (C) memset, memmove */
#include <cstdio>

/* (C) strlen */
#include <cstring>

/* (C++) STL: string */
#include <string>

/* (C++) stringstream */
#include <sstream>

/* (C++) STL: thread */
#include <thread>

/* (C++) STL: unordered map */
#include <unordered_map>

/* (C++) STL: vector */
#include <vector>

using namespace std;

typedef std::chrono::high_resolution_clock CLOCK;
typedef std::chrono::microseconds MICROSECONDS;

#define DEBUG 0
#define TRACE(x) if (DEBUG) { x }

const int MAX_UE_COUNT = 10000;

class Utils {
public:
    void handle_type1_error(int, string);

    void handle_type2_error(int, string);

    char *allocate_str_mem(int);

    uint8_t *allocate_uint8_mem(int);

    void time_check(time_t, double, bool &);

    int max_ele(vector<int> inp);
};

extern Utils g_utils;

#endif /* UTILS_H */

