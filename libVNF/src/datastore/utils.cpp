#include "datastore/utils.hpp"

Utils g_utils;

/* Action - Exit the program */
void Utils::handle_type1_error(int arg, string msg) {
    if (arg < 0) {
        msg = to_string(errno) + ": " + msg;
        perror(msg.c_str());
        exit(EXIT_FAILURE);
    }
}

/* Action - Check for error conditions. Do not exit. */
void Utils::handle_type2_error(int arg, string msg) {
    if (arg < 0) {
        msg = to_string(errno) + ": " + msg;
        perror(msg.c_str());
    }
}

char *Utils::allocate_str_mem(int len) {
    char *tem;

    if (len <= 0) {
        handle_type1_error(-1, "Memory length error: utils_allocatestrmem");
    }
    tem = (char *) malloc(len * sizeof(char));
    if (tem != NULL) {
        memset(tem, 0, len * sizeof(char));
        return tem;
    } else {
        handle_type1_error(-1, "Memory allocation error: utils_allocatestrmem");
        return nullptr;
    }
}

uint8_t *Utils::allocate_uint8_mem(int len) {
    uint8_t *tem;

    if (len <= 0) {
        handle_type1_error(-1, "Memory length error: utils_allocateuint8mem");
    }
    tem = (uint8_t *) malloc(len * sizeof(uint8_t));
    if (tem != NULL) {
        memset(tem, 0, len * sizeof(uint8_t));
        return tem;
    } else {
        handle_type1_error(-1, "Memory allocation error: utils_allocateuint8mem");
        return nullptr;
    }
}

void Utils::time_check(time_t start_time, double dur_time, bool &time_exceeded) {
    double elapsed_time;

    if ((elapsed_time = difftime(time(0), start_time)) > dur_time) {
        time_exceeded = true;
    }
}

int Utils::max_ele(vector<int> inp) {
    int ans;
    int size;
    int i;

    ans = 0;
    size = inp.size();
    for (i = 0; i < size; i++) {
        ans = max(ans, inp[i]);
    }
    return ans;
}

