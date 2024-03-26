#ifndef DDCKV_DEBUG_H_
#define DDCKV_DEBUG_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <infiniband/verbs.h>

#include <vector>

#include "kv_utils.h"
#include "hashtable.h"
#include "ib.h"

enum {
    INFO = 0,
    DEBUG
};

static const char * str_prefix[] = {"[INFO]", "[DEBUG]"};

static inline void kv_assert(bool value) {
#ifdef _DEBUG
    assert(value);
#endif
}

static inline void print_log(int log_level, const char * fmt, ...) {
    if (log_level == DEBUG) {
#ifdef _DEBUG
    int fmt_len = strlen(fmt);
    char * new_fmt_buf = (char *)malloc(fmt_len + 10);
    sprintf(new_fmt_buf, "%s %s\n", str_prefix[log_level], fmt);
    va_list args;
    va_start(args, fmt);
    vprintf(new_fmt_buf, args);
    va_end(args);
#endif
    } else {
        int fmt_len = strlen(fmt);
        char * new_fmt_buf = (char *)malloc(fmt_len + 10);
        sprintf(new_fmt_buf, "%s %s\n", str_prefix[log_level], fmt);
        va_list args;
        va_start(args, fmt);
        vprintf(new_fmt_buf, args);
        va_end(args);
    }
}

static inline void print_sr_list(struct ibv_send_wr * sr_list) {
    struct ibv_send_wr * p;
    for (p = sr_list; p != NULL; p = p->next) {
        // print_log(DEBUG, "wr_id(%ld) raddr(%lx) rkey(%x)", p->wr_id, p->wr.rdma.remote_addr, 
        //     p->wr.rdma.rkey);
    }
}

static inline void print_sr_lists(std::vector<IbvSrList *> & sr_list_batch,
        std::vector<uint32_t> & sr_list_num_batch) {
    for (size_t i = 0; i < sr_list_batch.size(); i ++) {
        uint8_t server_id;
        for (int j = 0; j < sr_list_num_batch[i]; j ++) {
            server_id = sr_list_batch[i][j].server_id;
            // print_log(DEBUG, "server_id(%d)", server_id);
            print_sr_list(sr_list_batch[i][j].sr_list);
        }
    }
}

static inline void print_key(char * key_addr, uint32_t key_len) {
    char keystr[256];
    memset(keystr, 0, 256);
    memcpy(keystr, key_addr, key_len);
    printf("%s", keystr);
}

#endif