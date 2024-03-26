#ifndef DDCKV_KV_UTILS_H
#define DDCKV_KV_UTILS_H

#include <infiniband/verbs.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <vector>

#define __OUT
#define DDCKV_MAX_SERVER 64

#define SERVER_ID_BMASK     0x3F
#define BLOCK_ADDR_BMASK    0x1FFFFFULL
#define BLOCK_OFF_BMASK     0x3FFFFULL
#define SUBBLOCK_NUM_BMASK  0xF
#define MAX_REP_NUM         10

// #define YCSB_10M
// #define SERVER_MM

enum ConnType {
    IB,
    ROCE,
};

enum Role {
    CLIENT,
    SERVER
};

// enum KVLogState {
//     KV_LOG_VALID = 1,
//     KV_LOG_COMMITTED = 1 << 1,
//     KV_LOG_GC = 1 << 2,
//     KV_LOG_INSERT = 1 << 3,
// };

enum KVLogOp {
    KV_OP_INSERT = 1,
    KV_OP_UPDATE,
    KV_OP_DELETE,
    KV_OP_FINISH
};

struct GlobalConfig {
    uint8_t  role;
    uint8_t  conn_type;
    uint32_t server_id;
    uint16_t udp_port;
    uint32_t memory_num; // 0 ~ memory_num -1 is the server id 
    char     memory_ips[16][16];

    uint32_t ib_dev_id;
    uint32_t ib_port_id;
    int32_t  ib_gid_idx;

    uint64_t server_base_addr;
    uint64_t server_data_len;
    uint64_t block_size;
    uint64_t subblock_size;
    uint64_t client_local_size;

    uint32_t num_replication; // 0 ~ num_replication_ - 1 is the meta replication server
    uint32_t num_idx_rep;
    uint32_t num_coroutines;

    uint32_t main_core_id;
    uint32_t poll_core_id;
    uint32_t bg_core_id;
    uint32_t gc_core_id;

    int is_recovery;

    // for master
    uint16_t master_port;
    char     master_ip[16];
    float    miss_rate_threash;
    int      workload_run_time;
    int      micro_workload_num;
};

struct GlobalInfo {
    int local_id;
    int num_clients;
    int num_memories;

    struct ibv_context * ctx;
    int port_index;
    int device_id;
    int dev_port_id;
    int numa_node_id;

    struct ibv_pd * pd;
    struct ibv_qp * ud_qp;

    int role;
    pthread_mutex_t lock;
};

enum KVMsgType {
    REQ_CONNECT,
    REQ_ALLOC,
    REQ_ALLOC_SUBTABLE,
    REP_CONNECT,
    REP_ALLOC,
    REP_ALLOC_SUBTABLE,
    REQ_REGISTER,
    REP_REGISTER,
    REQ_RECOVER,
    REP_RECOVER,
    REQ_HEARTBEAT,
    REP_HEAETBEAT
};

struct QpInfo {
    uint32_t qp_num;
    uint16_t lid;
    uint8_t  port_num;
    uint8_t  gid[16];
    uint8_t  gid_idx;
};

struct MrInfo {
    uint64_t addr;
    uint32_t rkey;
};

struct IbInfo {
    uint8_t conn_type;
    struct ibv_context   * ib_ctx;
    struct ibv_pd        * ib_pd;
    struct ibv_cq        * ib_cq;
    struct ibv_port_attr * ib_port_attr;
    union  ibv_gid       * ib_gid;
};

struct ConnInfo {
    struct QpInfo qp_info;
    struct MrInfo gc_info;
};

struct KVMsg {
    uint16_t  type;
    uint16_t  id;
    union {
        struct ConnInfo conn_info;
        struct MrInfo   mr_info;
    } body;
};

enum MMBlockRole {
    PRIMARY,
    BACKUP
};

struct KVLogHeader {
    uint8_t  is_valid;
    uint16_t key_length;
    uint32_t value_length;
};

struct KVLogTail {
    uint8_t next_addr[6];
    uint8_t prev_addr[6];
    uint64_t old_value;
    uint8_t  crc;
    uint8_t  op;
};

typedef struct TagClientLogMetaInfo {
    uint8_t  pr_server_id;
    uint64_t pr_log_head;
    uint64_t pr_log_tail;
} ClientLogMetaInfo;

typedef struct TagEncodedClientGCSlot {
    // off: 18bit
    // block addr: 21 * 2
    // len: 4bit
    uint64_t meta_gc_addr;
} EncodedClientGCSlot;

typedef struct TagClientMetaAddrInfo {
    uint8_t  meta_info_type;
    uint8_t  server_id_list[MAX_REP_NUM];
    uint64_t addr_list[MAX_REP_NUM];
} ClientMetaAddrInfo;

typedef struct TagDecodedClientGCSlot {
    uint64_t pr_addr;
    uint64_t bk_addr;
    uint8_t  num_subblocks;
} DecodedClientGCSlot;

static inline uint64_t roundup_256(uint64_t len) {
    if (len % 256 == 0) {
        return len;
    }
    return (len / 256 + 1) * 256;
}

static inline bool log_is_valid(KVLogHeader * head) {
    return head->is_valid == true;
}

static inline bool log_is_committed(KVLogTail * tail) {
    return tail->old_value != 0;
}

static inline bool log_is_insert(KVLogTail * tail) {
    return tail->op == KV_OP_INSERT;
}

static inline uint64_t time_spent_us(struct timeval * st, struct timeval * et) {
    return (et->tv_sec - st->tv_sec) * 1000000 + (et->tv_usec - st->tv_usec);
}

static inline uint64_t round_up(uint64_t addr, uint32_t align) {
    return ((addr) + align - 1) - ((addr + align - 1) % align);
}

void serialize_kvmsg(__OUT struct KVMsg * kvmsg);
void deserialize_kvmsg(__OUT struct KVMsg * kvmsg);
void serialize_qp_info(__OUT struct QpInfo * qp_info);
void deserialize_qp_info(__OUT struct QpInfo * qp_info);
void serialize_mr_info(__OUT struct MrInfo * mr_info);
void deserialize_mr_info(__OUT struct MrInfo * mr_info);
void serialize_conn_info(__OUT struct ConnInfo * conn_info);
void deserialize_conn_info(__OUT struct ConnInfo * conn_info);

int load_config(const char * fname, __OUT struct GlobalConfig * config);

void encode_gc_slot(DecodedClientGCSlot * d_gc_slot, __OUT uint64_t * e_gc_slot);
void decode_gc_slot(uint64_t e_gc_slot, __OUT DecodedClientGCSlot * d_gc_slot);

int stick_this_thread_to_core(int core_id);

uint64_t current_time_us();

void dump_lat_file(char * fname, const std::vector<uint64_t> & lat_vec);

#endif