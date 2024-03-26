#ifndef DDCKV_HASH_TABLE_H_
#define DDCKV_HASH_TABLE_H_

#include <stdint.h>
#include "kv_utils.h"

#define RACE_HASH_GLOBAL_DEPTH              (5)
#define RACE_HASH_INIT_LOCAL_DEPTH          (5)
#define RACE_HASH_SUBTABLE_NUM              (1 << RACE_HASH_GLOBAL_DEPTH)
#define RACE_HASH_INIT_SUBTABLE_NUM         (1 << RACE_HASH_INIT_LOCAL_DEPTH)
#define RACE_HASH_MAX_GLOBAL_DEPTH          (5)
#define RACE_HASH_MAX_SUBTABLE_NUM          (1 << RACE_HASH_MAX_GLOBAL_DEPTH)
#define RACE_HASH_ADDRESSABLE_BUCKET_NUM    (34000ULL)
#define RACE_HASH_SUBTABLE_BUCKET_NUM       (RACE_HASH_ADDRESSABLE_BUCKET_NUM * 3 / 2)
#define RACE_HASH_ASSOC_NUM                 (7)
#define RACE_HASH_RESERVED_MAX_KV_NUM       (1024ULL * 1024 * 10)
#define RACE_HASH_KVOFFSET_RING_NUM         (1024ULL * 1024 * 16)
#define RACE_HASH_KV_BLOCK_LENGTH           (64ULL)
#define SUBTABLE_USED_HASH_BIT_NUM          (32)
#define RACE_HASH_MASK(n)                   ((1 << n) - 1)

#define ROOT_RES_LEN          (sizeof(RaceHashRoot))
#define SUBTABLE_LEN          (RACE_HASH_ADDRESSABLE_BUCKET_NUM * sizeof(RaceHashBucket))
#define SUBTABLE_RES_LEN      (RACE_HASH_MAX_SUBTABLE_NUM * SUBTABLE_LEN)
#define KV_RES_LEN            (RACE_HASH_RESERVED_MAX_KV_NUM * RACE_HASH_KV_BLOCK_LENGTH)
// #define META_AREA_LEN         (128 * 1024 * 1024)
#define META_AREA_LEN         (256 * 1024 * 1024)
// #define GC_AREA_LEN           (128 * 1024 * 1024)
#define GC_AREA_LEN           (0)
#define HASH_AREA_LEN         (128 * 1024 * 1024)
#define CLIENT_META_LEN       (1 * 1024 * 1024)
#define CLIENT_GC_LEN         (1 * 1024 * 1024)
// #define RACE_HASH_MAX_NUM_REP (10)


typedef struct __attribute__((__packed__)) TagRaceHashSlot {
    uint8_t fp;
    uint8_t kv_len;
    uint8_t server_id;
    uint8_t pointer[5];
} RaceHashSlot;

typedef struct __attribute__((__packed__)) TagRacsHashBucket {
    uint32_t local_depth;
    uint32_t prefix;
    RaceHashSlot slots[RACE_HASH_ASSOC_NUM];
} RaceHashBucket;

typedef struct TagRaceHashSubtableEntry {
    uint8_t lock;
    uint8_t local_depth;
    uint8_t server_id;
    uint8_t pointer[5];
} RaceHashSubtableEntry;

typedef struct TagRaceHashRoot {
    uint64_t global_depth;
    uint64_t init_local_depth;
    uint64_t max_global_depth;
    uint64_t prefix_num;
    uint64_t subtable_res_num;
    uint64_t subtable_init_num;
    uint64_t subtable_hash_num;
    uint64_t subtable_hash_range;
    uint64_t subtable_bucket_num;
    uint64_t seed;

    uint64_t mem_id;
    uint64_t root_offset;
    uint64_t subtable_offset;
    uint64_t kv_offset;
    uint64_t kv_len;
    
    uint64_t lock;
    RaceHashSubtableEntry subtable_entry[RACE_HASH_MAX_SUBTABLE_NUM][MAX_REP_NUM];
} RaceHashRoot;

typedef struct TagRaceHashSearchContext {
    int32_t  result;
    int32_t  no_back;
    uint64_t hash_value;
    uint8_t  fp; // fingerprint
    // HashIndexSearchReq * req;
    uint64_t f_com_bucket_addr;
    uint64_t s_com_bucket_addr;
    uint64_t read_kv_addr;

    uint64_t f_remote_com_bucket_offset;
    uint64_t s_remote_com_bucket_offset;

    uint64_t read_kv_offset;
    uint32_t read_kv_len;

    RaceHashRoot * local_root;

    void * key;
    uint32_t key_len;
    uint32_t value_len;

    bool sync_root_done;
    bool is_resizing;
} RaceHashSearchContext;

typedef struct TagKVTableAddrInfo {
    uint8_t     server_id_list[MAX_REP_NUM];
    uint64_t    f_bucket_addr[MAX_REP_NUM];
    uint64_t    s_bucket_addr[MAX_REP_NUM];
    uint32_t    f_bucket_addr_rkey[MAX_REP_NUM];
    uint32_t    s_bucket_addr_rkey[MAX_REP_NUM];
    uint32_t    f_main_idx;
    uint32_t    s_main_idx;
    uint32_t    f_idx;
    uint32_t    s_idx;
} KVTableAddrInfo;

typedef struct TagKVHashInfo {
    uint64_t hash_value;
    uint64_t prefix;
    uint8_t  fp;
    uint8_t  local_depth;
} KVHashInfo;

typedef struct TagKVInfo {
    void   * l_addr;
    uint32_t lkey;
    uint32_t key_len;
    uint32_t value_len;
} KVInfo;

typedef struct TagKVRWAddr {
    uint8_t  server_id;
    uint64_t r_kv_addr;
    uint64_t l_kv_addr;
    uint32_t rkey;
    uint32_t lkey;
    uint32_t length;
} KVRWAddr;

typedef struct TagKVCASAddr {
    uint8_t  server_id;
    uint64_t r_kv_addr;
    uint64_t l_kv_addr;
    uint32_t rkey;
    uint32_t lkey;
    uint64_t orig_value;
    uint64_t swap_value;
} KVCASAddr;

typedef struct TagLocalCacheEntry {
    uint64_t       r_slot_addr[MAX_REP_NUM];
    RaceHashSlot   l_slot_ptr;
    uint32_t       miss_cnt;
    uint32_t       acc_cnt;
} LocalCacheEntry;

static inline uint64_t SubtableFirstIndex(uint64_t hash_value, uint64_t capacity) {
    return hash_value % (capacity / 2);
}

static inline uint64_t SubtableSecondIndex(uint64_t hash_value, uint64_t f_index, uint64_t capacity) {
    uint32_t hash = hash_value;
    uint16_t partial = (uint16_t)(hash >> 16);
    uint16_t non_sero_tag = (partial >> 1 << 1) + 1;
    uint64_t hash_of_tag = (uint64_t)(non_sero_tag * 0xc6a4a7935bd1e995);
    return (uint64_t)(((uint64_t)(f_index) ^ hash_of_tag) % (capacity / 2) + capacity / 2);
}

static inline uint64_t HashIndexConvert40To64Bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 40) | ((uint64_t)addr[1] << 32)
        | ((uint64_t)addr[2] << 24) | ((uint64_t)addr[3] << 16) 
        | ((uint64_t)addr[4] << 8);
}

static inline void HashIndexConvert64To40Bits(uint64_t addr, __OUT uint8_t * o_addr) {
    o_addr[0] = (uint8_t)((addr >> 40) & 0xFF);
    o_addr[1] = (uint8_t)((addr >> 32) & 0xFF);
    o_addr[2] = (uint8_t)((addr >> 24) & 0xFF);
    o_addr[3] = (uint8_t)((addr >> 16) & 0xFF);
    o_addr[4] = (uint8_t)((addr >> 8)  & 0xFF);
}

static inline void ConvertSlotToAddr(RaceHashSlot * slot, __OUT KVRWAddr * kv_addr) {
    kv_addr->server_id = slot->server_id;
    kv_addr->r_kv_addr = HashIndexConvert40To64Bits(slot->pointer);
}

static inline uint64_t ConvertSlotToInt(RaceHashSlot * slot) {
    return *(uint64_t *)slot;
}

uint64_t VariableLengthHash(const void * data, uint64_t length, uint64_t seed);
uint8_t  HashIndexComputeFp(uint64_t hash);
uint32_t GetFreeSlotNum(RaceHashBucket * bucekt, uint32_t * free_idx);
bool IsEmptyPointer(uint8_t * pointer, uint32_t num);
bool CheckKey(void * r_key_addr, uint32_t r_key_len, void * l_key_addr, uint32_t l_key_len);

#endif