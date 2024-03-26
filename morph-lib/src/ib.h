#ifndef DDCKV_IB_H_
#define DDCKV_IB_H_

#include "kv_utils.h"

#include <infiniband/verbs.h>
#include <stdlib.h>

#include <vector>
#include <map>
#include <unordered_map>

typedef struct TagIbvSrList {
    struct ibv_send_wr * sr_list;
    uint32_t num_sr;
    uint32_t server_id;
} IbvSrList;

struct ibv_context * ib_get_ctx(uint32_t dev_id, uint32_t port_id);
struct ibv_qp * ib_create_rc_qp(struct ibv_pd * ib_pd, struct ibv_qp_init_attr * qp_init_attr);

int ib_connect_qp(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info, uint8_t conn_type, uint8_t role);

// merge wr_lists and set the last wr to be signaled
struct ibv_send_wr * ib_merge_sr_lists_unsignaled(std::vector<IbvSrList *> sr_lists);
struct ibv_send_wr * ib_merge_sr_lists(std::vector<IbvSrList *> sr_lists, __OUT uint64_t * last_wr_id);
void   ib_free_sr_lists(IbvSrList * sr_lists, uint32_t num_sr_list);
void   ib_free_sr_lists_batch(std::vector<IbvSrList *> & sr_lists_batch, std::vector<uint32_t> & sr_list_num_batch);
void   ib_free_sr_list(IbvSrList * sr_list);

inline bool ib_is_all_wrid_finished(const std::map<uint64_t, struct ibv_wc *> & wait_wrid_wc_map) {
    std::map<uint64_t, struct ibv_wc *>::const_iterator it;
    for (it = wait_wrid_wc_map.begin(); it != wait_wrid_wc_map.end(); it ++) {
        if (it->second == NULL) {
            return false;
        }
    }
    return true;
}

inline uint64_t ib_gen_wr_id(uint32_t coro_id, uint8_t dst_server_id, uint32_t req_type_st, uint32_t req_seq) {
    return (((uint64_t)coro_id << 8) + dst_server_id) * 1000 + req_type_st + req_seq;
}

inline uint32_t wrid_to_fiber_id(uint64_t wr_id) {
    return (uint32_t)((wr_id / 1000) >> 8);
}

inline uint8_t wrid_to_dst_sid(uint64_t wr_id) {
    return (uint8_t)((wr_id / 1000) & 0xFF);
}

inline uint32_t wrid_to_req_seq(uint64_t wr_id) {
    return (uint32_t)(wr_id % 1000);
}

inline uint64_t wr_id_to_server_wr_id(uint64_t wr_id);
inline uint8_t  wr_id_to_server_id(uint64_t wr_id);


#endif