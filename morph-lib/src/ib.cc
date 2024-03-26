#include "ib.h"

#include <assert.h>

static int modify_qp_to_init(struct ibv_qp * qp, const struct QpInfo * local_qp_info) {
    struct ibv_qp_attr attr;
    int    attr_mask;
    int    rc;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = local_qp_info->port_num;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | 
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    attr_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, attr_mask);
    // assert(rc == 0);
    return 0;
}

static int modify_qp_to_rtr(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info,
    const struct QpInfo * remote_qp_info,
    uint8_t conn_type) {
    struct ibv_qp_attr attr;
    int    attr_mask;
    int    rc;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state    = IBV_QPS_RTR;
    attr.path_mtu    = IBV_MTU_1024;
    attr.dest_qp_num = remote_qp_info->qp_num;
    attr.rq_psn      = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid  = remote_qp_info->lid;
    attr.ah_attr.sl    = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = local_qp_info->port_num;
    if (conn_type == ROCE) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num  = local_qp_info->port_num;
        memcpy(&attr.ah_attr.grh.dgid, remote_qp_info->gid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit  = 1;
        attr.ah_attr.grh.sgid_index = local_qp_info->gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    attr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(local_qp, &attr, attr_mask);
    // assert(rc == 0);
    return 0;
}

static int modify_qp_to_rts(struct ibv_qp * local_qp) {
    struct ibv_qp_attr attr;
    int    attr_mask;
    int    rc;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12;
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 16;
    attr_mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(local_qp, &attr, attr_mask);
    // assert(rc == 0);
    return 0;
}

struct ibv_context * ib_get_ctx(uint32_t dev_id, uint32_t port_id) {
    struct ibv_device ** ib_dev_list;
    struct ibv_device *  ib_dev;
    int    num_device;

    ib_dev_list = ibv_get_device_list(&num_device);
    // assert(ib_dev_list != NULL && num_device > dev_id);
    ib_dev = ib_dev_list[dev_id];

    struct ibv_context * ret = ibv_open_device(ib_dev);
    // assert(ret != NULL);
    ibv_free_device_list(ib_dev_list);
    return ret;
}

struct ibv_qp * ib_create_rc_qp(struct ibv_pd * ib_pd, 
    struct ibv_qp_init_attr * qp_init_attr) {
    return ibv_create_qp(ib_pd, qp_init_attr);
}

int ib_connect_qp(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info, 
    uint8_t conn_type, uint8_t role) {
    int rc = 0;
    rc = modify_qp_to_init(local_qp, local_qp_info);
    // assert(rc == 0);

    rc = modify_qp_to_rtr(local_qp, local_qp_info, remote_qp_info, conn_type);
    // assert(rc == 0);

    if (role == SERVER) {
        return 0;
    }
    
    // assert(role == CLIENT);
    rc = modify_qp_to_rts(local_qp);
    // assert(rc == 0);
    return 0;
}

struct ibv_send_wr * ib_merge_sr_lists_unsignaled(std::vector<IbvSrList *> sr_lists) {
    struct ibv_send_wr * ret_sr_head = sr_lists[0]->sr_list;
    for (size_t i = 1; i < sr_lists.size(); i ++) {
        uint32_t pre_num_sr = sr_lists[i - 1]->num_sr;
        sr_lists[i - 1]->sr_list[pre_num_sr - 1].next = sr_lists[i]->sr_list;
    }

    size_t last_idx = sr_lists.size() - 1;
    uint32_t num_sr = sr_lists[last_idx]->num_sr;
    sr_lists[last_idx]->sr_list[num_sr - 1].next = NULL;

    return ret_sr_head;
}

struct ibv_send_wr * ib_merge_sr_lists(std::vector<IbvSrList *> sr_lists, __OUT uint64_t * last_wr_id) {
    struct ibv_send_wr * ret_sr_head = sr_lists[0]->sr_list;
    for (size_t i = 1; i < sr_lists.size(); i ++) {
        uint32_t pre_num_sr = sr_lists[i - 1]->num_sr;
        sr_lists[i - 1]->sr_list[pre_num_sr - 1].next = sr_lists[i]->sr_list;
    }

    size_t last_idx = sr_lists.size() - 1;
    uint32_t num_sr = sr_lists[last_idx]->num_sr;
    sr_lists[last_idx]->sr_list[num_sr - 1].next = NULL;
    sr_lists[last_idx]->sr_list[num_sr - 1].send_flags |= IBV_SEND_SIGNALED;

    *last_wr_id = sr_lists[last_idx]->sr_list[num_sr - 1].wr_id;

    return ret_sr_head;
}

void ib_free_sr_lists(IbvSrList * sr_lists, uint32_t num_sr_list) {
    // TODO: finish this
    return;
    // free(sr_lists[0].sr_list->sg_list);
    // free(sr_lists[0].sr_list);
    // free(sr_lists);
}

void ib_free_sr_lists_batch(std::vector<IbvSrList *> & sr_lists_batch, std::vector<uint32_t> & sr_list_num_batch) {
    for (int i = 0; i < sr_lists_batch.size(); i ++) {
        ib_free_sr_lists(sr_lists_batch[i], sr_list_num_batch[i]);
    }
}

inline uint64_t gen_wr_id(uint8_t server_id, uint64_t wr_id) {
    return server_id * 1000 + wr_id;
}

inline uint64_t wr_id_to_server_wr_id(uint64_t wr_id) {
    return wr_id % 1000;
}

inline uint8_t wr_id_to_server_id(uint64_t wr_id) {
    return (uint8_t)(wr_id / 1000);
}