#include "nm.h"

#include <netdb.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>

#include <boost/fiber/all.hpp>

#include "ib.h"
#include "kv_debug.h"

int UDPNetworkManager::UDPCMInitClient(const struct GlobalConfig * conf) {
    this->num_server_ = conf->memory_num;
    this->server_addr_list_ = (struct sockaddr_in *)malloc(
        this->num_server_ * sizeof(struct sockaddr_in));
    // assert(this->server_addr_list_ != NULL);
    memset(this->server_addr_list_, 0, sizeof(struct sockaddr_in) * this->num_server_);

    this->rc_qp_list_.resize(conf->memory_num);
    this->mr_info_list_.reserve(conf->memory_num);
    // create deep cq for client
    this->ib_cq_ = ibv_create_cq(this->ib_ctx_, 1024, NULL, NULL, 0);
    // assert(this->ib_cq_ != NULL);

    for (int i = 0; i < conf->memory_num; i ++) {
        server_addr_list_[i].sin_family = AF_INET;
        server_addr_list_[i].sin_port   = htons(this->udp_port_);
        server_addr_list_[i].sin_addr.s_addr = inet_addr(conf->memory_ips[i]);
    }
    return 0;
}

int UDPNetworkManager::UDPCMInitServer(const struct GlobalConfig * conf) {
    // set sock option
    int ret = 0;
    struct timeval timeout;
    timeout.tv_sec  = 1;
    timeout.tv_usec = 0;
    ret = setsockopt(udp_sock_, SOL_SOCKET, SO_RCVTIMEO, &timeout, 
        sizeof(struct timeval));
    // assert(ret == 0);

    num_server_       = 1;
    server_addr_list_ = (struct sockaddr_in *)malloc(num_server_ * sizeof(struct sockaddr_in));
    // assert(server_addr_list_ != NULL);
    memset(server_addr_list_, 0, sizeof(struct sockaddr_in) * num_server_);

    server_addr_list_[0].sin_family = AF_INET;
    server_addr_list_[0].sin_port   = htons(udp_port_);
    server_addr_list_[0].sin_addr.s_addr = htonl(INADDR_ANY);

    // create shallow cq for server
    ib_cq_ = ibv_create_cq(ib_ctx_, 1, NULL, NULL, 0);
    // assert(this->ib_cq_ != NULL);

    ret = bind(udp_sock_, (struct sockaddr *)&server_addr_list_[0], sizeof(struct sockaddr_in));
    // assert(ret >= 0);

    return 0;
}

UDPNetworkManager::UDPNetworkManager(const struct GlobalConfig * conf) {
    // initialize udp socket
    int ret = 0;
    udp_sock_ = socket(AF_INET, SOCK_DGRAM, 0);
    // assert(this->udp_sock_ >= 0);

    role_        = conf->role;
    server_id_   = conf->server_id;
    conn_type_   = conf->conn_type;
    udp_port_    = conf->udp_port;
    ib_port_num_ = conf->ib_port_id;

    stop_polling_ = false;

    // create ib structs
    ib_ctx_ = ib_get_ctx(conf->ib_dev_id, conf->ib_port_id);
    // assert(ib_ctx_ != NULL);

    ib_pd_ = ibv_alloc_pd(this->ib_ctx_);
    // assert(ib_pd_ != NULL);

    ret = ibv_query_port(ib_ctx_, conf->ib_port_id, &ib_port_attr_);
    // assert(ret == 0);

    ret = ibv_query_device(ib_ctx_, &ib_device_attr_);
    // assert(ret == 0);

    if (conn_type_ == ROCE) {
        ib_gid_ = (union ibv_gid *)malloc(sizeof(union ibv_gid));
        ret = ibv_query_gid(ib_ctx_, conf->ib_port_id, conf->ib_gid_idx, ib_gid_);
        // assert(ret == 0);
    } else {
        // assert(conn_type_ == IB);
        ib_gid_ = NULL;
    }

    // initialize client
    if (role_ == CLIENT) {
        ret = UDPCMInitClient(conf);
    } else {
        // assert(role_ == SERVER);
        ret = UDPCMInitServer(conf);
    }
    // assert(ret == 0);
}

UDPNetworkManager::~UDPNetworkManager() {
    close(this->udp_sock_);
    free(server_addr_list_);
}

int UDPNetworkManager::nm_on_connect_new_qp(const struct KVMsg * request, 
    __OUT struct QpInfo * qp_info) {
    int rc = 0;
    struct ibv_qp * new_rc_qp = this->server_create_rc_qp();
    // assert(new_rc_qp != NULL);

    if (this->rc_qp_list_.size() <= request->id) {
        this->rc_qp_list_.resize(request->id + 1);
    }
    if (this->rc_qp_list_[request->id] != NULL) {
        ibv_destroy_qp(rc_qp_list_[request->id]);
    }
    this->rc_qp_list_[request->id] = new_rc_qp;

    rc = this->get_qp_info(new_rc_qp, qp_info);
    // assert(rc == 0);
    return 0;
}

int UDPNetworkManager::nm_on_connect_connect_qp(uint32_t client_id, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info) {
    int rc = 0;
    struct ibv_qp * qp = this->rc_qp_list_[client_id];
    rc = ib_connect_qp(qp, local_qp_info, remote_qp_info, 
        this->conn_type_, this->role_);
    // assert(rc == 0);
    return 0;
}

int UDPNetworkManager::client_connect_all_rc_qp() {
    int rc = 0;
    for (int i = 0; i < num_server_; i ++) {
        rc = client_connect_one_rc_qp(i);
        // assert(rc == 0);
    }
    return 0;
}

int UDPNetworkManager::client_connect_one_rc_qp(uint32_t server_id) {
    int rc = 0;
    struct ibv_qp * new_rc_qp = client_create_rc_qp();
    // assert(new_rc_qp != NULL);
    
    struct KVMsg request;
    memset(&request, 0, sizeof(struct KVMsg));
    request.type = REQ_CONNECT;
    request.id   = this->server_id_;
    rc = get_qp_info(new_rc_qp, &request.body.conn_info.qp_info);
    // assert(rc == 0);
    serialize_kvmsg(&request);

    rc = sendto(this->udp_sock_, &request, sizeof(struct KVMsg), 
        0, (struct sockaddr *)&this->server_addr_list_[server_id], 
        sizeof(struct sockaddr_in));
    // assert(rc == sizeof(struct KVMsg));

    struct KVMsg reply;
    int len;
    rc = recvfrom(this->udp_sock_, &reply, sizeof(struct KVMsg), 
        0, NULL, NULL);
    // assert(rc == sizeof(struct KVMsg));
    deserialize_kvmsg(&reply);
    deserialize_kvmsg(&request);

    // assert(reply.type == REP_CONNECT);
    
    rc = ib_connect_qp(new_rc_qp, &request.body.conn_info.qp_info, 
        &reply.body.conn_info.qp_info, this->conn_type_, this->role_);
    // assert(rc == 0);

    // record this rc_qp
    this->rc_qp_list_[server_id] = new_rc_qp;
    // record the memory info
    struct MrInfo * mr_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
    memcpy(mr_info, &reply.body.conn_info.gc_info, sizeof(struct MrInfo));
    this->mr_info_list_[server_id] = mr_info;
    return 0;
}

int UDPNetworkManager::client_connect_one_rc_qp(uint32_t server_id, 
        __OUT struct MrInfo * mr_info) {
    int rc = 0;
    struct ibv_qp * new_rc_qp = client_create_rc_qp();
    // assert(new_rc_qp != NULL);
    
    struct KVMsg request;
    memset(&request, 0, sizeof(struct KVMsg));
    request.type = REQ_CONNECT;
    request.id   = server_id_;
    rc = get_qp_info(new_rc_qp, &request.body.conn_info.qp_info);
    // assert(rc == 0);
    serialize_kvmsg(&request);

    rc = sendto(udp_sock_, &request, sizeof(struct KVMsg), 
        0, (struct sockaddr *)&server_addr_list_[server_id], 
        sizeof(struct sockaddr_in));
    // assert(rc == sizeof(struct KVMsg));

    struct KVMsg reply;
    int len;
    rc = recvfrom(udp_sock_, &reply, sizeof(struct KVMsg), 
        0, NULL, NULL);
    // assert(rc == sizeof(struct KVMsg));
    deserialize_kvmsg(&reply);
    deserialize_kvmsg(&request);

    // assert(reply.type == REP_CONNECT);
    
    rc = ib_connect_qp(new_rc_qp, &request.body.conn_info.qp_info, 
        &reply.body.conn_info.qp_info, conn_type_, role_);
    // assert(rc == 0);

    // record this rc_qp
    struct MrInfo * new_mr_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
    memcpy(new_mr_info, &reply.body.conn_info.gc_info, sizeof(struct MrInfo));
    rc_qp_list_[server_id] = new_rc_qp;
    mr_info_list_[server_id] = new_mr_info;
    // return the memory info
    memcpy(mr_info, &reply.body.conn_info.gc_info, sizeof(struct MrInfo));
    return 0;
}

struct ibv_qp * UDPNetworkManager::client_create_rc_qp() {
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));

    qp_init_attr.qp_type    = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq    = this->ib_cq_;
    qp_init_attr.recv_cq    = this->ib_cq_;
    qp_init_attr.cap.max_send_wr  = 512;
    qp_init_attr.cap.max_recv_wr  = 1;
    qp_init_attr.cap.max_send_sge = 16;
    qp_init_attr.cap.max_recv_sge = 16;

    return ib_create_rc_qp(this->ib_pd_, &qp_init_attr);
}

struct ibv_qp * UDPNetworkManager::server_create_rc_qp() {
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));

    qp_init_attr.qp_type    = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq    = this->ib_cq_;
    qp_init_attr.recv_cq    = this->ib_cq_;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.cap.max_inline_data = 256;

    return ib_create_rc_qp(this->ib_pd_, &qp_init_attr);
}

int UDPNetworkManager::get_qp_info(struct ibv_qp * qp, __OUT struct QpInfo * qp_info) {
    qp_info->qp_num   = qp->qp_num;
    qp_info->lid      = this->ib_port_attr_.lid;
    qp_info->port_num = this->ib_port_num_;
    if (this->conn_type_ == ROCE) {
        memcpy(qp_info->gid, this->ib_gid_, sizeof(union ibv_gid));
    } else {
        memset(qp_info->gid, 0, sizeof(union ibv_gid));
    }
    return 0;
}

void UDPNetworkManager::get_ib_info(__OUT struct IbInfo * ib_info) {
    ib_info->conn_type = this->conn_type_;
    ib_info->ib_ctx    = this->ib_ctx_;
    ib_info->ib_pd     = this->ib_pd_;
    ib_info->ib_cq     = this->ib_cq_;
    ib_info->ib_port_attr = &this->ib_port_attr_;
    ib_info->ib_gid       = this->ib_gid_;
}

int UDPNetworkManager::rdma_post_send_batch_async(uint32_t server_id, struct ibv_send_wr * wr_list) {
    struct ibv_qp * send_qp = this->rc_qp_list_[server_id];
    struct ibv_send_wr * bad_wr;
    return ibv_post_send(send_qp, wr_list, &bad_wr);
}

int UDPNetworkManager::rdma_post_send_batch_sync(uint32_t server_id, struct ibv_send_wr * wr_list) {
    struct ibv_qp * send_qp = this->rc_qp_list_[server_id];
    struct ibv_send_wr * bad_wr;
    int ret = 0;

    ret = ibv_post_send(send_qp, wr_list, &bad_wr);
    // assert(ret == 0);

    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    // assert(wc.status == IBV_WC_SUCCESS);
    return 0;
}

int UDPNetworkManager::nm_recv_udp_msg(__OUT struct KVMsg * kvmsg, 
        __OUT struct sockaddr_in * src_addr, __OUT socklen_t * src_addr_len) {
    int rc = recvfrom(this->udp_sock_, kvmsg, sizeof(struct KVMsg), 
        0, (struct sockaddr *)src_addr, src_addr_len);
    if (rc != sizeof(struct KVMsg))
        return -1;
    return 0;
}

int UDPNetworkManager::nm_send_udp_msg(struct KVMsg * kvmsg, 
    struct sockaddr_in * dest_addr, socklen_t dest_addr_len) {
    int rc = sendto(this->udp_sock_, kvmsg, sizeof(struct KVMsg), 
        0, (struct sockaddr *)dest_addr, dest_addr_len);
    if (rc != sizeof(struct KVMsg))
        return -1;
    return 0;
}

void UDPNetworkManager::close_udp_sock() {
    close(udp_sock_);
}

inline int UDPNetworkManager::rdma_poll_one_completion(struct ibv_wc * wc) {
    int num_polled = 0;
    while (num_polled == 0) {
        num_polled = ibv_poll_cq(ib_cq_, 1, wc);
    }
    // assert(num_polled == 1);
    return 0;
}

int UDPNetworkManager::nm_send_udp_msg_to_server(struct KVMsg * kvmsg, uint32_t server_id) {
    struct sockaddr_in * dest_addr = &server_addr_list_[server_id];
    socklen_t dest_addr_len = sizeof(struct sockaddr_in);
    return nm_send_udp_msg(kvmsg, dest_addr, dest_addr_len);
}

int UDPNetworkManager::nm_rdma_read_from_sid_sync(void * local_addr, uint32_t local_lkey, 
        uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_addr;
    sge.length = size;
    sge.lkey   = local_lkey;

    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 1;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_READ;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey = remote_rkey;

    int ret = rdma_post_send_batch_async(server_id, &sr);
    if (ret != 0) {
        printf("post send error\n");
    }
    // assert(ret == 0);

    std::map<uint64_t, struct ibv_wc *> wait_wrid_wc_map;
    wait_wrid_wc_map[1] = NULL;
    ret = nm_poll_completion_sync(wait_wrid_wc_map);
    // assert(ret == 0);
    if (wait_wrid_wc_map[1]->status != IBV_WC_SUCCESS) {
        printf("wc error\n");
    }
    // assert(wait_wrid_wc_map[1]->status == IBV_WC_SUCCESS);
    return 0;
}

int UDPNetworkManager::nm_rdma_write_inl_to_sid_sync(void * data, uint32_t size, uint64_t remote_addr,
        uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     send_sge;
    memset(&send_sge, 0, sizeof(struct ibv_sge));
    send_sge.addr   = (uint64_t)data;
    send_sge.length = size;
    send_sge.lkey   = 0;

    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 0;
    sr.sg_list = &send_sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_WRITE;
    sr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED | IBV_SEND_FENCE;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey = remote_rkey;

    // print_log(DEBUG, "\t  [%s] writing to server(%d) raddr(%lx) rkey(%x)", 
    //     __FUNCTION__, server_id, remote_addr, remote_rkey);
    
    int ret = rdma_post_send_batch_async(server_id, &sr);
    // assert(ret == 0);

    std::map<uint64_t, struct ibv_wc *> wait_wrid_wc_map;
    wait_wrid_wc_map[0] = NULL;
    ret = nm_poll_completion_sync(wait_wrid_wc_map);
    // assert(ret == 0);
    // assert(wait_wrid_wc_map[0]->status == IBV_WC_SUCCESS);

    return 0;
}

int UDPNetworkManager::nm_rdma_write_inl_to_sid(void * data, uint32_t size, uint64_t remote_addr,
        uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     send_sge;
    memset(&send_sge, 0, sizeof(struct ibv_sge));
    send_sge.addr   = (uint64_t)data;
    send_sge.length = size;
    send_sge.lkey   = 0;

    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 100;
    sr.sg_list = &send_sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_WRITE;
    sr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED | IBV_SEND_FENCE;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey = remote_rkey;
    
    int ret = rdma_post_send_batch_async(server_id, &sr);
    // assert(ret == 0);

    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        // print_log(DEBUG, "WC status(%d) id(%ld)", wc.status, wc.wr_id);
    }
    // assert(wc.status == IBV_WC_SUCCESS);
    // assert(wc.wr_id  == 100);
    // assert(wc.opcode == IBV_WC_RDMA_WRITE);
    return 0;
}

int UDPNetworkManager::nm_rdma_write_to_sid(void * local_addr, uint32_t local_lkey, 
        uint32_t size, uint64_t remote_addr, uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_addr;
    sge.length = size;
    sge.lkey   = local_lkey;

    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id   = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_WRITE;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey        = remote_rkey;

    int ret = rdma_post_send_batch_async(server_id, &sr);
    // assert(ret == 0);

    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        // print_log(DEBUG, "WC statud(%d)", wc.status);
    }
    // assert(wc.status == IBV_WC_SUCCESS);
    // assert(wc.wr_id  == 101);
    // assert(wc.opcode == IBV_WC_RDMA_WRITE);
    return 0;
}

int UDPNetworkManager::nm_rdma_read_from_sid(void * local_addr, uint32_t local_lkey,
        uint32_t size, uint64_t remote_addr,
        uint32_t remote_rkey, uint32_t server_id) {
    struct ibv_send_wr sr;
    struct ibv_sge     sge;
    memset(&sge, 0, sizeof(struct ibv_sge));
    sge.addr   = (uint64_t)local_addr;
    sge.length = size;
    sge.lkey   = local_lkey;

    memset(&sr, 0, sizeof(struct ibv_send_wr));
    sr.wr_id = 101;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.next    = NULL;
    sr.opcode  = IBV_WR_RDMA_READ;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey = remote_rkey;

    int ret = rdma_post_send_batch_async(server_id, &sr);
    // assert(ret == 0);

    struct ibv_wc wc;
    ret = rdma_poll_one_completion(&wc);
    if (wc.status != IBV_WC_SUCCESS) {
        printf("WC status: %d, wr_id: %ld\n", wc.status, wc.wr_id);
    }
    // assert(wc.status == IBV_WC_SUCCESS);
    // assert(wc.wr_id  == 101);
    // assert(wc.opcode == IBV_WC_RDMA_READ);
    return 0;
}

int UDPNetworkManager::rdma_post_sr_lists_sync_unsignaled(IbvSrList * sr_lists,
        uint32_t num_sr_lists) {
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < num_sr_lists; i ++) {
        IbvSrList * cur_sr_list = &sr_lists[i];
        server_id_sr_list_map[cur_sr_list->server_id].push_back(cur_sr_list);
    }

    std::map<uint8_t, struct ibv_send_wr *>  post_sr_map;
    std::map<uint64_t, bool> comp_wrid_map;
    std::map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists_unsignaled(it->second);
        post_sr_map[it->first] = sr_list_head;
    }

    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
    }

    return 0;
}

int UDPNetworkManager::rdma_post_sr_lists_sync(IbvSrList * sr_lists, 
        uint32_t num_sr_lists, __OUT struct ibv_wc * wc) {
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < num_sr_lists; i ++) {
        IbvSrList * cur_sr_list = &sr_lists[i];
        server_id_sr_list_map[cur_sr_list->server_id].push_back(cur_sr_list);
    }

    std::map<uint8_t, struct ibv_send_wr *>  post_sr_map;
    std::map<uint64_t, bool> comp_wrid_map;
    std::map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        uint64_t last_wr_id;
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists(it->second, &last_wr_id);
        post_sr_map[it->first] = sr_list_head;
        comp_wrid_map[last_wr_id] = false;
    }

    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
        // assert(ret == 0);
    }

    // poll cq
    int num_wc = post_sr_map.size();
    struct ibv_wc * tmp_wc = (struct ibv_wc *)malloc(sizeof(struct ibv_wc) * num_wc);
    do {
        ret = ibv_poll_cq(ib_cq_, num_wc, tmp_wc);
        // assert(ret >= 0);
        for (int i = 0; i < ret; i ++) {
            if (tmp_wc[i].status != IBV_WC_SUCCESS) {
                // print_log(DEBUG, "\t[%s] wc: %d", __FUNCTION__, tmp_wc[i].status);
            }
            // assert(tmp_wc[i].status == IBV_WC_SUCCESS);
            comp_wrid_map[tmp_wc[i].wr_id] = true;
        }
    } while (!is_all_complete(comp_wrid_map));

    return 0;
}

int UDPNetworkManager::rdma_post_sr_lists_async_unsignaled(IbvSrList * sr_lists,
        uint32_t num_sr_lists) {
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < num_sr_lists; i ++) {
        IbvSrList * cur_sr_list = &sr_lists[i];
        server_id_sr_list_map[cur_sr_list->server_id].push_back(cur_sr_list);
    }

    std::map<uint8_t, struct ibv_send_wr *>  post_sr_map;
    std::map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists_unsignaled(it->second);
        post_sr_map[it->first] = sr_list_head;
    }

    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
    }
    return 0;
}

int UDPNetworkManager::rdma_post_sr_lists_async(IbvSrList * sr_lists,
        uint32_t num_sr_lists, __OUT std::map<uint64_t, struct ibv_wc *> & wait_wrid_wc_map) {
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < num_sr_lists; i ++) {
        IbvSrList * cur_sr_list = &sr_lists[i];
        server_id_sr_list_map[cur_sr_list->server_id].push_back(cur_sr_list);
    }

    std::map<uint8_t, struct ibv_send_wr *>  post_sr_map;
    std::map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        uint64_t last_wr_id;
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists(it->second, &last_wr_id);
        post_sr_map[it->first] = sr_list_head;
        wait_wrid_wc_map[last_wr_id] = NULL;
    }

    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        // struct ibv_send_wr * srp;
        // for (srp = sr_it->second; srp != NULL; srp = srp->next) {
        //     if (srp->opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
        //         printf("wrid: %ld, send_flag: %x, raddr: %lx, rkey: %x\n", srp->wr_id, srp->send_flags,
        //             srp->wr.atomic.remote_addr, srp->wr.atomic.rkey);
        //     }
        // }
        
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
        // assert(ret == 0);
    }
    return 0;
}

int UDPNetworkManager::rdma_post_sr_list_batch_sync(std::vector<IbvSrList *> & sr_list_batch,
        std::vector<uint32_t> & sr_list_num_batch, __OUT struct ibv_wc * wc) {
    // print_sr_lists(sr_list_batch, sr_list_num_batch);
    // print_log(DEBUG, "\t\t[%s] 0. get server map", __FUNCTION__);
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < sr_list_batch.size(); i ++) {
        uint8_t server_id;
        for (int j = 0; j < sr_list_num_batch[i]; j ++) {
            server_id = sr_list_batch[i][j].server_id;
            server_id_sr_list_map[server_id].push_back(&sr_list_batch[i][j]);
        }
    }

    // print_log(DEBUG, "\t\t[%s] 1. merge wr lists", __FUNCTION__);
    std::map<uint8_t, struct ibv_send_wr *>  post_sr_map;
    std::map<uint64_t, bool> comp_wrid_map;
    std::map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        uint64_t last_wr_id;
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists(it->second, &last_wr_id);

        post_sr_map[it->second[0]->server_id] = sr_list_head;
        comp_wrid_map[last_wr_id] = false;
    }

    // print_log(DEBUG, "\t\t[%s] 2. post sends", __FUNCTION__);
    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        // print_log(DEBUG, "\t  rc_qp_list_.size() = %d, it->first = %d", __FUNCTION__, rc_qp_list_.size(), sr_it->first);
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        // print_log(DEBUG, "\t\t  post send to server(%d) qp(%x)", it->first, send_qp->qp_num);
        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
        // assert(ret == 0);
    }

    // poll cq
    // print_log(DEBUG, "\t[%s] 3. poll completion", __FUNCTION__);
    int num_wc = post_sr_map.size();
    struct ibv_wc * tmp_wc = (struct ibv_wc *)malloc(sizeof(struct ibv_wc) * num_wc);
    do {
        ret = ibv_poll_cq(ib_cq_, num_wc, tmp_wc);
        // assert(ret >= 0);
        for (int i = 0; i < ret; i ++) {
            if (tmp_wc[i].status != IBV_WC_SUCCESS) {
                // print_log(DEBUG, "\t\t  wc status(%d)", tmp_wc[i].status);
            }
            // print_log(DEBUG, "\t\t  wc(%ld) polled", tmp_wc[i].wr_id);
            // assert(tmp_wc[i].status == IBV_WC_SUCCESS);
            comp_wrid_map[tmp_wc[i].wr_id] = true;
        }
    } while (!is_all_complete(comp_wrid_map));
    // print_log(DEBUG, "\t\t[%s] 4. finish", __FUNCTION__);

    return 0;
}

int UDPNetworkManager::rdma_post_sr_list_batch_async(std::vector<IbvSrList *> & sr_list_batch, 
        std::vector<uint32_t> & sr_list_num_batch, __OUT std::map<uint64_t, struct ibv_wc *> & wait_wrid_wc_map) {
    // print_log(DEBUG, "\t\t[%s] 0. get server map", __FUNCTION__);
    std::map<uint8_t, std::vector<IbvSrList *> > server_id_sr_list_map;
    for (int i = 0; i < sr_list_batch.size(); i ++) {
        uint8_t server_id;
        for (int j = 0; j < sr_list_num_batch[i]; j ++) {
            server_id = sr_list_batch[i][j].server_id;
            server_id_sr_list_map[server_id].push_back(&sr_list_batch[i][j]);
        }
    }

    // print_log(DEBUG, "\t\t[%s] 1. merge wr lists", __FUNCTION__);
    std::map<uint8_t, struct ibv_send_wr *>  post_sr_map;
    std::map<uint8_t, std::vector<IbvSrList *> >::iterator it;
    for (it = server_id_sr_list_map.begin(); it != server_id_sr_list_map.end(); it ++) {
        uint64_t last_wr_id;
        struct ibv_send_wr * sr_list_head = ib_merge_sr_lists(it->second, &last_wr_id);

        post_sr_map[it->second[0]->server_id] = sr_list_head;
        // print_log(DEBUG, "\t\t[%s]  server: %d wait_wrid: %ld", __FUNCTION__, it->second[0]->server_id, last_wr_id);
        wait_wrid_wc_map[last_wr_id] = NULL;
    }

    // print_log(DEBUG, "\t\t[%s] 2. post sends", __FUNCTION__);
    std::map<uint8_t, struct ibv_send_wr *>::iterator sr_it;
    int ret = 0;
    for (sr_it = post_sr_map.begin(); sr_it != post_sr_map.end(); sr_it ++) {
        struct ibv_qp * send_qp = rc_qp_list_[sr_it->first]; 
        struct ibv_send_wr * bad_wr;
        // print_log(DEBUG, "\t\t[%s]  post send to server(%d) qp(%x)", __FUNCTION__, sr_it->first, send_qp->qp_num);

        // debug
        // for (bad_wr = sr_it->second; bad_wr != NULL; bad_wr = bad_wr->next) {
        //     printf("\t\twrid(%ld) op(%d) signal(%d)\n", bad_wr->wr_id, bad_wr->opcode, bad_wr->send_flags & IBV_SEND_SIGNALED);
        // }

        ret = ibv_post_send(send_qp, sr_it->second, &bad_wr);
        // assert(ret == 0);
    }
    return 0;
}

bool UDPNetworkManager::is_all_complete(const std::map<uint64_t, bool> & wr_id_comp_map) {
    std::map<uint64_t, bool>::const_iterator it;
    for (it = wr_id_comp_map.begin(); it != wr_id_comp_map.end(); it ++) {
        if (it->second == false) {
            return false;
        }
    }
    return true;
}

int UDPNetworkManager::nm_poll_completion_sync(std::map<uint64_t, struct ibv_wc *> & wait_wrid_wc_map) {
    std::map<uint64_t, struct ibv_wc *>::iterator it;
    while (ib_is_all_wrid_finished(wait_wrid_wc_map) == false) {
        // wrid_wc_map_lock_.lock();
        for (it = wait_wrid_wc_map.begin(); it != wait_wrid_wc_map.end(); it ++) {
            uint64_t wrid = it->first;
            tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::const_accessor acc;
            if (wrid_wc_map_.find(acc, wrid)) {
                wait_wrid_wc_map[wrid] = acc->second;
                // print_log(DEBUG, "\t\t[%s fb%lx] erase %ld", __FUNCTION__, boost::this_fiber::get_id(), acc->first);
                wrid_wc_map_.erase(acc);
            }
            // wrid_wc_map_.erase(find_it);
        }
        // wrid_wc_map_lock_.unlock();
    }
    return 0;
}

int UDPNetworkManager::nm_check_completion(std::map<uint64_t, struct ibv_wc *> & wait_wrid_wc_map) {
    std::map<uint64_t, struct ibv_wc *>::iterator it;
    // std::map<uint64_t, struct ibv_wc *>::iterator find_it;
    // while (wrid_wc_map_lock_.try_lock() == false) {
    //     boost::this_fiber::yield();
    // }
    // should be locked
    for (it = wait_wrid_wc_map.begin(); it != wait_wrid_wc_map.end(); it ++) {
        uint64_t wrid = it->first;
        tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::const_accessor acc;
        if (wrid_wc_map_.find(acc, wrid)) {
            wait_wrid_wc_map[wrid] = acc->second;
            // print_log(DEBUG, "\t\t[%s fb%lx] erase %ld", __FUNCTION__, boost::this_fiber::get_id(), acc->first);
            wrid_wc_map_.erase(acc);
        }
    }
    // wrid_wc_map_lock_.unlock();
    return 0;
}

void UDPNetworkManager::nm_thread_polling() {
    int ret = 0;
    int poll_num = 1024;
    int polled_num = 0;
    struct ibv_wc * wc_buf = (struct ibv_wc *)malloc(sizeof(struct ibv_wc) * poll_num);
    while (stop_polling_ == false) {
        // print_log(DEBUG, "hahaha\n");
        polled_num = ibv_poll_cq(ib_cq_, poll_num, wc_buf);
        // kv_assert(polled_num >= 0);

        // record polled wc to a map
        for (int i = 0; i < polled_num; i ++) {
            uint64_t wr_id = wc_buf[i].wr_id;
            if (wc_buf[i].status != IBV_WC_SUCCESS) {
                printf("%ld polled state %d (fb: %ld dst: %ld lwrid: %ld)\n", wr_id, wc_buf[i].status, 
                    (wr_id / 1000) >> 8, (wr_id / 1000) & 0xFF, wr_id % 1000);
            }
            assert(wc_buf[i].status == IBV_WC_SUCCESS);
            // print_log(DEBUG, "\t\t[%s] %ld(fiber: %d dst: %d lwrid: %d) polled status(%d)", __FUNCTION__, wr_id, 
            //     (wr_id / 1000) >> 8, (wr_id / 1000) & 0xFF, wr_id % 1000, wc_buf[i].status);
            struct ibv_wc * store_wc = (struct ibv_wc *)malloc(sizeof(struct ibv_wc));
            memcpy(store_wc, &wc_buf[i], sizeof(struct ibv_wc));
            
            // tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::const_accessor acc;
            // if (wrid_wc_map_.find(acc, wr_id)) {
            //     assert(0);
            // }
            
            tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::value_type value(wr_id, store_wc);
            wrid_wc_map_.insert(std::move(value));
        }
    }
}

void UDPNetworkManager::nm_fiber_polling() {
    // print_log(DEBUG, "\t\t[%s fb%lx] start", __FUNCTION__, boost::this_fiber::get_id());
    int ret = 0;
    int poll_num = 5;
    int polled_num = 0;
    struct ibv_wc * wc_buf = (struct ibv_wc *)malloc(sizeof(struct ibv_wc) * poll_num);
    while (stop_polling_ == false) {
        polled_num = ibv_poll_cq(ib_cq_, poll_num, wc_buf);
        // kv_assert(polled_num >= 0);

        // record polled wc to a map
        for (int i = 0; i < polled_num; i ++) {
            uint64_t wr_id = wc_buf[i].wr_id;
            // print_log(DEBUG, "\t\t[%s] %ld(fiber: %d dst: %d lwrid: %d) polled status(%d)", __FUNCTION__, wr_id, 
            //     (wr_id / 1000) >> 8, (wr_id / 1000) & 0xFF, wr_id % 1000, wc_buf[i].status);
            struct ibv_wc * store_wc = (struct ibv_wc *)malloc(sizeof(struct ibv_wc));
            memcpy(store_wc, &wc_buf[i], sizeof(struct ibv_wc));
            // wrid_wc_map_lock_.lock();
            // tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::const_accessor acc;
            // if (wrid_wc_map_.find(acc, wr_id)) {
            //     assert(0);
            // }
            tbb::concurrent_hash_map<uint64_t, struct ibv_wc *>::value_type value(wr_id, store_wc);
            wrid_wc_map_.insert(value);
            // wrid_wc_map_[wr_id] = store_wc;
            // wrid_wc_map_lock_.unlock();
        }
        // yield to other fiber
        boost::this_fiber::yield();
    }
}

void UDPNetworkManager::stop_polling() {
    stop_polling_ = true;
}

void * nm_polling_thread(void * args) {
    NMPollingThreadArgs * poll_args = (NMPollingThreadArgs *)args;
    UDPNetworkManager * nm = poll_args->nm;
    int ret = 0;

    ret = stick_this_thread_to_core(poll_args->core_id);    
    // assert(ret == 0);

    pthread_t this_tid = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpu_set_t), &cpuset);
    // assert(ret == 0);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("poll thread on core: %d\n", i);
        }
    }

    nm->nm_thread_polling();
    return NULL;
}

void * nm_polling_fiber(void * args) {
    UDPNetworkManager * nm = (UDPNetworkManager *)args;
    nm->nm_fiber_polling();
    return NULL;
}