#include "kv_utils.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <arpa/inet.h>
#include <sys/time.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>

#include <sstream>
#include <fstream>
#include <string>

inline static uint64_t htonll(uint64_t val) {
    return (((uint64_t) htonl(val)) << 32) + htonl(val >> 32);
}
 
inline static uint64_t ntohll(uint64_t val) {
    return (((uint64_t) ntohl(val)) << 32) + ntohl(val >> 32);
}

void serialize_kvmsg(__OUT struct KVMsg * kvmsg) {
    switch (kvmsg->type) {
    case REQ_CONNECT:
    case REP_CONNECT:
        serialize_conn_info(&kvmsg->body.conn_info);
        break;
    case REQ_ALLOC:
    case REP_ALLOC:
    case REQ_ALLOC_SUBTABLE:
    case REP_ALLOC_SUBTABLE:
        serialize_mr_info(&kvmsg->body.mr_info);
        break;
    default:
        break;
    }
    kvmsg->type = htons(kvmsg->type);
    kvmsg->id   = htons(kvmsg->id);
}

void deserialize_kvmsg(__OUT struct KVMsg * kvmsg) {
    kvmsg->type = ntohs(kvmsg->type);
    kvmsg->id   = ntohs(kvmsg->id);
    switch (kvmsg->type) {
    case REQ_CONNECT:
    case REP_CONNECT:
        deserialize_conn_info(&kvmsg->body.conn_info);
        break;
    case REQ_ALLOC:
    case REP_ALLOC:
    case REQ_ALLOC_SUBTABLE:
    case REP_ALLOC_SUBTABLE:
        deserialize_mr_info(&kvmsg->body.mr_info);
        break;
    default:
        break;
    }
}

void serialize_qp_info(__OUT struct QpInfo * qp_info) {
    qp_info->qp_num = htonl(qp_info->qp_num);
    qp_info->lid    = htons(qp_info->lid);
}

void deserialize_qp_info(__OUT struct QpInfo * qp_info) {
    qp_info->qp_num = ntohl(qp_info->qp_num);
    qp_info->lid    = ntohs(qp_info->lid);
}

void serialize_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = htonll(mr_info->addr);
    mr_info->rkey = htonl(mr_info->rkey);
}

void deserialize_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = ntohll(mr_info->addr);
    mr_info->rkey = ntohl(mr_info->rkey);
}

void serialize_conn_info(__OUT struct ConnInfo * conn_info) {
    serialize_qp_info(&conn_info->qp_info);
    serialize_mr_info(&conn_info->gc_info);
}

void deserialize_conn_info(__OUT struct ConnInfo * conn_info) {
    deserialize_qp_info(&conn_info->qp_info);
    deserialize_mr_info(&conn_info->gc_info);
}

int load_config(const char * fname, __OUT struct GlobalConfig * config) {
    std::fstream config_fs(fname);
    // assert(config_fs.is_open());

    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(config_fs, pt);
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }

    try {
        std::string role_str = pt.get<std::string>("role");
        if (role_str == std::string("SERVER")) {
            config->role = SERVER;
        } else {
            // assert(role_str == std::string("CLIENT"));
            config->role = CLIENT;
        }

        std::string conn_type_str = pt.get<std::string>("conn_type");
        if (conn_type_str == std::string("IB")) {
            config->conn_type = IB;
        } else {
            // assert(conn_type_str == std::string("ROCE"));
            config->conn_type = ROCE;
        }

        config->server_id = pt.get<uint32_t>("server_id");
        config->udp_port  = pt.get<uint16_t>("udp_port");
        config->memory_num = pt.get<uint16_t>("memory_num");

        int i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("memory_ips")) {
            // assert(v.first.empty());
            std::string ip = v.second.get<std::string>("");
            // assert(ip.length() > 0 && ip.length() < 16);
            strcpy(config->memory_ips[i], ip.c_str());
            i ++;
        }
        // assert(i == config->memory_num);

        config->ib_dev_id = pt.get<uint32_t>("ib_dev_id");
        config->ib_port_id = pt.get<uint32_t>("ib_port_id");
        config->ib_gid_idx = pt.get<uint32_t>("ib_gid_idx", -1);
        
        std::string server_base_addr_str = pt.get<std::string>("server_base_addr");
        sscanf(server_base_addr_str.c_str(), "0x%lx", &config->server_base_addr);

        config->server_data_len   = pt.get<uint64_t>("server_data_len");
        config->block_size        = pt.get<uint64_t>("block_size");
        config->subblock_size     = pt.get<uint64_t>("subblock_size");
        config->client_local_size = pt.get<uint64_t>("client_local_size");

        config->num_replication   = pt.get<uint32_t>("num_replication");
        config->num_coroutines    = pt.get<uint32_t>("num_coroutines", 1);

        config->main_core_id = pt.get<uint32_t>("main_core_id", 0);
        config->poll_core_id = pt.get<uint32_t>("poll_core_id", 0);
        config->bg_core_id   = pt.get<uint32_t>("bg_core_id", 0);
        config->gc_core_id   = pt.get<uint32_t>("gc_core_id", 0);

        config->is_recovery  = pt.get<uint32_t>("is_recovery", 0);
        
        config->num_idx_rep  = pt.get<uint32_t>("num_idx_rep", 1);
        config->miss_rate_threash = pt.get<float>("miss_rate_threash", 0.1);
        config->workload_run_time = pt.get<int>("workload_run_time", 10);
        config->micro_workload_num = pt.get<int>("micro_workload_num", 10000);
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }
    return 0;
}

void encode_gc_slot(DecodedClientGCSlot * d_gc_slot, __OUT uint64_t * e_gc_slot) {
    uint64_t masked_block_off = (d_gc_slot->pr_addr >> 8) & BLOCK_OFF_BMASK;
    uint64_t masked_pr_addr = (d_gc_slot->pr_addr >> 26) & BLOCK_ADDR_BMASK;
    uint64_t masked_bk_addr = (d_gc_slot->bk_addr >> 26) & BLOCK_ADDR_BMASK;
    uint64_t masked_num_subblock = d_gc_slot->num_subblocks & SUBBLOCK_NUM_BMASK;
    *(e_gc_slot) = (masked_block_off << 46) | (masked_pr_addr << 25) 
        | (masked_bk_addr << 4) | (masked_num_subblock);
}

void decode_gc_slot(uint64_t e_gc_slot, __OUT DecodedClientGCSlot * d_gc_slot) {
    uint64_t block_offset = e_gc_slot >> 46;
    uint64_t pr_block_addr = (e_gc_slot >> 25) & BLOCK_ADDR_BMASK;
    uint64_t bk_block_addr = (e_gc_slot >> 4)  & BLOCK_ADDR_BMASK;
    uint8_t  num_subblocks = e_gc_slot & SUBBLOCK_NUM_BMASK;
    d_gc_slot->pr_addr = (pr_block_addr << 26) | (block_offset << 8);
    d_gc_slot->bk_addr = (bk_block_addr << 26) | (block_offset << 8);
    d_gc_slot->num_subblocks = num_subblocks;
}

int stick_this_thread_to_core(int core_id) {
    int num_cores = sysconf(_SC_NPROCESSORS_CONF);
    if (core_id < 0 || core_id >= num_cores) {
        return -1;
    }
    
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

uint64_t current_time_us() {
    struct timeval now;
    gettimeofday(&now, NULL);
    return now.tv_usec;
}

void dump_lat_file(char * fname, const std::vector<uint64_t> & lat_vec) {
    if (lat_vec.size() == 0) {
        return;
    }
    FILE * out_fp = fopen(fname, "w");
    for (size_t i = 0; i < lat_vec.size(); i ++) {
        fprintf(out_fp, "%ld\n", lat_vec[i]);
    }
}