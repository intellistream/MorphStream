#ifndef LibPacket_H
#define LibPacket_H

/* (C) ip_hdr */
#include <netinet/ip.h>

//#include "diameter.h"
//#include "gtp.h"
//#include "s1ap.h"
#include "utils.hpp"

#define BUF_SIZE 1024
#define IP_HDR_LEN 20
#define DATA_SIZE 800

class DSPacketHandler {
public:
//	Gtp gtp_hdr;
//	S1ap s1ap_hdr;
//	Diameter diameter_hdr;
    uint8_t *data;
    int data_ptr;
    int len;

    DSPacketHandler();

    DSPacketHandler(const DSPacketHandler &);

    friend void swap(DSPacketHandler &, DSPacketHandler &);

    DSPacketHandler &operator=(DSPacketHandler);

    DSPacketHandler(DSPacketHandler &&);

    void append_item(bool);

    void append_item(int);

    void append_item(uint8_t);

    void append_item(uint16_t);

    void append_item(uint32_t);

    void append_item(uint64_t);

    void append_item(vector<uint64_t>);

    void append_item(uint8_t *, int);

    void append_item(const char *);

    void append_item(string);

    void prepend_item(uint8_t *, int);

//	void prepend_gtp_hdr(uint8_t, uint8_t, uint16_t, uint32_t);
//	void prepend_s1ap_hdr(uint8_t, uint16_t, uint32_t, uint32_t);
//	void prepend_diameter_hdr(uint8_t, uint16_t);
    void prepend_len();

    void extract_item(bool &);

    void extract_item(int &);

    void extract_item(uint8_t &);

    void extract_item(uint16_t &);

    void extract_item(uint32_t &);

    void extract_item(uint64_t &);

    void extract_item(vector<uint64_t> &, int);

    void extract_item(uint8_t *, int);

    void extract_item(char *, int);

    void extract_item(string &);

//	void extract_gtp_hdr();
//	void extract_s1ap_hdr();
//	void extract_diameter_hdr();
    void truncate();

    void clear_pkt();

    struct ip *allocate_ip_hdr_mem(int);

    ~DSPacketHandler();
};

#endif

