//#include "json.hpp"
#include "core.hpp"
//#include "utils.hpp"
using namespace vnf;
/*----------------------------------------------------------------------------*/

struct nm_desc *lib_netmap_desc;
struct netmap_if *nifp;
struct netmap_ring *send_ring, *receive_ring;
struct nmreq nmr;
struct pollfd fds;
int fd, length;
int do_abort = 1;
char *src_ip; // = "169.254.127.246";    //lb_ip
const char *src_mac = "00:aa:bb:cc:dd:04"; //dummy value can be null
struct ether_addr *src_byte;

unordered_map<int, CallbackFn> funct_ptr;
unordered_map<int, CallbackFn> err_funct_ptr; //sock_id to handle error function
unordered_map<int, fn_ctrl> funct_ctrl_ptr;
unordered_map<int, string> conn_map;
int map_index = 0;
//data store part
boost::simple_segregated_storage<std::size_t> storageds; //memory pool for data store
std::vector<char> mp_ds(64 * 131072);                    //assuming value size 64 TODO
unordered_map<int, void *> ds_map1;                      //data store if option is local //TODO make it general using boost
unordered_map<void *, int> local_list;                   //local list of addr for clearing cache..local dnt remove
unordered_map<int, void *> cache_list;                   //cache list of addr for clearing cache..cache remove
unordered_map<void *, int> cache_void_list;              //cache list of addr for clearing cache..cache remove
unordered_map<void *, int> reqptr_list;                  //list of addr pointed in req object needed for clearing cache..pointed dnt remove
mutex mct, eparr, sock_c, f_ptr_lock, mp_lock, ds_lock, ds_conn_lock;
//this were per core variables
unordered_map<int, void *> mem_ptr;
unordered_map<int, int> client_list;
std::unordered_map<int, int>::const_iterator got;
boost::simple_segregated_storage<std::size_t> storage1;
boost::simple_segregated_storage<std::size_t> storage2;
boost::simple_segregated_storage<std::size_t> storage3;
boost::simple_segregated_storage<std::size_t> storage4;
int memory_size[4];
//
int ds_size = 0; //to keep count. If exceeds threshold clear
int ds_threshold = 131072, ds_sizing = 1;
/*
 * Convert an ASCII representation of an ethernet address to
 * binary form.
 */

bool useRemoteDataStore = false;
int maxCores;
int bufferSize;
string dataStoreIP;
vector<int> dataStorePorts;
int dataStoreThreshold;
bool isLibvnfInitialized = false;


int initLibvnf(int _maxCores, int _bufferSize, string _dataStoreIP, vector<int> _dataStorePorts,
               int _dataStoreThreshold, bool _useRemoteDataStore)
{
    maxCores = _maxCores;
    bufferSize = _bufferSize;
    dataStoreIP = _dataStoreIP;
    dataStorePorts = _dataStorePorts;
    dataStoreThreshold = _dataStoreThreshold;
    isLibvnfInitialized = true;
    useRemoteDataStore = _useRemoteDataStore;

    return 0;
}
/*
int initLibvnf(const string &jsonFilePath)
{
    std::ifstream jsonFileInputStream(jsonFilePath);
    nlohmann::json json;
    jsonFileInputStream >> json;

    return initLibvnf(json["maxCores"].get<int>(),
                      json["bufferSize"].get<int>(),
                      json["dataStoreIP"],
                      json["dataStorePorts"].get<vector<int> >(),
                      json["dataStoreThreshold"].get<int>(),
                      json["useRemoteDataStore"].get<bool>());
}
*/
/*
This function finds the request block size in power of 2, closest to size specified by the user
new name: initReqPool
*/
void initReqPool(int msize[], int m_tot)
{ //size of chunks for request pool and total number of sizes sizeof(msize[])
    int p = 1, i, j;
    cout << "reached here" << endl;
    int temp_memory_size[4];
    if (m_tot > 4)
    {
        cout << "Only 4 pools allowed" << endl;
        return; //TODO error handling
    }
    for (i = 0; i < m_tot; i++)
    {
        p = 1;
        temp_memory_size[i] = 0;
        if (msize[i] && !(msize[i] & (msize[i] - 1)))
        {
            temp_memory_size[i] = msize[i];
            continue;
        }
        while (p < msize[i])
            p <<= 1;

        temp_memory_size[i] = p;
    }
    cout << "MEMORY_size is " << temp_memory_size[0] << endl;
    for (i = 0; i < maxCores; i++)
    {
        for (j = 0; j < m_tot; j++)
        {
            memory_size[j] = temp_memory_size[j];
        }
    }
}

void free_ds_pool()
{
    std::unordered_map<void *, int>::const_iterator gotds;
    for (auto it = cache_void_list.begin(); it != cache_void_list.end(); ++it)
    {
        gotds = reqptr_list.find(it->first);
        if (gotds == reqptr_list.end())
        {
            cache_list.erase(it->second);
            ds_map1.erase(it->second);
            storageds.free(it->first);
        }
    }
    cache_void_list.clear();
    ds_size = 0;
}
struct ether_addr *ether_aton_dst(const char *a)
{
    int i;
    static struct ether_addr o;
    unsigned int o0, o1, o2, o3, o4, o5;

    i = sscanf(a, "%x:%x:%x:%x:%x:%x", &o0, &o1, &o2, &o3, &o4, &o5);

    if (i != 6)
        return (NULL);

    o.ether_addr_octet[0] = o0;
    o.ether_addr_octet[1] = o1;
    o.ether_addr_octet[2] = o2;
    o.ether_addr_octet[3] = o3;
    o.ether_addr_octet[4] = o4;
    o.ether_addr_octet[5] = o5;

    return ((struct ether_addr *)&o);
}

struct ether_addr *ether_aton_src(const char *a)
{
    int i;
    static struct ether_addr q;
    unsigned int o0, o1, o2, o3, o4, o5;

    i = sscanf(a, "%x:%x:%x:%x:%x:%x", &o0, &o1, &o2, &o3, &o4, &o5);

    if (i != 6)
        return (NULL);

    q.ether_addr_octet[0] = o0;
    q.ether_addr_octet[1] = o1;
    q.ether_addr_octet[2] = o2;
    q.ether_addr_octet[3] = o3;
    q.ether_addr_octet[4] = o4;
    q.ether_addr_octet[5] = o5;

    return ((struct ether_addr *)&q);
}

ConnId& vnf::ConnId::registerCallback(enum EventType t2, void callbackFnPtr(ConnId&, int, void *, char *, int, int, int))
{
    int vnf_connid = this->socketId;
    if (vnf_connid != -1)
    {
        if (t2 != ERROR)
        {
            funct_ptr[vnf_connid] = callbackFnPtr;
        }
        else
        {
            err_funct_ptr[vnf_connid] = callbackFnPtr;
        }
    }
    else
        funct_ptr[vnf_connid] = callbackFnPtr;
    return *this;
}
void vnf::registerforNotification(string controller_ip, void callbackFnPtr(string task, string vnf_name, string vnf_ip, string event))
{
    funct_ctrl_ptr[20] = callbackFnPtr;
}
/* Define a struct for ARP header */
typedef struct _arp_hdr arp_hdr;
struct _arp_hdr
{
    uint16_t htype;
    uint16_t ptype;
    uint8_t hlen;
    uint8_t plen;
    uint16_t opcode;
    uint8_t sender_mac[6];
    uint32_t sender_ip;
    uint8_t target_mac[6];
    uint32_t target_ip;
} __attribute__((__packed__));

/* ARP packet */
struct arp_pkt
{
    struct ether_header eh;
    arp_hdr ah;
} __attribute__((__packed__));

struct arp_cache_entry
{
    uint32_t ip;
    struct ether_addr mac;
};

static struct arp_cache_entry arp_cache[ARP_CACHE_LEN];
void insert_arp_cache(uint32_t ip, struct ether_addr mac)
{
    int i;
    struct arp_cache_entry *entry;
    char ip_str[INET_ADDRSTRLEN];
    for (i = 0; i < ARP_CACHE_LEN; i++)
    {
        entry = &arp_cache[i];
        if (entry->ip == ip)
        {
            return;
        }
        if (entry->ip == 0)
        {
            entry->ip = ip;
            entry->mac = mac;
            return;
        }
    }
}
/*
 * Change the destination mac field with ether_addr from given eth header
 */

void change_dst_mac(struct ether_header **ethh, struct ether_addr *p)
{
    (*ethh)->ether_dhost[0] = p->ether_addr_octet[0];
    (*ethh)->ether_dhost[1] = p->ether_addr_octet[1];
    (*ethh)->ether_dhost[2] = p->ether_addr_octet[2];
    (*ethh)->ether_dhost[3] = p->ether_addr_octet[3];
    (*ethh)->ether_dhost[4] = p->ether_addr_octet[4];
    (*ethh)->ether_dhost[5] = p->ether_addr_octet[5];
}

/* 
 * Change the source mac field with ether_addr from given eth header
 */

void change_src_mac(struct ether_header **ethh, struct ether_addr *p)
{
    (*ethh)->ether_shost[0] = p->ether_addr_octet[0];
    (*ethh)->ether_shost[1] = p->ether_addr_octet[1];
    (*ethh)->ether_shost[2] = p->ether_addr_octet[2];
    (*ethh)->ether_shost[3] = p->ether_addr_octet[3];
    (*ethh)->ether_shost[4] = p->ether_addr_octet[4];
    (*ethh)->ether_shost[5] = p->ether_addr_octet[5];
}

/*---------------------------------------------------------------------*/
/*
 * Prepares ARP packet in the buffer passed as parameter
 */
void prepare_arp_packet(struct arp_pkt *arp_pkt, const uint32_t *src_ip, const uint32_t *dest_ip, struct ether_addr *src_mac, struct ether_addr *dest_mac, uint16_t htype)
{
    memcpy(arp_pkt->eh.ether_shost, src_mac, 6);
    memcpy(arp_pkt->eh.ether_dhost, dest_mac, 6);
    arp_pkt->eh.ether_type = htons(ETHERTYPE_ARP);

    arp_pkt->ah.htype = htons(1);
    arp_pkt->ah.ptype = htons(ETHERTYPE_IP);
    arp_pkt->ah.hlen = 6;
    arp_pkt->ah.plen = 4;
    arp_pkt->ah.opcode = htype;

    arp_pkt->ah.sender_ip = *src_ip;
    arp_pkt->ah.target_ip = *dest_ip;

    memcpy(arp_pkt->ah.sender_mac, src_mac, 6);
    if (ntohs(htype) == 1)
    {
        memset(arp_pkt->ah.target_mac, 0, 6 * sizeof(uint8_t));
    }
    else
    {
        memcpy(arp_pkt->ah.target_mac, dest_mac, 6);
    }
}

void arp_reply(struct arp_pkt *arppkt)
{
    unsigned char *tx_buf = NETMAP_BUF(send_ring, send_ring->slot[send_ring->cur].buf_idx);
    struct netmap_slot *slot = &send_ring->slot[send_ring->cur];
    struct arp_pkt *arp_reply = (struct arp_pkt *)(tx_buf);
    struct ether_addr d;
    memcpy(&d, (struct ether_addr *)arppkt->ah.sender_mac, 6);
    struct ether_addr s = *ether_aton_src(src_mac);
    prepare_arp_packet(arp_reply, &arppkt->ah.target_ip, &arppkt->ah.sender_ip, &s, &d, htons(2));
    slot->len = sizeof(struct arp_pkt);
    send_ring->cur = nm_ring_next(send_ring, send_ring->cur);
    send_ring->head = send_ring->cur;
    ioctl(fds.fd, NIOCTXSYNC, NULL);
    char arp_src_ip[INET_ADDRSTRLEN];
    char arp_target_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(arp_reply->ah.target_ip), arp_target_ip, INET_ADDRSTRLEN);
    inet_ntop(AF_INET, &(arp_reply->ah.sender_ip), arp_src_ip, INET_ADDRSTRLEN);
}

void arp_request(const uint32_t *dest_ip)
{
    unsigned char *tx_buf = NETMAP_BUF(send_ring, send_ring->slot[send_ring->cur].buf_idx);
    struct netmap_slot *slot = &send_ring->slot[send_ring->cur];
    struct arp_pkt *arp_request_pkt = (struct arp_pkt *)(tx_buf);
    uint32_t source_ip;
    inet_pton(AF_INET, src_ip, &(source_ip));
    struct ether_addr source_mac = *ether_aton_src(src_mac);
    struct ether_addr dest_mac = *ether_aton_dst("ff:ff:ff:ff:ff:ff");
    prepare_arp_packet(arp_request_pkt, &source_ip, dest_ip, &source_mac, &dest_mac, htons(1));

    slot->len = sizeof(struct arp_pkt);
    send_ring->cur = nm_ring_next(send_ring, send_ring->cur);
    send_ring->head = send_ring->cur;
    ioctl(fds.fd, NIOCTXSYNC, NULL);
}
void vnf::handle_arp_packet(char *buffer)
{
    struct arp_pkt *arppkt;
    struct ether_addr sender_mac;
    arppkt = (struct arp_pkt *)buffer;
    char arp_target_ip[INET_ADDRSTRLEN];
    char arp_sender_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(arppkt->ah.target_ip), arp_target_ip, INET_ADDRSTRLEN);
    inet_ntop(AF_INET, &(arppkt->ah.sender_ip), arp_sender_ip, INET_ADDRSTRLEN);

    memcpy(&sender_mac, (struct ether_addr *)arppkt->ah.sender_mac, 6);
    insert_arp_cache(arppkt->ah.sender_ip, sender_mac);

    if (strcmp(arp_target_ip, src_ip) == 0)
    {
        if (ntohs(arppkt->ah.opcode) == ARP_REQUEST)
        {
            /* send arp reply */
            arp_reply(arppkt);
        }
        if (ntohs(arppkt->ah.opcode) == ARP_REPLY)
        {
        }
    }
}
char* vnf::ConnId::getPktBuf()
{
    char *dst = NETMAP_BUF(send_ring, send_ring->slot[send_ring->cur].buf_idx);
    return dst;
}
void setSlotLen(int length)
{
    send_ring->slot[send_ring->cur].len = length;
}

ConnId vnf::ConnId::createClient(string local_ip, string remoteServerIP, int remoteServerPort, string protocol)
{
    //netmap fd passed instead of id
    map_index = map_index + 1;
    conn_map[map_index] = remoteServerIP;
    return ConnId(0, map_index);
}
ConnId& vnf::ConnId::sendData(char *packetToSend, int size, int streamNum)
{
    int vnf_connid = this->socketId;
    struct ether_header *ethh = (struct ether_header *)packetToSend;
    struct ether_addr backend_mac;
    struct arp_cache_entry *entry;
    int i;
    uint32_t dst_ip;
    inet_pton(AF_INET, (conn_map[vnf_connid]).c_str(), &(dst_ip));
    for (i = 0; i < ARP_CACHE_LEN; i++)
    {
        entry = &arp_cache[i];
        if (entry->ip == dst_ip)
        {
            //mac address exist
            backend_mac = entry->mac;
            break;
        }
    }
    if (i == ARP_CACHE_LEN)
    {
        // mac not in arp cache, send arp request to get destination mac //
        arp_request(&dst_ip);
        // For now relying on TCP retransmission
        return;
    }
    change_dst_mac(&ethh, &backend_mac);
    change_src_mac(&ethh, src_byte);
    send_ring->slot[send_ring->cur].len = size;
    send_ring->cur = nm_ring_next(send_ring, send_ring->cur);
    return *this;
}

void send_batch()
{
    send_ring->head = send_ring->cur;
    ioctl(fds.fd, NIOCTXSYNC, NULL);
}

static void
sigint_h(int sig)
{
    (void)sig; /* UNUSED */
    do_abort = 1;
    nm_close(lib_netmap_desc);
    signal(SIGINT, SIG_DFL);
}

ConnId vnf::initServer(string inter_face, string server_ip, int server_port, string protocol)
{
    src_ip = new char[server_ip.size() + 1];
    std::copy(server_ip.begin(), server_ip.end(), src_ip);
    src_ip[server_ip.size()] = '\0';
    memset(arp_cache, 0, ARP_CACHE_LEN * sizeof(struct arp_cache_entry));
    struct nmreq base_req;
    memset(&base_req, 0, sizeof(base_req));
    base_req.nr_flags |= NR_ACCEPT_VNET_HDR;
    string iface = "netmap:";
    string if_name = iface + inter_face;
    lib_netmap_desc = nm_open(if_name.c_str(), &base_req, 0, 0);
    fds.fd = NETMAP_FD(lib_netmap_desc);
    fds.events = POLLIN;
    receive_ring = NETMAP_RXRING(lib_netmap_desc->nifp, 0);
    send_ring = NETMAP_TXRING(lib_netmap_desc->nifp, 0);

    if (ds_sizing == 1)
    {
        storageds.add_block(&mp_ds.front(), mp_ds.size(), 64);
        ds_sizing = 0;
    }
    ConnId connId = ConnId(0, fds.fd);
    return connId;
}
//needed to check packet from controller
void process_ip_packet_lib(int fd1, const unsigned char *buffer, struct ip *iph1, int length)
{
    fn_ctrl fn_ptr;
    CallbackFn fn_ptr1;
    int my_fd = fd1;
    if (iph1->ip_p == IPPROTO_UDP)
    {
        struct ether_header *ethh = (struct ether_header *)buffer;
        struct ip *ipd = (struct ip *)(ethh + 1);
        struct udphdr *udp1 = (struct udphdr *)(ipd + 1);
        char *ctrl_data = (char *)(udp1 + 1);
        std::string data_ct1(ctrl_data);
        if (data_ct1.length() > 2)
        {
            char task = data_ct1.at(0);
            cout << "task is " << task << endl;
            string data_ct = data_ct1.substr(1, data_ct1.length() - 1);
            if (data_ct == "169.254.9.28")
            {
                cout << "packet from controller " << data_ct << endl;
                fn_ptr = funct_ctrl_ptr[20];
                if (task == 'A')
                    fn_ptr("ADD", "b", data_ct, "overload");
                else
                    fn_ptr("DEL", "b", data_ct, "overload");
            }
        }
    }
    else
    {
        fn_ptr1 = funct_ptr[my_fd];
        mem_ptr[my_fd] = NULL;                   //memory for request object
	ConnId connId = ConnId(0, my_fd);
        fn_ptr1(connId, 0, NULL, (char*)buffer, length, 0, 0); //length pass instead of id
    }
}

//needed to check packet from controller
void process_receive_buffer_lib(int fd1, int len, char *buffer)
{
    CallbackFn fn_ptr;
    int my_fd = fd1;
    int vnf_connid = fd1;
    struct ether_header *ethh1 = (struct ether_header *)buffer;
    if (ntohs(ethh1->ether_type) == ETHERTYPE_IP)
    {
        process_ip_packet_lib(vnf_connid, (const unsigned char*)buffer, (struct ip *)(ethh1 + 1), len);
    }
    else
    {
        fn_ptr = funct_ptr[my_fd];
        mem_ptr[my_fd] = NULL;                       //memory for request object
	ConnId connId = ConnId(0, my_fd);
        fn_ptr(connId, 0, NULL, buffer, length, 0, 0); //length pass instead of id
    }
}

void startEventLoop()
{
    CallbackFn fn_ptr;
    int r;
    char *src;
    int my_fd = fds.fd;
    signal(SIGINT, sigint_h);
    //datastore part
    std::vector<char> mp_v1;
    std::vector<char> mp_v2;
    std::vector<char> mp_v3;
    std::vector<char> mp_v4;
    if (memory_size[0] != 0)
    {
        mp_v1.resize((memory_size[0]) * 2097152);
        cout << "vector size is " << mp_v1.size() << endl;
        storage1.add_block(&mp_v1.front(), mp_v1.size(), memory_size[0]); //uncomment nov22
    }
    if (memory_size[1] != 0)
    {
        mp_v2.resize((memory_size[1]) * 2097152);
        cout << "vector size is " << mp_v2.size() << endl;
        storage2.add_block(&mp_v2.front(), mp_v2.size(), memory_size[1]); //uncomment nov22
    }
    if (memory_size[2] != 0)
    {
        mp_v3.resize((memory_size[2]) * 2097152);
        cout << "vector size is " << mp_v3.size() << endl;
        storage3.add_block(&mp_v3.front(), mp_v3.size(), memory_size[2]); //uncomment nov22
    }
    if (memory_size[3] != 0)
    {
        mp_v4.resize((memory_size[3]) * 2097152);
        cout << "vector size is " << mp_v4.size() << endl;
        storage4.add_block(&mp_v4.front(), mp_v4.size(), memory_size[3]); //uncomment nov22
    }

    // set source mac
    src_byte = ether_aton_src(src_mac);
    int n, rx;
    int cur = receive_ring->cur;
    while (do_abort)
    {
        poll(&fds, 1, -1);
        n = nm_ring_space(receive_ring);
        for (rx = 0; rx < n; rx++)
        {
            struct netmap_slot *slot = &receive_ring->slot[cur];
            src = NETMAP_BUF(receive_ring, slot->buf_idx);
            length = slot->len;
            process_receive_buffer_lib(my_fd, length, src); //length pass instead of id
            cur = nm_ring_next(receive_ring, cur);
        }
        receive_ring->head = receive_ring->cur = cur;
        send_batch();
    }
}

void* vnf::ConnId::allocReqObj(int index, int reqObjId)
{
    int id, alloc_sockid;
    alloc_sockid =  this->socketId;
    client_list[alloc_sockid] = alloc_sockid;
    if (index == 1)
    {
        mem_ptr[alloc_sockid] = static_cast<void *>(storage1.malloc()); //lock TODO
    }
    else if (index == 2)
    {
        mem_ptr[alloc_sockid] = static_cast<void *>(storage2.malloc()); //lock TODO
    }
    else if (index == 3)
    {
        mem_ptr[alloc_sockid] = static_cast<void *>(storage3.malloc()); //lock TODO
    }
    else if (index == 4)
    {
        mem_ptr[alloc_sockid] = static_cast<void *>(storage4.malloc()); //lock TODO
    }
    if (mem_ptr[alloc_sockid] == 0)
    {
        cout << "could not malloc" << endl;
    }
    return mem_ptr[alloc_sockid];
}
ConnId& vnf::ConnId::freeReqObj(int index, int reqObjId)
{
    int id, alloc_sockid;
    alloc_sockid = this->socketId;
    got = client_list.find(alloc_sockid);
    if (got == client_list.end())
    {
        mem_ptr.erase(alloc_sockid);
    }
    else
    {
        if (index == 1)
        {
            storage1.free(static_cast<void *>(mem_ptr[alloc_sockid]));
        }
        else if (index == 2)
        {
            storage2.free(static_cast<void *>(mem_ptr[alloc_sockid]));
        }
        else if (index == 3)
        {
            storage3.free(static_cast<void *>(mem_ptr[alloc_sockid]));
        }
        else if (index == 4)
        {
            storage4.free(static_cast<void *>(mem_ptr[alloc_sockid]));
        }
        mem_ptr.erase(alloc_sockid);
        client_list.erase(alloc_sockid);
    }
    return *this;
}

ConnId& vnf::ConnId::storeData(string table_name, int key, enum DataLocation localRemote, void *value, int value_len, void callbackFnPtr(ConnId&, int, void *, void *, int, int))
{
    int vnf_connid = this->socketId;
    if (callbackFnPtr != NULL)
        registerCallback(ERROR, (CallbackFn)callbackFnPtr);
    if (localRemote == LOCAL)
    {

        value = value + '\0';
        void *setds;
        ds_lock.lock();
        if (ds_size == ds_threshold)
        {
            free_ds_pool();
        }
        setds = storageds.malloc();
        ds_size++;
        memcpy(setds, value, value_len);
        //cout << "setdata" << endl;
        ds_map1[key] = setds;
        local_list[setds] = key;
        ds_lock.unlock();
    }
    return *this;
}

ConnId& vnf::ConnId::retrieveData(string table_name, int key, enum DataLocation localRemote, void callbackFnPtr(ConnId&, int, void *, void *, int, int), int reqObjId)
{
    int vnf_connid = this->socketId;
    registerCallback(READ, (CallbackFn)callbackFnPtr);
    // Serial execution if it's local mode.
    if (localRemote == LOCAL)
    {
        CallbackFn fn_ptr;
        char *ds_value;
        ds_lock.lock();
        ds_value = (char*)ds_map1[key];
        ds_lock.unlock();
        fn_ptr = funct_ptr[vnf_connid];
	ConnId connId = ConnId(0, vnf_connid);
        fn_ptr(connId,0, mem_ptr[vnf_connid], ds_value, length, 0, 0);
    }
    // Why there is no data fetching from the remote side ???
    return *this;
}

ConnId& vnf::ConnId::delData(string table_name, int key, enum DataLocation localRemote)
{
    int vnf_connid = this->socketId;
    if (localRemote == REMOTE)
    {
        std::unordered_map<int, void *>::const_iterator gotds;
        ds_lock.lock();
        gotds = cache_list.find(key);
        ds_lock.unlock();
        if (gotds != cache_list.end())
        {
            ds_lock.lock();
            cache_void_list.erase(cache_list[key]);
            storageds.free(cache_list[key]);
            cache_list.erase(key);
            ds_map1.erase(key);
            ds_lock.unlock();
        }
    }
    else
    {
        void *temp_ds;
        ds_lock.lock();
        temp_ds = ds_map1[key];
        local_list.erase(temp_ds);
        storageds.free(ds_map1[key]);
        ds_map1.erase(key);
        ds_lock.unlock();
    }
    return *this;
}
