#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <csignal>
#include <mutex>
#include <vector>

#define MAXCON 100
#define MAXEVENTS 110
#define MAX_THREADS 5
#define THC 1000

//for multi-core B
#define MSB32                           0x80000000
#define MSB16                           0x8000
#define KEY_CACHE_LEN                   96
#define SEED                            'B'

using namespace std;

string clientIp;
int clientPort;

string serverIp;
int serverPort;

int duration;

mutex p_vec_lock;
struct thread_data {
    int id;
    int status;
};

vector<vector<uint16_t> > p_vec; //vector for 10000 ports/thread
vector<int> req_count; //request counter per thread
void signalHandler(int signum) {
    for (int i = 0; i < 20; i++)
        cout << "Req_count not closed thread is " << req_count[i] << "\n";

    // cleanup and close up stuff here
    // terminate program

    exit(signum);
}
/*---------------------------------------------------------------------*/
/**
 *  * The cache table is used to pick a nice seed for the hash value. It is
 *   * built only once when sym_hash_fn is called for the very first time
 *    */
static void
build_sym_key_cache(uint32_t *cache, int cache_len) {

    static const uint8_t key[] = {
            0x50, 0x6d, 0x50, 0x6d,
            0x50, 0x6d, 0x50, 0x6d,
            0x50, 0x6d, 0x50, 0x6d,
            0x50, 0x6d, 0x50, 0x6d,
            0xcb, 0x2b, 0x5a, 0x5a,
            0xb4, 0x30, 0x7b, 0xae,
            0xa3, 0x2d, 0xcb, 0x77,
            0x0c, 0xf2, 0x30, 0x80,
            0x3b, 0xb7, 0x42, 0x6a,
            0xfa, 0x01, 0xac, 0xbe};

    uint32_t result = (((uint32_t) key[0]) << 24) |
                      (((uint32_t) key[1]) << 16) |
                      (((uint32_t) key[2]) << 8) |
                      ((uint32_t) key[3]);

    uint32_t idx = 32;
    int i;

    for (i = 0; i < cache_len; i++, idx++) {
        uint8_t shift = (idx % (sizeof(uint8_t) * 8));
        uint32_t bit;

        cache[i] = result;
        bit = ((key[idx / (sizeof(uint8_t) * 8)] << shift)
               & 0x80) ? 1 : 0;
        result = ((result << 1) | bit);
    }
}

/*---------------------------------------------------------------------*/
/**
 ** Computes symmetric hash based on the 4-tuple header data
 **/
static uint32_t
sym_hash_fn(uint32_t sip, uint32_t dip, uint16_t sp, uint32_t dp) {

    uint32_t rc = 0;
    int i;
    static int first_time = 1;
    static uint32_t key_cache[KEY_CACHE_LEN] = {0};

    if (first_time) {
        build_sym_key_cache(key_cache, KEY_CACHE_LEN);
        first_time = 0;
    }
    for (i = 0; i < 32; i++) {
        if (sip & MSB32)
            rc ^= key_cache[i];
        sip <<= 1;
    }
    for (i = 0; i < 32; i++) {
        if (dip & MSB32)
            rc ^= key_cache[32 + i];
        dip <<= 1;
    }
    for (i = 0; i < 16; i++) {
        if (sp & MSB16)
            rc ^= key_cache[64 + i];
        sp <<= 1;
    }
    for (i = 0; i < 16; i++) {
        if (dp & MSB16)
            rc ^= key_cache[80 + i];
        dp <<= 1;
    }

    return rc;
}

void populate_ports(vector<uint16_t> &ports_vec, const char *src_ip, const char *dest_ip, const uint16_t sport,
                    const uint16_t dport) {
    struct in_addr src, dest;
    uint32_t num_cores = 0, find_core;
    inet_aton(clientIp.c_str(), &src);
    inet_aton(serverIp.c_str(), &dest);
    uint32_t sip = ntohl(src.s_addr);
    uint32_t dip = ntohl(dest.s_addr);

    for (uint16_t i = 0; i < 10000; i++) {
        find_core = sym_hash_fn(sip, dip, clientPort + i + SEED, serverPort + SEED) % MAX_THREADS;
        if (!ports_vec[find_core]) {
            num_cores++;
            ports_vec[find_core] = clientPort + i;
            if (num_cores == MAX_THREADS) {
                return;
            }
        }
    }
}

void populate_ports1(uint16_t numth, int server_cores) {
    struct in_addr src, dest;
    uint32_t num_cores = server_cores, find_core;
    inet_aton(clientIp.c_str(), &src);
    inet_aton(serverIp.c_str(), &dest);
    uint32_t sip = ntohl(src.s_addr);
    uint32_t dip = ntohl(dest.s_addr);
    int j = 0, k = 0;
    for (int i = 0; i < numth; i++) {
        j = 0;
        p_vec[i].resize(500);
        while (j < 500) {
            find_core = (sym_hash_fn(sip, dip, clientPort + k + SEED, serverPort + SEED)) % server_cores;
            p_vec[i][j] = clientPort + k;
            j++;
            k++;
        }

    }
}

void printinput() {
    printf("Run : ./a.out <num-threads> <duration-seconds>\n");
}

long long diff(timespec start, timespec end) {
    timespec temp;
    long long ret;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    ret = (temp.tv_sec * 1000000000) + temp.tv_nsec;
    return ret;
}

int socketfd;
mutex read_lock,write_lock;

void *action(void *arg) {

    int n;
    struct sockaddr_in address;  //set port number for client
    int opt = 1;
    time_t start, end;
    double elapsed;
    int my_duration = duration;
    timespec time1, time2;
    long long lat, t;
    vector<long long> callat;
    struct thread_data *my_data;
    my_data = (struct thread_data *) arg;
    int i = my_data->id, j = 0;
    int my_port, td_val = i;
    char buf[50];
    start = time(NULL);
    t = 0;
    while (1) {
        memcpy(buf, "A: Hello B, How are you?", 50);
        if (j == 500) {
            j = 0;
        }
        end = time(NULL);
        elapsed = difftime(end, start);
        if (elapsed >= my_duration)
            break;
        t++;
        if (t == 50) {
            clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time1);
        }
        write_lock.lock();
        /* printf("write: %s\n", buf); */
        n = write(socketfd, buf, 50);
        write_lock.unlock();
        if (n <= 0) {
            cout << "Error : Wirite Error" << errno << '\n';
            close(socketfd);
            break;
        }
        read_lock.lock();
        n = read(socketfd, buf, 50);
        /* printf("read: %s\n", buf); */
        read_lock.unlock();
        if (n <= 0) {
            cout << "Error: Read Error" << errno << '\n';
            continue;
        }
        if (t == 50) {
            clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time2);
            lat = diff(time1, time2);
            callat.push_back(lat);
            t = 0;
        }
        req_count[i]++;
        continue;
    }
    cout << "T: Exiting" << i << '\n';
    lat = 0;
    for (t = 0; t < callat.size(); t++) {
        lat += callat[t];
    }
    lat = lat / t;
    cout << "req count " << req_count[i] << endl;
    cout << "Latency for " << i << " :" << lat << ';' << endl;
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    int i, n, numth, rc;
    signal(SIGINT, signalHandler);

    if (argc != 7) {
        printinput();
        exit(0);
    }

    numth = atoi(argv[1]);
    duration = atoi(argv[2]);
    clientIp = argv[3];
    clientPort = atoi(argv[4]);
    serverIp = argv[5];
    serverPort = atoi(argv[6]);

    pthread_t th[THC];
    struct thread_data td[THC];

    pthread_attr_t attr;
    void *status;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    p_vec.resize(numth);
    req_count.resize(numth);
    populate_ports1(numth, 1);

    int portno = serverPort;//5000;
    struct sockaddr_in rcvr_addr;
    rcvr_addr.sin_family = AF_INET;
    rcvr_addr.sin_addr.s_addr = inet_addr(serverIp.c_str());  //lb
    rcvr_addr.sin_port = htons(portno);
    socketfd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketfd < 0) {
        cout << "Error: Opening Socket" << '\n';
        exit(-1);
    }

    struct timeval tv1;
    tv1.tv_sec = 2; //timeout in seconds
    tv1.tv_usec = 0;
    setsockopt(socketfd, SOL_SOCKET, SO_RCVTIMEO, (const char *) &tv1, sizeof tv1);

    if (connect(socketfd, (struct sockaddr *) &rcvr_addr, sizeof(rcvr_addr)) < 0) {
        cout << "Error : connecting" << errno << '\n';
        close(socketfd);
        exit(-1);
    }

    for (i = 0; i < numth; i++) {
        td[i].id = i;
        rc = pthread_create(&th[i], &attr, action, (void *) &td[i]);
        if (rc) {
            cout << "Error thread" << endl;
        }
    }


    for (i = 0; i < numth; i++) {
        rc = pthread_join(th[i], &status);

        if (rc) {
            cout << "Error:unable to join," << rc << endl;
            exit(-1);
        }
    }

    pthread_exit(NULL);

    int cls = close(socketfd);

}
