#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <libvnf/datastore/dspackethandler.hpp>
#include <hiredis-vip/hiredis.h>
#include <hiredis-vip/async.h>
#include <hiredis-vip/adapters/libevent.h>

#define MAXCON 8192
#define MAXEVENTS 1024
#define DATASTORE_IP "169.254.9.18"
int pkt_sent = 0;

void SignalHandler(int signum) {
    fflush(stdout);
    printf("Sent: %d\n", pkt_sent);
}

struct getdata {
    int send_sockid;
    int sockid;
    int buf_key;
};

int write_stream(int conn_fd, uint8_t *buf, int len) {
    int ptr;
    int retval;
    int written_bytes;
    int remaining_bytes;

    ptr = 0;
    remaining_bytes = len;
    if (conn_fd < 0 || len <= 0) {
        return -1;
    }
    while (1) {
        written_bytes = write(conn_fd, buf + ptr, remaining_bytes);
        if (written_bytes <= 0) {
            retval = written_bytes;
            break;
        }
        ptr += written_bytes;
        remaining_bytes -= written_bytes;
        if (remaining_bytes == 0) {
            retval = len;
            break;
        }
    }
    return retval;
}

int make_socket_nb(int sfd) {
    int flags, s;

    flags = fcntl(sfd, F_GETFL, 0);
    if (flags == -1) {
        printf("Error: NBS fcntl\n");
        return -1;
    }

    flags |= O_NONBLOCK;
    s = fcntl(sfd, F_SETFL, flags);
    if (s == -1) {
        printf("Error: NBS fcntl flags\n");
        return -1;
    }

    return 0;
}

void getCallback1(redisClusterAsyncContext *c, void *r, void *privdata) {
    redisClusterAsyncCommand(c, (redisClusterCallbackFn *) NULL, NULL, "EXEC");
    redisReply *reply = (redisReply *) r;
    int j, ret;
    if (reply == NULL) return;
    getdata *val1 = (getdata *) privdata;
    string rep = reply->str;
    DSPacketHandler pkt1;
    pkt1.clear_pkt();
    pkt1.append_item(val1->sockid);
    pkt1.append_item(val1->buf_key);
    pkt1.append_item(rep);
    pkt1.prepend_len();
    ret = write_stream(val1->send_sockid, pkt1.data, pkt1.len);  //sep25  //uncomment oct25
    if (ret < 0) {
        cout << "Error: Hss data not sent after accept" << endl;
        exit(-1);
    }
    pkt_sent++;
    redisClusterAsyncDisconnect(c);  //sep20 //oct4
}

void setCallback1(redisClusterAsyncContext *c, void *r, void *privdata) {
    redisClusterAsyncCommand(c, (redisClusterCallbackFn *) NULL, NULL, "EXEC");
    redisClusterAsyncDisconnect(c);
}

void connectCallback(const redisClusterAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
}

void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
    printf("Disconnected...\n");
}

int read_stream(int conn_fd, uint8_t *buf, int len) {
    int ptr;
    int retval;
    int read_bytes;
    int remaining_bytes;

    ptr = 0;
    remaining_bytes = len;
    if (conn_fd < 0 || len <= 0) {
        return -1;
    }
    while (1) {
        read_bytes = read(conn_fd, buf + ptr, remaining_bytes);
        if (read_bytes <= 0) {
            retval = read_bytes;
            break;
        }
        ptr += read_bytes;
        remaining_bytes -= read_bytes;
        if (remaining_bytes == 0) {
            retval = len;
            break;
        }
    }
    return retval;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, SignalHandler);
    struct event_base *base = event_base_new();

    redisClusterAsyncContext *c = redisClusterAsyncConnect("127.0.0.1:8000", HIRCLUSTER_FLAG_NULL);
    if (c->err) {
        /* Let *c leak for now... */
        printf("Error: %s\n", c->errstr);
        return 1;
    }

    redisClusterLibeventAttach(c, base);
    redisClusterAsyncSetConnectCallback(c, (redisConnectCallback *) connectCallback);
    redisClusterAsyncCommand(c, (redisClusterCallbackFn *) setCallback1, NULL, "SET %s %s", "foo", "hello world");
    printf("callback to be called\n");
    redisClusterAsyncCommand(c, (redisClusterCallbackFn *) setCallback1, NULL, "SET %s %s", "hoo", "hello world1");
    event_base_dispatch(base);
    int lsfd, acfd, data, n, numev, i, ccfd, cafd, cret, trf, loop_count = 0, redis_ret;
    char buf[110], buf1[110];
    int buf_sockid, buf_key;
    string buf_cmd, buf_table, buf_value, buf_value1, b_key;
    char *buf2;
    redisReply *reply;
    long long transactions = 0;

    int count, tcount;

    struct sockaddr_in server, c_addr;
    struct hostent *c_ip;


    lsfd = socket(AF_INET, SOCK_STREAM, 0);

    if (lsfd < 0) {
        printf("ERROR : opening socket\n");
        exit(-1);
    }

    bzero((char *) &server, sizeof(server));
    int port_nos = atoi(argv[1]);
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(DATASTORE_IP);
    server.sin_port = htons(port_nos);

    if (bind(lsfd, (struct sockaddr *) &server, sizeof(server)) < 0) {
        printf("ERROR: BIND ERROR\n");
        exit(-1);
    }

    make_socket_nb(lsfd);

    listen(lsfd, MAXCON);

    int epfd = epoll_create(MAXEVENTS + 5);
    if (epfd == -1) {
        printf("Error: epoll create\n");
        exit(-1);
    }

    int retval;
    struct epoll_event ev, *rev;

    ev.data.fd = lsfd;
    ev.events = EPOLLIN | EPOLLET;

    retval = epoll_ctl(epfd, EPOLL_CTL_ADD, lsfd, &ev);
    if (retval == -1) {
        printf("Error: epoll ctl lsfd add\n");
        exit(-1);
    }
    rev = (struct epoll_event *) calloc(MAXEVENTS, sizeof(struct epoll_event));   //sep20
    printf("Entering Loop\n");
    count = 0;
    tcount = 0;

    trf = 0;
    transactions = 0;
    while (1) {
        numev = epoll_wait(epfd, rev, MAXEVENTS, -1);
        if (numev < 0) {
            printf("Error: EPOLL wait!\n");
            exit(-1);
        }
        for (i = 0; i < numev; i++) {
            trf = 1;
            if ((rev[i].events & EPOLLERR) ||
                (rev[i].events & EPOLLHUP)
                    ) {
                printf("ERROR: epoll monitoring failed, closing fd\n");
                if (rev[i].data.fd == lsfd) {
                    printf("lsfd error\n");
                    exit(-1);
                }
                close(rev[i].data.fd);
                continue;
            } else if (rev[i].data.fd == lsfd) {
                acfd = accept(lsfd, NULL, NULL);

                if (acfd < 0) {

                    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                    } else
                        printf("Error on accept\n");
                    break;
                }

                make_socket_nb(acfd);
                ev.data.fd = acfd;
                ev.events = EPOLLIN | EPOLLET;
                retval = epoll_ctl(epfd, EPOLL_CTL_ADD, acfd, &ev);
                if (retval == -1) {
                    printf("Error: epoll ctl lsfd add\n");
                    exit(-1);
                }

                printf("client accepted\n");

            } else if (rev[i].events & EPOLLIN) {

                while (1) {
                    DSPacketHandler pkt;
                    int pkt_len, retval;

                    pkt.clear_pkt();
                    retval = read_stream(rev[i].data.fd, pkt.data, sizeof(int));
                    if (retval < 0) {
                        if (errno == EAGAIN) {
                            break;
                        }
                    } else {
                        memmove(&pkt_len, pkt.data, sizeof(int) * sizeof(uint8_t));
                        pkt.clear_pkt();
                        retval = read_stream(rev[i].data.fd, pkt.data, pkt_len);
                        pkt.data_ptr = 0;
                        pkt.len = retval;
                        if (retval < 0) {
                            TRACE(cout << "Error: Packet from HSS Corrupt, break" << endl;)
                            break;
                        }
                    }

                    pkt.extract_item(buf_sockid);
                    pkt.extract_item(buf_cmd);
                    pkt.extract_item(buf_table);
                    pkt.extract_item(buf_key);
                    if (buf_cmd == "set") {
                        pkt.extract_item(buf_value);
                        redisClusterAsyncCommand(c, (redisClusterCallbackFn *) NULL, NULL, "MULTI");
                        redisClusterAsyncCommand(c, (redisClusterCallbackFn *) setCallback1, NULL, "SET %d %s", buf_key,
                                                 buf_value.c_str());
                        event_base_dispatch(base);
                    } else if (buf_cmd == "get") {
                        getdata *val1 = new getdata;
                        val1->sockid = buf_sockid;
                        val1->buf_key = buf_key;
                        val1->send_sockid = rev[i].data.fd;
                        redisClusterAsyncCommand(c, (redisClusterCallbackFn *) NULL, NULL, "MULTI");
                        redis_ret = redisClusterAsyncCommand(c, (redisClusterCallbackFn *) getCallback1, (void *) val1,
                                                             "GET %d", buf_key);
                        event_base_dispatch(base);
                    }
                }
            }
        }
    }
    return 0;
}
