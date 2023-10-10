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
#include <spdlog/spdlog.h>
#include <sys/epoll.h>

#define MAX_THREADS 1000
#define BUFFER_SIZE 1024
#define MAX_EVENTS 5

using namespace std;

string serverIp;
int serverPort;
int socketfd;

struct ThreadArgs {
    int id;
    int status;
};

void signalHandler(int signum) {
    exit(signum);
}

int makeSocketNonBlocking(int socketFd) {
    int flags = fcntl(socketFd, F_GETFL, 0);
    if (flags == -1) {
        spdlog::error("NBS fcntl");
        return -1;
    }

    flags |= O_NONBLOCK;
    int s = fcntl(socketFd, F_SETFL, flags);
    if (s == -1) {
        spdlog::error("NBS fcntl flags");
        return -1;
    }

    return 0;
}

void *action(void *arg) {

    struct epoll_event event, events[MAX_EVENTS];
    int epoll_fd = epoll_create1(0);
    event.events = EPOLLIN | EPOLLEXCLUSIVE;
    event.data.fd = socketfd;
    
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socketfd, &event))
    {
        fprintf(stderr, "Failed to add file descriptor to epoll\n");
        close(epoll_fd);
        return NULL;
    }

    struct ThreadArgs *my_data = (struct ThreadArgs *) arg;
    int n, len, cls, j = 0, event_count, i;

    char buf[BUFFER_SIZE];
    // string message = "Hello from server " + to_string(my_data->id);
    char reply[] = "Reply from C";

    struct sockaddr_in cliaddr;
    len = sizeof(cliaddr);
 
    while (true) {
        event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for(i = 0; i < event_count; i++)
        {
            if(events[i].events & EPOLLIN){
                n = recvfrom(events[i].data.fd, (char *)buf, BUFFER_SIZE,  
                        MSG_WAITALL, ( struct sockaddr *) &cliaddr, 
                        (socklen_t *)&len); 
                buf[n] = '\0';
                spdlog::debug("Server: {}, Client: {}, n: {}, port: {}\n", my_data->id, buf, n, cliaddr.sin_port);
                
                sendto(events[i].data.fd, reply, strlen(reply),  
                    MSG_CONFIRM, (const struct sockaddr *) &cliaddr, 
                        len);
            }
        }

        // spdlog::info("thread {}, cpu {}\n", my_data->id, sched_getcpu());
    }
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%^%L%$][%t][%H:%M:%S.%f] %v");

    signal(SIGINT, signalHandler);

    if (argc != 4) {
        spdlog::critical("Run : {} <num-threads> <serverIp> <serverPort>", argv[0]);
        exit(0);
    }

    int numThreads = atoi(argv[1]);
    serverIp = argv[2];
    serverPort = atoi(argv[3]);

    int my_portno = serverPort;//5000;
    struct sockaddr_in rcvr_addr;
    rcvr_addr.sin_family = AF_INET;
    rcvr_addr.sin_addr.s_addr = inet_addr(serverIp.c_str());  //lb
    rcvr_addr.sin_port = htons(my_portno);

    socketfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socketfd < 0) {
        spdlog::error("Could not open socket");
        exit(-1);
    }
    makeSocketNonBlocking(socketfd);
    if ( bind(socketfd, (const struct sockaddr *)&rcvr_addr,  
            sizeof(rcvr_addr)) < 0 ) 
    { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    } 

    pthread_t threads[MAX_THREADS];
    struct ThreadArgs threadsArgsArr[MAX_THREADS];

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    for (int i = 0; i < numThreads; i++) {
        threadsArgsArr[i].id = i;
        int rc = pthread_create(&threads[i], &attr, action, (void *) &threadsArgsArr[i]);
        if (rc) {
            spdlog::error("Error while creating thread. Error Code: {}", rc);
        }
    }

    for (int i = 0; i < numThreads; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc) {
            spdlog::error("Unable to join. Error Code: {}", rc);
            exit(-1);
        }
    }

    pthread_exit(NULL);
}
