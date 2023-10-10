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

#define MAX_THREADS 1000
#define BUFFER_SIZE 1024

using namespace std;

string clientIp;
int clientPort;

string serverIp;
int serverPort;

int duration;

mutex p_vec_lock;

struct ThreadArgs {
    int id;
    int status;
};

void signalHandler(int signum) {
    exit(signum);
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

void *action(void *arg) {
    int socketfd, n, cls, j = 0;
    time_t start, end;
    double elapsed;
    timespec time1, time2;
    long long lat, t;
    vector<long long> callat;

    char buf[BUFFER_SIZE];
    const string message = "This is a long message from A to B. Sent with lots of love.";

    int my_portno = serverPort;//5000;
    int my_duration = duration;
    struct ThreadArgs *my_data = (struct ThreadArgs *) arg;
    struct sockaddr_in rcvr_addr;
    rcvr_addr.sin_family = AF_INET;
    rcvr_addr.sin_addr.s_addr = inet_addr(serverIp.c_str());  //lb
    rcvr_addr.sin_port = htons(my_portno);
    start = time(NULL);
    t = 0;
    while (true) {
        if (j == 500) {
            j = 0;
        }
        end = time(NULL);
        elapsed = difftime(end, start);
        if (elapsed >= my_duration) {
            break;
        }
        t++;
        if (t == 50) {
            clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time1);
        }

        socketfd = socket(AF_INET, SOCK_STREAM, 0);
        if (socketfd < 0) {
            spdlog::error("Could not open socket");
            exit(-1);
        }
        struct timeval tv1;
        tv1.tv_sec = 2; //timeout in seconds
        tv1.tv_usec = 0;
        setsockopt(socketfd, SOL_SOCKET, SO_RCVTIMEO, (const char *) &tv1, sizeof tv1);

        if (connect(socketfd, (struct sockaddr *) &rcvr_addr, sizeof(rcvr_addr)) < 0) {
            spdlog::error("Could not connect. Error code: {}", errno);
            close(socketfd);
            exit(-1);
        }

        memcpy(buf, message.c_str(), message.size());
        n = write(socketfd, buf, message.size());
        if (n <= 0) {
            spdlog::error("Write error. Error code: {}", errno);
            close(socketfd);
            continue;
        }
        if (n < message.size()) {
          spdlog::warn("Incomplete write: {}/{} bytes", n, message.size());
        }
        spdlog::debug("Wrote {}/{} bytes of \"{}\"", n, message.size(), message.c_str());

        bzero(buf, BUFFER_SIZE);
        n = read(socketfd, buf, BUFFER_SIZE);
        if (n <= 0) {
            spdlog::error("Read error. Error code: {}", errno);
            continue;
        }
        if (n == BUFFER_SIZE) {
          spdlog::warn("Read operation filled the buffer. There might be a bit more of message left, but it is being left. You might want to increase buffer size");
        }
        spdlog::debug("Read \"{}\" of {} bytes", buf, n);

        cls = close(socketfd);
        if (t == 50) {
            clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time2);
            lat = diff(time1, time2);
            callat.push_back(lat);
            t = 0;
        }
    }
    lat = 0;
    for (t = 0; t < callat.size(); t++) {
        lat += callat[t];
    }
    lat = lat / t;
    spdlog::info("Latency for {}: {}", my_data->id, lat);
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%^%L%$][%t][%H:%M:%S.%f] %v");

    signal(SIGINT, signalHandler);

    if (argc != 7) {
        spdlog::critical("Run : ./a.out <num-threads> <duration-seconds> <clientIp> <clientPort> <serverIp> <serverPort>");
        exit(0);
    }

    int numThreads = atoi(argv[1]);
    duration = atoi(argv[2]);
    clientIp = argv[3];
    clientPort = atoi(argv[4]);
    serverIp = argv[5];
    serverPort = atoi(argv[6]);

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
