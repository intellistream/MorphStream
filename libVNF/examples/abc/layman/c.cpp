#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <iostream>
#include <csignal>
#include <spdlog/spdlog.h>

#define BUFFER_SIZE 1024

using namespace std;

void *connectionHandler(void *);

void signalHandler(int signum) {
    exit(signum);
}

int main(int argc, char *argv[]) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%^%L%$][%t][%H:%M:%S.%f] %v");

    if (argc != 3) {
        spdlog::critical("Run : ./c.out <ip> <port>");
        exit(0);
    }

    signal(SIGINT, signalHandler);

    int client_sock, *new_sock;
    struct sockaddr_in server, client;
    string my_ip = argv[1];
    int my_port = atoi(argv[2]);

    int socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_desc == -1) {
        spdlog::error("Could not create socket");
        exit(-1);
    }
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(my_ip.c_str());
    server.sin_port = htons(my_port);
    if (bind(socket_desc, (struct sockaddr *) &server, sizeof(server)) < 0) {
        spdlog::error("Bind failed");
        exit(-1);
    }
    if (listen(socket_desc, 10000)) {
        spdlog::error("Listen failed");
        exit(-1);
    }

    spdlog::info("Waiting for incoming connections...");
    int c = sizeof(struct sockaddr_in);
    while ((client_sock = accept(socket_desc, (struct sockaddr *) &client, (socklen_t *) &c))) {
        if (client_sock < 0) {
            spdlog::error("Accept failed");
            exit(-1);
        }

        pthread_t sniffer_thread;
        new_sock = (int *) malloc(sizeof(int));
        *new_sock = client_sock;
        if (pthread_create(&sniffer_thread, NULL, connectionHandler, (void *) new_sock) < 0) {
            spdlog::error("Could not create thread");
            exit(-1);
        }

        pthread_join(sniffer_thread, NULL);
        free(new_sock);
    }
    if (client_sock < 0) {
        spdlog::error("Accept failed");
        exit(-1);
    }
    return 0;
}

void *connectionHandler(void *socket_desc) {
    int socketFd = *(int *) socket_desc;
    char clientMessage[BUFFER_SIZE];
    bzero(clientMessage, BUFFER_SIZE);

    int readSize = read(socketFd, clientMessage, BUFFER_SIZE);
    if (readSize == 0) {
        cout << "read error" << endl;
        close(socketFd);
        return 0;
    }
    if (readSize == BUFFER_SIZE) {
      spdlog::warn("Read operation filled the buffer. There might be a bit more of message left, but it is being left. You might want to increase buffer size");
    }
    spdlog::debug("Read \"{}\" of {} bytes", clientMessage, readSize);

    int a = 0;
    for (int i = 0; i < 200; i++) {
        a = a + i;
    }

    int writeSize = write(socketFd, clientMessage, readSize);
    if (writeSize == 0) {
        cout << "write error" << endl;
        close(socketFd);
        return 0;
    }
    if (writeSize < readSize) {
      spdlog::warn("Incomplete write: {}/{} bytes", writeSize, readSize);
    }
    spdlog::debug("Wrote {}/{} bytes of \"{}\"", writeSize, readSize, clientMessage);

    close(socketFd);

    return 0;
}
