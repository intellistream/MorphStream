#include<stdio.h>
#include<string.h>    //strlen
#include<stdlib.h>    //strlen
#include<sys/socket.h>
#include<arpa/inet.h> //inet_addr
#include<unistd.h>    //write
#include<pthread.h> //for threading , link with lpthread
#include <iostream>
#include <csignal>

// C socket server example, handles multiple clients using threads
using namespace std;

//the thread function
void *connection_handler(void *);

int new_sock_count = 0;

void signalHandler(int signum) {
    cout << new_sock_count << endl;
    // cleanup and close up stuff here
    // terminate program

    exit(signum);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        exit(0);
    }

    signal(SIGINT, signalHandler);
    int socket_desc, client_sock, c, *new_sock;
    struct sockaddr_in server, client;
    string my_ip = argv[1];
    //Create socket
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_desc == -1) {
        printf("Could not create socket");
    }

    //Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(my_ip.c_str());
    server.sin_port = htons(atoi(argv[2]));

    //Bind
    if (bind(socket_desc, (struct sockaddr *) &server, sizeof(server)) < 0) {
        //print the error message
        perror("bind failed. Error");
        return 1;
    }
    //  puts("bind done");

    //Listen
    listen(socket_desc, 10000);

    //Accept and incoming connection
    puts("Waiting for incoming connections...");
    c = sizeof(struct sockaddr_in);
    while ((client_sock = accept(socket_desc, (struct sockaddr *) &client, (socklen_t *) &c))) {
        pthread_t sniffer_thread;
        new_sock = (int *) malloc(1);
        *new_sock = client_sock;
        if (pthread_create(&sniffer_thread, NULL, connection_handler, (void *) new_sock) < 0) {
            cout << "could not create thread" << endl;
            perror("could not create thread");
            return 1;
        }

        //Now join the thread , so that we dont terminate before the thread
        pthread_join(sniffer_thread, NULL);

        if (client_sock < 0) {
            cout << "acept failed" << endl;
            perror("accept failed");
            return 1;
        }
    }
    if (client_sock < 0) {
        cout << "acept failed" << endl;
        perror("accept failed");
        return 1;
    }
    return 0;
}

/*
 * This will handle connection for each client
 * */
void *connection_handler(void *socket_desc) {
    //Get the socket descriptor
    int sock = *(int *) socket_desc;
    int read_size, write_size;
    int client_message_size = 1024;
    char *message, client_message[client_message_size];
    bzero(client_message, client_message_size);

    //Receive a message from client
    read_size = recv(sock, client_message, client_message_size, 0);
    if (read_size == 0) {
        cout << "read error" << endl;
        close(sock);
        free(socket_desc);
        return 0;
    }
    int a = 0;
    for (int i = 0; i < 200; i++) {
        a = a + i;
    }

    /* printf("%s\n", client_message); */
    write_size = write(sock, client_message, client_message_size);
    if (write_size == 0) {
        cout << "write error" << endl;
        close(sock);
        free(socket_desc);
        return 0;
    }

    //Free the socket pointer
    close(sock);
    free(socket_desc);

    return 0;
}
