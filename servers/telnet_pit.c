#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <poll.h>
#include <sys/time.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <stdbool.h>
#include <signal.h>
#include <time.h>
#include "../shared/structs.h"

// #define PORT 23
// #define DELAY_MS 100
// #define HEARTBEAT_INTERVAL_MS 600000 // 10 minutes
// #define FD_LIMIT 4096
#define SERVER_ID "Telnet"

#define IAC 255
#define DO 253
#define DONT 254
#define WILL 251
#define WONT 252

int port;
int delay;
int maxNoClients;

// Telnet negotiation options
unsigned char negotiations[][3] = {
    {IAC, WILL, 1}, 
    {IAC, DO, 3}, 
    {IAC, DONT, 5},
    {IAC, WILL, 31}, 
    {IAC, DO, 24}, 
    {IAC, WONT, 39}
};
int num_options = sizeof(negotiations) / sizeof(negotiations[0]);

// void heartbeatLog() {
//     syslog(LOG_INFO, "Server is running with %d connected clients. Number of most concurrent connected clients is %d", clientQueueTelnet.length, statsTelnet.mostConcurrentConnections);
//     syslog(LOG_INFO, "Current statistics: wasted time: %lld ms. Total connected clients: %ld", statsTelnet.totalWastedTime, statsTelnet.totalConnects);
// }

void initializeStats(){
    statsTelnet.totalConnects = 0;
    statsTelnet.totalWastedTime = 0;
    statsTelnet.mostConcurrentConnections = 0;
}

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);
    
    // testing
    // char msg[256];
    // snprintf(msg, sizeof(msg), "%s connect %s\n",
    //     SERVER_ID, "82.211.213.247");
    // fprintf(stderr, "%s", msg);
    // sendMetric(msg);
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <port> <delay> <max_clients>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    port = atoi(argv[1]);
    delay = atoi(argv[2]);
    maxNoClients = atoi(argv[3]);
    if (port <= 0 || port > 65535 || delay < 0 || maxNoClients <= 0) {
        fprintf(stderr, "Error: invalid parameter values. Port must be 1-65535, "
                "delay must be >= 0, max_clients must be > 0\n");
        exit(EXIT_FAILURE);
    }
    initializeStats();
    setFdLimit(maxNoClients);
    signal(SIGPIPE, SIG_IGN); // Ignore 
    queue_init(&clientQueueTelnet);
    
    int serverSock = createServer(port);
    if (serverSock < 0) {
        fprintf(stderr, "Invalid server socket fd: %d", serverSock);
        exit(EXIT_FAILURE);
    }
    
    struct sockaddr_in clientAddr;
    socklen_t addrLen = sizeof(clientAddr);
    
    struct pollfd fds;
    memset(&fds, 0, sizeof(fds));
    fds.fd = serverSock;
    fds.events = POLLIN;
    
    // long long lastHeartbeat = currentTimeMs();
    while (1) {
        long long now = currentTimeMs();
        int timeout = -1;

        // if (now - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
        //     heartbeatLog();
        //     lastHeartbeat = now;
        // }

        // Process clients in queue
        while (clientQueueTelnet.head) {
            if(clientQueueTelnet.head->sendNext <= now){
                struct baseClient *bc = queue_pop(&clientQueueTelnet);
                struct telnetAndUpnpClient *c = (struct telnetAndUpnpClient *)bc;
                
                int optionIndex = rand() % num_options;
                ssize_t out = write(c->fd, negotiations[optionIndex], sizeof(negotiations[optionIndex]));
                
                if (out == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) { // Avoid blocking
                        c->base.sendNext = now + delay;
                        c->base.timeConnected += delay;
                        statsTelnet.totalWastedTime += delay;
                        queue_append(&clientQueueTelnet, (struct baseClient *)c);
                    } else {
                        long long timeTrapped = c->base.timeConnected;
                        char msg[256];
                        snprintf(msg, sizeof(msg), "%s disconnect %s %lld\n",
                            SERVER_ID, c->base.ipaddr, timeTrapped);
                        printf("%s", msg);
                        sendMetric(msg);
                        close(c->fd);
                        free(c);
                    }
                } else {
                    c->base.sendNext = now + delay;
                    c->base.timeConnected += delay;
                    statsTelnet.totalWastedTime += delay;
                    queue_append(&clientQueueTelnet, (struct baseClient *)c);
                }
            } else {
                timeout = clientQueueTelnet.head->sendNext - now;
                break;
            }
        }
        
        int pollResult = poll(&fds, 1, timeout);
        now = currentTimeMs(); // Poll will cause old value to be misrepresenting
        if (pollResult < 0) {
            if (errno == EINTR) continue;
            fprintf(stderr, "Telnet poll error: %s\n", strerror(errno));
            continue;
        }

        // Accept new connections
        if (fds.revents & POLLIN) {
            int clientFd = accept(serverSock, (struct sockaddr *)&clientAddr, &addrLen);
            if(clientFd == -1) {
                fprintf(stderr, "Failed accepting new client with error %s", strerror(errno));
                continue;
            }

            if (fcntl(clientFd, F_SETFL, O_NONBLOCK) == -1) {
                fprintf(stderr, "Telnet fcntl(O_NONBLOCK) failed: %s\n", strerror(errno));
                close(clientFd);
                continue;
            }
            struct telnetAndUpnpClient* newClient = malloc(sizeof(struct telnetAndUpnpClient));
            if (!newClient) {
                fprintf(stderr, "Telnet: malloc failed for new client: %s\n", strerror(errno));
                close(clientFd);
                continue;
            }

            statsTelnet.totalConnects += 1;
            newClient->fd = clientFd;
            newClient->base.sendNext = now + delay;
            newClient->base.timeConnected = 0;
            snprintf(newClient->base.ipaddr, INET_ADDRSTRLEN, "%s", inet_ntoa(clientAddr.sin_addr));
            queue_append(&clientQueueTelnet, (struct baseClient*)newClient);

            if(statsTelnet.mostConcurrentConnections < clientQueueTelnet.length) {
                statsTelnet.mostConcurrentConnections = clientQueueTelnet.length;
            }

            char msg[256];
            snprintf(msg, sizeof(msg), "%s connect %s\n",
                SERVER_ID, newClient->base.ipaddr);
            printf("%s", msg);
            sendMetric(msg);
        }
    }

    close(serverSock);
    return 0;
}
