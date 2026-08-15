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
#include <sys/socket.h>
#include <time.h>
#include "../shared/structs.h"
#include "../shared/metric_events.h"
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
#include <pthread.h>
#endif

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
#define NOP 241
#define GA 249

int port;
int delay;
int maxNoClients;

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
static pthread_mutex_t telnetMetricStateLock = PTHREAD_MUTEX_INITIALIZER;
static uint32_t telnetActiveConnections = 0;
static bool telnetMetricEmitterReady = false;
static volatile sig_atomic_t telnetStopRequested = 0;

static void requestTelnetStop(int signalNumber) {
    (void)signalNumber;
    telnetStopRequested = 1;
}
#endif

struct iacOption{
    unsigned char bytes[3];
    int length;
};

struct iacOption options[]={
    {{IAC,WILL,1},3},
    {{IAC,DO,3},3},
    {{IAC,DONT,5},3},
    {{IAC,WILL,31},3},
    {{IAC,DO,24},3},
    {{IAC,WONT,39},3},
    {{IAC,WILL,32},3},
    {{IAC,DO,34},3},
    {{IAC,WONT,35},3},
    {{IAC,NOP,0},2},
    {{IAC,GA,0},2},
};
int num_options = sizeof(options)/sizeof(options[0]);

// void heartbeatLog() {
//     syslog(LOG_INFO, "Server is running with %d connected clients. Number of most concurrent connected clients is %d", clientQueueTelnet.length, statsTelnet.mostConcurrentConnections);
//     syslog(LOG_INFO, "Current statistics: wasted time: %lld ms. Total connected clients: %ld", statsTelnet.totalWastedTime, statsTelnet.totalConnects);
// }

void initializeStats(){
    statsTelnet.totalConnects = 0;
    statsTelnet.totalWastedTime = 0;
    statsTelnet.mostConcurrentConnections = 0;
}

static long long telnetNowMs(void) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    return currentMonotonicTimeMs();
#else
    return currentTimeMs();
#endif
}

static bool telnetShouldContinue(void) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    return telnetStopRequested == 0;
#else
    return true;
#endif
}

static const char *telnetFinalizationName(
    enum metric_telnet_finalization_reason reason) {
    switch (reason) {
        case METRIC_TELNET_FINALIZATION_PEER_CLOSED:
            return "peer_closed";
        case METRIC_TELNET_FINALIZATION_READ_ERROR:
            return "read_error";
        case METRIC_TELNET_FINALIZATION_WRITE_ERROR:
            return "write_error";
        case METRIC_TELNET_FINALIZATION_SERVER_SHUTDOWN:
            return "server_shutdown";
        case METRIC_TELNET_FINALIZATION_BOUNDED_POLICY:
            return "bounded_policy";
    }
    return "bounded_policy";
}

#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
static const char *legacyTelnetFinalizationName(
    enum metric_telnet_finalization_reason reason) {
    if (reason == METRIC_TELNET_FINALIZATION_PEER_CLOSED) {
        return "client_disconnect";
    }
    return telnetFinalizationName(reason);
}
#endif

static void completeTelnetSession(
    struct telnetAndUpnpClient *client,
    long long now,
    enum metric_telnet_finalization_reason finalizationReason,
    enum metric_io_reason ioReason) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    pthread_mutex_lock(&telnetMetricStateLock);
#endif
    unsigned int depthLevel;
    if (!interactionDepthFinalize(&client->boundedInteractionDepth, &depthLevel)) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
        pthread_mutex_unlock(&telnetMetricStateLock);
#endif
        return;
    }
    long long durationMs = now - client->sessionStartMs;
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if (telnetActiveConnections > 0) {
        telnetActiveConnections -= 1;
    }
    if (telnetMetricEmitterReady) {
        (void)metric_event_telnet_connection_finalized(
            finalizationReason,
            durationMs >= 0 ? (uint64_t)durationMs : 0,
            (uint8_t)depthLevel,
            telnetActiveConnections,
            ioReason);
    }
    pthread_mutex_unlock(&telnetMetricStateLock);
    const char *reason = telnetFinalizationName(finalizationReason);
#else
    (void)ioReason;
    const char *reason = legacyTelnetFinalizationName(finalizationReason);
    bool firstResponseExit = client->firstResponseSent && depthLevel == 0;
    char msg[256];
    snprintf(msg, sizeof(msg), "%s disconnect %s %lld %lld %u %s %d\n",
        SERVER_ID,
        client->base.ipaddr,
        client->base.timeConnected,
        durationMs,
        depthLevel,
        reason,
        firstResponseExit ? 1 : 0);
    printf("%s", msg);
    sendMetric(msg);
#endif
    session_events_write_disconnect("telnet", client->sessionId, durationMs, reason, depthLevel);
}

#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
static void emitProtocolActionMetric(const char *action) {
    char msg[64];
    snprintf(msg, sizeof(msg), "%s protocol_action %s\n", SERVER_ID, action);
    sendMetric(msg);
}
#endif

static void emitTelnetWriteEvent(struct telnetAndUpnpClient *client, const char *result, int delayMs, ssize_t bytesSent) {
    char fields[256];
    snprintf(fields, sizeof(fields),
        "\"delay_ms\":%d,\"write_result\":\"%s\",\"bytes_sent\":%zd,\"interaction_depth\":%u",
        delayMs, result, bytesSent > 0 ? bytesSent : 0,
        interactionDepthLevel(&client->boundedInteractionDepth));
    session_events_write_action("telnet", client->sessionId, "write", fields);
}

static void emitTelnetInputEvent(struct telnetAndUpnpClient *client, ssize_t bytesReceived) {
    char fields[128];
    snprintf(fields, sizeof(fields),
        "\"bytes_received\":%zd,\"interaction_depth\":%u",
        bytesReceived > 0 ? bytesReceived : 0,
        interactionDepthLevel(&client->boundedInteractionDepth));
    session_events_write_action("telnet", client->sessionId, "input", fields);
}

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);

    // testing
    // char msg[256];
    // snprintf(msg, sizeof(msg), "%s connect %s\n",
    //     SERVER_ID, "82.211.213.247");
    // fprintf(stderr, "%s", msg);
    // sendMetric(msg);
    (void)argc;
    port = atoi(argv[1]);
    delay = atoi(argv[2]);
    maxNoClients = atoi(argv[3]);
    initializeStats();
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    telnetMetricEmitterReady = metric_event_emitter_init(
        getenv("EVENTHORIZON_METRIC_SOCKET"));
#endif
    session_events_init(NULL);
    setFdLimit(maxNoClients);
    signal(SIGPIPE, SIG_IGN); // Ignore
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    struct sigaction stopAction;
    memset(&stopAction, 0, sizeof(stopAction));
    stopAction.sa_handler = requestTelnetStop;
    sigemptyset(&stopAction.sa_mask);
    sigaction(SIGTERM, &stopAction, NULL);
    sigaction(SIGINT, &stopAction, NULL);
#endif
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
    while (telnetShouldContinue()) {
        long long now = telnetNowMs();
        int timeout = -1;

        // if (now - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
        //     heartbeatLog();
        //     lastHeartbeat = now;
        // }

        // Process clients in queue
        while (clientQueueTelnet.head && telnetShouldContinue()) {
            if(clientQueueTelnet.head->sendNext <= now){
                struct baseClient *bc = queue_pop(&clientQueueTelnet);
                struct telnetAndUpnpClient *c = (struct telnetAndUpnpClient *)bc;

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                unsigned char pendingByte;
                ssize_t peerState = recv(
                    c->fd, &pendingByte, sizeof(pendingByte),
                    MSG_PEEK | MSG_DONTWAIT);
                if (peerState == 0) {
                    completeTelnetSession(
                        c,
                        telnetNowMs(),
                        METRIC_TELNET_FINALIZATION_PEER_CLOSED,
                        METRIC_IO_NONE);
                    close(c->fd);
                    free(c);
                    continue;
                }
                if (peerState < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
                    errno != EINTR) {
                    int readError = errno;
                    completeTelnetSession(
                        c,
                        telnetNowMs(),
                        METRIC_TELNET_FINALIZATION_READ_ERROR,
                        metric_io_reason_from_unrecoverable_errno(
                            readError, false));
                    close(c->fd);
                    free(c);
                    continue;
                }
#endif

                int optionIndex = rand() % num_options;
                ssize_t out = write(c->fd, options[optionIndex].bytes, options[optionIndex].length);
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
                sendByteMetric(SERVER_ID, "sent", out);
#endif

                if (out == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) { // Avoid blocking
                        c->base.sendNext = now + delay;
                        c->base.timeConnected += delay;
                        statsTelnet.totalWastedTime += delay;
                        emitTelnetWriteEvent(c, errno == EINTR ? "interrupted" : "would_block", delay, 0);
                        queue_append(&clientQueueTelnet, (struct baseClient *)c);
                    } else {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                        int writeError = errno;
#else
                        sendReliabilityMetric(SERVER_ID, "write_error", metricReasonFromErrno(errno));
#endif
                        emitTelnetWriteEvent(c, "write_error", delay, 0);
                        completeTelnetSession(
                            c,
                            telnetNowMs(),
                            METRIC_TELNET_FINALIZATION_WRITE_ERROR,
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            metric_io_reason_from_unrecoverable_errno(writeError, false));
#else
                            METRIC_IO_OTHER);
#endif
                        close(c->fd);
                        free(c);
                    }
                } else {
                    c->base.sendNext = now + delay;
                    c->base.timeConnected += delay;
                    statsTelnet.totalWastedTime += delay;
                    if (out > 0) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                        long long writeObservedMs = telnetNowMs();
                        bool firstPositiveWrite = !c->firstResponseSent;
                        long long durationMs = firstPositiveWrite
                            ? writeObservedMs - c->sessionStartMs
                            : writeObservedMs - (long long)c->lastPositiveWriteMs;
                        c->firstResponseSent = true;
                        c->lastPositiveWriteMs = (uint64_t)writeObservedMs;
                        if (telnetMetricEmitterReady) {
                            if (firstPositiveWrite) {
                                (void)metric_event_telnet_first_positive_write(
                                    durationMs >= 0 ? (uint64_t)durationMs : 0,
                                    (uint64_t)out);
                            } else {
                                (void)metric_event_telnet_subsequent_positive_write(
                                    durationMs >= 0 ? (uint64_t)durationMs : 0,
                                    (uint64_t)out);
                            }
                        }
#else
                        emitProtocolActionMetric(c->firstResponseSent ? "write" : "banner");
                        c->firstResponseSent = true;
#endif
                    }
                    emitTelnetWriteEvent(c, "success", delay, out);
                    char buf[65];
                    ssize_t r=read(c->fd, buf, sizeof(buf)-1);
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
                    sendByteMetric(SERVER_ID, "received", r);
#endif
                    if(r<0){
                        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            int readError = errno;
#else
                            sendReliabilityMetric(SERVER_ID, "read_error", metricReasonFromErrno(errno));
#endif
                            completeTelnetSession(
                                c,
                                telnetNowMs(),
                                METRIC_TELNET_FINALIZATION_READ_ERROR,
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                                metric_io_reason_from_unrecoverable_errno(readError, false));
#else
                                METRIC_IO_OTHER);
#endif
                            close(c->fd);
                            free(c);
                            continue;
                        }
                    }else if(r==0){
                        completeTelnetSession(
                            c,
                            telnetNowMs(),
                            METRIC_TELNET_FINALIZATION_PEER_CLOSED,
                            METRIC_IO_NONE);
                        close(c->fd);
                        free(c);
                        continue;
                    }else{
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                        if (telnetMetricEmitterReady) {
                            (void)metric_event_telnet_positive_read((uint64_t)r);
                        }
#endif
                        interactionDepthObserveTelnet(&c->boundedInteractionDepth,
                            (const uint8_t *)buf, (size_t)r);
                        //terminate null
                        buf[r]='\0';
                        for(int i=0;i<r;i++){
                            if(buf[i]<32 || buf[i]>126) buf[i]='.';
                            if(buf[i]=='\t') buf[i]=' ';
                        }

                        //send metric
                        char msg[256];
                        snprintf(msg, sizeof(msg), "%s action %s %s\n",
                            SERVER_ID, c->base.ipaddr, buf);
                        printf("%s", msg);

#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
                        sendMetric(msg);
#endif
                        emitTelnetInputEvent(c, r);
                    }
                    queue_append(&clientQueueTelnet, (struct baseClient *)c);
                }
            }else{
                timeout = clientQueueTelnet.head->sendNext - now;
                break;

            }
        }

        int pollResult = poll(&fds, 1, timeout);
        now = telnetNowMs(); // Poll will cause old value to be misrepresenting
        if (pollResult < 0) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
            if (errno == EINTR && !telnetShouldContinue()) {
                break;
            }
#endif
            fprintf(stderr, "Poll error with error %s", strerror(errno));
            continue;
        }

        // Accept new connections
        if (fds.revents & POLLIN) {
            int clientFd = accept(serverSock, (struct sockaddr *)&clientAddr, &addrLen);
            if(clientFd == -1) {
                fprintf(stderr, "Failed accepting new client with error %s", strerror(errno));
                continue;
            }

            fcntl(clientFd, F_SETFL, O_NONBLOCK); // Set non-blocking mode
            struct telnetAndUpnpClient* newClient = malloc(sizeof(struct telnetAndUpnpClient));
            if (!newClient) {
                fprintf(stderr, "Out of memory");
                close(clientFd);
                continue;
            }

            statsTelnet.totalConnects += 1;
            newClient->fd = clientFd;
            newClient->base.sendNext = now + delay;
            newClient->base.timeConnected = 0;
            newClient->sessionStartMs = now;
            newClient->interactionDepth = 0;
            interactionDepthInit(&newClient->boundedInteractionDepth);
            newClient->firstResponseSent = false;
            newClient->lastPositiveWriteMs = 0;
            snprintf(newClient->base.ipaddr, INET_ADDRSTRLEN, "%s", inet_ntoa(clientAddr.sin_addr));
            session_events_make_id(newClient->sessionId, sizeof(newClient->sessionId), "telnet", newClient->sessionStartMs, newClient->fd);
            queue_append(&clientQueueTelnet, (struct baseClient*)newClient);

            if(statsTelnet.mostConcurrentConnections < clientQueueTelnet.length) {
                statsTelnet.mostConcurrentConnections = clientQueueTelnet.length;
            }

            char msg[256];
            snprintf(msg, sizeof(msg), "%s connect %s\n",
                SERVER_ID, newClient->base.ipaddr);
            printf("%s", msg);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
            pthread_mutex_lock(&telnetMetricStateLock);
            if (telnetActiveConnections < UINT32_MAX) {
                telnetActiveConnections += 1;
                if (telnetMetricEmitterReady) {
                    (void)metric_event_telnet_connection_accepted(
                        telnetActiveConnections);
                }
            }
            pthread_mutex_unlock(&telnetMetricStateLock);
#else
            sendMetric(msg);
#endif
            session_events_write_connect("telnet", newClient->sessionId);
        }
    }

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    long long shutdownTimeMs = telnetNowMs();
    while (clientQueueTelnet.head) {
        struct baseClient *baseClient = queue_pop(&clientQueueTelnet);
        struct telnetAndUpnpClient *client =
            (struct telnetAndUpnpClient *)baseClient;
        completeTelnetSession(
            client,
            shutdownTimeMs,
            METRIC_TELNET_FINALIZATION_SERVER_SHUTDOWN,
            METRIC_IO_NONE);
        close(client->fd);
        free(client);
    }
#endif
    close(serverSock);
    return 0;
}
