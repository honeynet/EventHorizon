#define _POSIX_C_SOURCE 199309L

#include <stdlib.h>
#include <syslog.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <time.h>
#include <sys/resource.h>
#include <sys/un.h>
#include <stdio.h>
#include <string.h>
#include "structs.h"

struct queue clientQueueTelnet;
struct queue clientQueueUpnp;
struct priorityQueue clientQueueCoap;
struct telnetStatistics statsTelnet;
struct upnpStatistics statsUpnp;
struct mqttStatistics statsMqtt;

void queue_init(struct queue *q) {
    q->head = q->tail = NULL;
    q->length = 0;
}

void queue_append(struct queue *q, struct baseClient *c) {
    c->next = NULL; // Removes old next value
    if (q->tail != NULL) {
        q->tail->next = c; // Update next pointer correctly
    } else {
        q->head = c;
    }
    q->tail = c;
    q->length++;
}

struct baseClient *queue_pop(struct queue *q) {
    if (q->head == NULL) return NULL;
    struct baseClient *c = q->head;
    q->head = c->next;
    if (!q->head) {
        q->tail = NULL; // Remove tail if only one element in queue
    }
    q->length--;
    return c;
}

void heap_init(struct priorityQueue *pq, int capacity) {
    pq->heapArray = malloc(sizeof(struct baseClient *) * capacity);
    if (!pq->heapArray) {
        fprintf(stderr, "malloc for priority queue failed\n");
        exit(EXIT_FAILURE);
    }
    pq->capacity = capacity;
    pq->size = 0;
}

void swap(struct baseClient **a, struct baseClient **b) {
    struct baseClient *temp = *a;
    *a = *b;
    *b = temp;
}

void heap_insert(struct priorityQueue *pq, struct baseClient *c) {
    if (pq->size >= pq->capacity) {
        fprintf(stderr, "Priority queue has hit capacity. Can't add any more clients\n");
        return;
    }

    int i = pq->size;
    pq->heapArray[i] = c;

    // Bubble up
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (pq->heapArray[parent]->sendNext <= pq->heapArray[i]->sendNext) {
            break;
        }
        swap(&pq->heapArray[i], &pq->heapArray[parent]);
        i = parent;
    }

    pq->size += 1;
}

struct baseClient *heap_pop(struct priorityQueue *pq) {
    if (pq->size == 0) return NULL;
    struct baseClient *root = pq->heapArray[0];
    pq->heapArray[0] = pq->heapArray[pq->size-1];
    pq->size -= 1;

    // Heapify down
    int i = 0;
    while (1) {
        int left = 2 * i + 1;
        int right = 2 * i + 2;
        int smallest = i;

        if (left < pq->size && pq->heapArray[left]->sendNext < pq->heapArray[smallest]->sendNext)
            smallest = left;
        if (right < pq->size && pq->heapArray[right]->sendNext < pq->heapArray[smallest]->sendNext)
            smallest = right;

        if (smallest == i) break;

        swap(&pq->heapArray[i], &pq->heapArray[smallest]);
        i = smallest;
    }

    return root;
}

int createServer(int port) {
    int r; 
    int sockfd;
    int value;

    // IPv4 TCP socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        syslog(LOG_ERR,"Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Enable SO_REUSEADDR for faster restarts
    value = 1;
    r = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value));
    if (r == -1) {
        syslog(LOG_ERR,"setsockopt failed");
    }

    // Set TCP receive window
    int winSize = 256; // Doubled
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &winSize, sizeof(winSize));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &winSize, sizeof(winSize));

    // Bind to IPv4 address and port
    struct sockaddr_in addr4 = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {INADDR_ANY}
    };
    r = bind(sockfd, (struct sockaddr *)&addr4, sizeof(addr4));
    if (r == -1) {
        syslog(LOG_ERR,"Bind failed");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // Listen with a very large backlog
    r = listen(sockfd, INT_MAX);
    if (r == -1) {
        syslog(LOG_ERR,"Listen failed");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    syslog(LOG_INFO,"Server listening on port %d...\n", port);
    return sockfd;
}

long long currentTimeMs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

long long currentMonotonicTimeMs(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

void setFdLimit(int limit) {
    struct rlimit rl;
    rl.rlim_cur = limit;
    rl.rlim_max = limit;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        fprintf(stderr, "setrlimit failed"); 
    }
}

void sendMetric(const char* message) {
    int sock = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (sock < 0) return;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", "/tmp/tarpit_exporter.sock");

    ssize_t sent = sendto(sock, message, strlen(message), 0, (struct sockaddr*)&addr, sizeof(addr));
    if (sent < 0) {
        perror("sendto failed");
    }
    close(sock);
}

const char *metricReasonFromErrno(int errorNumber) {
    switch (errorNumber) {
        case ETIMEDOUT:
            return "timeout";
        case ECONNRESET:
            return "reset";
        case EPIPE:
        case ENOTCONN:
        case ECONNABORTED:
        case EBADF:
            return "closed";
        default:
            return "unknown";
    }
}

void sendReliabilityMetric(const char *server, const char *event, const char *reason) {
    if (!server || !event) {
        return;
    }

    char msg[128];
    snprintf(msg, sizeof(msg), "%s %s %s\n",
        server,
        event,
        reason && reason[0] ? reason : "unknown");
    sendMetric(msg);
}

bool formatByteMetric(char *message, size_t messageSize, const char *server,
                      const char *direction, ssize_t ioResult) {
    if (!message || messageSize == 0 || !server || !direction || ioResult <= 0) {
        return false;
    }
    if (strcmp(server, "Telnet") != 0 && strcmp(server, "MQTT") != 0) {
        return false;
    }
    const char *command = NULL;
    if (strcmp(direction, "sent") == 0) {
        command = "bytes_sent";
    } else if (strcmp(direction, "received") == 0) {
        command = "bytes_received";
    } else {
        return false;
    }

    int written = snprintf(message, messageSize, "%s %s %zd\n", server, command, ioResult);
    return written > 0 && (size_t)written < messageSize;
}

void sendByteMetric(const char *server, const char *direction, ssize_t ioResult) {
    char msg[128];
    if (!formatByteMetric(msg, sizeof(msg), server, direction, ioResult)) {
        return;
    }
    sendMetric(msg);
}
