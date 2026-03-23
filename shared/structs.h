#ifndef STRUCTS_H
#define STRUCTS_H

#include <netinet/in.h>
#include <stdbool.h>
#include "uthash.h"

enum Request { CONNECT, PING, SUBSCRIBE, PUBREC, DISCONNECT, PUBLISH, UNSUBSCRIBE, PUBCOMP, UNSUPPORTED_REQUEST };
enum MqttVersion { V5, V311, V31 };
enum ClientType { TELNET_CLIENT, COAP_CLIENT };

struct baseClient {
    enum ClientType type;
    long long sendNext;
    struct baseClient *next;
    long long timeConnected;
    char ipaddr[INET_ADDRSTRLEN];
};

struct telnetAndUpnpClient {
    struct baseClient base;
    int fd;
};

struct coapClient {
    struct baseClient base;
    bool receivedAck;
    bool receivedRst;
    bool receivedGet;
    int retransmits;
    uint32_t blockNumber;
    uint16_t messageId;
    uint8_t token[8];
    uint8_t tkl;
    struct sockaddr_in clientAddr;
    socklen_t addrLen;
    UT_hash_handle hh;
};

struct mqttClient {
    int fd;
    char ipaddr[INET_ADDRSTRLEN];
    uint16_t port;
    uint8_t buffer[1024];
    uint16_t bytesWrittenToBuffer;
    uint16_t keepAlive;
    uint64_t lastActivityMs;
    uint64_t lastPubrelMs;
    long long timeOfConnection;
    enum MqttVersion version;
    UT_hash_handle hh;
};

struct queue {
    struct baseClient *head;
    struct baseClient *tail;
    int length;
};

struct priorityQueue {
    struct baseClient **heapArray; // array of pointers
    int size;
    int capacity;
};

extern struct queue clientQueueTelnet;
extern struct queue clientQueueUpnp;
extern struct priorityQueue clientQueueCoap;

struct telnetStatistics {
    unsigned long totalConnects;
    unsigned long long totalWastedTime;
    int mostConcurrentConnections;
};

struct upnpStatistics {
    unsigned long totalHttpRequests;
    unsigned long totalXmlRequests;
    unsigned long long totalWastedTime;
    unsigned long otherHttpRequests;
    unsigned long ssdpResponses;
    int mostConcurrentConnections;
};

struct mqttStatistics {
    unsigned long totalConnects;
    unsigned long long totalWastedTime;
    uint16_t mostConcurrentConnections;
};

extern struct telnetStatistics statsTelnet;
extern struct upnpStatistics statsUpnp;
extern struct mqttStatistics statsMqtt;

/**
 * @brief Initializes a queue.
 * @param q Pointer to the queue to initialize.
 */
void queue_init(struct queue *q);

/**
 * @brief Appends a client to the queue.
 * @param q Pointer to the queue.
 * @param c Pointer to the client to append.
 */
void queue_append(struct queue *q, struct baseClient *c);

/**
 * @brief Removes and returns the first client from the queue.
 * @param q Pointer to the queue.
 * @return Pointer to the removed client or NULL if the queue is empty.
 */
struct baseClient *queue_pop(struct queue *q);

void heap_init(struct priorityQueue *pq, int capacity);

void heap_insert(struct priorityQueue *pq, struct baseClient *c);

struct baseClient *heap_pop(struct priorityQueue *pq);

/**
 * @brief Creates a standard TCP server with very large backlog
 * @param port What port the server should be assigned
 * @return File descriptor for the server
 */
int createServer(int port);

/**
 * @return Returns the current time in milliseconds
 */
long long currentTimeMs();

/**
 * @return Sets the maximum number of fd's
 */
void setFdLimit(int limit);

/**
 * @brief Sends a metric to a Unix domain socket
 */
void sendMetric(const char* message);

#endif
