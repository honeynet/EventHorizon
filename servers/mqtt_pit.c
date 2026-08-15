// Idea: Never finish the 4-way handshake that is required for QoS 2. Specifications on this is undefined, since a server is expected to always complete the handshake
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <stdbool.h>
#include <signal.h>
#include <time.h>
#include <sys/socket.h>
#include "../shared/structs.h"
#include "../shared/metric_events.h"
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
#include <pthread.h>
#endif

// #define PORT 1883
// #define MAX_EVENTS 4096
// #define EPOLL_TIMEOUT_INTERVAL_MS 5000
// #define PUBREL_INTERVAL_MS 10000
// #define HEARTBEAT_INTERVAL_MS 600000 // 10 minutes
// #define MAX_PACKETS_PER_CLIENTS 50
// #define FD_LIMIT 4096
#define SERVER_ID "MQTT"

int port;
int maxEvents;
int epollTimeoutInterval;
uint32_t pubrelInterval;
uint32_t maxPacketsPerClient;
int maxNoClients;

struct mqttClient* clients = NULL;

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
static pthread_mutex_t mqttMetricStateLock = PTHREAD_MUTEX_INITIALIZER;
static uint32_t mqttActiveConnections = 0;
static bool mqttMetricEmitterReady = false;
static volatile sig_atomic_t mqttStopRequested = 0;

static void requestMqttStop(int signalNumber) {
    (void)signalNumber;
    mqttStopRequested = 1;
}
#endif

enum mqttSendResult {
    MQTT_SEND_COMPLETE,
    MQTT_SEND_PENDING,
    MQTT_SEND_FATAL,
};

struct mqttParsedPublishData {
    uint16_t topicLength;
    char topic[256];
    const uint8_t *payload;
    uint32_t payloadLength;
};

static long long mqttNowMs(void) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    return currentMonotonicTimeMs();
#else
    return currentTimeMs();
#endif
}

static const char *mqttFinalizationName(
    enum metric_mqtt_finalization_reason reason) {
    switch (reason) {
        case METRIC_MQTT_FINALIZATION_PEER_CLOSED:
            return "peer_closed";
        case METRIC_MQTT_FINALIZATION_DISCONNECT_RECEIVED:
            return "disconnect_received";
        case METRIC_MQTT_FINALIZATION_CONNECT_REFUSED:
            return "connect_refused";
        case METRIC_MQTT_FINALIZATION_OPERATION_REFUSED:
            return "operation_refused";
        case METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR:
            return "protocol_error";
        case METRIC_MQTT_FINALIZATION_KEEP_ALIVE_TIMEOUT:
            return "keep_alive_timeout";
        case METRIC_MQTT_FINALIZATION_READ_ERROR:
            return "read_error";
        case METRIC_MQTT_FINALIZATION_WRITE_ERROR:
            return "write_error";
        case METRIC_MQTT_FINALIZATION_SERVER_SHUTDOWN:
            return "server_shutdown";
        case METRIC_MQTT_FINALIZATION_BOUNDED_POLICY:
            return "bounded_policy";
    }
    return "protocol_error";
}

#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
static const char *legacyMqttFinalizationName(
    enum metric_mqtt_finalization_reason reason) {
    switch (reason) {
        case METRIC_MQTT_FINALIZATION_PEER_CLOSED:
        case METRIC_MQTT_FINALIZATION_DISCONNECT_RECEIVED:
            return "client_disconnect";
        case METRIC_MQTT_FINALIZATION_KEEP_ALIVE_TIMEOUT:
            return "inactivity";
        case METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR:
            return "read_error";
        default:
            return mqttFinalizationName(reason);
    }
}
#endif

struct mqttClient* lookupClient(int fd) {
    struct mqttClient* client;

    HASH_FIND_INT(clients, &fd, client);
    return client;
}

void addClient(struct mqttClient* client) {
    HASH_ADD_INT(clients, fd, client);
}

void deleteClient(struct mqttClient* client) {
    HASH_DEL(clients, client);
}

// void heartbeatLog() {
//     syslog(LOG_INFO, "Server is running with %d connected clients. Number of most concurrent connected clients is %d", HASH_COUNT(clients), statsMqtt.mostConcurrentConnections);
//     syslog(LOG_INFO, "The total amount of wasted time is %lld. Total connected clients: %ld", statsMqtt.totalWastedTime, statsMqtt.totalConnects);
// }

void initializeStats(){
    statsMqtt.totalConnects = 0;
    statsMqtt.totalWastedTime = 0;
    statsMqtt.mostConcurrentConnections = 0;
}

static const char *mqttVersionString(enum MqttVersion version) {
    switch (version) {
        case V5:
            return "v5";
        case V311:
            return "v3.1.1";
        case V31:
            return "v3.1";
        default:
            return "unknown";
    }
}

static const char *boolString(bool value) {
    return value ? "true" : "false";
}

static void emitMqttAction(struct mqttClient *client, const char *action, const char *fields) {
    session_events_write_action("mqtt", client->sessionId, action, fields);
}

bool decodeVarint(const uint8_t* buffer, uint32_t packetEnd, uint32_t* offset, uint32_t* value) {
    uint32_t result = 0;
    int multiplier = 1;
    uint8_t byte;
    int bytesRead = 0;

    do {
        if (*offset >= packetEnd) {
            fprintf(stderr, "Incomplete variable byte integer");
            return false;
        }
        byte = buffer[(*offset)++];
        result += (byte & 0b01111111) * multiplier;
        multiplier *= 128;
        bytesRead++;

        if (bytesRead > 4) {
            fprintf(stderr, "Variable byte integer exceeds maximum length");
            return false;
        }
    } while ((byte & 0b10000000) != 0);

    *value = result;
    return true;
}

uint8_t readConnreq(uint8_t* buffer, uint32_t packetEnd, uint32_t offset, struct mqttClient* client){
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "CONNECT request too small for fixed header");
        return 0x80; // Unspecified error
    } 

    uint16_t protocolName = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if ((protocolName != 4 && protocolName != 6) || offset + protocolName > packetEnd) {
        fprintf(stderr, "Malformed CONNECT protocol name length");
        return 0x01;
    }
    bool isV31 = protocolName == 6 && memcmp(&buffer[offset], "MQIsdp", 6) == 0;
    if ((protocolName != 4 || memcmp(&buffer[offset], "MQTT", 4) != 0) && !isV31){
        char wrong[7] = {0};
        memcpy(wrong, &buffer[offset], protocolName < 7 ? protocolName : 4);
        fprintf(stderr, "Malformed CONNECT request. Expected \"MQTT\" or \"MQIsdp\" but got \"%s\"", wrong);
        return 0x01; // Unacceptable protocol version
    } 
    if(isV31) {
        offset += 6;
    } else {
        offset += 4;
    }

    // Protocol Version
    if (offset >= packetEnd) {
        fprintf(stderr, "No protocol version given for CONNECT request");
        return 0x80;
    }
    uint8_t proto_level = buffer[offset++];
    char msg[256];
    if(proto_level == 0b101) {
        snprintf(msg, sizeof(msg), "%s CONNECT %s\n",
            SERVER_ID, "v5");
        client->version = V5;
     } else if (proto_level == 0b100) {
        snprintf(msg, sizeof(msg), "%s CONNECT %s\n",
        SERVER_ID, "v3.1.1");
        client->version = V311;
    } else if (proto_level == 0b011) {
        snprintf(msg, sizeof(msg), "%s CONNECT %s\n",
        SERVER_ID, "v3.1");
        client->version = V31;
    } else {
        fprintf(stderr, "Unsupported MQTT version: %d", proto_level);
        return 0x01; // Unacceptable protocol version
    }
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if ((isV31 && proto_level != 0x03) ||
        (!isV31 && proto_level == 0x03)) {
        return 0x01;
    }
#endif
    printf("%s", msg);
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
    sendMetric(msg);
#endif

    // Connect Flags
    if (offset >= packetEnd) {
        fprintf(stderr, "No connect flags supplied");
        return 0x80;
    }
    uint8_t connect_flags = buffer[offset++];
    if ((connect_flags & 0x01) != 0) {
        fprintf(stderr, "CONNECT reserved flag bit is set");
        return 0x80;
    }
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    bool cleanSession = (connect_flags & 0x02) != 0;
    bool willFlag = (connect_flags & 0x04) != 0;
    uint8_t willQos = (connect_flags >> 3) & 0x03;
    bool willRetain = (connect_flags & 0x20) != 0;
    bool usernameFlag = (connect_flags & 0x80) != 0;
    bool passwordFlag = (connect_flags & 0x40) != 0;
    if (willQos == 3 || (!willFlag && (willQos != 0 || willRetain)) ||
        (passwordFlag && !usernameFlag)) {
        return 0x80;
    }
    bool unsupportedProfile = !cleanSession || willFlag;
#endif
    // printf("Connect Flags: 0x%02X\n", connect_flags);

    // Keep Alive
    if (offset + 2 > packetEnd){
        fprintf(stderr, "No keep-alive value supplied");
        return 0x80;
    } 
    int keepAlive = (buffer[offset] << 8) | buffer[offset + 1];
    if(keepAlive < 0) {
        fprintf(stderr, "Negative keep-alive value received: %d", keepAlive);
        return 0x80;
    }
    client->keepAlive = keepAlive;
    offset += 2;

    if(client->version == V5) {
        // Properties Length (varint)
        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            fprintf(stderr, "Unable to decode varint");
            return 0x80;
        }

        uint32_t props_end = offset + varint;
        if (props_end > packetEnd) {
            return 0x80;
        }
        while (offset < props_end && offset < packetEnd) {
            offset++; // Don't parse props, just skip
        }
    }

    // Payload: Client ID
    if (offset + 2 > packetEnd) return 0x80;
    uint16_t clientIdLength = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if (offset + clientIdLength > packetEnd) {
        fprintf(stderr, "clientId too long for packet");
        return 0x02;
    }
    offset += clientIdLength;

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if (willFlag) {
        if (client->version == V5) {
            uint32_t willPropertiesLength;
            if (!decodeVarint(
                    buffer, packetEnd, &offset,
                    &willPropertiesLength) ||
                offset + willPropertiesLength > packetEnd) {
                return 0x80;
            }
            offset += willPropertiesLength;
        }
        if (offset + 2 > packetEnd) {
            return 0x80;
        }
        uint16_t willTopicLength =
            (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;
        if (willTopicLength == 0 || offset + willTopicLength > packetEnd) {
            return 0x80;
        }
        offset += willTopicLength;

        if (offset + 2 > packetEnd) {
            return 0x80;
        }
        uint16_t willPayloadLength =
            (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;
        if (offset + willPayloadLength > packetEnd) {
            return 0x80;
        }
        offset += willPayloadLength;
    }
#endif

    // Username
    char username[256] = {0};
    bool usernamePresent = false;
    if (connect_flags & 0b10000000) {
        usernamePresent = true;
        if (offset + 2 > packetEnd) {
            fprintf(stderr, "Username flag supplied, but with no username");
            return 0x80;
        } 
        uint16_t user_len = (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;

        if (offset + user_len > packetEnd) {
            fprintf(stderr, "Username too long");
            return 0x80;
        }

        uint16_t safeLength = user_len < 255 ? user_len : 255;
        memcpy(username, &buffer[offset], safeLength);
        offset += user_len;
        // syslog(LOG_INFO, "Username: %s\n", username);
    }

    // Password
    char password[256] = {0};
    bool passwordPresent = false;
    if (connect_flags & 0b1000000) {
        passwordPresent = true;
        if (offset + 2 > packetEnd) {
            fprintf(stderr, "Password flag supplied, but with no password");
            return 0x80;
        } 
        uint16_t passwordLength = (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;
        if (offset + passwordLength > packetEnd){
            fprintf(stderr, "Password too long");
            return 0x80;
        }

        
        uint16_t safeLength = passwordLength < 255 ? passwordLength : 255;
        memcpy(password, &buffer[offset], safeLength);
        offset += passwordLength;
        // syslog(LOG_INFO, "Password: %s\n", password);
    }

    // syslog(LOG_INFO, "Successfully read CONNECT request with keep-alive: %d username: %s password: %s", keepAlive, username, password);
    // char msg[256];
    snprintf(msg, sizeof(msg), "%s credentials %.100s %.100s\n",
        SERVER_ID, username, password);
    printf("%s", msg);
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
    sendMetric(msg);

    interactionDepthObserveMqttConnect(&client->interactionDepth);
#endif
    char fields[256];
    snprintf(fields, sizeof(fields),
        "\"mqtt_version\":\"%s\",\"client_id_present\":%s,\"username_present\":%s,\"password_present\":%s,\"keep_alive_seconds\":%d,\"interaction_depth\":%u",
        mqttVersionString(client->version),
        boolString(clientIdLength > 0),
        boolString(usernamePresent),
        boolString(passwordPresent),
        keepAlive,
        interactionDepthLevel(&client->interactionDepth));
    emitMqttAction(client, "CONNECT", fields);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if (offset != packetEnd) {
        return 0x80;
    }
    if (client->version != V311) {
        client->connectRefused = true;
        return client->version == V5 ? 0x84 : 0x01;
    }
    if (unsupportedProfile) {
        client->connectRefused = true;
        return 0x03;
    }
#endif
    return 0x00; // Success
}

bool readSubscribe(uint8_t* buffer, uint32_t packetEnd, uint32_t offset, struct mqttClient* client) {
    // syslog(LOG_INFO, "Reading SUBSCRIBE request");
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "SUBSCRIBE request too short for fixed header");
        return false;
    }

    // *packetId = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2; // packetId
    if (client->version == V5) {
        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            fprintf(stderr, "SUBSCRIBE Failed decoding varint");
            return false;
        }

        // parse actual properties here if needed
        if (offset + varint > packetEnd) {
            return false;
        }
        offset += varint;
    }

    if (offset + 3 > packetEnd) { // 2 bytes topic + 1 byte options
        fprintf(stderr, "SUBSCRIBE topic section too short");
        return false;
    }

    uint16_t topicLength = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if (offset + topicLength + 1 > packetEnd) {
        fprintf(stderr, "SUBSCRIBE topic filter length exceeds packet size");
        return false;
    }

    char topic[256];
    uint16_t safeLength = topicLength < 255 ? topicLength : 255;
    memcpy(topic, &buffer[offset], safeLength);
    topic[safeLength] = '\0';
    offset += topicLength;

    uint8_t options = buffer[offset++];
    uint8_t qos = options & 0b11;
    if (topicLength == 0 || qos > 2) {
        return false;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "%s SUBSCRIBE %.100s %d\n",
        SERVER_ID, topic, qos);
    printf("%s", msg);
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
    sendMetric(msg);

    char fields[160];
    interactionDepthObserveMqttOperation(&client->interactionDepth);
    snprintf(fields, sizeof(fields),
        "\"qos\":%d,\"topic_present\":%s,\"interaction_depth\":%u",
        qos, boolString(topicLength > 0), interactionDepthLevel(&client->interactionDepth));
    emitMqttAction(client, "SUBSCRIBE", fields);
#endif

    // syslog(LOG_INFO, "Successfully read SUBSCRIBE request with topic: %s and QoS %d", topic, qos);
    return true;
}

void generateFakeMatchingTopic(char* sub, size_t length) {
    const char* fakeFolder = "confidential";
    const char* fakeLeaf = "data";
    char buffer[256] = {0};
    size_t bufOffset = 0;

    const char* p = sub;
    while (*p && bufOffset < sizeof(buffer) - 1) {
        if (*p == '+') {
            bufOffset += snprintf(buffer + bufOffset, sizeof(buffer) - bufOffset, "%s", fakeFolder);
            p++;  // skip '+'
        } else if (*p == '#') {
            // '#' must be last, fill with a multi-level tail
            bufOffset += snprintf(buffer + bufOffset, sizeof(buffer) - bufOffset, "%s/%s", fakeFolder, fakeLeaf);
            break; // '#' ends the pattern
        } else {
            buffer[bufOffset++] = *p++;
        }
    }

    buffer[bufOffset] = '\0';
    strncpy(sub, buffer, length);
}

enum mqttSendResult sendConnack(
    struct mqttClient* client,
    uint8_t reasonCode,
    enum metric_io_reason *failureReason) {
    *failureReason = METRIC_IO_NONE;
    int size = client->version == V5
        ? (reasonCode == 0x00 ? 8 : 5)
        : 4;
    uint8_t arr[8] = {0};

    if (client->version == V5) {
        arr[0] = 0x20;       // CONNACK fixed header
        arr[1] = reasonCode == 0x00 ? 0x06 : 0x03;
        arr[2] = 0x00;       // Connect Acknowledge Flags (Session Present = 0)
        arr[3] = reasonCode; // Reason Code
        if (reasonCode == 0x00) {
            arr[4] = 0x03;       // Properties Length
            arr[5] = 0x21;       // Property ID: Receive Maximum
            arr[6] = 0x00;       // MSB
            arr[7] = 0x01;       // LSB (Receive Maximum = 1)
        } else {
            arr[4] = 0x00;       // No properties in refusal response
        }
    } else {
        arr[0] = 0x20;       // CONNACK fixed header
        arr[1] = 0x02;       // Remaining Length
        arr[2] = 0x00;       // Connect Acknowledge Flags (Session Present = 0)
        arr[3] = reasonCode; // Return Code
    }

    ssize_t w = write(client->fd, arr, (size_t)size);
    int writeError = errno;
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if (w > 0 && mqttMetricEmitterReady) {
        (void)metric_event_mqtt_positive_write((uint64_t)w);
    }
#else
    sendByteMetric(SERVER_ID, "sent", w);
#endif
    if (w < 0) {
        fprintf(stderr, "sendConnack: write failed. May retry.");
        if (writeError != EAGAIN && writeError != EWOULDBLOCK &&
            writeError != EINTR) {
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
            sendReliabilityMetric(SERVER_ID, "write_error", metricReasonFromErrno(errno));
#endif
            *failureReason = metric_io_reason_from_unrecoverable_errno(
                writeError, false);
            return MQTT_SEND_FATAL;
        }
        return MQTT_SEND_PENDING;
    }
    if (w != size) {
        return MQTT_SEND_PENDING;
    }

    {
        char msg[256];
        snprintf(msg, sizeof(msg), "%s CONNACK\n",
            SERVER_ID);
        printf("%s", msg);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
        if (reasonCode == 0x00 && client->connectAccepted &&
            !client->connackSent) {
            long long now = mqttNowMs();
            long long durationMs = now - (long long)client->connectAcceptedMs;
            client->connackSent = true;
            if (mqttMetricEmitterReady) {
                (void)metric_event_mqtt_connack_sent(
                    durationMs >= 0 ? (uint64_t)durationMs : 0);
            }
        }
#else
        sendMetric(msg);
        if (w > 0 && !client->firstResponseSent) {
            client->firstResponseSent = true;
            client->interactionDepthAtFirstResponse = interactionDepthLevel(&client->interactionDepth);
        }
#endif
        // syslog(LOG_INFO, "Sent CONNACK to client fd=%d\n", client->fd);
    }

    return MQTT_SEND_COMPLETE;
}

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
static enum mqttSendResult sendMqttActionPacket(
    struct mqttClient *client,
    const uint8_t *packet,
    size_t packetLength,
    enum metric_mqtt_action action,
    enum metric_io_reason *failureReason) {
    *failureReason = METRIC_IO_NONE;
    ssize_t written = write(client->fd, packet, packetLength);
    int writeError = errno;
    if (written > 0 && mqttMetricEmitterReady) {
        (void)metric_event_mqtt_positive_write((uint64_t)written);
    }
    if (written < 0) {
        if (writeError == EAGAIN || writeError == EWOULDBLOCK ||
            writeError == EINTR) {
            return MQTT_SEND_PENDING;
        }
        *failureReason = metric_io_reason_from_unrecoverable_errno(
            writeError, false);
        return MQTT_SEND_FATAL;
    }
    if ((size_t)written != packetLength) {
        return MQTT_SEND_PENDING;
    }
    if (mqttMetricEmitterReady) {
        (void)metric_event_mqtt_protocol_action(action);
    }
    return MQTT_SEND_COMPLETE;
}

enum mqttOperationParseResult {
    MQTT_OPERATION_VALID,
    MQTT_OPERATION_MALFORMED,
    MQTT_OPERATION_REFUSED,
};

struct mqttParsedSubscribe {
    uint16_t packetId;
    uint8_t filterCount;
    uint16_t filterLengths[MQTT_MAX_SUBSCRIPTIONS];
    char filters[MQTT_MAX_SUBSCRIPTIONS]
                [MQTT_MAX_TOPIC_FILTER_LENGTH + 1];
    uint8_t results[MQTT_MAX_SUBSCRIPTIONS];
};

static bool mqttFilterContainsWildcard(const char *filter, uint16_t length) {
    for (uint16_t index = 0; index < length; index++) {
        if (filter[index] == '+' || filter[index] == '#') {
            return true;
        }
    }
    return false;
}

static enum mqttOperationParseResult parseMqttSubscribe(
    const uint8_t *buffer,
    uint32_t packetEnd,
    uint32_t offset,
    struct mqttParsedSubscribe *parsed) {
    memset(parsed, 0, sizeof(*parsed));
    if (offset + 2 > packetEnd) {
        return MQTT_OPERATION_MALFORMED;
    }
    parsed->packetId = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
    if (parsed->packetId == 0) {
        return MQTT_OPERATION_MALFORMED;
    }

    uint16_t totalFilters = 0;
    while (offset < packetEnd) {
        if (offset + 2 > packetEnd) {
            return MQTT_OPERATION_MALFORMED;
        }
        uint16_t filterLength =
            (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;
        if (filterLength == 0 ||
            filterLength > MQTT_MAX_TOPIC_FILTER_LENGTH ||
            offset + filterLength + 1 > packetEnd) {
            return MQTT_OPERATION_MALFORMED;
        }
        for (uint16_t index = 0; index < filterLength; index++) {
            if (buffer[offset + index] == 0) {
                return MQTT_OPERATION_MALFORMED;
            }
        }
        uint8_t options = buffer[offset + filterLength];
        uint8_t requestedQos = options & 0x03;
        if ((options & 0xfc) != 0 || requestedQos > 2) {
            return MQTT_OPERATION_MALFORMED;
        }
        if (totalFilters < MQTT_MAX_SUBSCRIPTIONS) {
            uint8_t resultIndex = (uint8_t)totalFilters;
            parsed->filterLengths[resultIndex] = filterLength;
            memcpy(
                parsed->filters[resultIndex], buffer + offset,
                filterLength);
            parsed->filters[resultIndex][filterLength] = '\0';
            parsed->results[resultIndex] = mqttFilterContainsWildcard(
                parsed->filters[resultIndex], filterLength)
                    ? 0x80
                    : 0x00;
        }
        totalFilters += 1;
        offset += filterLength + 1;
    }
    if (totalFilters == 0) {
        return MQTT_OPERATION_MALFORMED;
    }
    if (totalFilters > MQTT_MAX_SUBSCRIPTIONS) {
        return MQTT_OPERATION_REFUSED;
    }
    parsed->filterCount = (uint8_t)totalFilters;
    return MQTT_OPERATION_VALID;
}

static bool mqttSubscriptionEquals(
    const struct mqttSubscription *subscription,
    const char *filter,
    uint16_t length) {
    return subscription->active && subscription->length == length &&
           memcmp(subscription->filter, filter, length) == 0;
}

static bool storeMqttSubscriptions(
    struct mqttClient *client,
    const struct mqttParsedSubscribe *parsed) {
    uint8_t neededSlots = 0;
    uint8_t freeSlots = 0;
    for (uint8_t subscriptionIndex = 0;
         subscriptionIndex < MQTT_MAX_SUBSCRIPTIONS;
         subscriptionIndex++) {
        if (!client->subscriptions[subscriptionIndex].active) {
            freeSlots += 1;
        }
    }
    for (uint8_t filterIndex = 0;
         filterIndex < parsed->filterCount;
         filterIndex++) {
        if (parsed->results[filterIndex] != 0x00) {
            continue;
        }
        bool alreadyStored = false;
        for (uint8_t subscriptionIndex = 0;
             subscriptionIndex < MQTT_MAX_SUBSCRIPTIONS;
             subscriptionIndex++) {
            if (mqttSubscriptionEquals(
                    &client->subscriptions[subscriptionIndex],
                    parsed->filters[filterIndex],
                    parsed->filterLengths[filterIndex])) {
                alreadyStored = true;
                break;
            }
        }
        if (!alreadyStored) {
            neededSlots += 1;
        }
    }
    if (neededSlots > freeSlots) {
        return false;
    }

    for (uint8_t filterIndex = 0;
         filterIndex < parsed->filterCount;
         filterIndex++) {
        if (parsed->results[filterIndex] != 0x00) {
            continue;
        }
        bool alreadyStored = false;
        for (uint8_t subscriptionIndex = 0;
             subscriptionIndex < MQTT_MAX_SUBSCRIPTIONS;
             subscriptionIndex++) {
            if (mqttSubscriptionEquals(
                    &client->subscriptions[subscriptionIndex],
                    parsed->filters[filterIndex],
                    parsed->filterLengths[filterIndex])) {
                alreadyStored = true;
                break;
            }
        }
        if (alreadyStored) {
            continue;
        }
        for (uint8_t subscriptionIndex = 0;
             subscriptionIndex < MQTT_MAX_SUBSCRIPTIONS;
             subscriptionIndex++) {
            struct mqttSubscription *subscription =
                &client->subscriptions[subscriptionIndex];
            if (!subscription->active) {
                subscription->active = true;
                subscription->length = parsed->filterLengths[filterIndex];
                memcpy(
                    subscription->filter,
                    parsed->filters[filterIndex],
                    subscription->length + 1);
                break;
            }
        }
    }
    return true;
}

struct mqttParsedUnsubscribe {
    uint16_t packetId;
    uint8_t filterCount;
    uint16_t filterLengths[MQTT_MAX_SUBSCRIPTIONS];
    char filters[MQTT_MAX_SUBSCRIPTIONS]
                [MQTT_MAX_TOPIC_FILTER_LENGTH + 1];
};

static enum mqttOperationParseResult parseMqttUnsubscribe(
    const uint8_t *buffer,
    uint32_t packetEnd,
    uint32_t offset,
    struct mqttParsedUnsubscribe *parsed) {
    memset(parsed, 0, sizeof(*parsed));
    if (offset + 2 > packetEnd) {
        return MQTT_OPERATION_MALFORMED;
    }
    parsed->packetId = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
    if (parsed->packetId == 0) {
        return MQTT_OPERATION_MALFORMED;
    }

    uint16_t totalFilters = 0;
    while (offset < packetEnd) {
        if (offset + 2 > packetEnd) {
            return MQTT_OPERATION_MALFORMED;
        }
        uint16_t filterLength =
            (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;
        if (filterLength == 0 ||
            filterLength > MQTT_MAX_TOPIC_FILTER_LENGTH ||
            offset + filterLength > packetEnd) {
            return MQTT_OPERATION_MALFORMED;
        }
        for (uint16_t index = 0; index < filterLength; index++) {
            if (buffer[offset + index] == 0) {
                return MQTT_OPERATION_MALFORMED;
            }
        }
        if (totalFilters < MQTT_MAX_SUBSCRIPTIONS) {
            uint8_t filterIndex = (uint8_t)totalFilters;
            parsed->filterLengths[filterIndex] = filterLength;
            memcpy(
                parsed->filters[filterIndex], buffer + offset,
                filterLength);
            parsed->filters[filterIndex][filterLength] = '\0';
        }
        totalFilters += 1;
        offset += filterLength;
    }
    if (totalFilters == 0) {
        return MQTT_OPERATION_MALFORMED;
    }
    if (totalFilters > MQTT_MAX_SUBSCRIPTIONS) {
        return MQTT_OPERATION_REFUSED;
    }
    parsed->filterCount = (uint8_t)totalFilters;
    return MQTT_OPERATION_VALID;
}

static void removeMqttSubscriptions(
    struct mqttClient *client,
    const struct mqttParsedUnsubscribe *parsed) {
    for (uint8_t filterIndex = 0;
         filterIndex < parsed->filterCount;
         filterIndex++) {
        for (uint8_t subscriptionIndex = 0;
             subscriptionIndex < MQTT_MAX_SUBSCRIPTIONS;
             subscriptionIndex++) {
            struct mqttSubscription *subscription =
                &client->subscriptions[subscriptionIndex];
            if (mqttSubscriptionEquals(
                    subscription,
                    parsed->filters[filterIndex],
                    parsed->filterLengths[filterIndex])) {
                memset(subscription, 0, sizeof(*subscription));
            }
        }
    }
}
#endif

bool readPublish(uint8_t* buffer, uint32_t packetEnd, uint32_t offset,
                 uint8_t firstByte, struct mqttClient* client,
                 uint8_t *parsedQos, uint16_t *parsedPacketId,
                 struct mqttParsedPublishData *parsedData) {
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "PUBLISH packet too short for topic length");
        return false;
    }

    uint16_t topicLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if (offset + topicLen > packetEnd) {
        fprintf(stderr, "PUBLISH topic exceeds packet bounds");
        return false;
    }
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if (topicLen > MQTT_MAX_TOPIC_FILTER_LENGTH) {
        return false;
    }
#endif

    char topic[256] = {0};
    memcpy(topic, &buffer[offset], topicLen < 255 ? topicLen : 255);
    offset += topicLen;

    if (topicLen == 0) {
        return false;
    }

    uint8_t qos = (firstByte & 0b00000110) >> 1;
    if (parsedQos) {
        *parsedQos = qos;
    }
    if (parsedPacketId) {
        *parsedPacketId = 0;
    }
    if (qos == 3) {
        return false;
    }
    if (qos > 0) {
        if (offset + 2 > packetEnd) return false;
        uint16_t packetId = (buffer[offset] << 8) | buffer[offset + 1];
        if (packetId == 0) {
            return false;
        }
        if (parsedPacketId) {
            *parsedPacketId = packetId;
        }
        offset += 2;
    }

    if(client->version == V5) {        
        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            return false;
        }
    
        // Skip properties
        if (offset + varint > packetEnd) {
            return false;
        }
        offset += varint;
    }

    // Remaining is payload
    if (offset > packetEnd) return false;

    char payload[512] = {0};
    uint32_t payloadLen = packetEnd - offset;
    uint32_t copyLen = payloadLen < sizeof(payload) - 1 ? payloadLen : sizeof(payload) - 1;
    memcpy(payload, &buffer[offset], copyLen);
    payload[copyLen] = '\0';
    if (parsedData) {
        memset(parsedData, 0, sizeof(*parsedData));
        parsedData->topicLength = topicLen;
        memcpy(parsedData->topic, topic, topicLen + 1);
        parsedData->payload = buffer + offset;
        parsedData->payloadLength = payloadLen;
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "%s PUBLISH %.100s %d\n",
        SERVER_ID, topic, qos);
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
    sendMetric(msg);
    char fields[192];
    interactionDepthObserveMqttOperation(&client->interactionDepth);
    snprintf(fields, sizeof(fields),
        "\"qos\":%d,\"topic_present\":%s,\"payload_present\":%s,\"interaction_depth\":%u",
        qos, boolString(topicLen > 0), boolString(payloadLen > 0),
        interactionDepthLevel(&client->interactionDepth));
    emitMqttAction(client, "PUBLISH", fields);
#endif
    printf("PUBLISH received. Topic: %s, Payload: %s, QoS: %d\n", topic, payload, qos);
    return true;
}

bool readUnsubscribe(uint8_t* buffer, uint32_t packetEnd, uint32_t offset,
                     struct mqttClient *client) {
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "UNSUBSCRIBE packet too short");
        return false;
    }

    uint16_t packetId = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if (client->version == V5) {
        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            return false;
        }

        // Skip properties
        if (offset + varint > packetEnd) {
            return false;
        }
        offset += varint;
    }

    bool observedTopic = false;
    while (offset + 2 <= packetEnd) {
        uint16_t topicLen = (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;

        if (topicLen == 0 || offset + topicLen > packetEnd) return false;

        char topic[256] = {0};
        memcpy(topic, &buffer[offset], topicLen < 255 ? topicLen : 255);
        offset += topicLen;

        char msg[256];
        snprintf(msg, sizeof(msg), "%s UNSUBSCRIBE %.200s\n",
            SERVER_ID, topic);
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
        sendMetric(msg);
#endif
        observedTopic = true;

        printf("UNSUBSCRIBE received for topic: %s (Packet ID: %u)\n", topic, packetId);
    }
    if (!observedTopic || offset != packetEnd) {
        return false;
    }
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
    interactionDepthObserveMqttOperation(&client->interactionDepth);
#endif
    return true;
}

bool sendPublish(struct mqttClient* client, const char* topic, const char* message) {
    uint16_t topicLength = strlen(topic);
    uint16_t payloadLength = strlen(message);
    uint8_t propertiesLength = 0; // No props

    size_t remainingLength = 2 + topicLength + 2; // length prefix + Topic + Packet ID (QoS2)

    if (client->version == V5) {
        remainingLength += 1 + propertiesLength; // Must add properties
    }
    remainingLength += payloadLength;

    uint8_t fixedHeader[5];
    size_t fixedHeaderLength = 0;
    // fixedHeader[fixedHeaderLength++] = 0b110100; // QoS 2
    fixedHeader[fixedHeaderLength++] = 0x34;

    // Encode Remaining Length
    size_t rem = remainingLength;
    do {
        uint8_t byte = rem % 128;
        rem /= 128;
        if (rem > 0) byte |= 128;
        fixedHeader[fixedHeaderLength++] = byte;
    } while (rem > 0);

    ssize_t packetLength = fixedHeaderLength + remainingLength;
    uint8_t* packet = malloc(packetLength);
    if (!packet) {
        fprintf(stderr, "Out of memory for publish packet");
        return false;
    }

    size_t offset = 0;
    memcpy(packet, fixedHeader, fixedHeaderLength);
    offset += fixedHeaderLength;

    // Big endian topic
    packet[offset++] = topicLength >> 8;
    packet[offset++] = topicLength & 0xFF;
    memcpy(packet + offset, topic, topicLength);
    offset += topicLength;

    // Big endian packetId
    static uint16_t packetId = 1234;
    packet[offset++] = packetId >> 8;
    packet[offset++] = packetId & 0xFF;

    if (client->version == V5) {
        packet[offset++] = propertiesLength;
    }
    memcpy(packet + offset, message, payloadLength);
    offset += payloadLength;

    ssize_t w = write(client->fd, packet, packetLength);
    sendByteMetric(SERVER_ID, "sent", w);
    free(packet);
    if (w < 0) {
        fprintf(stderr, "sendPublish: write failed. May retry.");
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            sendReliabilityMetric(SERVER_ID, "write_error", metricReasonFromErrno(errno));
            return false;
        }
    } else {
        // syslog(LOG_INFO, "Sent PUBLISH to client (fd=%d), topic=%s\n", client->fd, topic);
    }
    
    return true;
}

void readPubrec(uint8_t* buffer, uint32_t packetEnd, uint32_t offset, struct mqttClient* client) {
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "PUBREC packet too short for Packet Identifier\n");
        return;
    }

    // uint16_t packetId = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
    // syslog(LOG_INFO, "PUBREC Packet ID: %u\n", packetId);
    if (client->version == V5) {
        if (offset >= packetEnd) {
            return;
        }
        uint8_t reasonCode = buffer[offset++];
        printf("PUBREC: Reason code: 0x%02X\n", reasonCode);

        if (offset >= packetEnd) {
            return;
        }

        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            return;
        }

        if (offset + varint > packetEnd) {
            return;
        }

        uint32_t propsEnd = offset + varint;
        while (offset < propsEnd && offset < packetEnd) {
            uint8_t propId = buffer[offset++];
            switch (propId) {
                case 0x1F: {  // Reason String
                    if (offset + 2 > propsEnd) {
                        fprintf(stderr, "PUBREC: Malformed Reason String");
                        return;
                    }
                    uint16_t strLen = (buffer[offset] << 8) | buffer[offset + 1];
                    offset += 2;
                    if (offset + strLen > propsEnd) {
                        fprintf(stderr, "PUBREC: Truncated Reason String");
                        return;
                    }
                    char reasonStr[256] = {0};
                    uint16_t copyLen = strLen < 255 ? strLen : 255;
                    memcpy(reasonStr, &buffer[offset], copyLen);
                    offset += strLen;
                    printf("PUBREC: Reason String: %s\n", reasonStr);
                    break;
                }

                case 0x26: {  // User Property (key-value pair)
                    // Read key
                    if (offset + 2 > propsEnd) return;
                    uint16_t keyLen = (buffer[offset] << 8) | buffer[offset + 1];
                    offset += 2;
                    if (offset + keyLen > propsEnd) return;

                    char key[128] = {0};
                    memcpy(key, &buffer[offset], keyLen < 127 ? keyLen : 127);
                    offset += keyLen;

                    // Read value
                    if (offset + 2 > propsEnd) return;
                    uint16_t valLen = (buffer[offset] << 8) | buffer[offset + 1];
                    offset += 2;
                    if (offset + valLen > propsEnd) return;

                    char val[128] = {0};
                    memcpy(val, &buffer[offset], valLen < 127 ? valLen : 127);
                    offset += valLen;

                    printf("PUBREC: User Property: %s = %s\n", key, val);
                    break;
                }
                default:
                    printf("PUBREC: Unknown property ID: 0x%02X\n", propId);
                    return;
            }
        }
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "%s PUBREC\n",
        SERVER_ID);
    sendMetric(msg);
    char fields[64];
    snprintf(fields, sizeof(fields), "\"interaction_depth\":%u",
        interactionDepthLevel(&client->interactionDepth));
    emitMqttAction(client, "PUBREC", fields);
    // syslog(LOG_INFO, "Received PUBREC for fd=%d and packet ID: %d\n", client->fd, packetId);
}

bool sendPubrel(struct mqttClient* client, uint16_t packetId) {
    int size = client->version == V5 ? 6 : 4;
    uint8_t* arr = malloc(size);

    if(client->version == V5){
        arr[0] = 0b01100010;         // Fixed header
        arr[1] = 0x04;               // Remaining Length
        arr[2] = packetId >> 8;      // packetId
        arr[3] = packetId & 0xFF;
    
        arr[4] = 0x00;               // Reason Code: Success
        arr[5] = 0x00;               // Property Length
    } else {
        arr[0] = 0b01100010;         // Fixed header
        arr[1] = 0x02;               // Remaining Length
        arr[2] = packetId >> 8;      // packetId
        arr[3] = packetId & 0xFF;
    }

    ssize_t w = write(client->fd, arr, size);
    sendByteMetric(SERVER_ID, "sent", w);
    if (w == -1) {
        fprintf(stderr, "sendPubrel: write failed");
        sendReliabilityMetric(SERVER_ID, "write_error", metricReasonFromErrno(errno));
        free(arr);
        return false;
    }

    // syslog(LOG_INFO, "Sent PUBREL to client fd=%d", client->fd);
    free(arr);
    return true;
}

void readPubcomp(uint32_t packetEnd, uint32_t offset) {
    // syslog(LOG_INFO, "Received PUBCOMP");
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "PUBCOMP packet too short for Packet Identifier");
        return;
    }

    // Extract Packet Identifier
    // uint16_t packetId = (buffer[offset] << 8) | buffer[offset + 1];
    // syslog(LOG_INFO, "PUBCOMP Packet ID: %u from client fd=%d", packetId, client->fd);
}

enum mqttSendResult sendPingresp(
    struct mqttClient* client,
    enum metric_io_reason *failureReason) {
    *failureReason = METRIC_IO_NONE;
    uint8_t packet[2] = { 0xD0, 0x00 };
    ssize_t w = write(client->fd, packet, sizeof(packet));
    int writeError = errno;
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if (w > 0 && mqttMetricEmitterReady) {
        (void)metric_event_mqtt_positive_write((uint64_t)w);
    }
#else
    sendByteMetric(SERVER_ID, "sent", w);
#endif

    if (w < 0) {
        fprintf(stderr, "sendPingresp: write failed. May retry.");
        if (writeError != EAGAIN && writeError != EWOULDBLOCK &&
            writeError != EINTR) {
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
            sendReliabilityMetric(SERVER_ID, "write_error", metricReasonFromErrno(errno));
#endif
            *failureReason = metric_io_reason_from_unrecoverable_errno(
                writeError, false);
            return MQTT_SEND_FATAL;
        }
        return MQTT_SEND_PENDING;
    }
    if (w != (ssize_t)sizeof(packet)) {
        return MQTT_SEND_PENDING;
    }
    fprintf(stderr, "Sent PINGRESP to client (fd=%d)\n", client->fd);
    return MQTT_SEND_COMPLETE;
}

void disconnectClient(
    struct mqttClient* client,
    int epollFd,
    long long now,
    enum metric_mqtt_finalization_reason finalizationReason,
    enum metric_io_reason ioReason) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    pthread_mutex_lock(&mqttMetricStateLock);
#endif
    unsigned int depthLevel;
    if (!interactionDepthFinalize(&client->interactionDepth, &depthLevel)) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
        pthread_mutex_unlock(&mqttMetricStateLock);
#endif
        return;
    }
    long long wastedTime = now - client->timeOfConnection;
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    if (mqttActiveConnections > 0) {
        mqttActiveConnections -= 1;
    }
    if (mqttMetricEmitterReady) {
        (void)metric_event_mqtt_connection_finalized(
            finalizationReason,
            wastedTime >= 0 ? (uint64_t)wastedTime : 0,
            (uint8_t)depthLevel,
            mqttActiveConnections,
            ioReason);
    }
    pthread_mutex_unlock(&mqttMetricStateLock);
    const char *reason = mqttFinalizationName(finalizationReason);
#else
    (void)ioReason;
    const char *reason = legacyMqttFinalizationName(finalizationReason);
    bool firstResponseExit = client->firstResponseSent &&
        depthLevel <= client->interactionDepthAtFirstResponse;

    char msg[256];
    snprintf(msg, sizeof(msg), "%s disconnect %s %lld %lld %u %s %d\n",
        SERVER_ID,
        client->ipaddr,
        wastedTime,
        wastedTime,
        depthLevel,
        reason,
        firstResponseExit ? 1 : 0);

    printf("%s", msg);
    sendMetric(msg);
#endif
    session_events_write_disconnect("mqtt", client->sessionId, wastedTime, reason, depthLevel);

    epoll_ctl(epollFd, EPOLL_CTL_DEL, client->fd, NULL);
    deleteClient(client);
    close(client->fd);
    free(client);
}

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
static bool mqttClientHasExactSubscription(
    const struct mqttClient *client,
    const char *topic,
    uint16_t topicLength) {
    for (uint8_t subscriptionIndex = 0;
         subscriptionIndex < MQTT_MAX_SUBSCRIPTIONS;
         subscriptionIndex++) {
        if (mqttSubscriptionEquals(
                &client->subscriptions[subscriptionIndex],
                topic, topicLength)) {
            return true;
        }
    }
    return false;
}

static size_t encodeMqttQos0Publish(
    uint8_t *packet,
    size_t capacity,
    const struct mqttParsedPublishData *publication) {
    uint32_t remainingLength =
        2U + publication->topicLength + publication->payloadLength;
    uint8_t encodedLength[4];
    size_t encodedLengthSize = 0;
    uint32_t remaining = remainingLength;
    do {
        uint8_t encodedByte = remaining % 128U;
        remaining /= 128U;
        if (remaining > 0) {
            encodedByte |= 0x80;
        }
        encodedLength[encodedLengthSize++] = encodedByte;
    } while (remaining > 0 && encodedLengthSize < sizeof(encodedLength));

    size_t totalLength = 1 + encodedLengthSize + remainingLength;
    if (remaining > 0 || totalLength > capacity) {
        return 0;
    }
    size_t offset = 0;
    packet[offset++] = 0x30;
    memcpy(packet + offset, encodedLength, encodedLengthSize);
    offset += encodedLengthSize;
    packet[offset++] = (uint8_t)(publication->topicLength >> 8);
    packet[offset++] = (uint8_t)(publication->topicLength & 0xff);
    memcpy(
        packet + offset, publication->topic,
        publication->topicLength);
    offset += publication->topicLength;
    memcpy(
        packet + offset, publication->payload,
        publication->payloadLength);
    offset += publication->payloadLength;
    return offset;
}

static bool deliverMqttPublication(
    struct mqttClient *publisher,
    int epollFd,
    const struct mqttParsedPublishData *publication) {
    uint8_t packet[1285];
    size_t packetLength = encodeMqttQos0Publish(
        packet, sizeof(packet), publication);
    if (packetLength == 0) {
        return true;
    }

    for (struct mqttClient *target = clients, *next = NULL;
         target != NULL;
         target = next) {
        next = target->hh.next;
        if (!target->connackSent ||
            !mqttClientHasExactSubscription(
                target, publication->topic,
                publication->topicLength)) {
            continue;
        }
        enum metric_io_reason deliveryFailureReason;
        enum mqttSendResult deliveryResult = sendMqttActionPacket(
            target, packet, packetLength,
            METRIC_MQTT_ACTION_SUBSCRIPTION_PUBLISH_SENT,
            &deliveryFailureReason);
        if (deliveryResult == MQTT_SEND_FATAL) {
            bool publisherFailed = target == publisher;
            disconnectClient(
                target, epollFd, mqttNowMs(),
                METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                deliveryFailureReason);
            if (publisherFailed) {
                return false;
            }
        }
    }
    return true;
}
#endif

enum Request determineRequest(uint8_t firstByte) {
    switch (firstByte >> 4)
    {
    case 0b0001:
        return CONNECT;
    case 0b0101:
        return PUBREC;
    case 0b0110:
        return PUBREL;
    case 0b1000:
        return SUBSCRIBE;
    case 0b1100:
        return PING;
    case 0b1110:
        return DISCONNECT;
    case 0b0011:
        return PUBLISH;
    case 0b1010:
        return UNSUBSCRIBE;
    case 0b0111:
        return PUBCOMP;
    default:
        fprintf(stderr, "Unknown request %d", firstByte >> 4);
        return UNSUPPORTED_REQUEST;
    }
}

void calculateTotalPacketLengths(uint8_t *buffer, uint32_t bytesWrittenToBuffer,
                                 uint32_t *packetLengths, uint32_t *packetStarts,
                                 uint32_t *packetCount) {
    *packetCount = 0;
    uint32_t offset = 0;

    while (offset < bytesWrittenToBuffer) {
        if (bytesWrittenToBuffer - offset < 2) {
            fprintf(stderr, "CALCULATE: Not enough data for fixed header");
            break; 
        }

        if (*packetCount == maxPacketsPerClient) {
            fprintf(stderr, "CALCULATE: Max count reached");
            break;
        }

        uint32_t remainingLength = 0;
        uint32_t multiplier = 1;
        uint32_t encodedBytes = 0;

        // varint
        for (int i = 0; i < 4; i++) {
            if (offset + 1 + i >= bytesWrittenToBuffer) {
                fprintf(stderr, "CALCULATE: Not enough data to finish varint");
                return;
            }
            uint8_t byte = buffer[offset + 1 + i];
            remainingLength += (byte & 0b01111111) * multiplier;
            multiplier *= 128;
            encodedBytes++;

            if ((byte & 0b10000000) == 0) {
                break; 
            }
        }

        // Fixed header + variable header
        uint32_t headerLengths = 1 + encodedBytes;
        uint32_t totalPacketLength = headerLengths + remainingLength;

        if (bytesWrittenToBuffer - offset >= totalPacketLength) {
            packetLengths[*packetCount] = totalPacketLength;
            packetStarts[*packetCount] = offset + headerLengths;
            // syslog(LOG_INFO, "Packet %u: total length = %u, variable header offset = %u",
            //     *packetCount, totalPacketLength, packetStarts[*packetCount]);
            (*packetCount)++;
            offset += totalPacketLength;
        } else {
            fprintf(stderr, "Incomplete packet at offset %u: expected length = %u, available = %u",
                offset, totalPacketLength, bytesWrittenToBuffer - offset);
            break;  // Incomplete packet
        }
    }
}

void cleanupBuffer(struct mqttClient* client, uint32_t packetLength){
    int leftover = client->bytesWrittenToBuffer - packetLength;
    memmove(client->buffer, client->buffer + packetLength, leftover);
    client->bytesWrittenToBuffer = leftover;
}

int main(int argc, char* argv[]) {
    setbuf(stdout, NULL);
    
    // testing
    // char msg[256];
    // snprintf(msg, sizeof(msg), "%s connect %s\n",
    //     SERVER_ID, "82.211.213.247");
    // fprintf(stderr, "%s", msg);
    // sendMetric(msg);
    (void)argc;
    port = atoi(argv[1]);
    maxEvents = atoi(argv[2]);
    epollTimeoutInterval = atoi(argv[3]);
    pubrelInterval = atoi(argv[4]);
    maxPacketsPerClient = atoi(argv[5]);
    maxNoClients = atoi(argv[6]);
    // openlog("mqtt_tarpit", LOG_PID | LOG_CONS, LOG_USER);
    initializeStats();
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    mqttMetricEmitterReady = metric_event_emitter_init(
        getenv("EVENTHORIZON_METRIC_SOCKET"));
#endif
    session_events_init(NULL);
    setFdLimit(maxNoClients);
    signal(SIGPIPE, SIG_IGN);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    struct sigaction stopAction;
    memset(&stopAction, 0, sizeof(stopAction));
    stopAction.sa_handler = requestMqttStop;
    sigemptyset(&stopAction.sa_mask);
    sigaction(SIGTERM, &stopAction, NULL);
    sigaction(SIGINT, &stopAction, NULL);
#endif
    
    int serverSock = createServer(port);
    if (serverSock < 0) {
        fprintf(stderr, "Invalid server socket fd: %d", serverSock);
        exit(EXIT_FAILURE);
    }
    
    struct sockaddr_in clientAddr;
    socklen_t addrLen = sizeof(clientAddr);

    struct epoll_event ev, eventsQueue[maxEvents];
    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        fprintf(stderr, "epoll_create1 failed");
        exit(EXIT_FAILURE);
    }

    ev.events = EPOLLIN;
    ev.data.fd = serverSock;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, serverSock, &ev) == -1) {
        fprintf(stderr, "epoll_ctl: server_sock");
        exit(EXIT_FAILURE);
    }

    // long long lastHeartbeat = currentTimeMs();
    while(
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
        mqttStopRequested == 0
#else
        true
#endif
    ) {
        long long now = mqttNowMs();

        // if (now - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
        //     heartbeatLog();
        //     lastHeartbeat = now;
        // }

        int nfds = epoll_wait(epollfd, eventsQueue, maxEvents, epollTimeoutInterval);
        if (nfds == -1) {
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
            if (errno == EINTR && mqttStopRequested != 0) {
                break;
            }
#endif
            fprintf(stderr, "epoll_wait");
            exit(EXIT_FAILURE);
        }

        // Update now, since epoll_wait made the value outdated. 
        now = mqttNowMs();
        for (int n = 0; n < nfds
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
             && mqttStopRequested == 0
#endif
             ; ++n) {
            int currentFd = eventsQueue[n].data.fd;
            if (currentFd == serverSock) {
                int clientFd = accept(serverSock, (struct sockaddr *) &clientAddr, &addrLen);
                if (clientFd == -1) {
                    fprintf(stderr, "Failed accepting new client with error %s", strerror(errno));
                    continue;
                }
                struct mqttClient* newClient = malloc(sizeof(struct mqttClient));
                if (newClient == NULL) {
                    fprintf(stderr, "Out of memory");
                    close(clientFd);
                    continue;
                }

                
                statsMqtt.totalConnects += 1;
                newClient->fd = clientFd;
                strncpy(newClient->ipaddr, inet_ntoa(clientAddr.sin_addr), INET_ADDRSTRLEN);
                newClient->ipaddr[INET_ADDRSTRLEN - 1] = '\0';
                newClient->bytesWrittenToBuffer = 0;
                newClient->lastActivityMs = now;
                newClient->timeOfConnection = now;
                newClient->lastPubrelMs = now;
                newClient->keepAlive = 0; // Initial value. Will be updated after connect
                // Keep pre-CONNECT parsing deterministic. Meaningful depth still
                // cannot advance until a CONNECT has been fully accepted.
                newClient->version = V311;
                interactionDepthInit(&newClient->interactionDepth);
                newClient->firstResponseSent = false;
                newClient->interactionDepthAtFirstResponse = 0;
                newClient->connectAccepted = false;
                newClient->connectRefused = false;
                newClient->connackSent = false;
                newClient->connectAcceptedMs = 0;
                newClient->qos2Active = false;
                newClient->qos2PubrelReceived = false;
                newClient->qos2PacketId = 0;
                newClient->qos2PacketLength = 0;
                memset(
                    newClient->subscriptions, 0,
                    sizeof(newClient->subscriptions));
                session_events_make_id(newClient->sessionId, sizeof(newClient->sessionId), "mqtt", newClient->timeOfConnection, newClient->fd);
                memset(newClient->buffer, 0, sizeof(newClient->buffer)); // Maybe not necessary
                // ev.events = EPOLLIN | EPOLLET;
                // ev.data.fd = clientFd;
                fcntl(clientFd, F_SETFL, O_NONBLOCK);
                struct epoll_event clientEv;
                clientEv.events = EPOLLIN;
                clientEv.data.fd = clientFd;
                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, clientFd, &clientEv) == -1) {
                    fprintf(stderr, "Failed adding client to epoll with error %s", strerror(errno));
                    close(clientFd);
                    free(newClient);
                    continue;
                }
                
                addClient(newClient);
                char msg[256];
                snprintf(msg, sizeof(msg), "%s connect %s\n",
                    SERVER_ID, newClient->ipaddr);
                printf("%s", msg);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                pthread_mutex_lock(&mqttMetricStateLock);
                if (mqttActiveConnections < UINT32_MAX) {
                    mqttActiveConnections += 1;
                    if (mqttMetricEmitterReady) {
                        (void)metric_event_mqtt_connection_accepted(
                            mqttActiveConnections);
                    }
                }
                pthread_mutex_unlock(&mqttMetricStateLock);
#else
                sendMetric(msg);
#endif
                session_events_write_connect("mqtt", newClient->sessionId);
                // if(statsMqtt.mostConcurrentConnections < HASH_COUNT(clients)) {
                //     statsMqtt.mostConcurrentConnections = HASH_COUNT(clients);
                // }
            } else {
                struct mqttClient* client = lookupClient(currentFd);
                if (client == NULL) {
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, currentFd, NULL);
                    close(currentFd);
                    continue;
                }
                ssize_t bytesRead = read(currentFd,
                          client->buffer + client->bytesWrittenToBuffer, // Avoid overwriting existing data
                          sizeof(client->buffer) - client->bytesWrittenToBuffer);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                if (bytesRead > 0 && mqttMetricEmitterReady) {
                    (void)metric_event_mqtt_positive_read((uint64_t)bytesRead);
                }
#else
                sendByteMetric(SERVER_ID, "received", bytesRead);
#endif

                if(bytesRead == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK ||
                        errno == EINTR) {
                        continue;
                    }
                    int readError = errno;
                    fprintf(stderr, "Failed reading. Disconnecting client. error: %s", strerror(errno));
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
                    sendReliabilityMetric(SERVER_ID, "read_error", metricReasonFromErrno(errno));
#endif
                    disconnectClient(
                        client, epollfd, mqttNowMs(),
                        METRIC_MQTT_FINALIZATION_READ_ERROR,
                        metric_io_reason_from_unrecoverable_errno(
                            readError, false));
                    continue;
                }

                if(bytesRead == 0) {
                    fprintf(stderr, "Client closed connection. Disconnecting client.");
                    disconnectClient(
                        client, epollfd, mqttNowMs(),
                        METRIC_MQTT_FINALIZATION_PEER_CLOSED,
                        METRIC_IO_NONE);
                    continue;
                }

                client->bytesWrittenToBuffer += bytesRead;

                if (client->bytesWrittenToBuffer >= sizeof(client->buffer)) {
                    fprintf(stderr, "Buffer full. Disconnecting client.");
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
                    sendReliabilityMetric(SERVER_ID, "read_error", "invalid_packet");
#endif
                    disconnectClient(
                        client, epollfd, mqttNowMs(),
                        METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                        METRIC_IO_NONE);
                    continue;
                }

                uint32_t packetLengths[maxPacketsPerClient]; // Length of each packet
                uint32_t packetStarts[maxPacketsPerClient]; // Points to the start of each packet, after the header values
                uint32_t packetCount = 0;

                calculateTotalPacketLengths(client->buffer, client->bytesWrittenToBuffer,
                            packetLengths, packetStarts, &packetCount);
                
                uint32_t processedPackets = 0;
                bool clientDisconnected = false;
                for (uint32_t i = 0; i < packetCount; i++) {
                    uint32_t packetLength = packetLengths[i];
                    uint32_t packetStart = packetStarts[i];
                    uint32_t packetEnd = processedPackets + packetLength;

                    if (packetLength == 0 || processedPackets + packetLength > client->bytesWrittenToBuffer) {
                        // syslog(LOG_INFO, "Incomplete packet");
                        break; // Incomplete packet
                    }

                    client->lastActivityMs = now;
                    enum Request request = determineRequest(client->buffer[processedPackets]);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                    if (request != CONNECT && !client->connackSent) {
                        disconnectClient(
                            client, epollfd, mqttNowMs(),
                            METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                            METRIC_IO_NONE);
                        clientDisconnected = true;
                        break;
                    }
#endif
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
                    bool pubSuccess = false;
#endif
                    switch (request) {
                        case CONNECT:
                            if (client->connectAccepted ||
                                client->buffer[processedPackets] != 0x10) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            uint8_t reasonCodeConn = readConnreq(client->buffer, packetEnd, packetStart, client);
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            enum metric_mqtt_finalization_reason refusalReason =
                                client->connectRefused
                                    ? METRIC_MQTT_FINALIZATION_CONNECT_REFUSED
                                    : METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR;
                            if (reasonCodeConn == 0x00) {
                                client->connectAccepted = true;
                                client->connectAcceptedMs = (uint64_t)mqttNowMs();
                                client->lastActivityMs = client->connectAcceptedMs;
                                interactionDepthObserveMqttConnect(
                                    &client->interactionDepth);
                                if (mqttMetricEmitterReady) {
                                    (void)metric_event_mqtt_protocol_action(
                                        METRIC_MQTT_ACTION_CONNECT_ACCEPTED);
                                }
                            }
#endif
                            if(reasonCodeConn != 0x00) {
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
                                char msg[256];
                                snprintf(msg, sizeof(msg), "%s malformedConnect",
                                    SERVER_ID);
                                sendMetric(msg);
#endif
                            }
                            enum metric_io_reason connackFailureReason;
                            enum mqttSendResult ackResult = MQTT_SEND_PENDING;
                            if (reasonCodeConn <= 0x05 ||
                                (client->version == V5 &&
                                 reasonCodeConn == 0x84)) {
                                ackResult = sendConnack(
                                    client, reasonCodeConn,
                                    &connackFailureReason);
                            }
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (reasonCodeConn != 0x00) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    refusalReason,
                                    METRIC_IO_NONE);
                                if (ackResult == MQTT_SEND_FATAL &&
                                    mqttMetricEmitterReady) {
                                    (void)metric_event_mqtt_secondary_write_error(
                                        connackFailureReason);
                                }
                                clientDisconnected = true;
                                break;
                            }
#endif
                            if(ackResult == MQTT_SEND_FATAL) {
                                fprintf(stderr, "Disconnecting client due to CONNACK failure");
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                    connackFailureReason);
                                clientDisconnected = true;
                                break;
                            }
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (ackResult != MQTT_SEND_COMPLETE) {
                                break;
                            }
#else
                            pubSuccess = sendPublish(client, "$SYS/credentials", "username=admin password=admin");
                            if(!pubSuccess) {
                                fprintf(stderr, "Disconnecting client due to publish failure");
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                    METRIC_IO_OTHER);
                                clientDisconnected = true;
                            }
#endif
                            break;
                        case SUBSCRIBE:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (client->buffer[processedPackets] != 0x82) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            struct mqttParsedSubscribe parsedSubscribe;
                            enum mqttOperationParseResult subscribeParseResult =
                                parseMqttSubscribe(
                                    client->buffer, packetEnd, packetStart,
                                    &parsedSubscribe);
                            if (subscribeParseResult ==
                                MQTT_OPERATION_MALFORMED) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            if (subscribeParseResult ==
                                    MQTT_OPERATION_REFUSED ||
                                !storeMqttSubscriptions(
                                    client, &parsedSubscribe)) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_OPERATION_REFUSED,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            interactionDepthObserveMqttOperation(
                                &client->interactionDepth);
                            char subscribeFields[64];
                            snprintf(
                                subscribeFields, sizeof(subscribeFields),
                                "\"filter_count\":%u,\"interaction_depth\":%u",
                                parsedSubscribe.filterCount,
                                interactionDepthLevel(
                                    &client->interactionDepth));
                            emitMqttAction(
                                client, "SUBSCRIBE", subscribeFields);
                            if (mqttMetricEmitterReady) {
                                (void)metric_event_mqtt_protocol_action(
                                    METRIC_MQTT_ACTION_SUBSCRIBE_RECEIVED);
                            }
                            uint8_t suback[4 + MQTT_MAX_SUBSCRIPTIONS] = {
                                0x90,
                                (uint8_t)(2 + parsedSubscribe.filterCount),
                                (uint8_t)(parsedSubscribe.packetId >> 8),
                                (uint8_t)(parsedSubscribe.packetId & 0xff),
                            };
                            memcpy(
                                suback + 4, parsedSubscribe.results,
                                parsedSubscribe.filterCount);
                            enum metric_io_reason subackFailureReason;
                            enum mqttSendResult subackResult =
                                sendMqttActionPacket(
                                    client, suback,
                                    4 + parsedSubscribe.filterCount,
                                    METRIC_MQTT_ACTION_SUBACK_SENT,
                                    &subackFailureReason);
                            if (subackResult == MQTT_SEND_FATAL) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                    subackFailureReason);
                                clientDisconnected = true;
                            }
#else
                            readSubscribe(client->buffer, packetEnd, packetStart, client);
#endif
                            break;
                        case PUBREC:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            disconnectClient(
                                client, epollfd, mqttNowMs(),
                                METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                METRIC_IO_NONE);
                            clientDisconnected = true;
#else
                            readPubrec(client->buffer, packetEnd, packetStart, client);
#endif
                            break;
                        case PUBREL:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (client->buffer[processedPackets] != 0x62 ||
                                packetLength != 4 ||
                                packetStart + 2 != packetEnd) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            uint16_t pubrelPacketId =
                                (client->buffer[packetStart] << 8) |
                                client->buffer[packetStart + 1];
                            if (pubrelPacketId == 0) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            bool matchingPubrel =
                                client->qos2Active &&
                                pubrelPacketId == client->qos2PacketId;
                            if (matchingPubrel &&
                                !client->qos2PubrelReceived) {
                                client->qos2PubrelReceived = true;
                                if (mqttMetricEmitterReady) {
                                    (void)metric_event_mqtt_protocol_action(
                                        METRIC_MQTT_ACTION_PUBREL_RECEIVED);
                                }
                            }
                            uint8_t pubcomp[4] = {
                                0x70, 0x02,
                                (uint8_t)(pubrelPacketId >> 8),
                                (uint8_t)(pubrelPacketId & 0xff),
                            };
                            enum metric_io_reason pubcompFailureReason;
                            enum mqttSendResult pubcompResult =
                                sendMqttActionPacket(
                                    client, pubcomp, sizeof(pubcomp),
                                    METRIC_MQTT_ACTION_PUBCOMP_SENT,
                                    &pubcompFailureReason);
                            if (pubcompResult == MQTT_SEND_FATAL) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                    pubcompFailureReason);
                                clientDisconnected = true;
                            } else if (pubcompResult == MQTT_SEND_COMPLETE &&
                                       matchingPubrel) {
                                client->qos2Active = false;
                                client->qos2PubrelReceived = false;
                                client->qos2PacketId = 0;
                                client->qos2PacketLength = 0;
                            }
#endif
                            break;
                        case PUBLISH:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (!client->connackSent) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            uint8_t publishQos =
                                (client->buffer[processedPackets] & 0x06) >> 1;
                            if (publishQos == 3) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            uint16_t publishPacketId = 0;
                            struct mqttParsedPublishData parsedPublish;
                            if (!readPublish(
                                    client->buffer, packetEnd, packetStart,
                                    client->buffer[processedPackets], client,
                                    &publishQos, &publishPacketId,
                                    &parsedPublish)) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            if (publishQos == 0 &&
                                (client->buffer[processedPackets] & 0x08) != 0) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            if ((client->buffer[processedPackets] & 0x01) != 0) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_OPERATION_REFUSED,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            bool newPublishOperation = true;
                            if (publishQos == 2 && client->qos2Active) {
                                bool exactDuplicate =
                                    (client->buffer[processedPackets] & 0x08) != 0 &&
                                    publishPacketId == client->qos2PacketId &&
                                    packetLength == client->qos2PacketLength &&
                                    (client->buffer[processedPackets] &
                                     (uint8_t)~0x08) == client->qos2Packet[0] &&
                                    memcmp(
                                        client->buffer + processedPackets + 1,
                                        client->qos2Packet + 1,
                                        packetLength - 1) == 0;
                                if (!exactDuplicate) {
                                    disconnectClient(
                                        client, epollfd, mqttNowMs(),
                                        METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                        METRIC_IO_NONE);
                                    clientDisconnected = true;
                                    break;
                                }
                                newPublishOperation = false;
                            }
                            if (publishQos == 2 && !client->qos2Active) {
                                client->qos2Active = true;
                                client->qos2PubrelReceived = false;
                                client->qos2PacketId = publishPacketId;
                                client->qos2PacketLength = (uint16_t)packetLength;
                                memcpy(
                                    client->qos2Packet,
                                    client->buffer + processedPackets,
                                    packetLength);
                                client->qos2Packet[0] &= (uint8_t)~0x08;
                            }
                            if (newPublishOperation) {
                                interactionDepthObserveMqttOperation(
                                    &client->interactionDepth);
                                char fields[64];
                                snprintf(
                                    fields, sizeof(fields),
                                    "\"qos\":%u,\"interaction_depth\":%u",
                                    publishQos,
                                    interactionDepthLevel(
                                        &client->interactionDepth));
                                emitMqttAction(client, "PUBLISH", fields);
                                if (mqttMetricEmitterReady) {
                                    (void)metric_event_mqtt_protocol_action(
                                        METRIC_MQTT_ACTION_PUBLISH_RECEIVED);
                                }
                            }
                            if (publishQos == 1) {
                                uint8_t puback[4] = {
                                    0x40, 0x02,
                                    (uint8_t)(publishPacketId >> 8),
                                    (uint8_t)(publishPacketId & 0xff),
                                };
                                enum metric_io_reason pubackFailureReason;
                                enum mqttSendResult pubackResult =
                                    sendMqttActionPacket(
                                        client, puback, sizeof(puback),
                                        METRIC_MQTT_ACTION_PUBACK_SENT,
                                        &pubackFailureReason);
                                if (pubackResult == MQTT_SEND_FATAL) {
                                    disconnectClient(
                                        client, epollfd, mqttNowMs(),
                                        METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                        pubackFailureReason);
                                    clientDisconnected = true;
                                }
                            } else if (publishQos == 2) {
                                uint8_t pubrec[4] = {
                                    0x50, 0x02,
                                    (uint8_t)(publishPacketId >> 8),
                                    (uint8_t)(publishPacketId & 0xff),
                                };
                                enum metric_io_reason pubrecFailureReason;
                                enum mqttSendResult pubrecResult =
                                    sendMqttActionPacket(
                                        client, pubrec, sizeof(pubrec),
                                        METRIC_MQTT_ACTION_PUBREC_SENT,
                                        &pubrecFailureReason);
                                if (pubrecResult == MQTT_SEND_FATAL) {
                                    disconnectClient(
                                        client, epollfd, mqttNowMs(),
                                        METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                        pubrecFailureReason);
                                    clientDisconnected = true;
                                }
                            }
                            if (!clientDisconnected &&
                                newPublishOperation &&
                                publishQos < 2 &&
                                !deliverMqttPublication(
                                    client, epollfd, &parsedPublish)) {
                                clientDisconnected = true;
                            }
#else
                            (void)readPublish(
                                client->buffer, packetEnd, packetStart,
                                client->buffer[processedPackets], client,
                                NULL, NULL, NULL);
#endif
                            break;
                        case PUBCOMP:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            disconnectClient(
                                client, epollfd, mqttNowMs(),
                                METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                METRIC_IO_NONE);
                            clientDisconnected = true;
#else
                            readPubcomp(packetEnd, packetStart);
                            pubSuccess = sendPublish(client, "$SYS/confidential", "username=admin123 password=admin321");
                            if(!pubSuccess) {
                                fprintf(stderr, "Disconnecting client due to publish failure");
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                    METRIC_IO_OTHER);
                                clientDisconnected = true;
                            }
#endif
                            break;
                        case UNSUBSCRIBE:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (client->buffer[processedPackets] != 0xa2) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            struct mqttParsedUnsubscribe parsedUnsubscribe;
                            enum mqttOperationParseResult unsubscribeParseResult =
                                parseMqttUnsubscribe(
                                    client->buffer, packetEnd, packetStart,
                                    &parsedUnsubscribe);
                            if (unsubscribeParseResult ==
                                MQTT_OPERATION_MALFORMED) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            if (unsubscribeParseResult ==
                                MQTT_OPERATION_REFUSED) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_OPERATION_REFUSED,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
                            removeMqttSubscriptions(
                                client, &parsedUnsubscribe);
                            interactionDepthObserveMqttOperation(
                                &client->interactionDepth);
                            char unsubscribeFields[64];
                            snprintf(
                                unsubscribeFields,
                                sizeof(unsubscribeFields),
                                "\"filter_count\":%u,\"interaction_depth\":%u",
                                parsedUnsubscribe.filterCount,
                                interactionDepthLevel(
                                    &client->interactionDepth));
                            emitMqttAction(
                                client, "UNSUBSCRIBE", unsubscribeFields);
                            if (mqttMetricEmitterReady) {
                                (void)metric_event_mqtt_protocol_action(
                                    METRIC_MQTT_ACTION_UNSUBSCRIBE_RECEIVED);
                            }
                            uint8_t unsuback[4] = {
                                0xb0, 0x02,
                                (uint8_t)(parsedUnsubscribe.packetId >> 8),
                                (uint8_t)(parsedUnsubscribe.packetId & 0xff),
                            };
                            enum metric_io_reason unsubackFailureReason;
                            enum mqttSendResult unsubackResult =
                                sendMqttActionPacket(
                                    client, unsuback, sizeof(unsuback),
                                    METRIC_MQTT_ACTION_UNSUBACK_SENT,
                                    &unsubackFailureReason);
                            if (unsubackResult == MQTT_SEND_FATAL) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                    unsubackFailureReason);
                                clientDisconnected = true;
                            }
#else
                            readUnsubscribe(client->buffer, packetEnd, packetStart, client);
#endif
                            break;
                        case PING:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (client->buffer[processedPackets] != 0xc0 ||
                                packetLength != 2) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                                clientDisconnected = true;
                                break;
                            }
#endif
                            enum metric_io_reason pingFailureReason;
                            enum mqttSendResult pingResult = sendPingresp(
                                client, &pingFailureReason);
                            if(pingResult == MQTT_SEND_FATAL){
                                fprintf(stderr, "Disconnecting client due to ping failure");
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
                                    pingFailureReason);
                                clientDisconnected = true;
                                break;
                            }
                            break;
                        case DISCONNECT:
                            fprintf(stderr, "Disconnecting client due to receiving DISCONNECT");
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            if (!client->connackSent ||
                                client->buffer[processedPackets] != 0xe0 ||
                                packetLength != 2) {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                    METRIC_IO_NONE);
                            } else {
                                disconnectClient(
                                    client, epollfd, mqttNowMs(),
                                    METRIC_MQTT_FINALIZATION_DISCONNECT_RECEIVED,
                                    METRIC_IO_NONE);
                            }
#else
                            char disconnectMsg[64];
                            snprintf(disconnectMsg, sizeof(disconnectMsg),
                                "%s protocol_action mqtt_disconnect\n", SERVER_ID);
                            sendMetric(disconnectMsg);
                            disconnectClient(
                                client, epollfd, mqttNowMs(),
                                METRIC_MQTT_FINALIZATION_DISCONNECT_RECEIVED,
                                METRIC_IO_NONE);
#endif
                            clientDisconnected = true;
                            break;
                        default:
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
                            disconnectClient(
                                client, epollfd, mqttNowMs(),
                                METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
                                METRIC_IO_NONE);
                            clientDisconnected = true;
#endif
                            break;
                    }
                    if (clientDisconnected) break;
                    processedPackets += packetLength;
                }
                if (!clientDisconnected) {
                    uint32_t leftover = client->bytesWrittenToBuffer - processedPackets;
                    if (leftover > 0) {
                        memmove(client->buffer, client->buffer + processedPackets, leftover);
                    }
                    client->bytesWrittenToBuffer = leftover;
                }
            }
            
        }
        
        // Detect dead clients and disconnect them
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
        if (mqttStopRequested != 0) {
            break;
        }
#endif
        for (struct mqttClient *c = clients, *tmp = NULL; c != NULL; c = tmp) {
            long long timeSinceLastActivityMs = now - c->lastActivityMs;
            tmp = c->hh.next;
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
            if (c->connectAccepted && c->keepAlive > 0 &&
                timeSinceLastActivityMs >= (long long)c->keepAlive * 1500LL) {
                disconnectClient(
                    c, epollfd, mqttNowMs(),
                    METRIC_MQTT_FINALIZATION_KEEP_ALIVE_TIMEOUT,
                    METRIC_IO_NONE);
            }
#else
            if ((now - c->lastPubrelMs > pubrelInterval) || (timeSinceLastActivityMs > c->keepAlive * 1400)) {
                bool success = sendPubrel(c, 1234);
                c->lastActivityMs = now;
                c->lastPubrelMs = now;

                if(!success) {
                    fprintf(stderr, "Disconnecting client due to inactivity");
                    disconnectClient(
                        c, epollfd, mqttNowMs(),
                        METRIC_MQTT_FINALIZATION_KEEP_ALIVE_TIMEOUT,
                        METRIC_IO_NONE);
                    continue;
                }
            }
#endif
        }
    }

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    while (clients != NULL) {
        disconnectClient(
            clients, epollfd, mqttNowMs(),
            METRIC_MQTT_FINALIZATION_SERVER_SHUTDOWN,
            METRIC_IO_NONE);
    }
#endif
    // closelog();
    close(epollfd);
    close(serverSock);
    return 0;
}
