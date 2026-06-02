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

    bool isV31 = memcmp(&buffer[offset], "MQIsdp", 6) == 0 ? true : false;
    if (memcmp(&buffer[offset], "MQTT", 4) != 0 && !isV31){
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
    printf("%s", msg);
    sendMetric(msg);

    // Connect Flags
    if (offset >= packetEnd) {
        fprintf(stderr, "No connect flags supplied");
        return 0x80;
    }
    uint8_t connect_flags = buffer[offset++];
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

    // Username
    char username[256] = {0};
    if (connect_flags & 0b10000000) {
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
    if (connect_flags & 0b1000000) {
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
    sendMetric(msg);
    return 0x00; // Success
}

void readSubscribe(uint8_t* buffer, uint32_t packetEnd, uint32_t offset, enum MqttVersion version) {
    // syslog(LOG_INFO, "Reading SUBSCRIBE request");
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "SUBSCRIBE request too short for fixed header");
        return;
    }

    // *packetId = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2; // packetId
    if (version == V5) {
        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            fprintf(stderr, "SUBSCRIBE Failed decoding varint");
            return;
        }

        // parse actual properties here if needed
        offset += varint;
    }

    if (offset + 3 > packetEnd) { // 2 bytes topic + 1 byte options
        fprintf(stderr, "SUBSCRIBE topic section too short");
        return;
    }

    uint16_t topicLength = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if (offset + topicLength + 1 > packetEnd) {
        fprintf(stderr, "SUBSCRIBE topic filter length exceeds packet size");
        return;
    }

    char topic[256];
    uint16_t safeLength = topicLength < 255 ? topicLength : 255;
    memcpy(topic, &buffer[offset], safeLength);
    topic[safeLength] = '\0';
    offset += topicLength;

    uint8_t options = buffer[offset++];
    uint8_t qos = options & 0b11;

    char msg[256];
    snprintf(msg, sizeof(msg), "%s SUBSCRIBE %.100s %d\n",
        SERVER_ID, topic, qos);
    printf("%s", msg);
    sendMetric(msg);

    // syslog(LOG_INFO, "Successfully read SUBSCRIBE request with topic: %s and QoS %d", topic, qos);
    return;
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

bool sendConnack(struct mqttClient* client, uint8_t reasonCode) {
    int size = client->version == V5 ? 8 : 4;
    uint8_t* arr = malloc(size);
    if (!arr) {
        fprintf(stderr, "malloc failed for connack packet");
        return false;
    } 

    if (client->version == V5) {
        arr[0] = 0x20;       // CONNACK fixed header
        arr[1] = 0x06;       // Remaining Length
        arr[2] = 0x00;       // Connect Acknowledge Flags (Session Present = 0)
        arr[3] = reasonCode; // Reason Code
        arr[4] = 0x03;       // Properties Length
        arr[5] = 0x21;       // Property ID: Receive Maximum
        arr[6] = 0x00;       // MSB
        arr[7] = 0x01;       // LSB (Receive Maximum = 1)
    } else {
        arr[0] = 0x20;       // CONNACK fixed header
        arr[1] = 0x02;       // Remaining Length
        arr[2] = 0x00;       // Connect Acknowledge Flags (Session Present = 0)
        arr[3] = reasonCode; // Return Code
    }

    ssize_t w = write(client->fd, arr, size);
    free(arr);
    if (w == -1) {
        fprintf(stderr, "sendConnack: write failed. May retry.");
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
    } else {
        char msg[256];
        snprintf(msg, sizeof(msg), "%s CONNACK\n",
            SERVER_ID);
        printf("%s", msg);
        sendMetric(msg);
        // syslog(LOG_INFO, "Sent CONNACK to client fd=%d\n", client->fd);
    }

    return true;
}

void readPublish(uint8_t* buffer, uint32_t packetEnd, uint32_t offset, enum MqttVersion version) {
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "PUBLISH packet too short for topic length");
        return;
    }

    uint16_t topicLen = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if (offset + topicLen > packetEnd) {
        fprintf(stderr, "PUBLISH topic exceeds packet bounds");
        return;
    }

    char topic[256] = {0};
    memcpy(topic, &buffer[offset], topicLen < 255 ? topicLen : 255);
    offset += topicLen;

    uint8_t qos = (buffer[0] & 0b00000110) >> 1;
    if (qos > 0) {
        if (offset + 2 > packetEnd) return;
        offset += 2; // packet id (don't care)
    }

    if(version == V5) {        
        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            return;
        }
    
        // Skip properties
        offset += varint;
    }

    // Remaining is payload
    if (offset >= packetEnd) return;

    char payload[512] = {0};
    uint32_t payloadLen = packetEnd - offset;
    uint32_t copyLen = payloadLen < sizeof(payload) - 1 ? payloadLen - 2: sizeof(payload) - 1;
    memcpy(payload, &buffer[offset], payloadLen < 511 ? payloadLen : 511);
    payload[copyLen] = '\0';

    char msg[256];
    snprintf(msg, sizeof(msg), "%s PUBLISH %.100s %d\n",
        SERVER_ID, topic, qos);
    sendMetric(msg);
    printf("PUBLISH received. Topic: %s, Payload: %s, QoS: %d\n", topic, payload, qos);
}

void readUnsubscribe(uint8_t* buffer, uint32_t packetEnd, uint32_t offset, enum MqttVersion version) {
    if (offset + 2 > packetEnd) {
        fprintf(stderr, "UNSUBSCRIBE packet too short");
        return;
    }

    uint16_t packetId = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;

    if (version == V5) {
        uint32_t varint;
        bool decodeSuccess = decodeVarint(buffer, packetEnd, &offset, &varint);
        if(!decodeSuccess) {
            return;
        }

        // Skip properties
        offset += varint;
    }

    while (offset + 2 <= packetEnd) {
        uint16_t topicLen = (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;

        if (offset + topicLen > packetEnd) return;

        char topic[256] = {0};
        memcpy(topic, &buffer[offset], topicLen < 255 ? topicLen : 255);
        offset += topicLen;

        char msg[256];
        snprintf(msg, sizeof(msg), "%s UNSUBSCRIBE %.200s\n",
            SERVER_ID, topic);
        sendMetric(msg);

        printf("UNSUBSCRIBE received for topic: %s (Packet ID: %u)\n", topic, packetId);
    }
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
    free(packet);
    if (w < 0) {
        fprintf(stderr, "sendPublish: write failed. May retry.");
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
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
    if (w == -1) {
        fprintf(stderr, "sendPubrel: write failed");
        return false;
    }

    // syslog(LOG_INFO, "Sent PUBREL to client fd=%d", client->fd);
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

bool sendPingresp(struct mqttClient* client) {
    uint8_t packet[2] = { 0xD0, 0x00 };
    ssize_t w = write(client->fd, packet, sizeof(packet));

    if (w == -1) {
        fprintf(stderr, "sendPingresp: write failed. May retry.");
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
    } else {
        fprintf(stderr, "Sent PINGRESP to client (fd=%d)\n", client->fd);
    }
    return true;
}

void disconnectClient(struct mqttClient* client, int epollFd, long long now){
    long long wastedTime = now - client->timeOfConnection;

    char msg[256];
    snprintf(msg, sizeof(msg), "%s disconnect %s %lld",
        SERVER_ID, client->ipaddr, wastedTime);

    printf("%s", msg);
    sendMetric(msg);

    epoll_ctl(epollFd, EPOLL_CTL_DEL, client->fd, NULL);
    deleteClient(client);
    close(client->fd);
    free(client);
}

enum Request determineRequest(uint8_t firstByte) {
    switch (firstByte >> 4)
    {
    case 0b0001:
        return CONNECT;
    case 0b0101:
        return PUBREC;
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
    setFdLimit(maxNoClients);
    signal(SIGPIPE, SIG_IGN);
    
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
    while(true) {
        long long now = currentTimeMs();

        // if (now - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
        //     heartbeatLog();
        //     lastHeartbeat = now;
        // }

        int nfds = epoll_wait(epollfd, eventsQueue, maxEvents, epollTimeoutInterval);
        if (nfds == -1) {
            fprintf(stderr, "epoll_wait");
            exit(EXIT_FAILURE);
        }

        // Update now, since epoll_wait made the value outdated. 
        now = currentTimeMs();
        for (int n = 0; n < nfds; ++n) {
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
                newClient->bytesWrittenToBuffer = 0;
                newClient->lastActivityMs = now;
                newClient->timeOfConnection = now;
                newClient->lastPubrelMs = now;
                newClient->keepAlive = 0; // Initial value. Will be updated after connect
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
                sendMetric(msg);
                // if(statsMqtt.mostConcurrentConnections < HASH_COUNT(clients)) {
                //     statsMqtt.mostConcurrentConnections = HASH_COUNT(clients);
                // }
            } else {
                struct mqttClient* client = lookupClient(currentFd);
                ssize_t bytesRead = read(currentFd,
                          client->buffer + client->bytesWrittenToBuffer, // Avoid overwriting existing data
                          sizeof(client->buffer) - client->bytesWrittenToBuffer);

                if(bytesRead == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        continue;
                    }
                    fprintf(stderr, "Failed reading. Disconnecting client. error: %s", strerror(errno));
                    disconnectClient(client, epollfd, now);
                    continue;
                }

                client->bytesWrittenToBuffer += bytesRead;

                if (client->bytesWrittenToBuffer >= sizeof(client->buffer)) {
                    fprintf(stderr, "Buffer full. Disconnecting client.");
                    disconnectClient(client, epollfd, now);
                    continue;
                }

                uint32_t packetLengths[maxPacketsPerClient]; // Length of each packet
                uint32_t packetStarts[maxPacketsPerClient]; // Points to the start of each packet, after the header values
                uint32_t packetCount = 0;

                calculateTotalPacketLengths(client->buffer, client->bytesWrittenToBuffer,
                            packetLengths, packetStarts, &packetCount);
                
                uint32_t processedPackets = 0;
                for (uint32_t i = 0; i < packetCount; i++) {
                    uint32_t packetLength = packetLengths[i];
                    uint32_t packetStart = packetStarts[i];
                    uint32_t packetEnd = packetStart + packetLength;
                    
                    if (packetLength == 0 || processedPackets + packetLength > client->bytesWrittenToBuffer) {
                        // syslog(LOG_INFO, "Incomplete packet");
                        break; // Incomplete packet
                    }

                    client->lastActivityMs = now;
                    enum Request request = determineRequest(client->buffer[processedPackets]);
                    bool pubSuccess = false;
                    switch (request) {
                        case CONNECT:
                            uint8_t reasonCodeConn = readConnreq(client->buffer, packetEnd, packetStart, client);
                            if(reasonCodeConn != 0x00) {
                                char msg[256];
                                snprintf(msg, sizeof(msg), "%s malformedConnect",
                                    SERVER_ID);
                                sendMetric(msg);
                            }
                            bool ackSuccess = sendConnack(client, reasonCodeConn);
                            if(!ackSuccess) {
                                fprintf(stderr, "Disconnecting client due to CONNACK failure");
                                disconnectClient(client, epollfd, now);
                                break;
                            }
                            pubSuccess = sendPublish(client, "$SYS/credentials", "username=admin password=admin");
                            if(!pubSuccess) {
                                fprintf(stderr, "Disconnecting client due to publish failure");
                                disconnectClient(client, epollfd, now);
                            }
                            break;
                        case SUBSCRIBE:
                            readSubscribe(client->buffer, packetEnd, packetStart, client->version);
                            break;
                        case PUBREC:
                            readPubrec(client->buffer, packetEnd, packetStart, client);
                            break;
                        case PUBLISH:
                            readPublish(client->buffer, packetEnd, packetStart, client->version);
                            break;
                        case PUBCOMP:
                            readPubcomp(packetEnd, packetStart);
                            pubSuccess = sendPublish(client, "$SYS/confidential", "username=admin123 password=admin321");
                            if(!pubSuccess) {
                                fprintf(stderr, "Disconnecting client due to publish failure");
                                disconnectClient(client, epollfd, now);
                            }
                            break;
                        case UNSUBSCRIBE:
                            readUnsubscribe(client->buffer, packetEnd, packetStart, client->version);
                            break;
                        case PING:
                            bool pingSuccess = sendPingresp(client);
                            if(!pingSuccess){
                                fprintf(stderr, "Disconnecting client due to ping failure");
                                disconnectClient(client, epollfd, now);
                                break;
                            }
                            break;
                        case DISCONNECT:
                            fprintf(stderr, "Disconnecting client due to receiving DISCONNECT");
                            disconnectClient(client, epollfd, now);
                            break;
                        default:
                            break;
                    }
                    processedPackets += packetLength;
                }
                uint32_t leftover = client->bytesWrittenToBuffer - processedPackets;
                if (leftover > 0) {
                    memmove(client->buffer, client->buffer + processedPackets, leftover);
                }
                client->bytesWrittenToBuffer = leftover;
            }
            
        }
        
        // Detect dead clients and disconnect them
        for (struct mqttClient *c = clients, *tmp = NULL; c != NULL; c = tmp) {
            long long timeSinceLastActivityMs = now - c->lastActivityMs;
            tmp = c->hh.next;
            if ((now - c->lastPubrelMs > pubrelInterval) || (timeSinceLastActivityMs > c->keepAlive * 1400)) {
                bool success = sendPubrel(c, 1234);
                c->lastActivityMs = now;
                c->lastPubrelMs = now;

                if(!success) {
                    fprintf(stderr, "Disconnecting client due to inactivity");
                    disconnectClient(c, epollfd, now);
                    continue;
                }
            }
        }
    }

    // closelog();
    close(serverSock);
    return 0;
}