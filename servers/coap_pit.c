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
#include "../shared/metric_events.h"
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
#include <pthread.h>
#endif

#define CLASS_REQUEST 0x0
#define DETAIL_GET 0x1
#define DETAIL_POST 0x2
#define DETAIL_PUT 0x3
#define DETAIL_DELETE 0x4
#define TYPE_CONFIRMABLE 0x0
#define TYPE_NON_CONFIRMABLE 0x1
#define TYPE_ACK 0x2
#define TYPE_RST 0x3
#define MAX_BUF_LEN 1024
#define SERVER_ID "CoAP"

struct coapClient *clients = NULL;

int port = 5683;
int timeout = -1;
int delay = 1000;
int ACK_TIMEOUT = 2000;
int MAX_RETRANSMIT = 4;
int maxNoClients = 4096;
int sockFd;
#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
static unsigned long coapEventCounter = 0;
#endif

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
struct coapMetricExchange {
    struct sockaddr_in endpoint;
    socklen_t endpointLength;
    uint8_t request[MAX_BUF_LEN];
    size_t requestLength;
    uint8_t token[8];
    uint8_t tokenLength;
    uint8_t requestType;
    uint16_t requestMessageId;
    uint64_t requestStartedMs;
    uint64_t nextActionMs;
    uint8_t response[32];
    size_t responseLength;
    uint16_t responseMessageId;
    uint32_t retransmitAttempts;
    bool requestActive;
    bool conActive;
    uint64_t conStartedMs;
    struct coapMetricExchange *next;
};

enum coapMetricSendResult {
    COAP_METRIC_SEND_COMPLETE,
    COAP_METRIC_SEND_PENDING,
    COAP_METRIC_SEND_FAILED,
};

enum coapProtocolDatagramBoundary {
    COAP_PROTOCOL_DATAGRAM_EMPTY_ACK,
    COAP_PROTOCOL_DATAGRAM_INITIAL_RESPONSE,
    COAP_PROTOCOL_DATAGRAM_RETRANSMISSION,
};

static pthread_mutex_t coapMetricStateLock = PTHREAD_MUTEX_INITIALIZER;
static struct coapMetricExchange *coapMetricExchanges = NULL;
static uint32_t coapActiveRequestExchanges = 0;
static uint32_t coapActiveCONResponseExchanges = 0;
static uint32_t coapMetricExchangeCount = 0;
static uint16_t coapNextResponseMessageId = 1;
static bool coapMetricEmitterReady = false;
#ifdef EVENTHORIZON_COAP_TEST_RUNTIME
static bool coapTestSendInjectionConsumed = false;
#endif

static uint64_t coapMonotonicNowMs(void) {
    long long now = currentMonotonicTimeMs();
    return now > 0 ? (uint64_t)now : 0;
}

static uint64_t coapElapsedMs(uint64_t started, uint64_t ended) {
    return ended >= started ? ended - started : 0;
}

static bool coapSameEndpoint(const struct sockaddr_in *left,
                             const struct sockaddr_in *right) {
    return left->sin_family == right->sin_family &&
           left->sin_port == right->sin_port &&
           left->sin_addr.s_addr == right->sin_addr.s_addr;
}

static bool coapSameToken(const struct coapMetricExchange *exchange,
                          const uint8_t *token, uint8_t tokenLength) {
    return exchange->tokenLength == tokenLength &&
           memcmp(exchange->token, token, tokenLength) == 0;
}

static struct coapMetricExchange *coapFindRequestCollision(
    const struct sockaddr_in *endpoint,
    const uint8_t *request,
    size_t requestLength,
    const uint8_t *token,
    uint8_t tokenLength,
    uint16_t messageId) {
    for (struct coapMetricExchange *exchange = coapMetricExchanges;
         exchange;
         exchange = exchange->next) {
        if (!coapSameEndpoint(&exchange->endpoint, endpoint)) {
            continue;
        }
        if ((exchange->requestMessageId == messageId) ||
            coapSameToken(exchange, token, tokenLength) ||
            (exchange->requestLength == requestLength &&
             memcmp(exchange->request, request, requestLength) == 0)) {
            return exchange;
        }
    }
    return NULL;
}

static struct coapMetricExchange *coapFindCONExchange(
    const struct sockaddr_in *endpoint, uint16_t messageId) {
    for (struct coapMetricExchange *exchange = coapMetricExchanges;
         exchange;
         exchange = exchange->next) {
        if (exchange->conActive &&
            exchange->responseMessageId == messageId &&
            coapSameEndpoint(&exchange->endpoint, endpoint)) {
            return exchange;
        }
    }
    return NULL;
}

static void coapAddMetricExchange(struct coapMetricExchange *exchange) {
    exchange->next = coapMetricExchanges;
    coapMetricExchanges = exchange;
    coapMetricExchangeCount += 1;
}

static void coapDeleteMetricExchange(struct coapMetricExchange *exchange) {
    struct coapMetricExchange **cursor = &coapMetricExchanges;
    while (*cursor && *cursor != exchange) {
        cursor = &(*cursor)->next;
    }
    if (*cursor == exchange) {
        *cursor = exchange->next;
        if (coapMetricExchangeCount > 0) {
            coapMetricExchangeCount -= 1;
        }
        free(exchange);
    }
}

static uint16_t coapAllocateResponseMessageId(void) {
    uint16_t result = coapNextResponseMessageId;
    coapNextResponseMessageId = (uint16_t)(coapNextResponseMessageId + 1);
    return result;
}

static size_t coapEncodeBlockResponse(
    uint8_t type,
    uint16_t messageId,
    const uint8_t *token,
    uint8_t tokenLength,
    uint32_t blockNumber,
    uint8_t *response,
    size_t capacity) {
    uint32_t blockOptionValue = (blockNumber << 4) | (1U << 3) | 0x02U;
    uint8_t blockLength = blockOptionValue <= 0xFFU ? 1 :
                          blockOptionValue <= 0xFFFFU ? 2 : 3;
    size_t responseLength = 4U + tokenLength + 2U + blockLength + 1U + 5U;
    if (tokenLength > 8 || responseLength > capacity) {
        return 0;
    }

    size_t index = 0;
    response[index++] = (uint8_t)((1U << 6) | ((type & 0x03U) << 4) |
                                  tokenLength);
    response[index++] = 0x45;
    response[index++] = (uint8_t)(messageId >> 8);
    response[index++] = (uint8_t)(messageId & 0xFFU);
    memcpy(&response[index], token, tokenLength);
    index += tokenLength;

    response[index++] = (uint8_t)((13U << 4) | blockLength);
    response[index++] = 10;
    if (blockLength == 1) {
        response[index++] = (uint8_t)blockOptionValue;
    } else if (blockLength == 2) {
        response[index++] = (uint8_t)(blockOptionValue >> 8);
        response[index++] = (uint8_t)blockOptionValue;
    } else {
        response[index++] = (uint8_t)(blockOptionValue >> 16);
        response[index++] = (uint8_t)(blockOptionValue >> 8);
        response[index++] = (uint8_t)blockOptionValue;
    }
    response[index++] = 0xFF;
    memcpy(&response[index], "AAAAA", 5);
    index += 5;
    return index;
}

static enum coapMetricSendResult coapClassifySendResult(
    ssize_t sent, size_t expectedLength, int sendError) {
    if (sent == (ssize_t)expectedLength) {
        return COAP_METRIC_SEND_COMPLETE;
    }
    if (sent < 0 && (sendError == EINTR || sendError == EAGAIN ||
                     sendError == EWOULDBLOCK)) {
        return COAP_METRIC_SEND_PENDING;
    }
    return COAP_METRIC_SEND_FAILED;
}

static ssize_t coapSendProtocolDatagram(
    enum coapProtocolDatagramBoundary boundary,
    const uint8_t *datagram,
    size_t datagramLength,
    const struct sockaddr_in *endpoint,
    socklen_t endpointLength) {
#ifdef EVENTHORIZON_COAP_TEST_RUNTIME
    const char *injection = getenv("EVENTHORIZON_COAP_TEST_SEND_INJECTION");
    bool injectShort = injection && !coapTestSendInjectionConsumed &&
        ((boundary == COAP_PROTOCOL_DATAGRAM_INITIAL_RESPONSE &&
          strcmp(injection, "initial_short") == 0) ||
         (boundary == COAP_PROTOCOL_DATAGRAM_RETRANSMISSION &&
          strcmp(injection, "retransmission_short") == 0));
    if (injectShort) {
        coapTestSendInjectionConsumed = true;
        return datagramLength > 0 ? (ssize_t)(datagramLength - 1) : 0;
    }
#else
    (void)boundary;
#endif
    return sendto(
        sockFd, datagram, datagramLength, 0,
        (const struct sockaddr *)endpoint, endpointLength);
}

static void coapEmitWriteFailure(ssize_t sent, int sendError) {
    if (!coapMetricEmitterReady) {
        return;
    }
    enum metric_io_reason reason = sent >= 0
        ? METRIC_IO_OTHER
        : metric_io_reason_from_unrecoverable_errno(sendError, false);
    (void)metric_event_coap_write_error(reason);
}

static void coapAcceptRequestExchange(struct coapMetricExchange *exchange) {
    pthread_mutex_lock(&coapMetricStateLock);
    exchange->requestActive = true;
    coapActiveRequestExchanges += 1;
    if (coapMetricEmitterReady) {
        (void)metric_event_coap_request_received(
            coapActiveRequestExchanges);
    }
    pthread_mutex_unlock(&coapMetricStateLock);
}

static void coapFinalizeRequestWithoutCON(
    struct coapMetricExchange *exchange,
    enum metric_coap_request_outcome outcome,
    uint64_t observedMs) {
    pthread_mutex_lock(&coapMetricStateLock);
    if (!exchange->requestActive) {
        pthread_mutex_unlock(&coapMetricStateLock);
        return;
    }
    exchange->requestActive = false;
    if (coapActiveRequestExchanges > 0) {
        coapActiveRequestExchanges -= 1;
    }
    if (coapMetricEmitterReady) {
        (void)metric_event_coap_request_finalized(
            outcome,
            coapElapsedMs(exchange->requestStartedMs, observedMs),
            coapActiveRequestExchanges);
    }
    pthread_mutex_unlock(&coapMetricStateLock);
}

static void coapStartCONResponseExchange(
    struct coapMetricExchange *exchange, uint64_t observedMs) {
    pthread_mutex_lock(&coapMetricStateLock);
    if (!exchange->requestActive || exchange->conActive) {
        pthread_mutex_unlock(&coapMetricStateLock);
        return;
    }
    exchange->requestActive = false;
    exchange->conActive = true;
    exchange->conStartedMs = observedMs;
    if (coapActiveRequestExchanges > 0) {
        coapActiveRequestExchanges -= 1;
    }
    coapActiveCONResponseExchanges += 1;
    if (coapMetricEmitterReady) {
        (void)metric_event_coap_con_response_sent(
            coapElapsedMs(exchange->requestStartedMs, observedMs),
            coapActiveRequestExchanges,
            coapActiveCONResponseExchanges);
    }
    pthread_mutex_unlock(&coapMetricStateLock);
}

static void coapFinalizeCONResponseExchange(
    struct coapMetricExchange *exchange,
    enum metric_coap_con_outcome outcome,
    uint64_t observedMs) {
    pthread_mutex_lock(&coapMetricStateLock);
    if (!exchange->conActive) {
        pthread_mutex_unlock(&coapMetricStateLock);
        return;
    }
    exchange->conActive = false;
    if (coapActiveCONResponseExchanges > 0) {
        coapActiveCONResponseExchanges -= 1;
    }
    if (coapMetricEmitterReady) {
        (void)metric_event_coap_con_response_finalized(
            outcome,
            coapElapsedMs(exchange->conStartedMs, observedMs),
            coapActiveCONResponseExchanges);
    }
    pthread_mutex_unlock(&coapMetricStateLock);
}

static uint64_t coapInitialResponseDelayMs(void) {
    return delay > 0 ? (uint64_t)delay : 0;
}

static uint64_t coapACKWaitMs(uint32_t retransmitAttempts) {
    uint64_t base = ACK_TIMEOUT > 0 ? (uint64_t)ACK_TIMEOUT : 1;
    uint32_t shift = retransmitAttempts < 31 ? retransmitAttempts : 31;
    uint64_t interval = base << shift;
    return interval > INT_MAX ? INT_MAX : interval;
}

static void coapProcessInitialResponse(
    struct coapMetricExchange *exchange, uint64_t now) {
    errno = 0;
    ssize_t sent = coapSendProtocolDatagram(
        COAP_PROTOCOL_DATAGRAM_INITIAL_RESPONSE,
        exchange->response,
        exchange->responseLength,
        &exchange->endpoint,
        exchange->endpointLength);
    int sendError = errno;
    enum coapMetricSendResult result = coapClassifySendResult(
        sent, exchange->responseLength, sendError);

    if (result == COAP_METRIC_SEND_PENDING) {
        uint64_t retryDelay = coapInitialResponseDelayMs();
        exchange->nextActionMs = now + (retryDelay > 0 ? retryDelay : 1);
        return;
    }
    if (result == COAP_METRIC_SEND_FAILED) {
        coapFinalizeRequestWithoutCON(
            exchange, METRIC_COAP_REQUEST_TERMINATED, now);
        coapEmitWriteFailure(sent, sendError);
        coapDeleteMetricExchange(exchange);
        return;
    }

    if (exchange->requestType == TYPE_NON_CONFIRMABLE) {
        coapFinalizeRequestWithoutCON(
            exchange, METRIC_COAP_REQUEST_RESPONSE_SENT, now);
        coapDeleteMetricExchange(exchange);
        return;
    }

    coapStartCONResponseExchange(exchange, now);
    exchange->nextActionMs = now + coapACKWaitMs(0);
}

static void coapProcessCONResponse(
    struct coapMetricExchange *exchange, uint64_t now) {
    if (exchange->retransmitAttempts >= (uint32_t)MAX_RETRANSMIT) {
        coapFinalizeCONResponseExchange(
            exchange, METRIC_COAP_CON_RETRY_EXHAUSTED, now);
        coapDeleteMetricExchange(exchange);
        return;
    }

    errno = 0;
    ssize_t sent = coapSendProtocolDatagram(
        COAP_PROTOCOL_DATAGRAM_RETRANSMISSION,
        exchange->response,
        exchange->responseLength,
        &exchange->endpoint,
        exchange->endpointLength);
    int sendError = errno;
    enum coapMetricSendResult result = coapClassifySendResult(
        sent, exchange->responseLength, sendError);
    if (result == COAP_METRIC_SEND_PENDING) {
        exchange->nextActionMs = now + 1;
        return;
    }

    exchange->retransmitAttempts += 1;
    if (result == COAP_METRIC_SEND_COMPLETE) {
        if (coapMetricEmitterReady) {
            (void)metric_event_coap_con_response_retransmitted();
        }
    } else {
        coapEmitWriteFailure(sent, sendError);
    }
    exchange->nextActionMs = now +
        coapACKWaitMs(exchange->retransmitAttempts);
}

static void coapProcessDueMetricExchanges(uint64_t now) {
    bool processed;
    do {
        processed = false;
        for (struct coapMetricExchange *exchange = coapMetricExchanges;
             exchange;
             exchange = exchange->next) {
            if (exchange->nextActionMs > now) {
                continue;
            }
            if (exchange->requestActive) {
                coapProcessInitialResponse(exchange, now);
            } else if (exchange->conActive) {
                coapProcessCONResponse(exchange, now);
            }
            processed = true;
            break;
        }
    } while (processed);
}

static int coapNextPollTimeout(uint64_t now) {
    bool found = false;
    uint64_t earliest = 0;
    for (struct coapMetricExchange *exchange = coapMetricExchanges;
         exchange;
         exchange = exchange->next) {
        if (!found || exchange->nextActionMs < earliest) {
            found = true;
            earliest = exchange->nextActionMs;
        }
    }
    if (!found) {
        return -1;
    }
    if (earliest <= now) {
        return 0;
    }
    uint64_t remaining = earliest - now;
    return remaining > INT_MAX ? INT_MAX : (int)remaining;
}

static bool coapIsSupportedRootGET(
    const uint8_t *request,
    size_t requestLength,
    uint8_t tokenLength,
    uint8_t type,
    uint8_t code) {
    return (type == TYPE_CONFIRMABLE || type == TYPE_NON_CONFIRMABLE) &&
           code == DETAIL_GET &&
           requestLength == (size_t)(4 + tokenLength) &&
           request != NULL;
}

static void coapHandleControlMessage(
    const uint8_t *message,
    size_t messageLength,
    const struct sockaddr_in *endpoint,
    uint8_t type,
    uint8_t code,
    uint8_t tokenLength,
    uint16_t messageId,
    uint64_t now) {
    if ((type != TYPE_ACK && type != TYPE_RST) || code != 0 ||
        tokenLength != 0 || messageLength != 4) {
        return;
    }
    (void)message;
    struct coapMetricExchange *exchange =
        coapFindCONExchange(endpoint, messageId);
    if (!exchange) {
        return;
    }
    coapFinalizeCONResponseExchange(
        exchange,
        type == TYPE_ACK ? METRIC_COAP_CON_ACK_RECEIVED
                         : METRIC_COAP_CON_RST_RECEIVED,
        now);
    coapDeleteMetricExchange(exchange);
}

static void coapHandleSupportedGET(
    const uint8_t *request,
    size_t requestLength,
    const struct sockaddr_in *endpoint,
    socklen_t endpointLength,
    uint8_t type,
    uint8_t tokenLength,
    uint16_t messageId,
    uint64_t now) {
    const uint8_t *token = &request[4];
    if (coapFindRequestCollision(
            endpoint, request, requestLength, token, tokenLength,
            messageId)) {
        return;
    }
    if (coapMetricExchangeCount >= (uint32_t)maxNoClients) {
        return;
    }

    struct coapMetricExchange *exchange = calloc(1, sizeof(*exchange));
    if (!exchange) {
        return;
    }
    exchange->endpoint = *endpoint;
    exchange->endpointLength = endpointLength;
    memcpy(exchange->request, request, requestLength);
    exchange->requestLength = requestLength;
    memcpy(exchange->token, token, tokenLength);
    exchange->tokenLength = tokenLength;
    exchange->requestType = type;
    exchange->requestMessageId = messageId;
    exchange->requestStartedMs = now;
    exchange->nextActionMs = now + coapInitialResponseDelayMs();
    exchange->responseMessageId = coapAllocateResponseMessageId();
    uint8_t responseType = type == TYPE_NON_CONFIRMABLE
        ? TYPE_NON_CONFIRMABLE
        : TYPE_CONFIRMABLE;
    exchange->responseLength = coapEncodeBlockResponse(
        responseType,
        exchange->responseMessageId,
        exchange->token,
        exchange->tokenLength,
        0,
        exchange->response,
        sizeof(exchange->response));
    if (exchange->responseLength == 0) {
        free(exchange);
        return;
    }

    coapAddMetricExchange(exchange);
    coapAcceptRequestExchange(exchange);

    if (type == TYPE_CONFIRMABLE) {
        uint8_t ack[4] = {
            (uint8_t)((1U << 6) | (TYPE_ACK << 4)),
            0,
            (uint8_t)(messageId >> 8),
            (uint8_t)messageId,
        };
        errno = 0;
        ssize_t sent = coapSendProtocolDatagram(
            COAP_PROTOCOL_DATAGRAM_EMPTY_ACK,
            ack,
            sizeof(ack),
            endpoint,
            endpointLength);
        int sendError = errno;
        enum coapMetricSendResult result = coapClassifySendResult(
            sent, sizeof(ack), sendError);
        if (result == COAP_METRIC_SEND_FAILED) {
            coapEmitWriteFailure(sent, sendError);
        }
    }
}

static int runJSONCoAPServer(void) {
    struct pollfd pollFd;
    memset(&pollFd, 0, sizeof(pollFd));
    pollFd.fd = sockFd;
    pollFd.events = POLLIN;

    while (1) {
        uint64_t now = coapMonotonicNowMs();
        coapProcessDueMetricExchanges(now);
        int pollTimeout = coapNextPollTimeout(now);
        int pollResult = poll(&pollFd, 1, pollTimeout);
        if (pollResult < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "Poll error with error %s", strerror(errno));
            continue;
        }
        if (pollResult == 0 || !(pollFd.revents & POLLIN)) {
            continue;
        }

        struct sockaddr_in endpoint;
        memset(&endpoint, 0, sizeof(endpoint));
        socklen_t endpointLength = sizeof(endpoint);
        uint8_t request[MAX_BUF_LEN];
        ssize_t received = recvfrom(
            sockFd, request, sizeof(request), 0,
            (struct sockaddr *)&endpoint, &endpointLength);
        now = coapMonotonicNowMs();
        if (received < 4) {
            continue;
        }

        uint8_t version = (request[0] >> 6) & 0x03U;
        uint8_t type = (request[0] >> 4) & 0x03U;
        uint8_t tokenLength = request[0] & 0x0FU;
        uint8_t code = request[1];
        uint16_t messageId =
            (uint16_t)(((uint16_t)request[2] << 8) | request[3]);
        if (version != 1 || tokenLength > 8 ||
            received < (ssize_t)(4 + tokenLength)) {
            continue;
        }

        if (type == TYPE_ACK || type == TYPE_RST) {
            coapHandleControlMessage(
                request, (size_t)received, &endpoint, type, code,
                tokenLength, messageId, now);
            continue;
        }
        if (!coapIsSupportedRootGET(
                request, (size_t)received, tokenLength, type, code)) {
            continue;
        }
        coapHandleSupportedGET(
            request, (size_t)received, &endpoint, endpointLength, type,
            tokenLength, messageId, now);
    }
    return 0;
}
#endif

#ifndef EVENTHORIZON_JSON_METRIC_EVENTS
static void makeCoapEventId(char *buffer, size_t len, long long eventMs) {
    coapEventCounter += 1;
    session_events_make_request_id(buffer, len, "coap", coapEventCounter, eventMs);
}

static const char *coapTypeName(uint8_t type) {
    switch (type) {
        case TYPE_CONFIRMABLE:
            return "confirmable";
        case TYPE_NON_CONFIRMABLE:
            return "non_confirmable";
        case TYPE_ACK:
            return "ack";
        case TYPE_RST:
            return "reset";
        default:
            return "unknown";
    }
}

static const char *coapMethodName(uint8_t class, uint8_t detail) {
    if (class != CLASS_REQUEST) {
        return "non_request";
    }

    switch (detail) {
        case DETAIL_GET:
            return "GET";
        case DETAIL_POST:
            return "POST";
        case DETAIL_PUT:
            return "PUT";
        case DETAIL_DELETE:
            return "DELETE";
        default:
            return "unknown";
    }
}

static void emitCoapAction(const char *eventId, const char *action, const char *fields) {
    session_events_write_action("coap", eventId, action, fields);
}

static void emitCoapSendEvent(const char *eventId, const char *responseKind,
                              int bytesSent, long long handlingDurationMs,
                              unsigned int interactionDepth) {
    char fields[256];
    snprintf(fields, sizeof(fields),
        "\"transport\":\"udp\",\"response_kind\":\"%s\",\"bytes_sent\":%d,\"write_result\":\"%s\",\"handling_duration_ms\":%lld,\"interaction_depth\":%u",
        responseKind,
        bytesSent > 0 ? bytesSent : 0,
        bytesSent >= 0 ? "success" : "write_error",
        handlingDurationMs < 0 ? 0 : handlingDurationMs,
        interactionDepth);
    emitCoapAction(eventId, "response_sent", fields);
}
#endif

void addClient(struct coapClient *client) {
    HASH_ADD(hh, clients, clientAddr, sizeof(struct sockaddr_in), client);
}

void deleteClient(struct coapClient *client) {
    HASH_DEL(clients, client);
    free(client);
}

struct coapClient *findExistingClient(struct sockaddr_in *addr) {
    struct coapClient *result = NULL;
    HASH_FIND(hh, clients, addr, sizeof(struct sockaddr_in), result);
    return result;
}

int sendCoapBlockResponse(uint16_t messageId, uint8_t* token, uint8_t tkl, uint32_t* blockNumber, struct sockaddr_in* addr, socklen_t addrLen) {
    if(*blockNumber > 0xFFFFF) {
        *blockNumber = 0;
    }
    // Block2 Option (delta = 23, length = 1)
    // NUM(20 bits) | (M=1) | SZX=2(64 bytes)
    uint32_t block_opt_value = (*blockNumber << 4) | (0b1 << 3) | 0x02;
    uint8_t block_len = (block_opt_value <= 0xFF) ? 1 :
                        (block_opt_value <= 0xFFFF) ? 2 : 3;
    
    int payloadLength = 5;
    int responseLength = 4           // base CoAP header
                   + tkl             // token length
                   + 1               // option delta+length byte
                   + 1               // extended delta byte
                   + block_len       // block2 value (1–3 bytes)
                   + 1               // payload marker
                   + payloadLength;  // actual payload
    char response[responseLength];
    
    // Version (1) | Type (CON) | TKL
    response[0] = (0b01 << 6) | (0b0 << 4) | (tkl & 0b1111);;
    // class (2) | detail (5). Content response
    response[1] = (0b010 << 5) | (0b101);
    response[2] = (messageId >> 8) & 0xFF;
    response[3] = messageId & 0xFF;

    int index = 4;

    // Token
    for (int i = 0; i < tkl; i++) {
        response[index++] = token[i];
    }

    // Option Delta 13 | block length
    response[index++] = (0b1101 << 4) | block_len;
    response[index++] = 23 - 13;  // Block2 option (rfc7959 sect. 6)
    if (block_len == 1) {
        response[index++] = block_opt_value & 0xFF;
    } else if (block_len == 2) {
        response[index++] = (block_opt_value >> 8) & 0xFF;
        response[index++] = block_opt_value & 0xFF;
    } else {
        response[index++] = (block_opt_value >> 16) & 0xFF;
        response[index++] = (block_opt_value >> 8) & 0xFF;
        response[index++] = block_opt_value & 0xFF;
    }

    // Payload marker
    response[index++] = 0xFF;

    // Payload
    for (int i = 0; i < payloadLength; i++) {
        response[index++] = 'A';
    }

    return sendto(sockFd, response, index, 0, (struct sockaddr *)addr, addrLen);
}

int sendPing(uint16_t messageId, struct sockaddr_in* addr, socklen_t addrLen) {
    uint8_t ping[4];
    ping[0] = (1 << 6) | (0 << 4) | 0;      // Version=1, Type=CON, TKL=0
    ping[1] = 0x00;                         // Code = 0.00 (Empty)
    ping[2] = (messageId >> 8) & 0xFF;      // Message ID MSB
    ping[3] = messageId & 0xFF;             // Message ID LSB

    return sendto(sockFd, ping, sizeof(ping), 0, (struct sockaddr *)addr, addrLen);
}

int main(int argc, char* argv[]) {
    setbuf(stdout, NULL);

    // testing
    // char msg[256];
    // snprintf(msg, sizeof(msg), "%s connect %s\n",
    //     SERVER_ID, "17.117.247.220");
    // fprintf(stderr, "%s", msg);
    // sendMetric(msg);
    //     // testing
    // snprintf(msg, sizeof(msg), "%s connect %s\n",
    //     SERVER_ID, "74.17.158.179");
    // fprintf(stderr, "%s", msg);
    // sendMetric(msg);
    (void)argc;
    port = atoi(argv[1]);
    delay = atoi(argv[2]);
    ACK_TIMEOUT = atoi(argv[3]);
    MAX_RETRANSMIT = atoi(argv[4]);
    maxNoClients = atoi(argv[5]);
    struct sockaddr_in serverAddr;
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    coapMetricEmitterReady = metric_event_emitter_init(
        getenv("EVENTHORIZON_METRIC_SOCKET"));
#else
    heap_init(&clientQueueCoap, maxNoClients);
#endif
    session_events_init(NULL);

    if ((sockFd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        fprintf(stderr, "SSDP Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Bind to all interfaces and ports
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(port);

    // Join the multicast group
    // struct ip_mreq mreq;
    // mreq.imr_multiaddr.s_addr = inet_addr("224.0.1.187");
    // mreq.imr_interface.s_addr = INADDR_ANY;
    // setsockopt(sockFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));

    if (bind(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        fprintf(stderr, "Bind failed");
        close(sockFd);
        exit(EXIT_FAILURE);
    }

    printf("CoAP listener started on port %d\n", port);

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    return runJSONCoAPServer();
#else
    struct pollfd pollFd;
    memset(&pollFd, 0, sizeof(pollFd));
    pollFd.fd = sockFd;
    pollFd.events = POLLIN;

    while (1) {
        long long now = currentTimeMs();

        while (clientQueueCoap.size > 0) {
            if(clientQueueCoap.heapArray[0]->sendNext <= now){
                struct baseClient *bc = heap_pop(&clientQueueCoap);
                struct coapClient *c = (struct coapClient *)bc;
                
                // Handle retransmits
                if(!c->receivedAck || !c->receivedRst) {
                    if(c->retransmits < MAX_RETRANSMIT) {
                        c->base.sendNext = now + (ACK_TIMEOUT << (c->retransmits));
                        c->base.timeConnected += (ACK_TIMEOUT << (c->retransmits));
                        c->retransmits += 1;

                        if(!c->receivedAck) {
                            int out = sendCoapBlockResponse(c->messageId, c->token, c->tkl, &c->blockNumber, &c->clientAddr, c->addrLen);
                            c->interactionDepth += 1;
                            emitCoapSendEvent(c->sessionId, "block2_retransmit", out, 0, c->interactionDepth);
                        } else {
                            int out = sendPing(c->messageId, &c->clientAddr, c->addrLen);
                            c->interactionDepth += 1;
                            emitCoapSendEvent(c->sessionId, "ping_retransmit", out, 0, c->interactionDepth);
                        }

                        // printf("Token contents: ");
                        // for (int i = 0; i < 8; i++) {
                        //     printf("%u ", c->token[i]);
                        // }
                        // printf("\n");
                        // printf("Sent block2 due to not receiving an ACK with out=%d messageId=%u tkl=%d blockNumber=%d\n", out, c->messageId, c->tkl, c->blockNumber);
                        heap_insert(&clientQueueCoap, (struct baseClient *)c);
                        continue;
                    } else {
                        // Disconnect client
                        long long timeTrapped = c->base.timeConnected - (ACK_TIMEOUT * ((0b1 << MAX_RETRANSMIT) - 1));
                        char msg[256];
                        snprintf(msg, sizeof(msg), "%s disconnect %s %lld\n",
                            SERVER_ID, c->base.ipaddr, timeTrapped);
                        printf("%s", msg);
                        sendMetric(msg);
                        deleteClient(c);
                        continue;
                    }
                } 
                
                if (c->receivedGet) {
                    int out = sendCoapBlockResponse(c->messageId, c->token, c->tkl, &c->blockNumber, &c->clientAddr, c->addrLen);
                    c->interactionDepth += 1;
                    emitCoapSendEvent(c->sessionId, "block2", out, 0, c->interactionDepth);
                    c->blockNumber += 1;
                    c->receivedAck = false;
                } else if (c->receivedRst) {
                    int out = sendPing(c->messageId, &c->clientAddr, c->addrLen);
                    c->interactionDepth += 1;
                    emitCoapSendEvent(c->sessionId, "ping", out, 0, c->interactionDepth);
                    c->receivedRst = false;
                } 
                
                c->base.timeConnected += delay;
                c->messageId += 1;
                c->base.sendNext = now + delay;
                heap_insert(&clientQueueCoap, (struct baseClient *)c);
            } else {
                timeout = clientQueueCoap.heapArray[0]->sendNext - now;
                break;
            }
        }

        int pollResult = poll(&pollFd, 1, timeout);
        now = currentTimeMs();
        if (pollResult < 0) {
            fprintf(stderr, "Poll error with error %s", strerror(errno));
            continue;
        }

        if (pollFd.revents & POLLIN) {
            struct sockaddr_in clientAddr;
            socklen_t addrLen = sizeof(clientAddr);
            char buffer[1024];

            int len = recvfrom(sockFd, buffer, MAX_BUF_LEN, 0, (struct sockaddr *)&clientAddr, &addrLen);
            long long requestStartMs = currentTimeMs();
            if(len < 4) {
                // Too short or something went wrong
                if (len > 0) {
                    char eventId[SESSION_EVENT_ID_LEN];
                    char fields[256];
                    makeCoapEventId(eventId, sizeof(eventId), requestStartMs);
                    snprintf(fields, sizeof(fields),
                        "\"transport\":\"udp\",\"bytes_received\":%d,\"malformed_reason\":\"too_short\",\"handling_duration_ms\":0",
                        len);
                    emitCoapAction(eventId, "malformed_request", fields);
                }
                continue;
            }

            uint8_t version = (buffer[0] >> 6) & 0b11;
            uint8_t type = (buffer[0] >> 4) & 0b11;
            uint8_t code = buffer[1];
            uint8_t class = (code >> 5) & 0b111;
            uint8_t detail = code & 0b11111;
            uint8_t tkl = buffer[0] & 0b1111;
            uint16_t msgId = (buffer[2] << 8) | buffer[3];
            uint8_t token[8] = {0};

            printf("Incoming request from %s:%d\n", inet_ntoa(clientAddr.sin_addr), ntohs(clientAddr.sin_port));

            // Header fields
            printf("Header:\n");
            printf("  Version : %u\n", version);
            printf("  Type    : %u\n", type);
            printf("  TKL     : %u\n", tkl);
            printf("  Code    : 0x%02X (Class: %u, Detail: %u)\n", code, class, detail);
            printf("  Msg ID  : %u\n", msgId);
            
            // Token (if any)
            printf("  Token   : ");
            for (int i = 0; i < tkl; i++) {
                printf("%02X ", buffer[4 + i]);
            }
            if (tkl == 0) {
                printf("(none)");
            }
            printf("\n");

            if (tkl > 8 || len < 4 + tkl) {
                // Malformed request. Send 4.00 Bad Request
                char eventId[SESSION_EVENT_ID_LEN];
                char fields[256];
                makeCoapEventId(eventId, sizeof(eventId), requestStartMs);
                snprintf(fields, sizeof(fields),
                    "\"transport\":\"udp\",\"bytes_received\":%d,\"coap_type\":\"%s\",\"coap_method\":\"%s\",\"message_id\":%u,\"token_length\":%u,\"malformed_reason\":\"invalid_token_length\",\"handling_duration_ms\":0",
                    len, coapTypeName(type), coapMethodName(class, detail), msgId, tkl);
                emitCoapAction(eventId, "malformed_request", fields);

                uint8_t response[4];
                uint8_t resp_type = (type == TYPE_CONFIRMABLE) ? TYPE_ACK : TYPE_NON_CONFIRMABLE;
            
                response[0] = (0b01 << 6) | (resp_type << 4) | 0; // Ver=1, Type=ACK/NON, TKL=0
                response[1] = (0b100 << 5) | 0b0;                 // Code 4.00 (Bad Request)
                response[2] = msgId >> 8;
                response[3] = msgId & 0b11111111;
                int resp_len = 4;

                int out = sendto(sockFd, response, resp_len, 0, (struct sockaddr *)&clientAddr, addrLen);
                emitCoapSendEvent(eventId, "bad_request", out, currentTimeMs() - requestStartMs, 0);
                continue;
            } 
            else if (version != 1){
                // Must be silently ignored
                char eventId[SESSION_EVENT_ID_LEN];
                char fields[256];
                makeCoapEventId(eventId, sizeof(eventId), requestStartMs);
                snprintf(fields, sizeof(fields),
                    "\"transport\":\"udp\",\"bytes_received\":%d,\"coap_version\":%u,\"malformed_reason\":\"unsupported_version\",\"handling_duration_ms\":0",
                    len, version);
                emitCoapAction(eventId, "malformed_request", fields);
                continue;
            } else if (tkl > 0) {
                memcpy(token, &buffer[4], tkl);
            }

            // TODO: Ignore extended methods (send "method not allowed" response)
            // TODO: Handle requests while the client is still receiving blocks. 
            struct coapClient* client = findExistingClient(&clientAddr);
            if(client == NULL) {
                client = malloc(sizeof(struct coapClient));
                if (!client) {
                    fprintf(stderr, "Out of memory");
                    continue;
                }

                client->clientAddr = clientAddr;
                client->addrLen = addrLen;
                client->base.sendNext = now + delay;
                client->base.timeConnected = 0;
                client->blockNumber = 0;
                client->tkl = tkl;
                client->interactionDepth = 0;
                client->retransmits = 0;
                client->messageId = 1;
                client->receivedAck = true;
                client->receivedRst = true;
                client->receivedGet = false;
                memcpy(client->token, token, 8);
                snprintf(client->base.ipaddr, INET_ADDRSTRLEN, "%s", inet_ntoa(clientAddr.sin_addr));
                makeCoapEventId(client->sessionId, sizeof(client->sessionId), requestStartMs);
                heap_insert(&clientQueueCoap, (struct baseClient*)client);
                addClient(client);

                char msg[256];
                snprintf(msg, sizeof(msg), "%s connect %s\n",
                    SERVER_ID, client->base.ipaddr);
                printf("%s", msg);
                sendMetric(msg);
            }

            client->interactionDepth += 1;
            char metricMsg[64];
            snprintf(metricMsg, sizeof(metricMsg), "%s protocol_action coap_request\n", SERVER_ID);
            sendMetric(metricMsg);
            char fields[256];
            snprintf(fields, sizeof(fields),
                "\"transport\":\"udp\",\"bytes_received\":%d,\"coap_type\":\"%s\",\"coap_method\":\"%s\",\"message_id\":%u,\"token_present\":%s,\"token_length\":%u,\"handling_duration_ms\":0,\"interaction_depth\":%u",
                len,
                coapTypeName(type),
                coapMethodName(class, detail),
                msgId,
                tkl > 0 ? "true" : "false",
                tkl,
                client->interactionDepth);
            emitCoapAction(client->sessionId, "request_received", fields);
            
            if (type == TYPE_RST) {
                client->receivedRst = true;
            }
            else if (type == TYPE_ACK) {
                client->receivedAck = true;
                client->retransmits = 0;
            }
            else if (class == CLASS_REQUEST && detail == DETAIL_GET) {
                printf("GET request from %s of type %d with tkl=%d and msgId1=%u\n", inet_ntoa(clientAddr.sin_addr), type, tkl, msgId);
                client->receivedGet = true;
            } 

            // If a CON (Confirmable) request, first send seperate ACK response. 
            // The response does not need to be confirmable. (5.2.2 and 5.2.3)
            if (type == TYPE_CONFIRMABLE) {
                uint8_t ack[4];
                ack[0] = (0b01 << 6) | (0b10 << 4) | 0;   // Version = 1, Type = ACK (2), TKL = 0
                ack[1] = 0;                               // Code = 0.00 (empty ACK)
                ack[2] = buffer[2];                       // Same Message ID MSB
                ack[3] = buffer[3];                       // Same Message ID LSB

                int out = sendto(sockFd, ack, sizeof(ack), 0, (struct sockaddr *)&clientAddr, addrLen);
                printf("ACK sendto: %d with messageId=%u\n", out, msgId);
                client->interactionDepth += 1;
                emitCoapSendEvent(client->sessionId, "ack", out, currentTimeMs() - requestStartMs, client->interactionDepth);
            }
        }
    }

    close(sockFd);
    return 0;
#endif
}
