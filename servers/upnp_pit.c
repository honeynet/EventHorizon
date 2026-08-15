#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <ifaddrs.h>
#include <stdint.h>
#include <strings.h>
#include <time.h>
#include "../shared/structs.h"
#include "../shared/metric_events.h"

#define SSDP_MULTICAST "239.255.255.250"
#define SERVER_ID "UPnP"

int httpPort;
int ssdpPort;
int delay;
int maxNoClients;

static pthread_mutex_t upnpEventCounterLock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long upnpEventCounter = 0;

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
#define UPNP_DESCRIPTION_DEADLINE_NS 30000000000ULL
#define UPNP_HTTP_REQUEST_LIMIT 1024
#define UPNP_HTTP_WRITE_CHUNK 256

struct upnpDescriptionWorker {
    int clientFd;
};

enum upnpWriteResult {
    UPNP_WRITE_POSITIVE,
    UPNP_WRITE_PENDING,
    UPNP_WRITE_ZERO,
    UPNP_WRITE_FAILED,
};

static const char UPNP_DEVICE_DESCRIPTION[] =
    "<?xml version=\"1.0\"?>\n"
    "<root xmlns=\"urn:schemas-upnp-org:device-1-0\">\n"
    "  <specVersion><major>1</major><minor>0</minor></specVersion>\n"
    "  <device>\n"
    "    <deviceType>urn:Philips:device:Basic:1</deviceType>\n"
    "    <friendlyName>EventHorizon Philips Hue Device</friendlyName>\n"
    "    <manufacturer>Philips</manufacturer>\n"
    "    <manufacturerURL>https://www.philips-hue.com</manufacturerURL>\n"
    "    <modelDescription>Philips Hue A19 White and Color Ambiance</modelDescription>\n"
    "    <modelName>Hue A19</modelName>\n"
    "    <modelNumber>9290012573A</modelNumber>\n"
    "    <UDN>uuid:31c79c6d-7d92-4bbf-bf72-5b68591e1731</UDN>\n"
    "  </device>\n"
    "</root>\n";

static pthread_mutex_t upnpMetricStateLock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t upnpWorkerCountLock = PTHREAD_MUTEX_INITIALIZER;
static uint32_t upnpActiveDescriptionResponses = 0;
static uint32_t upnpDescriptionWorkerCount = 0;
static bool upnpMetricEmitterReady = false;

static bool upnpMonotonicNowNS(uint64_t *result) {
    struct timespec now;
    if (!result || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
        now.tv_sec < 0 || now.tv_nsec < 0) {
        return false;
    }
    *result = (uint64_t)now.tv_sec * 1000000000ULL +
              (uint64_t)now.tv_nsec;
    return true;
}

static uint64_t upnpElapsedMS(uint64_t startedNS, uint64_t endedNS) {
    return endedNS >= startedNS
        ? (endedNS - startedNS) / 1000000ULL
        : 0;
}

static void upnpSleepConfiguredDelay(void) {
    if (delay <= 0) {
        return;
    }
    struct timespec remaining = {
        .tv_sec = delay / 1000,
        .tv_nsec = (long)(delay % 1000) * 1000000L,
    };
    while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
    }
}

static enum upnpWriteResult upnpClassifyWrite(
    ssize_t written, int writeError) {
    if (written > 0) {
        return UPNP_WRITE_POSITIVE;
    }
    if (written == 0) {
        return UPNP_WRITE_ZERO;
    }
    if (writeError == EINTR || writeError == EAGAIN ||
        writeError == EWOULDBLOCK) {
        return UPNP_WRITE_PENDING;
    }
    return UPNP_WRITE_FAILED;
}

static void upnpEmitWriteError(ssize_t written, int writeError) {
    if (!upnpMetricEmitterReady) {
        return;
    }
    enum metric_io_reason reason = written >= 0
        ? METRIC_IO_OTHER
        : metric_io_reason_from_unrecoverable_errno(writeError, false);
    (void)metric_event_upnp_write_error(reason);
}

static void upnpDescriptionStarted(
    bool *started, uint64_t *startedNS, uint64_t observedNS) {
    pthread_mutex_lock(&upnpMetricStateLock);
    if (!*started) {
        *started = true;
        *startedNS = observedNS;
        upnpActiveDescriptionResponses += 1;
        if (upnpMetricEmitterReady) {
            (void)metric_event_upnp_description_response_started(
                upnpActiveDescriptionResponses);
        }
    }
    pthread_mutex_unlock(&upnpMetricStateLock);
}

static void upnpDescriptionFinalized(
    bool *finalized,
    uint64_t startedNS,
    uint64_t observedNS,
    enum metric_upnp_description_outcome outcome) {
    pthread_mutex_lock(&upnpMetricStateLock);
    if (!*finalized) {
        *finalized = true;
        if (upnpActiveDescriptionResponses > 0) {
            upnpActiveDescriptionResponses -= 1;
        }
        if (upnpMetricEmitterReady) {
            (void)metric_event_upnp_description_response_finalized(
                outcome,
                upnpElapsedMS(startedNS, observedNS),
                upnpActiveDescriptionResponses);
        }
    }
    pthread_mutex_unlock(&upnpMetricStateLock);
}

static bool upnpHeaderValueIsToken(const char *value) {
    if (!value || value[0] == '\0') {
        return false;
    }
    for (const unsigned char *cursor = (const unsigned char *)value;
         *cursor;
         cursor++) {
        if (*cursor <= 0x20 || *cursor >= 0x7f) {
            return false;
        }
    }
    return true;
}

static char *upnpTrimHeaderValue(char *value) {
    while (*value == ' ' || *value == '\t') {
        value++;
    }
    char *end = value + strlen(value);
    while (end > value && (end[-1] == ' ' || end[-1] == '\t')) {
        *--end = '\0';
    }
    return value;
}

static bool upnpParseDescriptionGET(char *request, size_t requestLength) {
    if (!request || requestLength < 4 ||
        memchr(request, '\0', requestLength) != NULL ||
        requestLength >= UPNP_HTTP_REQUEST_LIMIT) {
        return false;
    }
    request[requestLength] = '\0';
    char *headersEnd = strstr(request, "\r\n\r\n");
    if (!headersEnd || headersEnd + 4 != request + requestLength) {
        return false;
    }
    *headersEnd = '\0';

    char *next = NULL;
    char *line = strtok_r(request, "\r\n", &next);
    if (!line || strcmp(line, "GET /hue-device.xml HTTP/1.1") != 0) {
        return false;
    }

    bool hostSeen = false;
    while ((line = strtok_r(NULL, "\r\n", &next)) != NULL) {
        char *colon = strchr(line, ':');
        if (!colon || colon == line) {
            return false;
        }
        *colon = '\0';
        char *value = upnpTrimHeaderValue(colon + 1);
        if (strcasecmp(line, "Host") == 0) {
            if (hostSeen || !upnpHeaderValueIsToken(value)) {
                return false;
            }
            hostSeen = true;
        } else if (strcasecmp(line, "Content-Length") == 0 ||
                   strcasecmp(line, "Transfer-Encoding") == 0) {
            return false;
        }
    }
    return hostSeen;
}

static bool upnpHasCompleteHTTPHeaders(const char *request, size_t length) {
    if (!request || length < 4) {
        return false;
    }
    for (size_t index = 0; index + 4 <= length; index++) {
        if (memcmp(request + index, "\r\n\r\n", 4) == 0) {
            return true;
        }
    }
    return false;
}

static ssize_t upnpReadCompleteHTTPRequest(
    int clientFd, char *request, size_t capacity) {
    uint64_t startedNS;
    if (!upnpMonotonicNowNS(&startedNS)) {
        return -1;
    }
    size_t used = 0;
    while (used + 1 < capacity) {
        uint64_t nowNS;
        if (!upnpMonotonicNowNS(&nowNS) ||
            nowNS - startedNS >= 1000000000ULL) {
            return 0;
        }
        int remainingMS = (int)((1000000000ULL - (nowNS - startedNS) +
                                 999999ULL) / 1000000ULL);
        struct pollfd descriptor = {
            .fd = clientFd,
            .events = POLLIN,
        };
        int ready;
        do {
            ready = poll(&descriptor, 1, remainingMS);
        } while (ready < 0 && errno == EINTR);
        if (ready <= 0 || !(descriptor.revents & POLLIN)) {
            return 0;
        }

        ssize_t received = read(clientFd, request + used, capacity - 1 - used);
        if (received > 0) {
            used += (size_t)received;
            if (upnpHasCompleteHTTPHeaders(request, used)) {
                return (ssize_t)used;
            }
            continue;
        }
        if (received == 0) {
            return 0;
        }
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        }
        return -1;
    }
    return 0;
}

static bool upnpBuildDescriptionResponse(
    char *response, size_t capacity, size_t *responseLength) {
    size_t bodyLength = strlen(UPNP_DEVICE_DESCRIPTION);
    int headerLength = snprintf(
        response, capacity,
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/xml\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n",
        bodyLength);
    if (headerLength < 0 || (size_t)headerLength >= capacity ||
        bodyLength > capacity - (size_t)headerLength) {
        return false;
    }
    memcpy(response + headerLength, UPNP_DEVICE_DESCRIPTION, bodyLength);
    *responseLength = (size_t)headerLength + bodyLength;
    return true;
}

static ssize_t upnpWriteDescriptionBytes(
    int clientFd, const char *bytes, size_t length, unsigned int writeNumber) {
#ifdef EVENTHORIZON_UPNP_TEST_RUNTIME
    const char *injection = getenv("EVENTHORIZON_UPNP_TEST_WRITE_INJECTION");
    if (injection && strcmp(injection, "prestart_failure") == 0 &&
        writeNumber == 0) {
        errno = EIO;
        return -1;
    }
    if (injection && strcmp(injection, "poststart_failure") == 0 &&
        writeNumber == 1) {
        errno = EPIPE;
        return -1;
    }
#else
    (void)writeNumber;
#endif
    return write(clientFd, bytes, length);
}

static uint64_t upnpObservedDescriptionStartNS(
    uint64_t requestAcceptedNS, uint64_t actualObservedNS) {
#ifdef EVENTHORIZON_UPNP_TEST_RUNTIME
    const char *finalTime = getenv("EVENTHORIZON_UPNP_TEST_FINAL_TIME");
    if (finalTime &&
        (strcmp(finalTime, "deadline") == 0 ||
         strcmp(finalTime, "just_over") == 0)) {
        return requestAcceptedNS;
    }
#else
    (void)requestAcceptedNS;
#endif
    return actualObservedNS;
}

static uint64_t upnpObservedDescriptionFinalNS(
    uint64_t requestAcceptedNS, uint64_t actualObservedNS) {
#ifdef EVENTHORIZON_UPNP_TEST_RUNTIME
    const char *finalTime = getenv("EVENTHORIZON_UPNP_TEST_FINAL_TIME");
    if (finalTime && strcmp(finalTime, "deadline") == 0) {
        return requestAcceptedNS + UPNP_DESCRIPTION_DEADLINE_NS;
    }
    if (finalTime && strcmp(finalTime, "just_over") == 0) {
        return requestAcceptedNS + UPNP_DESCRIPTION_DEADLINE_NS + 500000ULL;
    }
#else
    (void)requestAcceptedNS;
#endif
    return actualObservedNS;
}

static void upnpReleaseDescriptionWorker(void) {
    pthread_mutex_lock(&upnpWorkerCountLock);
    if (upnpDescriptionWorkerCount > 0) {
        upnpDescriptionWorkerCount -= 1;
    }
    pthread_mutex_unlock(&upnpWorkerCountLock);
}

static void *upnpDescriptionWorkerMain(void *argument) {
    struct upnpDescriptionWorker *worker = argument;
    int clientFd = worker->clientFd;
    free(worker);

    char request[UPNP_HTTP_REQUEST_LIMIT];
    ssize_t requestLength = upnpReadCompleteHTTPRequest(
        clientFd, request, sizeof(request));
    uint64_t requestAcceptedNS;
    bool validGET = requestLength > 0 &&
        upnpMonotonicNowNS(&requestAcceptedNS) &&
        upnpParseDescriptionGET(request, (size_t)requestLength);
    if (!validGET) {
        close(clientFd);
        upnpReleaseDescriptionWorker();
        return NULL;
    }
    if (upnpMetricEmitterReady) {
        (void)metric_event_upnp_protocol_action(
            METRIC_UPNP_ACTION_DESCRIPTION_GET_RECEIVED);
    }

    char response[2048];
    size_t responseLength = 0;
    if (!upnpBuildDescriptionResponse(
            response, sizeof(response), &responseLength)) {
        close(clientFd);
        upnpReleaseDescriptionWorker();
        return NULL;
    }

    bool responseStarted = false;
    bool responseFinalized = false;
    uint64_t responseStartedNS = 0;
    size_t responseOffset = 0;
    unsigned int writeNumber = 0;
    uint64_t deadlineNS = requestAcceptedNS + UPNP_DESCRIPTION_DEADLINE_NS;

    while (responseOffset < responseLength) {
        uint64_t beforeWriteNS;
        if (!upnpMonotonicNowNS(&beforeWriteNS)) {
            break;
        }
        if (beforeWriteNS > deadlineNS) {
            if (responseStarted) {
                upnpDescriptionFinalized(
                    &responseFinalized,
                    responseStartedNS,
                    beforeWriteNS,
                    METRIC_UPNP_DESCRIPTION_TERMINATED);
            }
            break;
        }

        size_t remaining = responseLength - responseOffset;
        size_t requested = remaining < UPNP_HTTP_WRITE_CHUNK
            ? remaining
            : UPNP_HTTP_WRITE_CHUNK;
        errno = 0;
        ssize_t written = upnpWriteDescriptionBytes(
            clientFd, response + responseOffset, requested, writeNumber++);
        int writeError = errno;
        enum upnpWriteResult result = upnpClassifyWrite(written, writeError);
        if (result == UPNP_WRITE_POSITIVE) {
            uint64_t actualObservedNS;
            if (!upnpMonotonicNowNS(&actualObservedNS)) {
                break;
            }
            uint64_t startObservedNS = upnpObservedDescriptionStartNS(
                requestAcceptedNS, actualObservedNS);
            upnpDescriptionStarted(
                &responseStarted, &responseStartedNS, startObservedNS);
            responseOffset += (size_t)written;
            if (responseOffset == responseLength) {
                uint64_t finalObservedNS = upnpObservedDescriptionFinalNS(
                    requestAcceptedNS, actualObservedNS);
                enum metric_upnp_description_outcome outcome =
                    finalObservedNS <= deadlineNS
                    ? METRIC_UPNP_DESCRIPTION_COMPLETED
                    : METRIC_UPNP_DESCRIPTION_TERMINATED;
                upnpDescriptionFinalized(
                    &responseFinalized,
                    responseStartedNS,
                    finalObservedNS,
                    outcome);
                break;
            }
        } else if (result == UPNP_WRITE_FAILED) {
            uint64_t observedNS;
            if (!upnpMonotonicNowNS(&observedNS)) {
                observedNS = beforeWriteNS;
            }
            if (responseStarted) {
                upnpDescriptionFinalized(
                    &responseFinalized,
                    responseStartedNS,
                    observedNS,
                    METRIC_UPNP_DESCRIPTION_TERMINATED);
            }
            upnpEmitWriteError(written, writeError);
            break;
        }
        upnpSleepConfiguredDelay();
    }

    close(clientFd);
    upnpReleaseDescriptionWorker();
    return NULL;
}

static void *upnpJSONHTTPServer(void *argument) {
    (void)argument;
    signal(SIGPIPE, SIG_IGN);
    int serverSock = createServer(httpPort);
    if (serverSock < 0) {
        fprintf(stderr, "Invalid server socket fd: %d", serverSock);
        exit(EXIT_FAILURE);
    }
    while (1) {
        int clientFd = accept(serverSock, NULL, NULL);
        if (clientFd < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "Failed accepting UPnP HTTP client: %s", strerror(errno));
            continue;
        }
        int flags = fcntl(clientFd, F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(clientFd, F_SETFL, flags | O_NONBLOCK);
        }

        pthread_mutex_lock(&upnpWorkerCountLock);
        bool atCapacity = maxNoClients <= 0 ||
            upnpDescriptionWorkerCount >= (uint32_t)maxNoClients;
        if (!atCapacity) {
            upnpDescriptionWorkerCount += 1;
        }
        pthread_mutex_unlock(&upnpWorkerCountLock);
        if (atCapacity) {
            close(clientFd);
            continue;
        }

        struct upnpDescriptionWorker *worker = malloc(sizeof(*worker));
        if (!worker) {
            close(clientFd);
            upnpReleaseDescriptionWorker();
            continue;
        }
        worker->clientFd = clientFd;
        pthread_t workerThread;
        if (pthread_create(
                &workerThread, NULL, upnpDescriptionWorkerMain, worker) != 0) {
            close(clientFd);
            free(worker);
            upnpReleaseDescriptionWorker();
            continue;
        }
        pthread_detach(workerThread);
    }
    return NULL;
}
#endif

static void makeUpnpEventId(char *buffer, size_t len, long long eventMs) {
    pthread_mutex_lock(&upnpEventCounterLock);
    unsigned long counter = ++upnpEventCounter;
    pthread_mutex_unlock(&upnpEventCounterLock);
    session_events_make_request_id(buffer, len, "upnp", counter, eventMs);
}

static const char *httpRouteClass(const char *method, const char *url) {
    if (strcmp(method, "GET") == 0 && strcmp(url, "/hue-device.xml") == 0) {
        return "device_description";
    }
    if (!method[0] || !url[0]) {
        return "malformed";
    }
    return "other";
}

static const char *httpMethodClass(const char *method) {
    return strcmp(method, "GET") == 0 ? "GET" : "other";
}

static void emitUpnpAction(const char *eventId, const char *action, const char *fields) {
    session_events_write_action("upnp", eventId, action, fields);
}

static ssize_t readHttpRequestWithTimeout(int clientFd, char *buffer, size_t len, int timeoutMs) {
    struct pollfd clientPoll;
    memset(&clientPoll, 0, sizeof(clientPoll));
    clientPoll.fd = clientFd;
    clientPoll.events = POLLIN;

    int ready;
    do {
        ready = poll(&clientPoll, 1, timeoutMs);
    } while (ready < 0 && errno == EINTR);

    if (ready <= 0 || !(clientPoll.revents & POLLIN)) {
        return 0;
    }

    ssize_t bytesReceived;
    do {
        bytesReceived = read(clientFd, buffer, len);
    } while (bytesReceived < 0 && errno == EINTR);

    if (bytesReceived < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return 0;
    }

    return bytesReceived;
}

// Can use Chunked Transfer Coding from rfc 2616 section 3.6.1
// Required to be a HTTP GET request (Section 2.1 from specifications)
const char *FAKE_DEVICE_DESCRIPTION =
    "<?xml version=\"1.0\"?>\n"
    "<root xmlns=\"urn:Philips:device-1-0\">\n"
    "  <specVersion>\n"
    "    <major>1</major>\n"
    "    <minor>0</minor>\n"
    "  </specVersion>\n"
    "  <device>\n"
    "    <deviceType>urn:Philips:device:insight:1</deviceType>\n"
    "    <friendlyName>Philips Hue Smart Bulb</friendlyName>\n"
    "      <manufacturer>Philips</manufacturer>\n"
    "      <manufacturerURL>https://www.philips-hue.com</manufacturerURL>\n"
    "      <modelDescription>Philips Hue A19 White and Color Ambiance</modelDescription>\n"
    "      <modelName>Hue A19</modelName>\n"
    "      <modelNumber>9290012573A</modelNumber>\n"
    "      <modelURL>https://www.philips-hue.com/en-us/p/hue-white-and-color-ambiance-a19</modelURL>\n"
    "    <serialNumber>PHL-00256739</serialNumber>\n"
    "    <UDN>uuid:31c79c6d-7d92-4bbf-bf72-5b68591e1731</UDN>\n"
    "      <UPC>123456789</UPC>\n"
    "    <macAddress>149182B3A4D0</macAddress>"
    "    <firmwareVersion>Philips_Hue_2.00.10966.PVT-OWRT-InsightV2</firmwareVersion>\n"
    "    <iconVersion>1|49153</iconVersion>\n"
    "    <binaryState>8</binaryState>\n"
    "        <iconList>\n" 
    "    <icon>\n"
    "      <mimetype>jpg</mimetype>\n"
    "      <width>100</width>\n"
    "      <height>100</height>\n"
    "      <depth>100</depth>\n"
    "        <url>icon.jpg</url>\n"
    "      </icon>\n"
    "    </iconList>\n"
    "    <serviceList>\n";

const char *FAKE_CHUNK =
    "      <service>\n"
    "        <serviceType>urn:Philips:service:SwitchPower:1</serviceType>\n"
    "        <serviceId>urn:upnp-org:serviceId:SwitchPower</serviceId>\n"
    "        <controlURL>/hue_control</controlURL>\n"
    "        <eventSubURL>/hue_event</eventSubURL>\n"
    "        <SCPDURL>/hue_service.xml</SCPDURL>\n"
    "      </service>\n";

// void heartbeatLog() {
//     syslog(LOG_INFO, "Server is running with %d connected clients. Number of most concurrent connected clients is %d", clientQueueUpnp.length, statsUpnp.mostConcurrentConnections);
//     syslog(LOG_INFO, "Current statistics: wasted time: %lld ms. Total HTTP requests: %ld. Total other HTTP requests: %ld. SSDP responses: %ld. XML requests: %ld", 
//         statsUpnp.totalWastedTime, statsUpnp.totalHttpRequests, statsUpnp.otherHttpRequests, statsUpnp.ssdpResponses, statsUpnp.totalXmlRequests);
// }

char* getLocalIpAddress() {
    struct ifaddrs *ifaddr, *ifa;
    static char ipAddress[INET_ADDRSTRLEN];  // Buffer to store IP

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return NULL;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET) {
            struct sockaddr_in *addr = (struct sockaddr_in *)ifa->ifa_addr;
            const char *ip = inet_ntoa(addr->sin_addr);

            if (strcmp(ifa->ifa_name, "lo") != 0) {
                snprintf(ipAddress, INET_ADDRSTRLEN, "%s", ip);
                freeifaddrs(ifaddr);
                return ipAddress;
            }
        }
    }

    freeifaddrs(ifaddr);
    return NULL;  // No valid IP found
}

char* ssdpResponse() {
    char *ipAddress = getLocalIpAddress();
    if (!ipAddress) {
        ipAddress = "127.0.0.1";
    }

    char *responseBuffer = (char*) malloc((512)*sizeof(char));
    snprintf(responseBuffer, 512,
        "HTTP/1.1 200 OK\r\n"
        "CACHE-CONTROL: max-age=1800\r\n"
        "EXT:\r\n"
        "LOCATION: http://%s:%d/hue-device.xml\r\n"
        "SERVER: Linux/3.14 UPnP/1.0 PhilipsHue/2.1\r\n"
        "ST: urn:Philips:device:Basic:1\r\n"
        "USN: uuid:bd752e88-91a9-49e4-8297-8433e05d1c22::urn:Philips:device:Basic:1\r\n"
        "BOOTID.UPNP.ORG: 1\r\n"
        "CONFIGID.UPNP.ORG: 1337\r\n"
        "\r\n", ipAddress, httpPort);
    return responseBuffer;
}

#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
static bool upnpParseMSearch(
    const char *datagram,
    size_t datagramLength,
    bool *targetMatched) {
    if (!datagram || !targetMatched || datagramLength < 4 ||
        datagramLength >= 1024 ||
        memchr(datagram, '\0', datagramLength) != NULL) {
        return false;
    }
    char request[1024];
    memcpy(request, datagram, datagramLength);
    request[datagramLength] = '\0';
    char *headersEnd = strstr(request, "\r\n\r\n");
    if (!headersEnd || headersEnd + 4 != request + datagramLength) {
        return false;
    }
    *headersEnd = '\0';

    char *next = NULL;
    char *line = strtok_r(request, "\r\n", &next);
    if (!line || strcmp(line, "M-SEARCH * HTTP/1.1") != 0) {
        return false;
    }

    bool hostSeen = false;
    bool manSeen = false;
    bool mxSeen = false;
    bool stSeen = false;
    char searchTarget[256] = "";
    while ((line = strtok_r(NULL, "\r\n", &next)) != NULL) {
        char *colon = strchr(line, ':');
        if (!colon || colon == line) {
            return false;
        }
        *colon = '\0';
        char *value = upnpTrimHeaderValue(colon + 1);
        if (strcasecmp(line, "HOST") == 0) {
            if (hostSeen || strcmp(value, SSDP_MULTICAST ":1900") != 0) {
                return false;
            }
            hostSeen = true;
        } else if (strcasecmp(line, "MAN") == 0) {
            if (manSeen || strcmp(value, "\"ssdp:discover\"") != 0) {
                return false;
            }
            manSeen = true;
        } else if (strcasecmp(line, "MX") == 0) {
            if (mxSeen || value[0] < '1' || value[0] > '5' || value[1] != '\0') {
                return false;
            }
            mxSeen = true;
        } else if (strcasecmp(line, "ST") == 0) {
            if (stSeen || !upnpHeaderValueIsToken(value) ||
                strlen(value) >= sizeof(searchTarget)) {
                return false;
            }
            memcpy(searchTarget, value, strlen(value) + 1);
            stSeen = true;
        }
    }
    if (!hostSeen || !manSeen || !mxSeen || !stSeen) {
        return false;
    }
    *targetMatched = strcmp(searchTarget, "ssdp:all") == 0 ||
        strcmp(searchTarget, "urn:Philips:device:Basic:1") == 0;
    return true;
}

static ssize_t upnpSendSSDPDatagram(
    int socketFd,
    const char *response,
    size_t responseLength,
    const struct sockaddr_in *clientAddress,
    socklen_t clientAddressLength) {
#ifdef EVENTHORIZON_UPNP_TEST_RUNTIME
    const char *injection = getenv("EVENTHORIZON_UPNP_TEST_SSDP_INJECTION");
    if (injection && strcmp(injection, "short") == 0) {
        return responseLength > 0 ? (ssize_t)(responseLength - 1) : 0;
    }
#endif
    return sendto(
        socketFd, response, responseLength, 0,
        (const struct sockaddr *)clientAddress, clientAddressLength);
}

static void *upnpJSONSSDPListener(void *argument) {
    (void)argument;
    char *response = ssdpResponse();
    if (!response) {
        exit(EXIT_FAILURE);
    }
    int socketFd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socketFd < 0) {
        fprintf(stderr, "SSDP Socket creation failed");
        exit(EXIT_FAILURE);
    }
    struct sockaddr_in serverAddress;
    memset(&serverAddress, 0, sizeof(serverAddress));
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(ssdpPort);
    if (bind(
            socketFd, (struct sockaddr *)&serverAddress,
            sizeof(serverAddress)) < 0) {
        fprintf(stderr, "SSDP Bind failed");
        close(socketFd);
        exit(EXIT_FAILURE);
    }
    printf("UPnP listener started on port %d\n", ssdpPort);

    while (1) {
        char request[1024];
        struct sockaddr_in clientAddress;
        memset(&clientAddress, 0, sizeof(clientAddress));
        socklen_t clientAddressLength = sizeof(clientAddress);
        ssize_t received = recvfrom(
            socketFd, request, sizeof(request), 0,
            (struct sockaddr *)&clientAddress, &clientAddressLength);
        if (received <= 0) {
            continue;
        }
        bool targetMatched = false;
        if (!upnpParseMSearch(
                request, (size_t)received, &targetMatched)) {
            continue;
        }
        if (upnpMetricEmitterReady) {
            (void)metric_event_upnp_protocol_action(
                METRIC_UPNP_ACTION_SSDP_MSEARCH_RECEIVED);
        }
        if (!targetMatched) {
            continue;
        }

        size_t responseLength = strlen(response);
        errno = 0;
        ssize_t sent = upnpSendSSDPDatagram(
            socketFd,
            response,
            responseLength,
            &clientAddress,
            clientAddressLength);
        int sendError = errno;
        enum upnpWriteResult result = upnpClassifyWrite(sent, sendError);
        if (sent == (ssize_t)responseLength) {
            if (upnpMetricEmitterReady) {
                (void)metric_event_upnp_protocol_action(
                    METRIC_UPNP_ACTION_SSDP_DISCOVERY_RESPONSE_SENT);
            }
        } else if (result == UPNP_WRITE_FAILED || sent >= 0) {
            upnpEmitWriteError(sent, sendError);
        }
    }
    free(response);
    close(socketFd);
    return NULL;
}
#endif

// Handles SSDP discovery requests and sends fake responses
void *ssdpListener(void *arg) {
    (void)arg;
    char* response = ssdpResponse();
    int sockFd;
    struct sockaddr_in serverAddr, client_addr;
    socklen_t addrLen = sizeof(client_addr);
    char buffer[1024];

    // printf("response: %s\n", response);

    if ((sockFd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) { // works
        fprintf(stderr, "SSDP Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Bind to all interfaces for unicast
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(ssdpPort);

    // Join the SSDP multicast group
    // struct ip_mreq mreq;
    // mreq.imr_multiaddr.s_addr = inet_addr(SSDP_MULTICAST);
    // mreq.imr_interface.s_addr = INADDR_ANY;
    // setsockopt(sockFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));

    if (bind(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        fprintf(stderr, "SSDP Bind failed");
        close(sockFd);
        exit(EXIT_FAILURE);
    }

    printf("UPnP listener started on port %d\n", ssdpPort);

    while (1) {
        memset(buffer, 0, sizeof(buffer));

        ssize_t bytesReceived = recvfrom(sockFd, buffer, sizeof(buffer), 0,
                     (struct sockaddr *)&client_addr, &addrLen);
        if (bytesReceived <= 0) {
            fprintf(stderr, "Error receiving SSDP request");
            continue;
        }
        long long requestStartMs = currentTimeMs();

        char eventId[SESSION_EVENT_ID_LEN];
        makeUpnpEventId(eventId, sizeof(eventId), requestStartMs);

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        
        char msg[256];
        int isMSearch = strstr(buffer, "M-SEARCH") != NULL;
        char fields[256];
        snprintf(fields, sizeof(fields),
            "\"transport\":\"udp\",\"bytes_received\":%zd,\"upnp_request_type\":\"%s\",\"handling_duration_ms\":0",
            bytesReceived, isMSearch ? "m_search" : "unknown");
        emitUpnpAction(eventId, isMSearch ? "ssdp_probe" : "unknown", fields);

        if (isMSearch) {
            ssize_t bytesSent = sendto(sockFd, response, strlen(response), 0,
                (struct sockaddr *)&client_addr, sizeof(client_addr));
            long long handlingDurationMs = currentTimeMs() - requestStartMs;
            snprintf(fields, sizeof(fields),
                "\"transport\":\"udp\",\"bytes_sent\":%zd,\"write_result\":\"%s\",\"handling_duration_ms\":%lld",
                bytesSent > 0 ? bytesSent : 0,
                bytesSent >= 0 ? "success" : "write_error",
                handlingDurationMs);
            emitUpnpAction(eventId, "response_sent", fields);
            
            snprintf(msg, sizeof(msg), "%s M-SEARCH %s\n", 
                SERVER_ID, client_ip);
        } else {
            snprintf(msg, sizeof(msg), "%s non-M-SEARCH %s\n", 
                SERVER_ID, client_ip);
        }
        
        printf("%s", msg);
        sendMetric(msg);
    }

    free(response);
    close(sockFd);
    return NULL;
}

void *httpServer(void *arg) {
    (void)arg;
    signal(SIGPIPE, SIG_IGN);
    queue_init(&clientQueueUpnp);
    int serverSock = createServer(httpPort);
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
    while (1){
        long long now = currentTimeMs();
        int timeout = -1;

        // long long res = now - lastHeartbeat;

        // if (res >= HEARTBEAT_INTERVAL_MS) {
        //     heartbeatLog();
        //     lastHeartbeat = now;
        // }

        while (clientQueueUpnp.head) {
            if(clientQueueUpnp.head->sendNext <= now){
                struct baseClient *bc = queue_pop(&clientQueueUpnp);
                struct telnetAndUpnpClient *c = (struct telnetAndUpnpClient *)bc;

                char chunk_size[10];
                snprintf(chunk_size, sizeof(chunk_size), "%X\r\n", (int)strlen(FAKE_CHUNK));
                write(c->fd, chunk_size, strlen(chunk_size));
                write(c->fd, FAKE_CHUNK, strlen(FAKE_CHUNK));
                ssize_t out = write(c->fd, "\r\n", 2);
                char fields[256];
                
                if (out == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) { // Avoid blocking
                        c->base.sendNext = now + delay;
                        c->base.timeConnected += delay;
                        statsUpnp.totalWastedTime += delay;
                        snprintf(fields, sizeof(fields),
                            "\"transport\":\"tcp\",\"write_result\":\"would_block\",\"bytes_sent\":0,\"interaction_depth\":%u",
                            c->interactionDepth);
                        emitUpnpAction(c->sessionId, "response_chunk_sent", fields);
                        queue_append(&clientQueueUpnp, (struct baseClient *)c);
                    } else {
                        long long timeTrapped = c->base.timeConnected;

                        char msg[256];
                        snprintf(msg, sizeof(msg), "%s disconnect %s %lld\n",
                            SERVER_ID, c->base.ipaddr, timeTrapped);
                        printf("%s", msg);
                        sendMetric(msg);

                        snprintf(fields, sizeof(fields),
                            "\"transport\":\"tcp\",\"write_result\":\"write_error\",\"bytes_sent\":0,\"interaction_depth\":%u,\"handling_duration_ms\":%lld",
                            c->interactionDepth, now - c->sessionStartMs);
                        emitUpnpAction(c->sessionId, "response_stream_closed", fields);
                        close(c->fd);
                        free(c);
                    }
                } else {
                    c->base.sendNext = now + delay;
                    c->base.timeConnected += delay;
                    statsUpnp.totalWastedTime += delay;
                    c->interactionDepth += 1;
                    snprintf(fields, sizeof(fields),
                        "\"transport\":\"tcp\",\"write_result\":\"success\",\"bytes_sent\":%zd,\"interaction_depth\":%u",
                        out > 0 ? out : 0, c->interactionDepth);
                    emitUpnpAction(c->sessionId, "response_chunk_sent", fields);
                    queue_append(&clientQueueUpnp, (struct baseClient *)c);
                }
            } else {
                timeout = clientQueueUpnp.head->sendNext - now;
                break;
            }
        }

        int pollResult = poll(&fds, 1, timeout);
        now = currentTimeMs(); // Poll will cause old value to be misrepresenting
        if (pollResult < 0) {
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
            statsUpnp.totalHttpRequests += 1;
            fcntl(clientFd, F_SETFL, O_NONBLOCK); // Set non-blocking mode
            long long requestStartMs = now;
            struct telnetAndUpnpClient* newClient = malloc(sizeof(struct telnetAndUpnpClient));
            if (newClient == NULL) {
                fprintf(stderr, "Out of memory");
                close(clientFd);
                continue;
            }

            char buffer[1024];
            memset(buffer, 0, 1024);
            ssize_t bytesReceived = readHttpRequestWithTimeout(clientFd, buffer, sizeof(buffer) - 1, 1000);
            char method[20] = "";
            char url[128] = "";
            if (bytesReceived > 0) {
                sscanf(buffer, "%19s %127s", method, url);
            }

            newClient->sessionStartMs = requestStartMs;
            newClient->interactionDepth = 0;
            makeUpnpEventId(newClient->sessionId, sizeof(newClient->sessionId), requestStartMs);

            char fields[256];
            snprintf(fields, sizeof(fields),
                "\"transport\":\"tcp\",\"bytes_received\":%zd,\"http_method\":\"%s\",\"route\":\"%s\",\"handling_duration_ms\":0",
                bytesReceived > 0 ? bytesReceived : 0,
                httpMethodClass(method),
                httpRouteClass(method, url));
            emitUpnpAction(newClient->sessionId, "http_request_received", fields);
            char actionMsg[64];
            snprintf(actionMsg, sizeof(actionMsg), "%s protocol_action upnp_http_request\n", SERVER_ID);
            sendMetric(actionMsg);

            if (strcmp(url, "/hue-device.xml") == 0 && strcmp(method, "GET") == 0) {
                // statsUpnp.totalXmlRequests += 1;
                char responseHeader[] =
                    "HTTP/1.1 200 OK\r\n"
                    "Transfer-Encoding: chunked\r\n"
                    "Trailer: X-Checksum\r\n"
                    "\r\n";
                
                ssize_t out = write(clientFd, responseHeader, strlen(responseHeader));
                if(out <= 0){
                    fprintf(stderr, "failed to write response header to %s\n", 
                        inet_ntoa(clientAddr.sin_addr));
                    snprintf(fields, sizeof(fields),
                        "\"transport\":\"tcp\",\"write_result\":\"write_error\",\"bytes_sent\":0,\"handling_duration_ms\":%lld,\"interaction_depth\":%u",
                        currentTimeMs() - requestStartMs, newClient->interactionDepth);
                    emitUpnpAction(newClient->sessionId, "response_sent", fields);
                    close(clientFd);
                    free(newClient);
                    continue;
                }

                char chunk_size[10];
                snprintf(chunk_size, sizeof(chunk_size), "%X\r\n", (int)strlen(FAKE_DEVICE_DESCRIPTION));
                ssize_t chunkSizeOut = write(clientFd, chunk_size, strlen(chunk_size));
                ssize_t bodyOut = write(clientFd, FAKE_DEVICE_DESCRIPTION, strlen(FAKE_DEVICE_DESCRIPTION));
                ssize_t terminatorOut = write(clientFd, "\r\n", 2);
                ssize_t totalBytesSent = out;
                if (chunkSizeOut > 0) totalBytesSent += chunkSizeOut;
                if (bodyOut > 0) totalBytesSent += bodyOut;
                if (terminatorOut > 0) totalBytesSent += terminatorOut;

                newClient->fd = clientFd;
                newClient->base.sendNext = now + delay;
                newClient->base.timeConnected = 0;
                snprintf(newClient->base.ipaddr, sizeof(newClient->base.ipaddr), "%s", inet_ntoa(clientAddr.sin_addr));
                newClient->interactionDepth += 1;
                snprintf(fields, sizeof(fields),
                    "\"transport\":\"tcp\",\"write_result\":\"success\",\"bytes_sent\":%zd,\"handling_duration_ms\":%lld,\"interaction_depth\":%u",
                    totalBytesSent, currentTimeMs() - requestStartMs, newClient->interactionDepth);
                emitUpnpAction(newClient->sessionId, "response_sent", fields);
                snprintf(actionMsg, sizeof(actionMsg), "%s protocol_action upnp_http_response\n", SERVER_ID);
                sendMetric(actionMsg);
                queue_append(&clientQueueUpnp, (struct baseClient*)newClient);

                if(statsUpnp.mostConcurrentConnections < clientQueueUpnp.length) {
                    statsUpnp.mostConcurrentConnections = clientQueueUpnp.length;
                }

                char msg[256];
                snprintf(msg, sizeof(msg), "%s connect %s\n",
                    SERVER_ID, newClient->base.ipaddr);
                printf("%s", msg);
                sendMetric(msg);
            // Ignore requests without a method or url
            // } else if (strcmp(method, "") == 0 || strcmp(url, "")) {
            //     continue;
            } else {
                // prometheus handles stats
                // statsUpnp.otherHttpRequests += 1;

                char msg[256];
                snprintf(msg, sizeof(msg), "%s otherHttpRequests %s %s\n",
                    SERVER_ID, method, url);
                printf("%s", msg);
                sendMetric(msg);

                snprintf(fields, sizeof(fields),
                    "\"transport\":\"tcp\",\"http_method\":\"%s\",\"route\":\"%s\",\"handling_duration_ms\":%lld",
                    httpMethodClass(method),
                    httpRouteClass(method, url),
                    currentTimeMs() - requestStartMs);
                emitUpnpAction(newClient->sessionId, "unknown_request", fields);
                close(clientFd);
                free(newClient);
                continue;
            }
        }
    }

    close(serverSock);
    return NULL;
}

void initializeStats(){
    statsUpnp.totalWastedTime = 0;
    statsUpnp.otherHttpRequests = 0;
    statsUpnp.ssdpResponses = 0;
    statsUpnp.mostConcurrentConnections = 0;
    statsUpnp.totalHttpRequests = 0;
    statsUpnp.totalXmlRequests = 0;
}

int main(int argc, char* argv[]) {
    setbuf(stdout, NULL);
    
    // testing
    // char msg[256];
    // snprintf(msg, sizeof(msg), "%s connect %s\n",
    //     SERVER_ID, "82.211.213.247");
    // fprintf(stderr, "%s", msg);
    // sendMetric(msg);
    // snprintf(msg, sizeof(msg), "%s connect %s\n",
    // SERVER_ID, "82.211.213.247");
    // fprintf(stderr, "%s", msg);
    // sendMetric(msg);
    (void)argc;
    httpPort = atoi(argv[1]);
    ssdpPort = atoi(argv[2]);
    delay = atoi(argv[3]);
    maxNoClients = atoi(argv[4]);
    // openlog("upnp_tarpit", LOG_PID | LOG_CONS, LOG_USER);
    initializeStats();
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    upnpMetricEmitterReady = metric_event_emitter_init(
        getenv("EVENTHORIZON_METRIC_SOCKET"));
#endif
    session_events_init(NULL);
    setFdLimit(maxNoClients);
    pthread_t ssdpThread, httpThread;
#ifdef EVENTHORIZON_JSON_METRIC_EVENTS
    pthread_create(&ssdpThread, NULL, upnpJSONSSDPListener, NULL);
    pthread_create(&httpThread, NULL, upnpJSONHTTPServer, NULL);
#else
    pthread_create(&ssdpThread, NULL, ssdpListener, NULL);
    pthread_create(&httpThread, NULL, httpServer, NULL);
#endif
    pthread_join(ssdpThread, NULL);
    pthread_join(httpThread, NULL);
    return 0;
}
