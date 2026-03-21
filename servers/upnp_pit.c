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
#include "../shared/structs.h"

#define SSDP_MULTICAST "239.255.255.250"
#define SERVER_ID "UPnP"

int httpPort;
int ssdpPort;
int delay;
int maxNoClients;

// Global flag for graceful shutdown
static volatile sig_atomic_t keepRunning = 1;

// Signal handler to flip the flag
void handleSignal(int sig){
    (void)sig;
    keepRunning = 0;
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

    struct timeval tv= {1, 0};
    setsockopt(sockFd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

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

    // Allow multiple sockets to bind to the same port
    int opt=1;
    setsockopt(sockFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(sockFd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    if (bind(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        fprintf(stderr, "SSDP Bind failed");
        close(sockFd);
        exit(EXIT_FAILURE);
    }

    printf("UPnP listener started on port %d\n", ssdpPort);

    while (keepRunning) {
        memset(buffer, 0, sizeof(buffer));

        if (recvfrom(sockFd, buffer, sizeof(buffer), 0,
                     (struct sockaddr *)&client_addr, &addrLen) <= 0) {
            if(errno ==EINTR || errno == EAGAIN) continue;
            fprintf(stderr, "Error receiving SSDP request");
            continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);

        char msg[256];
        int isMSearch = strstr(buffer, "M-SEARCH") != NULL;

        if (isMSearch) {
            sendto(sockFd, response, strlen(response), 0,
                (struct sockaddr *)&client_addr, sizeof(client_addr));

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
    printf("SSDP listener thread exiting\n");
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
    while (keepRunning){
        long long now = currentTimeMs();
        int timeout = 1000;

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

                if (out == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) { // Avoid blocking
                        c->base.sendNext = now + delay;
                        c->base.timeConnected += delay;
                        statsUpnp.totalWastedTime += delay;
                        queue_append(&clientQueueUpnp, (struct baseClient *)c);
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
                    statsUpnp.totalWastedTime += delay;
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
            if(errno ==EINTR) continue;
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
            struct telnetAndUpnpClient* newClient = malloc(sizeof(struct telnetAndUpnpClient));
            if (newClient == NULL) {
                fprintf(stderr, "Out of memory");
                close(clientFd);
                continue;
            }

            char buffer[1024];
            memset(buffer, 0, 1024);
            int r= read(clientFd, buffer, 1024-1);
            if(r>0){
                char method[20], url[128];
                sscanf(buffer, "%19s %255s", method, url);

            if (strcmp(url, "/hue-device.xml") == 0 && strcmp(method, "GET") == 0) {
                // statsUpnp.totalXmlRequests += 1;
                char responseHeader[] =
                    "HTTP/1.1 200 OK\r\n"
                    "Transfer-Encoding: chunked\r\n"
                    "Trailer: X-Checksum\r\n"
                    "\r\n";

                ssize_t out = write(clientFd, responseHeader, strlen(responseHeader));
                if(out <= 0){
                    close(clientFd);
                    free(newClient);
                    continue;
                }

                char chunk_size[10];
                snprintf(chunk_size, sizeof(FAKE_DEVICE_DESCRIPTION), "%X\r\n", (int)strlen(FAKE_DEVICE_DESCRIPTION));
                write(clientFd, chunk_size, strlen(chunk_size));
                write(clientFd, FAKE_DEVICE_DESCRIPTION, strlen(FAKE_DEVICE_DESCRIPTION));
                write(clientFd, "\r\n", 2);

                newClient->fd = clientFd;
                newClient->base.sendNext = now + delay;
                newClient->base.timeConnected = 0;
                snprintf(newClient->base.ipaddr, sizeof(newClient->base.ipaddr), "%s", inet_ntoa(clientAddr.sin_addr));
                queue_append(&clientQueueUpnp, (struct baseClient*)newClient);

                snprintf(newClient->base.ipaddr, sizeof(newClient->base.ipaddr), "%s", inet_ntoa(clientAddr.sin_addr));
                queue_append(&clientQueueUpnp, (struct baseClient*)newClient);
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

                close(clientFd);
                free(newClient);
            }
        } else {
            close(clientFd);
            free(newClient);
        }
    }
}
printf("Shutting down HTTP server gracefully\n");
while(clientQueueUpnp.head){
    struct baseClient *bc = queue_pop(&clientQueueUpnp);
    struct telnetAndUpnpClient *c = (struct telnetAndUpnpClient *)bc;
    long long timeTrapped = c->base.timeConnected;
    char msg[256];
    snprintf(msg, sizeof(msg), "%s disconnect %s %lld\n",
        SERVER_ID, c->base.ipaddr, timeTrapped);
    printf("%s", msg);
    sendMetric(msg);
    close(c->fd);
    free(c);
}
close(serverSock);
printf("HTTP server thread exiting\n");
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
    if(argc < 5){
        fprintf(stderr, "Usage: %s <httpPort> <ssdpPort> <delay> <maxNoClients>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

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
    httpPort = atoi(argv[1]);
    ssdpPort = atoi(argv[2]);
    delay = atoi(argv[3]);
    maxNoClients = atoi(argv[4]);
    // openlog("upnp_tarpit", LOG_PID | LOG_CONS, LOG_USER);
    initializeStats();
    setFdLimit(maxNoClients);
    struct sigaction sa;
    sa.sa_handler = handleSignal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
    pthread_t ssdpThread, httpThread;
    pthread_create(&ssdpThread, NULL, ssdpListener, NULL);
    pthread_create(&httpThread, NULL, httpServer, NULL);
    pthread_join(ssdpThread, NULL);
    pthread_join(httpThread, NULL);
    printf("UPnP pit shut down successfully\n");
    return 0;
}
