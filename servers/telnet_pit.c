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

#define SERVER_ID "Telnet"

#define IAC 255
#define DO 253
#define DONT 254
#define WILL 251
#define WONT 252

int port;
int delay;
int maxNoClients;

unsigned char negotiations[][3] = {
    {IAC, WILL, 1}, 
    {IAC, DO, 3}, 
    {IAC, DONT, 5},
    {IAC, WILL, 31}, 
    {IAC, DO, 24}, 
    {IAC, WONT, 39}
};
int num_options = sizeof(negotiations) / sizeof(negotiations[0]);

void initializeStats(){
    statsTelnet.totalConnects = 0;
    statsTelnet.totalWastedTime = 0;
    statsTelnet.mostConcurrentConnections = 0;
}

// ── Buffered read helper ─────────────────────────────────────────────────────
// Reads available bytes into c->inputBuf, echoes printable chars back.
// Returns 1 when Enter is pressed and buffer has content, 0 otherwise.
static int readUntilEnter(struct telnetAndUpnpClient *c) {
    char chunk[256];
    ssize_t n = read(c->fd, chunk, sizeof(chunk) - 1);

    if (n <= 0) return 0;

    for (int i = 0; i < n; i++) {
        unsigned char ch = chunk[i];

        // Skip IAC negotiation sequences (3 bytes: IAC + cmd + option)
        if (ch == IAC) { i += 2; continue; }

        // Skip non-printable, non-CR/LF bytes
        if (ch < 32 && ch != '\r' && ch != '\n') continue;

        // Echo printable chars back so attacker sees what they type
        if (ch >= 32 && ch <= 126) {
            write(c->fd, &ch, 1);
        }

        if (ch == '\r' || ch == '\n') {
            if (c->inputBufLen > 0) {
                c->inputBuf[c->inputBufLen] = '\0';
                c->inputBufLen = 0;
                return 1;   // Enter pressed with content
            }
        } else {
            if (c->inputBufLen < (int)sizeof(c->inputBuf) - 1) {
                c->inputBuf[c->inputBufLen++] = ch;
            }
        }
    }
    return 0;
}

// ── Log credentials to file ──────────────────────────────────────────────────
static void logCredentials(const char *ip, const char *username, const char *password) {
    FILE *f = fopen("credentials.log", "a");
    if (!f) return;
    time_t t = time(NULL);
    char timeStr[64];
    strncpy(timeStr, ctime(&t), sizeof(timeStr) - 1);
    timeStr[strcspn(timeStr, "\n")] = '\0';   // strip trailing newline
    fprintf(f, "[%s] ip=%s user=%s pass=%s\n", timeStr, ip, username, password);
    fclose(f);
}

int main(int argc, char *argv[]) {
    setbuf(stdout, NULL);

    (void)argc;
    port          = atoi(argv[1]);
    delay         = atoi(argv[2]);
    maxNoClients  = atoi(argv[3]);
    initializeStats();
    setFdLimit(maxNoClients);
    signal(SIGPIPE, SIG_IGN);
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
    fds.fd     = serverSock;
    fds.events = POLLIN;

    while (1) {
        long long now     = currentTimeMs();
        int       timeout = -1;

        // ── Process clients in queue ─────────────────────────────────────────
        while (clientQueueTelnet.head) {
            if (clientQueueTelnet.head->sendNext > now) {
                timeout = clientQueueTelnet.head->sendNext - now;
                break;
            }

            struct baseClient          *bc = queue_pop(&clientQueueTelnet);
            struct telnetAndUpnpClient *c  = (struct telnetAndUpnpClient *)bc;

            ssize_t out = 0;

            // ── Step 1: Send "login: " prompt ────────────────────────────────
            if (c->loginPromptSent == 0) {
                out = write(c->fd, "login: ", 7);
                c->loginPromptSent = 1;
            }

            // ── Step 2: Read username ────────────────────────────────────────
            else if (c->inputCaptured == 0) {
                if (readUntilEnter(c)) {
                    strncpy(c->username, c->inputBuf, sizeof(c->username) - 1);
                    printf("Username from %s: %s\n", c->base.ipaddr, c->username);
                    c->inputCaptured = 1;

                    // Send password prompt immediately
                    write(c->fd, "\r\nPassword: ", 12);
                    c->passwordPromptSent = 1;
                }
                out = write(c->fd, negotiations[rand() % num_options], 3);
            }

            // ── Step 3: Read password ────────────────────────────────────────
            else if (c->passwordPromptSent && c->passwordCaptured == 0) {
                if (readUntilEnter(c)) {
                    printf("Credentials from %s — user: %s  pass: %s\n",
                           c->base.ipaddr, c->username, c->inputBuf);
                    logCredentials(c->base.ipaddr, c->username, c->inputBuf);
                    c->passwordCaptured = 1;

                    // Fake "Login incorrect" and restart to catch more attempts
                    write(c->fd, "\r\nLogin incorrect\r\n\r\nlogin: ", 28);
                    c->inputCaptured      = 0;
                    c->passwordPromptSent = 0;
                    c->passwordCaptured   = 0;
                    memset(c->username, 0, sizeof(c->username));
                }
                out = write(c->fd, negotiations[rand() % num_options], 3);
            }

            // ── Step 4: Hold connection with negotiations ────────────────────
            else {
                out = write(c->fd, negotiations[rand() % num_options], 3);
            }

            // ── Error / requeue handling (unchanged) ────────────────────────
            if (out == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    c->base.sendNext     = now + delay;
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
                c->base.sendNext      = now + delay;
                c->base.timeConnected += delay;
                statsTelnet.totalWastedTime += delay;
                queue_append(&clientQueueTelnet, (struct baseClient *)c);
            }
        }

        // ── Poll for new connections ─────────────────────────────────────────
        int pollResult = poll(&fds, 1, timeout);
        now = currentTimeMs();
        if (pollResult < 0) {
            fprintf(stderr, "Poll error with error %s", strerror(errno));
            continue;
        }

        if (fds.revents & POLLIN) {
            int clientFd = accept(serverSock, (struct sockaddr *)&clientAddr, &addrLen);
            if (clientFd == -1) {
                fprintf(stderr, "Failed accepting new client with error %s", strerror(errno));
                continue;
            }

            fcntl(clientFd, F_SETFL, O_NONBLOCK);

            struct telnetAndUpnpClient *newClient = malloc(sizeof(struct telnetAndUpnpClient));
            if (!newClient) {
                fprintf(stderr, "Out of memory");
                close(clientFd);
                continue;
            }

            // Initialise all new fields
            memset(newClient, 0, sizeof(struct telnetAndUpnpClient));
            newClient->fd                 = clientFd;
            newClient->base.sendNext      = now + delay;
            newClient->base.timeConnected = 0;
            snprintf(newClient->base.ipaddr, INET_ADDRSTRLEN, "%s",
                     inet_ntoa(clientAddr.sin_addr));

            queue_append(&clientQueueTelnet, (struct baseClient *)newClient);

            statsTelnet.totalConnects += 1;
            if (statsTelnet.mostConcurrentConnections < clientQueueTelnet.length)
                statsTelnet.mostConcurrentConnections = clientQueueTelnet.length;

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