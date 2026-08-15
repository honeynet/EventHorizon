#define _POSIX_C_SOURCE 199309L

#include "session_events.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static FILE *session_events_file = NULL;
static int session_events_open_attempted = 0;
static pthread_mutex_t session_events_lock = PTHREAD_MUTEX_INITIALIZER;

static const char *value_or_default(const char *value, const char *fallback) {
    return value && value[0] ? value : fallback;
}

static const char *session_mode(void) {
    return value_or_default(getenv("EVENTHORIZON_MODE"), "baseline");
}

static const char *counter_fingerprint_method(void) {
    return value_or_default(getenv("EVENTHORIZON_COUNTER_FINGERPRINT_METHOD"), "none");
}

static void write_json_string(FILE *file, const char *value) {
    const unsigned char *p = (const unsigned char *)value_or_default(value, "");
    fputc('"', file);
    while (*p) {
        switch (*p) {
            case '\\':
                fputs("\\\\", file);
                break;
            case '"':
                fputs("\\\"", file);
                break;
            case '\b':
                fputs("\\b", file);
                break;
            case '\f':
                fputs("\\f", file);
                break;
            case '\n':
                fputs("\\n", file);
                break;
            case '\r':
                fputs("\\r", file);
                break;
            case '\t':
                fputs("\\t", file);
                break;
            default:
                if (*p < 0x20) {
                    fprintf(file, "\\u%04x", *p);
                } else {
                    fputc(*p, file);
                }
                break;
        }
        p++;
    }
    fputc('"', file);
}

static void current_iso8601_utc(char *buffer, size_t len) {
    struct timespec ts;
    struct tm tm_utc;

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        snprintf(buffer, len, "1970-01-01T00:00:00.000Z");
        return;
    }

    gmtime_r(&ts.tv_sec, &tm_utc);
    snprintf(buffer, len, "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ",
             tm_utc.tm_year + 1900,
             tm_utc.tm_mon + 1,
             tm_utc.tm_mday,
             tm_utc.tm_hour,
             tm_utc.tm_min,
             tm_utc.tm_sec,
             ts.tv_nsec / 1000000L);
}

static void session_events_init_unlocked(const char *path) {
    const char *configured_path = path;

    if (session_events_file || session_events_open_attempted) {
        return;
    }

    if (!configured_path || configured_path[0] == '\0') {
        configured_path = getenv("EVENTHORIZON_SESSION_LOG");
    }
    if (!configured_path || configured_path[0] == '\0') {
        configured_path = SESSION_EVENT_DEFAULT_PATH;
    }

    session_events_open_attempted = 1;
    session_events_file = fopen(configured_path, "a");
    if (!session_events_file) {
        fprintf(stderr, "Failed to open session event log %s: %s\n",
                configured_path, strerror(errno));
    }
}

void session_events_init(const char *path) {
    pthread_mutex_lock(&session_events_lock);
    session_events_init_unlocked(path);
    pthread_mutex_unlock(&session_events_lock);
}

void session_events_make_id(char *buffer, size_t len, const char *protocol, long long start_ms, int fd) {
    snprintf(buffer, len, "%s-%ld-%d-%lld",
             value_or_default(protocol, "unknown"), (long)getpid(), fd, start_ms);
}

void session_events_make_request_id(char *buffer, size_t len, const char *protocol,
                                    unsigned long counter, long long event_ms) {
    snprintf(buffer, len, "%s-%ld-%lu-%lld",
             value_or_default(protocol, "unknown"), (long)getpid(), counter, event_ms);
}

static void write_common_fields(const char *event_type, const char *protocol, const char *session_id) {
    char ts[32];
    current_iso8601_utc(ts, sizeof(ts));

    fputs("{\"ts\":", session_events_file);
    write_json_string(session_events_file, ts);
    fputs(",\"event_type\":", session_events_file);
    write_json_string(session_events_file, event_type);
    fputs(",\"protocol\":", session_events_file);
    write_json_string(session_events_file, protocol);
    fputs(",\"session_id\":", session_events_file);
    write_json_string(session_events_file, session_id);
    fputs(",\"mode\":", session_events_file);
    write_json_string(session_events_file, session_mode());
    fputs(",\"counter_fingerprint_method\":", session_events_file);
    write_json_string(session_events_file, counter_fingerprint_method());
}

void session_events_write_connect(const char *protocol, const char *session_id) {
    pthread_mutex_lock(&session_events_lock);
    session_events_init_unlocked(NULL);
    if (!session_events_file) {
        pthread_mutex_unlock(&session_events_lock);
        return;
    }

    write_common_fields("connect", protocol, session_id);
    fputs("}\n", session_events_file);
    fflush(session_events_file);
    pthread_mutex_unlock(&session_events_lock);
}

void session_events_write_action(const char *protocol, const char *session_id,
                                 const char *action, const char *fields_json) {
    pthread_mutex_lock(&session_events_lock);
    session_events_init_unlocked(NULL);
    if (!session_events_file) {
        pthread_mutex_unlock(&session_events_lock);
        return;
    }

    write_common_fields("protocol_action", protocol, session_id);
    fputs(",\"action\":", session_events_file);
    write_json_string(session_events_file, action);
    if (fields_json && fields_json[0]) {
        fputc(',', session_events_file);
        fputs(fields_json, session_events_file);
    }
    fputs("}\n", session_events_file);
    fflush(session_events_file);
    pthread_mutex_unlock(&session_events_lock);
}

void session_events_write_disconnect(const char *protocol, const char *session_id,
                                     long long duration_ms, const char *disconnect_reason,
                                     unsigned int interaction_depth) {
    pthread_mutex_lock(&session_events_lock);
    session_events_init_unlocked(NULL);
    if (!session_events_file) {
        pthread_mutex_unlock(&session_events_lock);
        return;
    }

    if (duration_ms < 0) {
        duration_ms = 0;
    }

    write_common_fields("disconnect", protocol, session_id);
    fprintf(session_events_file, ",\"duration_ms\":%lld,\"interaction_depth\":%u,\"disconnect_reason\":",
            duration_ms, interaction_depth);
    write_json_string(session_events_file, value_or_default(disconnect_reason, "unknown"));
    fputs("}\n", session_events_file);
    fflush(session_events_file);
    pthread_mutex_unlock(&session_events_lock);
}
