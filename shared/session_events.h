#ifndef SESSION_EVENTS_H
#define SESSION_EVENTS_H

#include <stddef.h>

#define SESSION_EVENT_ID_LEN 128
#define SESSION_EVENT_DEFAULT_PATH "/tmp/eventhorizon_sessions.jsonl"

void session_events_init(const char *path);
void session_events_make_id(char *buffer, size_t len, const char *protocol, long long start_ms, int fd);
void session_events_make_request_id(char *buffer, size_t len, const char *protocol,
                                    unsigned long counter, long long event_ms);
void session_events_write_connect(const char *protocol, const char *session_id);
void session_events_write_action(const char *protocol, const char *session_id,
                                 const char *action, const char *fields_json);
void session_events_write_disconnect(const char *protocol, const char *session_id,
                                     long long duration_ms, const char *disconnect_reason,
                                     unsigned int interaction_depth);

#endif
