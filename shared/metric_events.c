#include "metric_events.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define METRIC_EVENT_DEFAULT_SOCKET "/tmp/tarpit_exporter.sock"

static pthread_mutex_t metric_event_emitter_mutex = PTHREAD_MUTEX_INITIALIZER;
static char metric_event_socket_path[sizeof(((struct sockaddr_un *)0)->sun_path)] =
    METRIC_EVENT_DEFAULT_SOCKET;

static const char *telnet_finalization_reason_name(
    enum metric_telnet_finalization_reason reason) {
    switch (reason) {
        case METRIC_TELNET_FINALIZATION_PEER_CLOSED:
            return "peer_closed";
        case METRIC_TELNET_FINALIZATION_READ_ERROR:
            return "read_error";
        case METRIC_TELNET_FINALIZATION_WRITE_ERROR:
            return "write_error";
        case METRIC_TELNET_FINALIZATION_SERVER_SHUTDOWN:
            return "server_shutdown";
        case METRIC_TELNET_FINALIZATION_BOUNDED_POLICY:
            return "bounded_policy";
    }
    return NULL;
}

static const char *io_reason_name(enum metric_io_reason reason) {
    switch (reason) {
        case METRIC_IO_NONE:
            return "none";
        case METRIC_IO_TIMEOUT:
            return "timeout";
        case METRIC_IO_RESET:
            return "reset";
        case METRIC_IO_CLOSED:
            return "closed";
        case METRIC_IO_OTHER:
            return "other";
    }
    return NULL;
}

static bool valid_telnet_finalization_io(
    enum metric_telnet_finalization_reason finalization_reason,
    enum metric_io_reason io_reason) {
    switch (finalization_reason) {
        case METRIC_TELNET_FINALIZATION_READ_ERROR:
        case METRIC_TELNET_FINALIZATION_WRITE_ERROR:
            return io_reason == METRIC_IO_TIMEOUT ||
                   io_reason == METRIC_IO_RESET ||
                   io_reason == METRIC_IO_CLOSED ||
                   io_reason == METRIC_IO_OTHER;
        case METRIC_TELNET_FINALIZATION_PEER_CLOSED:
        case METRIC_TELNET_FINALIZATION_SERVER_SHUTDOWN:
        case METRIC_TELNET_FINALIZATION_BOUNDED_POLICY:
            return io_reason == METRIC_IO_NONE;
    }
    return false;
}

static bool is_io_failure_reason(enum metric_io_reason reason) {
    switch (reason) {
        case METRIC_IO_TIMEOUT:
        case METRIC_IO_RESET:
        case METRIC_IO_CLOSED:
        case METRIC_IO_OTHER:
            return true;
        case METRIC_IO_NONE:
            return false;
    }
    return false;
}

enum metric_io_reason metric_io_reason_from_unrecoverable_errno(
    int error_number,
    bool configured_socket_timeout_expired) {
    if (configured_socket_timeout_expired) {
        return METRIC_IO_TIMEOUT;
    }
    switch (error_number) {
        case ECONNRESET:
            return METRIC_IO_RESET;
        case EPIPE:
        case ENOTCONN:
        case ESHUTDOWN:
            return METRIC_IO_CLOSED;
        default:
            return METRIC_IO_OTHER;
    }
}

static const char *mqtt_finalization_reason_name(
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
    return NULL;
}

static bool valid_mqtt_finalization_io(
    enum metric_mqtt_finalization_reason finalization_reason,
    enum metric_io_reason io_reason) {
    switch (finalization_reason) {
        case METRIC_MQTT_FINALIZATION_READ_ERROR:
        case METRIC_MQTT_FINALIZATION_WRITE_ERROR:
            return is_io_failure_reason(io_reason);
        case METRIC_MQTT_FINALIZATION_PEER_CLOSED:
        case METRIC_MQTT_FINALIZATION_DISCONNECT_RECEIVED:
        case METRIC_MQTT_FINALIZATION_CONNECT_REFUSED:
        case METRIC_MQTT_FINALIZATION_OPERATION_REFUSED:
        case METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR:
        case METRIC_MQTT_FINALIZATION_KEEP_ALIVE_TIMEOUT:
        case METRIC_MQTT_FINALIZATION_SERVER_SHUTDOWN:
        case METRIC_MQTT_FINALIZATION_BOUNDED_POLICY:
            return io_reason == METRIC_IO_NONE;
    }
    return false;
}

static const char *mqtt_action_name(enum metric_mqtt_action action) {
    switch (action) {
        case METRIC_MQTT_ACTION_CONNECT_ACCEPTED:
            return "connect_accepted";
        case METRIC_MQTT_ACTION_PUBLISH_RECEIVED:
            return "publish_received";
        case METRIC_MQTT_ACTION_SUBSCRIBE_RECEIVED:
            return "subscribe_received";
        case METRIC_MQTT_ACTION_UNSUBSCRIBE_RECEIVED:
            return "unsubscribe_received";
        case METRIC_MQTT_ACTION_SUBACK_SENT:
            return "suback_sent";
        case METRIC_MQTT_ACTION_UNSUBACK_SENT:
            return "unsuback_sent";
        case METRIC_MQTT_ACTION_PUBACK_SENT:
            return "puback_sent";
        case METRIC_MQTT_ACTION_PUBREC_SENT:
            return "pubrec_sent";
        case METRIC_MQTT_ACTION_PUBREL_RECEIVED:
            return "pubrel_received";
        case METRIC_MQTT_ACTION_PUBCOMP_SENT:
            return "pubcomp_sent";
        case METRIC_MQTT_ACTION_SUBSCRIPTION_PUBLISH_SENT:
            return "subscription_publish_sent";
    }
    return NULL;
}

static const char *coap_request_outcome_name(
    enum metric_coap_request_outcome outcome) {
    switch (outcome) {
        case METRIC_COAP_REQUEST_RESPONSE_SENT:
            return "response_sent";
        case METRIC_COAP_REQUEST_TERMINATED:
            return "terminated";
    }
    return NULL;
}

static const char *coap_con_outcome_name(enum metric_coap_con_outcome outcome) {
    switch (outcome) {
        case METRIC_COAP_CON_ACK_RECEIVED:
            return "ack_received";
        case METRIC_COAP_CON_RST_RECEIVED:
            return "rst_received";
        case METRIC_COAP_CON_RETRY_EXHAUSTED:
            return "retry_exhausted";
    }
    return NULL;
}

static const char *upnp_action_name(enum metric_upnp_action action) {
    switch (action) {
        case METRIC_UPNP_ACTION_SSDP_MSEARCH_RECEIVED:
            return "ssdp_msearch_received";
        case METRIC_UPNP_ACTION_SSDP_DISCOVERY_RESPONSE_SENT:
            return "ssdp_discovery_response_sent";
        case METRIC_UPNP_ACTION_DESCRIPTION_GET_RECEIVED:
            return "description_get_received";
    }
    return NULL;
}

static const char *upnp_description_outcome_name(
    enum metric_upnp_description_outcome outcome) {
    switch (outcome) {
        case METRIC_UPNP_DESCRIPTION_COMPLETED:
            return "completed";
        case METRIC_UPNP_DESCRIPTION_TERMINATED:
            return "terminated";
    }
    return NULL;
}

static const char *ssh_observation_end_reason_name(
    enum metric_ssh_observation_end_reason observation_end_reason) {
    switch (observation_end_reason) {
        case METRIC_SSH_OBSERVATION_END_WRITE_FAILED:
            return "write_failed";
        case METRIC_SSH_OBSERVATION_END_SERVER_SHUTDOWN:
            return "server_shutdown";
    }
    return NULL;
}

static bool send_metric_event(const char *buffer, int encoded_length) {
    if (!buffer || encoded_length < 0 ||
        encoded_length > METRIC_EVENT_DATAGRAM_LIMIT) {
        return false;
    }

    bool success = false;
    pthread_mutex_lock(&metric_event_emitter_mutex);

    int socket_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (socket_fd >= 0) {
        struct sockaddr_un address;
        memset(&address, 0, sizeof(address));
        address.sun_family = AF_UNIX;
        memcpy(address.sun_path, metric_event_socket_path,
               strlen(metric_event_socket_path) + 1);

        ssize_t sent = sendto(socket_fd, buffer, (size_t)encoded_length, 0,
                              (struct sockaddr *)&address, sizeof(address));
        success = sent == (ssize_t)encoded_length;
        close(socket_fd);
    }

    pthread_mutex_unlock(&metric_event_emitter_mutex);
    return success;
}

bool metric_event_emitter_init(const char *socket_path) {
    const char *selected_path = socket_path ? socket_path : METRIC_EVENT_DEFAULT_SOCKET;
    if (selected_path[0] == '\0' ||
        strlen(selected_path) >= sizeof(metric_event_socket_path)) {
        return false;
    }

    pthread_mutex_lock(&metric_event_emitter_mutex);
    memcpy(metric_event_socket_path, selected_path, strlen(selected_path) + 1);
    pthread_mutex_unlock(&metric_event_emitter_mutex);
    return true;
}

bool metric_event_telnet_connection_accepted(uint32_t active_count_after) {
    if (active_count_after == 0) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"connection_accepted\","
        "\"active_count_after\":%" PRIu32 "}",
        active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_telnet_connection_finalized(
    enum metric_telnet_finalization_reason finalization_reason,
    uint64_t duration_ms,
    uint8_t depth_level,
    uint32_t active_count_after,
    enum metric_io_reason io_reason) {
    const char *finalization_name =
        telnet_finalization_reason_name(finalization_reason);
    const char *io_name = io_reason_name(io_reason);
    if (!finalization_name || !io_name ||
        duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER || depth_level > 3 ||
        !valid_telnet_finalization_io(finalization_reason, io_reason)) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"connection_finalized\","
        "\"finalization_reason\":\"%s\",\"duration_ms\":%" PRIu64
        ",\"depth_level\":%" PRIu8 ",\"active_count_after\":%" PRIu32
        ",\"io_reason\":\"%s\"}",
        finalization_name, duration_ms, depth_level, active_count_after,
        io_name);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_telnet_positive_read(uint64_t bytes) {
    if (bytes == 0 || bytes > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"positive_read\","
        "\"bytes\":%" PRIu64 "}",
        bytes);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_telnet_first_positive_write(uint64_t duration_ms,
                                              uint64_t bytes) {
    if (duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER || bytes == 0 ||
        bytes > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"first_positive_write\","
        "\"duration_ms\":%" PRIu64 ",\"bytes\":%" PRIu64 "}",
        duration_ms, bytes);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_telnet_subsequent_positive_write(uint64_t duration_ms,
                                                   uint64_t bytes) {
    if (duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER || bytes == 0 ||
        bytes > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"subsequent_positive_write\","
        "\"duration_ms\":%" PRIu64 ",\"bytes\":%" PRIu64 "}",
        duration_ms, bytes);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_mqtt_connection_accepted(uint32_t active_count_after) {
    if (active_count_after == 0) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"connection_accepted\","
        "\"active_count_after\":%" PRIu32 "}",
        active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_mqtt_connection_finalized(
    enum metric_mqtt_finalization_reason finalization_reason,
    uint64_t duration_ms,
    uint8_t depth_level,
    uint32_t active_count_after,
    enum metric_io_reason io_reason) {
    const char *finalization_name =
        mqtt_finalization_reason_name(finalization_reason);
    const char *io_name = io_reason_name(io_reason);
    if (!finalization_name || !io_name ||
        duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER || depth_level > 3 ||
        !valid_mqtt_finalization_io(finalization_reason, io_reason)) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"connection_finalized\","
        "\"finalization_reason\":\"%s\",\"duration_ms\":%" PRIu64
        ",\"depth_level\":%" PRIu8 ",\"active_count_after\":%" PRIu32
        ",\"io_reason\":\"%s\"}",
        finalization_name, duration_ms, depth_level, active_count_after,
        io_name);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_mqtt_positive_read(uint64_t bytes) {
    if (bytes == 0 || bytes > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"positive_read\","
        "\"bytes\":%" PRIu64 "}",
        bytes);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_mqtt_positive_write(uint64_t bytes) {
    if (bytes == 0 || bytes > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"positive_write\","
        "\"bytes\":%" PRIu64 "}",
        bytes);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_mqtt_protocol_action(enum metric_mqtt_action action) {
    const char *action_name = mqtt_action_name(action);
    if (!action_name) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"protocol_action\","
        "\"action\":\"%s\"}",
        action_name);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_mqtt_connack_sent(uint64_t duration_ms) {
    if (duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"connack_sent\","
        "\"duration_ms\":%" PRIu64 "}",
        duration_ms);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_mqtt_secondary_write_error(enum metric_io_reason io_reason) {
    const char *io_name = io_reason_name(io_reason);
    if (!io_name || !is_io_failure_reason(io_reason)) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"secondary_write_error\","
        "\"io_reason\":\"%s\"}",
        io_name);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_coap_request_received(uint32_t request_active_count_after) {
    if (request_active_count_after == 0) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"request_received\","
        "\"request_active_count_after\":%" PRIu32 "}",
        request_active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_coap_request_finalized(
    enum metric_coap_request_outcome outcome,
    uint64_t duration_ms,
    uint32_t request_active_count_after) {
    const char *outcome_name = coap_request_outcome_name(outcome);
    if (!outcome_name || duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"request_finalized\","
        "\"outcome\":\"%s\",\"duration_ms\":%" PRIu64
        ",\"request_active_count_after\":%" PRIu32 "}",
        outcome_name, duration_ms, request_active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_coap_con_response_sent(uint64_t request_duration_ms,
                                         uint32_t request_active_count_after,
                                         uint32_t con_active_count_after) {
    if (request_duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER ||
        con_active_count_after == 0) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"con_response_sent\","
        "\"request_duration_ms\":%" PRIu64
        ",\"request_active_count_after\":%" PRIu32
        ",\"con_active_count_after\":%" PRIu32 "}",
        request_duration_ms, request_active_count_after,
        con_active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_coap_con_response_retransmitted(void) {
    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"con_response_retransmitted\"}");
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_coap_con_response_finalized(
    enum metric_coap_con_outcome outcome,
    uint64_t duration_ms,
    uint32_t con_active_count_after) {
    const char *outcome_name = coap_con_outcome_name(outcome);
    if (!outcome_name || duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"con_response_finalized\","
        "\"outcome\":\"%s\",\"duration_ms\":%" PRIu64
        ",\"con_active_count_after\":%" PRIu32 "}",
        outcome_name, duration_ms, con_active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_coap_write_error(enum metric_io_reason io_reason) {
    const char *io_name = io_reason_name(io_reason);
    if (!io_name || !is_io_failure_reason(io_reason)) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"write_error\","
        "\"io_reason\":\"%s\"}",
        io_name);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_upnp_protocol_action(enum metric_upnp_action action) {
    const char *action_name = upnp_action_name(action);
    if (!action_name) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"protocol_action\","
        "\"action\":\"%s\"}",
        action_name);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_upnp_description_response_started(
    uint32_t active_count_after) {
    if (active_count_after == 0) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"description_response_started\","
        "\"active_count_after\":%" PRIu32 "}",
        active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_upnp_description_response_finalized(
    enum metric_upnp_description_outcome outcome,
    uint64_t duration_ms,
    uint32_t active_count_after) {
    const char *outcome_name = upnp_description_outcome_name(outcome);
    if (!outcome_name || duration_ms > METRIC_EVENT_MAX_EXACT_INTEGER ||
        (outcome == METRIC_UPNP_DESCRIPTION_COMPLETED && duration_ms > 30000)) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"description_response_finalized\","
        "\"outcome\":\"%s\",\"duration_ms\":%" PRIu64
        ",\"active_count_after\":%" PRIu32 "}",
        outcome_name, duration_ms, active_count_after);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_upnp_write_error(enum metric_io_reason io_reason) {
    const char *io_name = io_reason_name(io_reason);
    if (!io_name || !is_io_failure_reason(io_reason)) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"write_error\","
        "\"io_reason\":\"%s\"}",
        io_name);
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_ssh_connection_accepted(void) {
    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"ssh\",\"event\":\"connection_accepted\"}");
    return send_metric_event(buffer, encoded_length);
}

bool metric_event_ssh_tracked_client_finalized(
    enum metric_ssh_observation_end_reason observation_end_reason,
    uint64_t lifetime_ms) {
    const char *reason_name =
        ssh_observation_end_reason_name(observation_end_reason);
    if (!reason_name || lifetime_ms > METRIC_EVENT_MAX_EXACT_INTEGER) {
        return false;
    }

    char buffer[METRIC_EVENT_DATAGRAM_LIMIT + 1];
    int encoded_length = snprintf(
        buffer, sizeof(buffer),
        "{\"v\":1,\"protocol\":\"ssh\",\"event\":\"tracked_client_finalized\","
        "\"observation_end_reason\":\"%s\",\"lifetime_ms\":%" PRIu64 "}",
        reason_name, lifetime_ms);
    return send_metric_event(buffer, encoded_length);
}
