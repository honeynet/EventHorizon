#ifndef METRIC_EVENTS_H
#define METRIC_EVENTS_H

#include <stdbool.h>
#include <stdint.h>

#define METRIC_EVENT_DATAGRAM_LIMIT 512
#define METRIC_EVENT_MAX_EXACT_INTEGER UINT64_C(9007199254740991)

enum metric_io_reason {
    METRIC_IO_NONE = 0,
    METRIC_IO_TIMEOUT,
    METRIC_IO_RESET,
    METRIC_IO_CLOSED,
    METRIC_IO_OTHER,
};

enum metric_telnet_finalization_reason {
    METRIC_TELNET_FINALIZATION_PEER_CLOSED = 0,
    METRIC_TELNET_FINALIZATION_READ_ERROR,
    METRIC_TELNET_FINALIZATION_WRITE_ERROR,
    METRIC_TELNET_FINALIZATION_SERVER_SHUTDOWN,
    METRIC_TELNET_FINALIZATION_BOUNDED_POLICY,
};

enum metric_mqtt_finalization_reason {
    METRIC_MQTT_FINALIZATION_PEER_CLOSED = 0,
    METRIC_MQTT_FINALIZATION_DISCONNECT_RECEIVED,
    METRIC_MQTT_FINALIZATION_CONNECT_REFUSED,
    METRIC_MQTT_FINALIZATION_OPERATION_REFUSED,
    METRIC_MQTT_FINALIZATION_PROTOCOL_ERROR,
    METRIC_MQTT_FINALIZATION_KEEP_ALIVE_TIMEOUT,
    METRIC_MQTT_FINALIZATION_READ_ERROR,
    METRIC_MQTT_FINALIZATION_WRITE_ERROR,
    METRIC_MQTT_FINALIZATION_SERVER_SHUTDOWN,
    METRIC_MQTT_FINALIZATION_BOUNDED_POLICY,
};

enum metric_mqtt_action {
    METRIC_MQTT_ACTION_CONNECT_ACCEPTED = 0,
    METRIC_MQTT_ACTION_PUBLISH_RECEIVED,
    METRIC_MQTT_ACTION_SUBSCRIBE_RECEIVED,
    METRIC_MQTT_ACTION_UNSUBSCRIBE_RECEIVED,
    METRIC_MQTT_ACTION_SUBACK_SENT,
    METRIC_MQTT_ACTION_UNSUBACK_SENT,
    METRIC_MQTT_ACTION_PUBACK_SENT,
    METRIC_MQTT_ACTION_PUBREC_SENT,
    METRIC_MQTT_ACTION_PUBREL_RECEIVED,
    METRIC_MQTT_ACTION_PUBCOMP_SENT,
    METRIC_MQTT_ACTION_SUBSCRIPTION_PUBLISH_SENT,
};

enum metric_coap_request_outcome {
    METRIC_COAP_REQUEST_RESPONSE_SENT = 0,
    METRIC_COAP_REQUEST_TERMINATED,
};

enum metric_coap_con_outcome {
    METRIC_COAP_CON_ACK_RECEIVED = 0,
    METRIC_COAP_CON_RST_RECEIVED,
    METRIC_COAP_CON_RETRY_EXHAUSTED,
};

enum metric_upnp_action {
    METRIC_UPNP_ACTION_SSDP_MSEARCH_RECEIVED = 0,
    METRIC_UPNP_ACTION_SSDP_DISCOVERY_RESPONSE_SENT,
    METRIC_UPNP_ACTION_DESCRIPTION_GET_RECEIVED,
};

enum metric_upnp_description_outcome {
    METRIC_UPNP_DESCRIPTION_COMPLETED = 0,
    METRIC_UPNP_DESCRIPTION_TERMINATED,
};

enum metric_ssh_observation_end_reason {
    METRIC_SSH_OBSERVATION_END_WRITE_FAILED = 0,
    METRIC_SSH_OBSERVATION_END_SERVER_SHUTDOWN,
};

/* Call only after the operation is known to be an unrecoverable I/O failure. */
enum metric_io_reason metric_io_reason_from_unrecoverable_errno(
    int error_number,
    bool configured_socket_timeout_expired);

/* Configure the Unix datagram destination before protocol workers start. */
bool metric_event_emitter_init(const char *socket_path);

bool metric_event_telnet_connection_accepted(uint32_t active_count_after);
bool metric_event_telnet_connection_finalized(
    enum metric_telnet_finalization_reason finalization_reason,
    uint64_t duration_ms,
    uint8_t depth_level,
    uint32_t active_count_after,
    enum metric_io_reason io_reason);
bool metric_event_telnet_positive_read(uint64_t bytes);
bool metric_event_telnet_first_positive_write(uint64_t duration_ms,
                                              uint64_t bytes);
bool metric_event_telnet_subsequent_positive_write(uint64_t duration_ms,
                                                   uint64_t bytes);

bool metric_event_mqtt_connection_accepted(uint32_t active_count_after);
bool metric_event_mqtt_connection_finalized(
    enum metric_mqtt_finalization_reason finalization_reason,
    uint64_t duration_ms,
    uint8_t depth_level,
    uint32_t active_count_after,
    enum metric_io_reason io_reason);
bool metric_event_mqtt_positive_read(uint64_t bytes);
bool metric_event_mqtt_positive_write(uint64_t bytes);
bool metric_event_mqtt_protocol_action(enum metric_mqtt_action action);
bool metric_event_mqtt_connack_sent(uint64_t duration_ms);
bool metric_event_mqtt_secondary_write_error(enum metric_io_reason io_reason);

bool metric_event_coap_request_received(uint32_t request_active_count_after);
bool metric_event_coap_request_finalized(
    enum metric_coap_request_outcome outcome,
    uint64_t duration_ms,
    uint32_t request_active_count_after);
bool metric_event_coap_con_response_sent(uint64_t request_duration_ms,
                                         uint32_t request_active_count_after,
                                         uint32_t con_active_count_after);
bool metric_event_coap_con_response_retransmitted(void);
bool metric_event_coap_con_response_finalized(
    enum metric_coap_con_outcome outcome,
    uint64_t duration_ms,
    uint32_t con_active_count_after);
bool metric_event_coap_write_error(enum metric_io_reason io_reason);

bool metric_event_upnp_protocol_action(enum metric_upnp_action action);
bool metric_event_upnp_description_response_started(uint32_t active_count_after);
bool metric_event_upnp_description_response_finalized(
    enum metric_upnp_description_outcome outcome,
    uint64_t duration_ms,
    uint32_t active_count_after);
bool metric_event_upnp_write_error(enum metric_io_reason io_reason);

bool metric_event_ssh_connection_accepted(void);
bool metric_event_ssh_tracked_client_finalized(
    enum metric_ssh_observation_end_reason observation_end_reason,
    uint64_t lifetime_ms);

#endif
