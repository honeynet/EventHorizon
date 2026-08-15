#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include "../shared/metric_events.h"

static int open_receiver(char *directory, size_t directory_size,
                         char *socket_path, size_t socket_path_size) {
    snprintf(directory, directory_size, "/tmp/eventhorizon-metric-test-XXXXXX");
    assert(mkdtemp(directory) != NULL);
    assert(snprintf(socket_path, socket_path_size, "%s/metrics.sock", directory) > 0);

    int receiver = socket(AF_UNIX, SOCK_DGRAM, 0);
    assert(receiver >= 0);

    struct timeval timeout = {.tv_sec = 0, .tv_usec = 100000};
    assert(setsockopt(receiver, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);

    struct sockaddr_un address;
    memset(&address, 0, sizeof(address));
    address.sun_family = AF_UNIX;
    assert(snprintf(address.sun_path, sizeof(address.sun_path), "%s", socket_path) > 0);
    assert(bind(receiver, (struct sockaddr *)&address, sizeof(address)) == 0);
    assert(metric_event_emitter_init(socket_path));
    return receiver;
}

static void assert_datagram(int receiver, const char *expected) {
    char buffer[513] = {0};
    ssize_t received = recv(receiver, buffer, sizeof(buffer), 0);
    assert(received == (ssize_t)strlen(expected));
    assert(memcmp(buffer, expected, (size_t)received) == 0);
}

static void assert_no_datagram(int receiver) {
    char buffer[513];
    errno = 0;
    assert(recv(receiver, buffer, sizeof(buffer), 0) == -1);
    assert(errno == EAGAIN || errno == EWOULDBLOCK);
}

int main(void) {
    char directory[64];
    char socket_path[108];
    int receiver = open_receiver(directory, sizeof(directory), socket_path,
                                 sizeof(socket_path));

    assert(metric_io_reason_from_unrecoverable_errno(EAGAIN, true) ==
           METRIC_IO_TIMEOUT);
    assert(metric_io_reason_from_unrecoverable_errno(ECONNRESET, false) ==
           METRIC_IO_RESET);
    assert(metric_io_reason_from_unrecoverable_errno(EPIPE, false) ==
           METRIC_IO_CLOSED);
    assert(metric_io_reason_from_unrecoverable_errno(ENOTCONN, false) ==
           METRIC_IO_CLOSED);
    assert(metric_io_reason_from_unrecoverable_errno(ESHUTDOWN, false) ==
           METRIC_IO_CLOSED);
    assert(metric_io_reason_from_unrecoverable_errno(EINVAL, false) ==
           METRIC_IO_OTHER);

    assert(metric_event_telnet_connection_accepted(1));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"connection_accepted\",\"active_count_after\":1}");

    assert(metric_event_telnet_positive_read(17));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"positive_read\",\"bytes\":17}");

    assert(metric_event_telnet_first_positive_write(100, 5));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"first_positive_write\",\"duration_ms\":100,\"bytes\":5}");

    assert(metric_event_telnet_subsequent_positive_write(150, 4));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"subsequent_positive_write\",\"duration_ms\":150,\"bytes\":4}");

    assert(metric_event_telnet_connection_finalized(
        METRIC_TELNET_FINALIZATION_READ_ERROR,
        1250,
        3,
        0,
        METRIC_IO_RESET));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"telnet\",\"event\":\"connection_finalized\",\"finalization_reason\":\"read_error\",\"duration_ms\":1250,\"depth_level\":3,\"active_count_after\":0,\"io_reason\":\"reset\"}");

    assert(metric_event_mqtt_connection_accepted(2));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"connection_accepted\",\"active_count_after\":2}");

    assert(metric_event_mqtt_positive_read(23));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"positive_read\",\"bytes\":23}");

    assert(metric_event_mqtt_positive_write(7));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"positive_write\",\"bytes\":7}");

    assert(metric_event_mqtt_protocol_action(METRIC_MQTT_ACTION_SUBSCRIBE_RECEIVED));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"protocol_action\",\"action\":\"subscribe_received\"}");

    assert(metric_event_mqtt_connack_sent(75));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"connack_sent\",\"duration_ms\":75}");

    assert(metric_event_mqtt_secondary_write_error(METRIC_IO_CLOSED));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"secondary_write_error\",\"io_reason\":\"closed\"}");

    assert(metric_event_mqtt_connection_finalized(
        METRIC_MQTT_FINALIZATION_READ_ERROR,
        30000,
        2,
        0,
        METRIC_IO_RESET));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"mqtt\",\"event\":\"connection_finalized\",\"finalization_reason\":\"read_error\",\"duration_ms\":30000,\"depth_level\":2,\"active_count_after\":0,\"io_reason\":\"reset\"}");

    assert(metric_event_coap_request_received(1));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"request_received\",\"request_active_count_after\":1}");

    assert(metric_event_coap_request_finalized(
        METRIC_COAP_REQUEST_RESPONSE_SENT, 250, 0));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"request_finalized\",\"outcome\":\"response_sent\",\"duration_ms\":250,\"request_active_count_after\":0}");

    assert(metric_event_coap_con_response_sent(400, 0, 1));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"con_response_sent\",\"request_duration_ms\":400,\"request_active_count_after\":0,\"con_active_count_after\":1}");

    assert(metric_event_coap_con_response_retransmitted());
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"con_response_retransmitted\"}");

    assert(metric_event_coap_con_response_finalized(
        METRIC_COAP_CON_ACK_RECEIVED, 2000, 0));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"con_response_finalized\",\"outcome\":\"ack_received\",\"duration_ms\":2000,\"con_active_count_after\":0}");

    assert(metric_event_coap_write_error(METRIC_IO_OTHER));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"coap\",\"event\":\"write_error\",\"io_reason\":\"other\"}");

    assert(metric_event_upnp_protocol_action(
        METRIC_UPNP_ACTION_SSDP_DISCOVERY_RESPONSE_SENT));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"protocol_action\",\"action\":\"ssdp_discovery_response_sent\"}");

    assert(metric_event_upnp_description_response_started(1));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"description_response_started\",\"active_count_after\":1}");

    assert(metric_event_upnp_description_response_finalized(
        METRIC_UPNP_DESCRIPTION_TERMINATED, 30000, 0));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"description_response_finalized\",\"outcome\":\"terminated\",\"duration_ms\":30000,\"active_count_after\":0}");

    assert(metric_event_upnp_write_error(METRIC_IO_TIMEOUT));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"upnp\",\"event\":\"write_error\",\"io_reason\":\"timeout\"}");

    /* SSH: the integrated Endlessh tarpit emits exactly two boundaries. */
    assert(metric_event_ssh_connection_accepted());
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"ssh\",\"event\":\"connection_accepted\"}");

    assert(metric_event_ssh_tracked_client_finalized(
        METRIC_SSH_OBSERVATION_END_WRITE_FAILED, 40012));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"ssh\",\"event\":\"tracked_client_finalized\","
        "\"observation_end_reason\":\"write_failed\",\"lifetime_ms\":40012}");

    assert(metric_event_ssh_tracked_client_finalized(
        METRIC_SSH_OBSERVATION_END_SERVER_SHUTDOWN, 0));
    assert_datagram(receiver,
        "{\"v\":1,\"protocol\":\"ssh\",\"event\":\"tracked_client_finalized\","
        "\"observation_end_reason\":\"server_shutdown\",\"lifetime_ms\":0}");

    assert(!metric_event_ssh_tracked_client_finalized(
        (enum metric_ssh_observation_end_reason)99, 1));
    assert_no_datagram(receiver);

    assert(!metric_event_ssh_tracked_client_finalized(
        METRIC_SSH_OBSERVATION_END_WRITE_FAILED,
        METRIC_EVENT_MAX_EXACT_INTEGER + 1));
    assert_no_datagram(receiver);

    assert(!metric_event_telnet_connection_finalized(
        (enum metric_telnet_finalization_reason)99,
        1,
        0,
        0,
        METRIC_IO_NONE));
    assert_no_datagram(receiver);

    close(receiver);
    unlink(socket_path);
    rmdir(directory);
    puts("metric event emitter tests passed");
    return 0;
}
