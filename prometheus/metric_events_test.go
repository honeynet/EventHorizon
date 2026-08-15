package main

import (
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// shortTempDir returns a temporary directory whose path is short enough to hold
// a Unix datagram socket. Linux caps socket paths at 108 bytes; t.TempDir()
// embeds the test name, which overflows that limit for the longer test names
// here on Go toolchains that do not truncate the pattern.
func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "eventhorizon")
	if err != nil {
		t.Fatalf("create temp directory: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(directory)
	})
	return directory
}

type gatheredFamilyContract struct {
	metricType string
	help       string
	series     int
}

type metricEventTestSocket struct {
	registry   *prometheus.Registry
	connection net.Conn
}

func newMetricEventTestSocket(t *testing.T) metricEventTestSocket {
	t.Helper()
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	return metricEventTestSocket{registry: registry, connection: connection}
}

func (socket metricEventTestSocket) write(t *testing.T, datagram string) {
	t.Helper()
	if written, err := socket.connection.Write([]byte(datagram)); err != nil {
		t.Fatalf("write metric event: %v", err)
	} else if written != len(datagram) {
		t.Fatalf("write metric event bytes = %d, want %d", written, len(datagram))
	}
}

func TestMetricEventSocketAppliesTelnetConnectionAccepted(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close metric event server: %v", err)
		}
	})

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	datagram := []byte(`{"v":1,"protocol":"telnet","event":"connection_accepted","active_count_after":1}`)
	if written, err := connection.Write(datagram); err != nil {
		t.Fatalf("write metric event: %v", err)
	} else if written != len(datagram) {
		t.Fatalf("write metric event bytes = %d, want %d", written, len(datagram))
	}

	assertGatheredValueEventually(t, registry, "total_connects", map[string]string{
		"server": "Telnet",
	}, 1)
	assertGatheredValueEventually(t, registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 1)
	assertGatheredValueEventually(t, registry, "eventhorizon_exporter_malformed_messages_total", map[string]string{
		"reason": "unsupported_event",
	}, 0)
}

func TestMetricEventSocketRejectsDuplicateFieldWithoutBusinessMutation(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	datagram := []byte(`{"v":1,"protocol":"telnet","event":"connection_accepted","active_count_after":1,"active_count_after":2}`)
	if written, err := connection.Write(datagram); err != nil {
		t.Fatalf("write metric event: %v", err)
	} else if written != len(datagram) {
		t.Fatalf("write metric event bytes = %d, want %d", written, len(datagram))
	}

	assertGatheredValueEventually(t, registry, "eventhorizon_exporter_malformed_messages_total", map[string]string{
		"reason": "unknown_format",
	}, 1)
	assertGatheredValueEventually(t, registry, "total_connects", map[string]string{
		"server": "Telnet",
	}, 0)
	assertGatheredValueEventually(t, registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 0)
}

func TestMetricEventSocketRejectsInvalidUTF8BeforeSchemaValidation(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	datagram := append([]byte(`{"v":1,"protocol":"tel`), 0xff)
	datagram = append(datagram, []byte(`net","event":"connection_accepted","active_count_after":1}`)...)
	if _, err := connection.Write(datagram); err != nil {
		t.Fatalf("write metric event: %v", err)
	}

	assertGatheredValueEventually(t, registry, "eventhorizon_exporter_malformed_messages_total", map[string]string{
		"reason": "unknown_format",
	}, 1)
	assertGatheredValueEventually(t, registry, "eventhorizon_exporter_malformed_messages_total", map[string]string{
		"reason": "unknown_server",
	}, 0)
	assertGatheredValueEventually(t, registry, "total_connects", map[string]string{
		"server": "Telnet",
	}, 0)
}

func TestMetricEventSocketAppliesTelnetPositiveReadBytes(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := connection.Write([]byte(`{"v":1,"protocol":"telnet","event":"connection_accepted","active_count_after":1}`)); err != nil {
		t.Fatalf("write accepted event: %v", err)
	}
	assertGatheredValueEventually(t, registry, "current_connected_clients", map[string]string{"server": "Telnet"}, 1)

	datagram := []byte(`{"v":1,"protocol":"telnet","event":"positive_read","bytes":17}`)
	if _, err := connection.Write(datagram); err != nil {
		t.Fatalf("write metric event: %v", err)
	}

	assertGatheredValueEventually(t, registry, "eventhorizon_bytes_received_total", map[string]string{
		"protocol": "telnet",
	}, 17)
	assertGatheredValueEventually(t, registry, "total_connects", map[string]string{
		"server": "Telnet",
	}, 1)
}

func TestMetricEventSocketAppliesTelnetFirstPositiveWrite(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := connection.Write([]byte(`{"v":1,"protocol":"telnet","event":"connection_accepted","active_count_after":1}`)); err != nil {
		t.Fatalf("write accepted event: %v", err)
	}
	assertGatheredValueEventually(t, registry, "current_connected_clients", map[string]string{"server": "Telnet"}, 1)

	datagram := []byte(`{"v":1,"protocol":"telnet","event":"first_positive_write","duration_ms":100,"bytes":5}`)
	if _, err := connection.Write(datagram); err != nil {
		t.Fatalf("write metric event: %v", err)
	}

	assertGatheredValueEventually(t, registry, "eventhorizon_bytes_sent_total", map[string]string{
		"protocol": "telnet",
	}, 5)
	assertGatheredHistogramEventually(t, registry, "eventhorizon_telnet_first_write_delay_ms", nil, 1, 100)
}

func TestMetricEventSocketAppliesTelnetSubsequentPositiveWrite(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := connection.Write([]byte(`{"v":1,"protocol":"telnet","event":"connection_accepted","active_count_after":1}`)); err != nil {
		t.Fatalf("write accepted event: %v", err)
	}
	if _, err := connection.Write([]byte(`{"v":1,"protocol":"telnet","event":"first_positive_write","duration_ms":100,"bytes":5}`)); err != nil {
		t.Fatalf("write first positive write event: %v", err)
	}
	assertGatheredHistogramEventually(t, registry, "eventhorizon_telnet_first_write_delay_ms", nil, 1, 100)

	datagram := []byte(`{"v":1,"protocol":"telnet","event":"subsequent_positive_write","duration_ms":150,"bytes":4}`)
	if _, err := connection.Write(datagram); err != nil {
		t.Fatalf("write metric event: %v", err)
	}

	assertGatheredValueEventually(t, registry, "eventhorizon_bytes_sent_total", map[string]string{
		"protocol": "telnet",
	}, 9)
	assertGatheredHistogramEventually(t, registry, "eventhorizon_telnet_inter_write_interval_ms", nil, 1, 150)
	assertGatheredHistogramEventually(t, registry, "eventhorizon_telnet_first_write_delay_ms", nil, 1, 100)
}

func TestMetricEventSocketAppliesTelnetFinalization(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	socketPath := filepath.Join(shortTempDir(t), "metrics.sock")

	server, err := startMetricEventServer(socketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	connection, err := net.Dial("unixgram", socketPath)
	if err != nil {
		t.Fatalf("dial metric event socket: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	accepted := []byte(`{"v":1,"protocol":"telnet","event":"connection_accepted","active_count_after":1}`)
	if _, err := connection.Write(accepted); err != nil {
		t.Fatalf("write accepted event: %v", err)
	}
	assertGatheredValueEventually(t, registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 1)

	finalized := []byte(`{"v":1,"protocol":"telnet","event":"connection_finalized","finalization_reason":"read_error","duration_ms":1250,"depth_level":3,"active_count_after":0,"io_reason":"reset"}`)
	if _, err := connection.Write(finalized); err != nil {
		t.Fatalf("write finalization event: %v", err)
	}

	assertGatheredValueEventually(t, registry, "eventhorizon_completed_sessions_total", map[string]string{
		"protocol":          "telnet",
		"disconnect_reason": "read_error",
	}, 1)
	assertGatheredHistogramEventually(t, registry, "eventhorizon_session_duration_ms", map[string]string{
		"protocol": "telnet",
	}, 1, 1250)
	assertGatheredValueEventually(t, registry, "eventhorizon_session_interaction_depth_total", map[string]string{
		"protocol":    "telnet",
		"depth_level": "3",
	}, 1)
	assertGatheredValueEventually(t, registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 0)
	assertGatheredValueEventually(t, registry, "eventhorizon_read_errors_total", map[string]string{
		"protocol": "telnet",
		"reason":   "reset",
	}, 1)
	assertGatheredValueEventually(t, registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "telnet",
		"reason":   "reset",
	}, 0)
}

func TestMetricEventSocketAppliesMQTTConnectionAccepted(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":2}`)

	assertGatheredValueEventually(t, socket.registry, "total_connects", map[string]string{
		"server": "MQTT",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "current_connected_clients", map[string]string{
		"server": "MQTT",
	}, 2)
}

func TestMetricEventSocketAppliesMQTTPositiveRead(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "current_connected_clients", map[string]string{"server": "MQTT"}, 1)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"positive_read","bytes":23}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_bytes_received_total", map[string]string{
		"protocol": "mqtt",
	}, 23)
}

func TestMetricEventSocketAppliesMQTTPositiveWrite(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "current_connected_clients", map[string]string{"server": "MQTT"}, 1)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"positive_write","bytes":7}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_bytes_sent_total", map[string]string{
		"protocol": "mqtt",
	}, 7)
}

func TestMetricEventSocketAppliesMQTTProtocolAction(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":1}`)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"protocol_action","action":"connect_accepted"}`)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connack_sent","duration_ms":10}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "mqtt", "action": "connack_sent",
	}, 1)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"protocol_action","action":"subscribe_received"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "mqtt",
		"action":   "subscribe_received",
	}, 1)
}

func TestMetricEventSocketAppliesMQTTConnackSent(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":1}`)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"protocol_action","action":"connect_accepted"}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "mqtt", "action": "connect_accepted",
	}, 1)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connack_sent","duration_ms":75}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "mqtt",
		"action":   "connack_sent",
	}, 1)
	assertGatheredHistogramEventually(t, socket.registry, "eventhorizon_mqtt_connect_to_connack_duration_ms", nil, 1, 75)
}

func TestMetricEventSocketAppliesMQTTSecondaryWriteError(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":1}`)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_finalized","finalization_reason":"peer_closed","duration_ms":50,"depth_level":0,"active_count_after":0,"io_reason":"none"}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_mqtt_network_connection_finalizations_total", map[string]string{
		"finalization_reason": "peer_closed",
	}, 1)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"secondary_write_error","io_reason":"closed"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "mqtt",
		"reason":   "closed",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_read_errors_total", map[string]string{
		"protocol": "mqtt",
		"reason":   "closed",
	}, 0)
}

func TestMetricEventSocketAppliesMQTTConnectionFinalization(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "current_connected_clients", map[string]string{
		"server": "MQTT",
	}, 1)

	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_finalized","finalization_reason":"read_error","duration_ms":30000,"depth_level":2,"active_count_after":0,"io_reason":"reset"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_mqtt_network_connection_finalizations_total", map[string]string{
		"finalization_reason": "read_error",
	}, 1)
	assertGatheredHistogramEventually(t, socket.registry, "eventhorizon_mqtt_network_connection_duration_ms", nil, 1, 30000)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_mqtt_network_connection_interaction_depth_total", map[string]string{
		"depth_level": "2",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "current_connected_clients", map[string]string{
		"server": "MQTT",
	}, 0)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_read_errors_total", map[string]string{
		"protocol": "mqtt",
		"reason":   "reset",
	}, 1)
}

func TestMetricEventSocketAppliesCoAPRequestReceived(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"coap","event":"request_received","request_active_count_after":1}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_request_received",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_request_exchanges", nil, 1)
}

func TestMetricEventSocketAppliesCoAPRequestFinalized(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"coap","event":"request_received","request_active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_request_exchanges", nil, 1)

	socket.write(t, `{"v":1,"protocol":"coap","event":"request_finalized","outcome":"response_sent","duration_ms":250,"request_active_count_after":0}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_response_sent",
	}, 1)
	assertGatheredHistogramEventually(t, socket.registry, "eventhorizon_coap_request_exchange_duration_ms", map[string]string{
		"outcome": "response_sent",
	}, 1, 250)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_request_exchanges", nil, 0)
}

func TestMetricEventSocketAppliesCoAPCONResponseSent(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"coap","event":"request_received","request_active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_request_exchanges", nil, 1)

	socket.write(t, `{"v":1,"protocol":"coap","event":"con_response_sent","request_duration_ms":400,"request_active_count_after":0,"con_active_count_after":1}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_response_sent",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_sent",
	}, 1)
	assertGatheredHistogramEventually(t, socket.registry, "eventhorizon_coap_request_exchange_duration_ms", map[string]string{
		"outcome": "response_sent",
	}, 1, 400)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_request_exchanges", nil, 0)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 1)
}

func TestMetricEventSocketAppliesCoAPCONResponseRetransmitted(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"coap","event":"request_received","request_active_count_after":1}`)
	socket.write(t, `{"v":1,"protocol":"coap","event":"con_response_sent","request_duration_ms":10,"request_active_count_after":0,"con_active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 1)
	socket.write(t, `{"v":1,"protocol":"coap","event":"con_response_retransmitted"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_retransmitted",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 1)
}

func TestMetricEventSocketAppliesCoAPCONResponseFinalized(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"coap","event":"request_received","request_active_count_after":1}`)
	socket.write(t, `{"v":1,"protocol":"coap","event":"con_response_sent","request_duration_ms":10,"request_active_count_after":0,"con_active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 1)

	socket.write(t, `{"v":1,"protocol":"coap","event":"con_response_finalized","outcome":"ack_received","duration_ms":2000,"con_active_count_after":0}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_ack_received",
	}, 1)
	assertGatheredHistogramEventually(t, socket.registry, "eventhorizon_coap_con_response_exchange_duration_ms", map[string]string{
		"outcome": "ack_received",
	}, 1, 2000)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 0)
}

func TestMetricEventSocketAppliesCoAPWriteError(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"coap","event":"request_received","request_active_count_after":1}`)
	socket.write(t, `{"v":1,"protocol":"coap","event":"con_response_sent","request_duration_ms":10,"request_active_count_after":0,"con_active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 1)
	socket.write(t, `{"v":1,"protocol":"coap","event":"write_error","io_reason":"other"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "coap",
		"reason":   "other",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_request_exchanges", nil, 0)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 1)
}

func TestMetricEventSocketAppliesUPnPProtocolAction(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"upnp","event":"protocol_action","action":"ssdp_discovery_response_sent"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "ssdp_discovery_response_sent",
	}, 1)
}

func TestMetricEventSocketAppliesUPnPDescriptionResponseStarted(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"upnp","event":"protocol_action","action":"description_get_received"}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp", "action": "description_get_received",
	}, 1)
	socket.write(t, `{"v":1,"protocol":"upnp","event":"description_response_started","active_count_after":1}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "description_response_started",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_upnp_active_description_responses", nil, 1)
}

func TestMetricEventSocketAppliesUPnPDescriptionResponseFinalized(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"upnp","event":"protocol_action","action":"description_get_received"}`)
	socket.write(t, `{"v":1,"protocol":"upnp","event":"description_response_started","active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_upnp_active_description_responses", nil, 1)

	socket.write(t, `{"v":1,"protocol":"upnp","event":"description_response_finalized","outcome":"completed","duration_ms":30000,"active_count_after":0}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "description_response_completed",
	}, 1)
	assertGatheredHistogramEventually(t, socket.registry, "eventhorizon_upnp_description_stream_duration_ms", map[string]string{
		"outcome": "completed",
	}, 1, 30000)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_upnp_active_description_responses", nil, 0)
}

func TestMetricEventSocketAppliesUPnPWriteError(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"upnp","event":"write_error","io_reason":"timeout"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "upnp",
		"reason":   "timeout",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_upnp_active_description_responses", nil, 0)
}

func TestMetricEventSocketUsesDeterministicRejectionReasons(t *testing.T) {
	socket := newMetricEventTestSocket(t)

	socket.write(t, " \n\t")
	socket.write(t, `{"v":1,"protocol":"telnet","event":"positive_read"}`)
	socket.write(t, `{"v":2,"protocol":"telnet","event":"positive_read","bytes":1}`)
	socket.write(t, `{"v":1,"protocol":7,"event":"positive_read","bytes":1}`)
	socket.write(t, `{"v":1,"protocol":"telnet","event":"positive_read","bytes":1e1}`)
	socket.write(t, `{"v":1,"protocol":"telnet","event":"not_supported"}`)
	socket.write(t, strings.Repeat(" ", metricEventDatagramLimit+1))

	for reason, want := range map[string]float64{
		"empty_message":     1,
		"missing_fields":    1,
		"unknown_format":    2,
		"unknown_server":    1,
		"invalid_number":    1,
		"unsupported_event": 1,
	} {
		assertGatheredValueEventually(t, socket.registry, "eventhorizon_exporter_malformed_messages_total", map[string]string{
			"reason": reason,
		}, want)
	}
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_bytes_received_total", map[string]string{
		"protocol": "telnet",
	}, 0)
}

func TestMetricEventSocketAppliesDuplicateValidDatagramsAgain(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	datagram := `{"v":1,"protocol":"telnet","event":"positive_read","bytes":3}`
	socket.write(t, datagram)
	socket.write(t, datagram)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_bytes_received_total", map[string]string{
		"protocol": "telnet",
	}, 6)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_exporter_malformed_messages_total", map[string]string{
		"reason": "unsupported_event",
	}, 0)
}

func TestMetricEventSocketRejectsMQTTFinalizationWithoutPartialMutation(t *testing.T) {
	socket := newMetricEventTestSocket(t)
	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_accepted","active_count_after":1}`)
	assertGatheredValueEventually(t, socket.registry, "current_connected_clients", map[string]string{
		"server": "MQTT",
	}, 1)

	socket.write(t, `{"v":1,"protocol":"mqtt","event":"connection_finalized","finalization_reason":"read_error","duration_ms":99,"depth_level":2,"active_count_after":0,"io_reason":"none"}`)

	assertGatheredValueEventually(t, socket.registry, "eventhorizon_exporter_malformed_messages_total", map[string]string{
		"reason": "unsupported_event",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_mqtt_network_connection_finalizations_total", map[string]string{
		"finalization_reason": "read_error",
	}, 0)
	assertGatheredHistogramEventually(t, socket.registry, "eventhorizon_mqtt_network_connection_duration_ms", nil, 0, 0)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_mqtt_network_connection_interaction_depth_total", map[string]string{
		"depth_level": "2",
	}, 0)
	assertGatheredValueEventually(t, socket.registry, "current_connected_clients", map[string]string{
		"server": "MQTT",
	}, 1)
	assertGatheredValueEventually(t, socket.registry, "eventhorizon_read_errors_total", map[string]string{
		"protocol": "mqtt",
		"reason":   "reset",
	}, 0)
}

func TestMetricEventMetricSurfaceIsFrozen(t *testing.T) {
	registry := prometheus.NewRegistry()
	newMetricEventMetrics(registry)

	expected := map[string]gatheredFamilyContract{
		"total_connects": {
			metricType: "COUNTER",
			help:       "Total accepted TCP connections tracked by the EventHorizon Telnet and MQTT tarpits and by the integrated Endlessh SSH tarpit.",
			series:     3,
		},
		"eventhorizon_ssh_tracked_client_lifetime_ms": {
			metricType: "HISTOGRAM",
			help:       "Observed lifetime in milliseconds of an Endlessh-tracked SSH client, from accepted TCP connection until removal from the tracked-client set.",
			series:     2,
		},
		"current_connected_clients": {
			metricType: "GAUGE",
			help:       "Current accepted TCP connections tracked by the EventHorizon Telnet and MQTT servers.",
			series:     2,
		},
		"eventhorizon_exporter_malformed_messages_total": {
			metricType: "COUNTER",
			help:       "Total malformed or unsupported metric datagrams observed and rejected by the EventHorizon exporter.",
			series:     6,
		},
		"eventhorizon_bytes_received_total": {
			metricType: "COUNTER",
			help:       "Positive bytes returned by Telnet and MQTT client-facing reads; includes protocol framing and incomplete input, and excludes EOF, failed, retryable, or zero-byte I/O and internal telemetry.",
			series:     2,
		},
		"eventhorizon_bytes_sent_total": {
			metricType: "COUNTER",
			help:       "Positive bytes returned by Telnet and MQTT client-facing writes; includes partial writes and protocol framing, and excludes failed, retryable, or zero-byte I/O and internal telemetry.",
			series:     2,
		},
		"eventhorizon_protocol_actions_total": {
			metricType: "COUNTER",
			help:       "Total protocol actions crossing their frozen EventHorizon server-side observation boundaries.",
			series:     26,
		},
		"eventhorizon_read_errors_total": {
			metricType: "COUNTER",
			help:       "Total primary unrecoverable read-side I/O failures that finalize tracked Telnet or MQTT TCP connections.",
			series:     8,
		},
		"eventhorizon_write_errors_total": {
			metricType: "COUNTER",
			help:       "Total write-side I/O failures crossing frozen EventHorizon reliability boundaries.",
			series:     16,
		},
		"eventhorizon_completed_sessions_total": {
			metricType: "COUNTER",
			help:       "Total finalized tracked Telnet TCP sessions by finalization reason.",
			series:     5,
		},
		"eventhorizon_session_duration_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds from accepted Telnet TCP connection to exactly-once session finalization.",
			series:     1,
		},
		"eventhorizon_session_interaction_depth_total": {
			metricType: "COUNTER",
			help:       "Total finalized tracked Telnet sessions by bounded meaningful application-interaction depth.",
			series:     4,
		},
		"eventhorizon_telnet_first_write_delay_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds from accepted Telnet TCP connection to its first positive server write.",
			series:     1,
		},
		"eventhorizon_telnet_inter_write_interval_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds between consecutive positive server writes on a tracked Telnet session.",
			series:     1,
		},
		"eventhorizon_mqtt_network_connection_finalizations_total": {
			metricType: "COUNTER",
			help:       "Total finalized tracked MQTT Network Connections by finalization reason.",
			series:     10,
		},
		"eventhorizon_mqtt_network_connection_duration_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds from accepted MQTT TCP connection to exactly-once Network Connection finalization.",
			series:     1,
		},
		"eventhorizon_mqtt_network_connection_interaction_depth_total": {
			metricType: "COUNTER",
			help:       "Total finalized MQTT Network Connections by bounded meaningful application-interaction depth.",
			series:     4,
		},
		"eventhorizon_mqtt_connect_to_connack_duration_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds from accepted supported MQTT CONNECT to complete successful CONNACK transmission.",
			series:     1,
		},
		"eventhorizon_coap_active_request_exchanges": {
			metricType: "GAUGE",
			help:       "Current accepted CoAP GET request exchanges awaiting response transmission or termination.",
			series:     1,
		},
		"eventhorizon_coap_request_exchange_duration_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds from accepted CoAP GET request to response transmission or termination.",
			series:     2,
		},
		"eventhorizon_coap_active_con_response_exchanges": {
			metricType: "GAUGE",
			help:       "Current transmitted CoAP Confirmable response exchanges awaiting ACK, RST, or retry exhaustion.",
			series:     1,
		},
		"eventhorizon_coap_con_response_exchange_duration_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds from first CoAP Confirmable response transmission to ACK, RST, or retry exhaustion.",
			series:     3,
		},
		"eventhorizon_upnp_active_description_responses": {
			metricType: "GAUGE",
			help:       "Current UPnP device-description responses that have started but not completed or terminated.",
			series:     1,
		},
		"eventhorizon_upnp_description_stream_duration_ms": {
			metricType: "HISTOGRAM",
			help:       "Duration in milliseconds from the first positive device-description response write to completion or termination.",
			series:     2,
		},
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather frozen metric surface: %v", err)
	}
	if len(families) != len(expected) {
		t.Fatalf("metric family count = %d, want %d", len(families), len(expected))
	}
	for _, family := range families {
		contract, exists := expected[family.GetName()]
		if !exists {
			t.Errorf("unexpected metric family %q", family.GetName())
			continue
		}
		if got := family.GetType().String(); got != contract.metricType {
			t.Errorf("%s type = %s, want %s", family.GetName(), got, contract.metricType)
		}
		if got := family.GetHelp(); got != contract.help {
			t.Errorf("%s help = %q, want %q", family.GetName(), got, contract.help)
		}
		if got := len(family.Metric); got != contract.series {
			t.Errorf("%s initialized populations = %d, want %d", family.GetName(), got, contract.series)
		}
	}

	for _, forbidden := range []string{
		"eventhorizon_exporter_messages_total",
		"eventhorizon_session_starts_total",
		"total_trapped_time_ms",
		"eventhorizon_early_disconnect_total",
		"eventhorizon_first_response_exit_total",
		"tarpitted_clients",
		"upnp_other_http_requests",
		"upnp_M-Search_requests",
		"upnp_non_M-Search_requests",
		"telnet_pit_input",
	} {
		if _, exists := expected[forbidden]; exists {
			t.Errorf("forbidden family %s present in frozen surface", forbidden)
		}
	}

	allowedLabels := map[string]struct{}{
		"server": {}, "reason": {}, "protocol": {}, "action": {},
		"disconnect_reason": {}, "depth_level": {}, "finalization_reason": {},
		"outcome": {}, "observation_end_reason": {},
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if _, allowed := allowedLabels[label.GetName()]; !allowed {
					t.Errorf("%s has forbidden label %s=%s", family.GetName(), label.GetName(), label.GetValue())
				}
			}
		}
	}

}

func TestMetricEventHistogramBucketsAreFrozen(t *testing.T) {
	registry := prometheus.NewRegistry()
	newMetricEventMetrics(registry)

	expected := map[string][]float64{
		"eventhorizon_session_duration_ms": {
			10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000,
		},
		"eventhorizon_telnet_first_write_delay_ms": {
			10, 25, 50, 75, 100, 125, 150, 200, 250, 500, 1000, 2500, 5000, 10000,
		},
		"eventhorizon_telnet_inter_write_interval_ms": {
			10, 25, 50, 75, 100, 125, 150, 200, 250, 500, 1000, 2500, 5000, 10000,
		},
		"eventhorizon_mqtt_network_connection_duration_ms": {
			10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000,
			120000, 300000, 600000, 1800000, 3600000, 21600000, 86400000, 604800000,
		},
		"eventhorizon_mqtt_connect_to_connack_duration_ms": {
			1, 2, 5, 10, 25, 50, 75, 100, 125, 150, 200, 250, 500, 1000,
			2500, 5000, 10000, 30000, 60000,
		},
		"eventhorizon_coap_request_exchange_duration_ms": {
			1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
			30000, 60000, 120000, 300000, 600000,
		},
		"eventhorizon_coap_con_response_exchange_duration_ms": {
			1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 3000, 6000,
			12000, 24000, 45000, 60000,
		},
		"eventhorizon_upnp_description_stream_duration_ms": {
			1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
			15000, 20000, 25000, 30000, 60000,
		},
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather histogram buckets: %v", err)
	}
	seen := make(map[string]bool, len(expected))
	for _, family := range families {
		want, exists := expected[family.GetName()]
		if !exists {
			continue
		}
		seen[family.GetName()] = true
		for _, metric := range family.Metric {
			gotBuckets := metric.GetHistogram().GetBucket()
			if len(gotBuckets) != len(want) {
				t.Errorf("%s finite bucket count = %d, want %d", family.GetName(), len(gotBuckets), len(want))
				continue
			}
			for index, wantUpperBound := range want {
				if got := gotBuckets[index].GetUpperBound(); got != wantUpperBound {
					t.Errorf("%s bucket %d = %v, want %v", family.GetName(), index, got, wantUpperBound)
				}
			}
		}
	}
	for name := range expected {
		if !seen[name] {
			t.Errorf("histogram family %s was not gathered", name)
		}
	}
}

func TestMetricEventMetricLabelsAreFrozen(t *testing.T) {
	registry := prometheus.NewRegistry()
	newMetricEventMetrics(registry)

	expected := map[string][]string{
		"total_connects":            {"server=MQTT", "server=SSH", "server=Telnet"},
		"current_connected_clients": {"server=MQTT", "server=Telnet"},
		"eventhorizon_ssh_tracked_client_lifetime_ms": {
			"observation_end_reason=server_shutdown", "observation_end_reason=write_failed",
		},
		"eventhorizon_exporter_malformed_messages_total": {
			"reason=empty_message", "reason=invalid_number", "reason=missing_fields",
			"reason=unknown_format", "reason=unknown_server", "reason=unsupported_event",
		},
		"eventhorizon_bytes_received_total": {"protocol=mqtt", "protocol=telnet"},
		"eventhorizon_bytes_sent_total":     {"protocol=mqtt", "protocol=telnet"},
		"eventhorizon_protocol_actions_total": {
			"action=con_response_ack_received,protocol=coap",
			"action=con_response_retransmitted,protocol=coap",
			"action=con_response_retry_exhausted,protocol=coap",
			"action=con_response_rst_received,protocol=coap",
			"action=con_response_sent,protocol=coap",
			"action=connack_sent,protocol=mqtt",
			"action=connect_accepted,protocol=mqtt",
			"action=description_get_received,protocol=upnp",
			"action=description_response_completed,protocol=upnp",
			"action=description_response_started,protocol=upnp",
			"action=description_response_terminated,protocol=upnp",
			"action=get_request_received,protocol=coap",
			"action=get_request_terminated,protocol=coap",
			"action=get_response_sent,protocol=coap",
			"action=puback_sent,protocol=mqtt",
			"action=pubcomp_sent,protocol=mqtt",
			"action=publish_received,protocol=mqtt",
			"action=pubrec_sent,protocol=mqtt",
			"action=pubrel_received,protocol=mqtt",
			"action=ssdp_discovery_response_sent,protocol=upnp",
			"action=ssdp_msearch_received,protocol=upnp",
			"action=suback_sent,protocol=mqtt",
			"action=subscribe_received,protocol=mqtt",
			"action=subscription_publish_sent,protocol=mqtt",
			"action=unsuback_sent,protocol=mqtt",
			"action=unsubscribe_received,protocol=mqtt",
		},
		"eventhorizon_read_errors_total": {
			"protocol=mqtt,reason=closed", "protocol=mqtt,reason=other",
			"protocol=mqtt,reason=reset", "protocol=mqtt,reason=timeout",
			"protocol=telnet,reason=closed", "protocol=telnet,reason=other",
			"protocol=telnet,reason=reset", "protocol=telnet,reason=timeout",
		},
		"eventhorizon_write_errors_total": {
			"protocol=coap,reason=closed", "protocol=coap,reason=other",
			"protocol=coap,reason=reset", "protocol=coap,reason=timeout",
			"protocol=mqtt,reason=closed", "protocol=mqtt,reason=other",
			"protocol=mqtt,reason=reset", "protocol=mqtt,reason=timeout",
			"protocol=telnet,reason=closed", "protocol=telnet,reason=other",
			"protocol=telnet,reason=reset", "protocol=telnet,reason=timeout",
			"protocol=upnp,reason=closed", "protocol=upnp,reason=other",
			"protocol=upnp,reason=reset", "protocol=upnp,reason=timeout",
		},
		"eventhorizon_completed_sessions_total": {
			"disconnect_reason=bounded_policy,protocol=telnet",
			"disconnect_reason=peer_closed,protocol=telnet",
			"disconnect_reason=read_error,protocol=telnet",
			"disconnect_reason=server_shutdown,protocol=telnet",
			"disconnect_reason=write_error,protocol=telnet",
		},
		"eventhorizon_session_duration_ms": {"protocol=telnet"},
		"eventhorizon_session_interaction_depth_total": {
			"depth_level=0,protocol=telnet", "depth_level=1,protocol=telnet",
			"depth_level=2,protocol=telnet", "depth_level=3,protocol=telnet",
		},
		"eventhorizon_telnet_first_write_delay_ms":    {""},
		"eventhorizon_telnet_inter_write_interval_ms": {""},
		"eventhorizon_mqtt_network_connection_finalizations_total": {
			"finalization_reason=bounded_policy", "finalization_reason=connect_refused",
			"finalization_reason=disconnect_received", "finalization_reason=keep_alive_timeout",
			"finalization_reason=operation_refused", "finalization_reason=peer_closed",
			"finalization_reason=protocol_error", "finalization_reason=read_error",
			"finalization_reason=server_shutdown", "finalization_reason=write_error",
		},
		"eventhorizon_mqtt_network_connection_duration_ms": {""},
		"eventhorizon_mqtt_network_connection_interaction_depth_total": {
			"depth_level=0", "depth_level=1", "depth_level=2", "depth_level=3",
		},
		"eventhorizon_mqtt_connect_to_connack_duration_ms": {""},
		"eventhorizon_coap_active_request_exchanges":       {""},
		"eventhorizon_coap_request_exchange_duration_ms": {
			"outcome=response_sent", "outcome=terminated",
		},
		"eventhorizon_coap_active_con_response_exchanges": {""},
		"eventhorizon_coap_con_response_exchange_duration_ms": {
			"outcome=ack_received", "outcome=retry_exhausted", "outcome=rst_received",
		},
		"eventhorizon_upnp_active_description_responses": {""},
		"eventhorizon_upnp_description_stream_duration_ms": {
			"outcome=completed", "outcome=terminated",
		},
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metric labels: %v", err)
	}
	for _, family := range families {
		want, exists := expected[family.GetName()]
		if !exists {
			t.Errorf("missing label contract for family %s", family.GetName())
			continue
		}
		got := make([]string, 0, len(family.Metric))
		for _, metric := range family.Metric {
			parts := make([]string, 0, len(metric.Label))
			for _, label := range metric.Label {
				parts = append(parts, label.GetName()+"="+label.GetValue())
			}
			sort.Strings(parts)
			got = append(got, strings.Join(parts, ","))
		}
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s label populations = %v, want %v", family.GetName(), got, want)
		}
	}
}

func assertGatheredValueEventually(
	t *testing.T,
	registry *prometheus.Registry,
	familyName string,
	wantLabels map[string]string,
	wantValue float64,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		value, found, err := gatheredValue(registry, familyName, wantLabels)
		if err != nil {
			t.Fatalf("gather %s: %v", familyName, err)
		}
		if found && value == wantValue {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s%v = %v (found=%v), want %v", familyName, wantLabels, value, found, wantValue)
		}
		time.Sleep(time.Millisecond)
	}
}

func gatheredValue(
	registry *prometheus.Registry,
	familyName string,
	wantLabels map[string]string,
) (float64, bool, error) {
	families, err := registry.Gather()
	if err != nil {
		return 0, false, err
	}
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.Metric {
			labels := make(map[string]string, len(metric.Label))
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if !sameLabels(labels, wantLabels) {
				continue
			}
			switch family.GetType().String() {
			case "COUNTER":
				return metric.GetCounter().GetValue(), true, nil
			case "GAUGE":
				return metric.GetGauge().GetValue(), true, nil
			default:
				return 0, false, nil
			}
		}
	}
	return 0, false, nil
}

func assertGatheredHistogramEventually(
	t *testing.T,
	registry *prometheus.Registry,
	familyName string,
	wantLabels map[string]string,
	wantCount uint64,
	wantSum float64,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		count, sum, found, err := gatheredHistogram(registry, familyName, wantLabels)
		if err != nil {
			t.Fatalf("gather %s: %v", familyName, err)
		}
		if found && count == wantCount && sum == wantSum {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s%v count/sum = %d/%v (found=%v), want %d/%v", familyName, wantLabels, count, sum, found, wantCount, wantSum)
		}
		time.Sleep(time.Millisecond)
	}
}

func gatheredHistogram(
	registry *prometheus.Registry,
	familyName string,
	wantLabels map[string]string,
) (uint64, float64, bool, error) {
	families, err := registry.Gather()
	if err != nil {
		return 0, 0, false, err
	}
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.Metric {
			labels := make(map[string]string, len(metric.Label))
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if !sameLabels(labels, wantLabels) {
				continue
			}
			return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum(), true, nil
		}
	}
	return 0, 0, false, nil
}

func sameLabels(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for name, value := range want {
		if got[name] != value {
			return false
		}
	}
	return true
}
