package main

import (
	"bytes"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type coapJSONProducerFixture struct {
	registry *prometheus.Registry
	port     int
	producer *exec.Cmd
	output   *bytes.Buffer
}

type coapJSONProducerOptions struct {
	delayMilliseconds string
	ackTimeoutMS      string
	maxRetransmit     string
	sendInjection     string
}

func startCoAPJSONProducerFixture(t *testing.T) *coapJSONProducerFixture {
	t.Helper()
	return startCoAPJSONProducerFixtureWithOptions(t, coapJSONProducerOptions{
		delayMilliseconds: "20",
		ackTimeoutMS:      "100",
		maxRetransmit:     "1",
	})
}

func startCoAPJSONProducerFixtureWithOptions(
	t *testing.T,
	options coapJSONProducerOptions,
) *coapJSONProducerFixture {
	t.Helper()
	testDirectory := shortTempDir(t)
	binaryPath := filepath.Join(testDirectory, "coap_pit_json_mvp")
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	compile := exec.Command(
		"gcc",
		"-Wall", "-Wextra", "-g", "-pthread",
		"-DEVENTHORIZON_JSON_METRIC_EVENTS",
		"-DEVENTHORIZON_COAP_TEST_RUNTIME",
		"-o", binaryPath,
		filepath.Join(repositoryRoot, "servers/coap_pit.c"),
		filepath.Join(repositoryRoot, "shared/structs.c"),
		filepath.Join(repositoryRoot, "shared/session_events.c"),
		filepath.Join(repositoryRoot, "shared/interaction_depth.c"),
		filepath.Join(repositoryRoot, "shared/metric_events.c"),
	)
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile isolated JSON CoAP producer: %v\n%s", err, output)
	}

	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	metricSocketPath := filepath.Join(testDirectory, "metrics.sock")
	metricServer, err := startMetricEventServer(metricSocketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = metricServer.Close() })

	port := reserveUDPPort(t)
	producerOutput := &bytes.Buffer{}
	producer := exec.Command(
		binaryPath,
		strconv.Itoa(port),
		options.delayMilliseconds,
		options.ackTimeoutMS,
		options.maxRetransmit,
		"64",
	)
	producer.Env = append(
		os.Environ(),
		"EVENTHORIZON_METRIC_SOCKET="+metricSocketPath,
		"EVENTHORIZON_SESSION_LOG="+filepath.Join(testDirectory, "sessions.jsonl"),
	)
	if options.sendInjection != "" {
		producer.Env = append(
			producer.Env,
			"EVENTHORIZON_COAP_TEST_SEND_INJECTION="+options.sendInjection,
		)
	}
	producer.Stdout = producerOutput
	producer.Stderr = producerOutput
	if err := producer.Start(); err != nil {
		t.Fatalf("start isolated JSON CoAP producer: %v", err)
	}
	t.Cleanup(func() {
		if producer.ProcessState == nil {
			_ = producer.Process.Kill()
			_ = producer.Wait()
		}
	})
	waitForUDPBind(t, port, producer, producerOutput)

	return &coapJSONProducerFixture{
		registry: registry,
		port:     port,
		producer: producer,
		output:   producerOutput,
	}
}

func TestCoAPJSONProducerCompletesNONRequestExchangeWithoutCONReliability(t *testing.T) {
	fixture := startCoAPJSONProducerFixture(t)
	connection, err := net.DialUDP(
		"udp4",
		nil,
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: fixture.port},
	)
	if err != nil {
		t.Fatalf("dial isolated JSON CoAP producer: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	request := []byte{0x51, 0x01, 0x12, 0x34, 0xa5}
	if written, err := connection.Write(request); err != nil {
		t.Fatalf("write CoAP NON GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("CoAP request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set CoAP response deadline: %v", err)
	}
	response := make([]byte, 64)
	responseLength, err := connection.Read(response)
	if err != nil {
		t.Fatalf("read CoAP NON response: %v\n%s", err, fixture.output.String())
	}
	response = response[:responseLength]
	if len(response) < 5 {
		t.Fatalf("CoAP response length = %d, want at least 5", len(response))
	}
	if got := (response[0] >> 4) & 0x03; got != 1 {
		t.Fatalf("CoAP response type = %d, want NON (1)", got)
	}
	if response[1] != 0x45 {
		t.Fatalf("CoAP response code = 0x%02x, want 2.05 Content", response[1])
	}
	if got := response[0] & 0x0f; got != 1 {
		t.Fatalf("CoAP response token length = %d, want 1", got)
	}
	if response[4] != request[4] {
		t.Fatalf("CoAP response token = 0x%02x, want 0x%02x", response[4], request[4])
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_request_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_response_sent",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_request_exchanges", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_request_exchange_duration_ms", map[string]string{
		"outcome": "response_sent",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_request_exchange_duration_ms", map[string]string{
		"outcome": "terminated",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 0)
	for _, action := range []string{
		"con_response_sent",
		"con_response_retransmitted",
		"con_response_ack_received",
		"con_response_rst_received",
		"con_response_retry_exhausted",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "coap",
			"action":   action,
		}, 0)
	}
}

func TestCoAPJSONProducerSeparatesCONReliabilityFromRequestExchange(t *testing.T) {
	fixture := startCoAPJSONProducerFixture(t)
	connection, err := net.DialUDP(
		"udp4",
		nil,
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: fixture.port},
	)
	if err != nil {
		t.Fatalf("dial isolated JSON CoAP producer: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set CoAP response deadline: %v", err)
	}

	request := []byte{0x41, 0x01, 0x22, 0x33, 0xb6}
	if written, err := connection.Write(request); err != nil {
		t.Fatalf("write CoAP CON GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("CoAP request bytes = %d, want %d", written, len(request))
	}

	emptyACK := readCoAPDatagram(t, connection, fixture.output)
	if len(emptyACK) != 4 || (emptyACK[0]>>4)&0x03 != 2 || emptyACK[1] != 0 ||
		emptyACK[2] != request[2] || emptyACK[3] != request[3] {
		t.Fatalf("CoAP empty ACK = %x, want type ACK/code Empty/request Message ID", emptyACK)
	}

	initialResponse := readCoAPDatagram(t, connection, fixture.output)
	if len(initialResponse) < 5 {
		t.Fatalf("CoAP CON response length = %d, want at least 5", len(initialResponse))
	}
	if got := (initialResponse[0] >> 4) & 0x03; got != 0 {
		t.Fatalf("CoAP response type = %d, want CON (0)", got)
	}
	if initialResponse[1] != 0x45 || initialResponse[4] != request[4] {
		t.Fatalf("CoAP CON response = %x, want matching 2.05 response", initialResponse)
	}

	retransmission := readCoAPDatagram(t, connection, fixture.output)
	if !bytes.Equal(retransmission, initialResponse) {
		t.Fatalf("CoAP retransmission = %x, want exact initial response %x", retransmission, initialResponse)
	}

	ack := []byte{0x60, 0x00, initialResponse[2], initialResponse[3]}
	if written, err := connection.Write(ack); err != nil {
		t.Fatalf("write matching CoAP ACK: %v", err)
	} else if written != len(ack) {
		t.Fatalf("CoAP ACK bytes = %d, want %d", written, len(ack))
	}

	for _, action := range []string{
		"get_request_received",
		"get_response_sent",
		"con_response_sent",
		"con_response_retransmitted",
		"con_response_ack_received",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "coap",
			"action":   action,
		}, 1)
	}
	for _, action := range []string{
		"get_request_terminated",
		"con_response_rst_received",
		"con_response_retry_exhausted",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "coap",
			"action":   action,
		}, 0)
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_request_exchanges", nil, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_request_exchange_duration_ms", map[string]string{
		"outcome": "response_sent",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_con_response_exchange_duration_ms", map[string]string{
		"outcome": "ack_received",
	}, 1)
	for _, outcome := range []string{"rst_received", "retry_exhausted"} {
		assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_con_response_exchange_duration_ms", map[string]string{
			"outcome": outcome,
		}, 0)
	}
}

func TestCoAPJSONProducerSuppressesActiveDuplicateAndAllowsLaterTokenReuse(t *testing.T) {
	fixture := startCoAPJSONProducerFixture(t)
	connection := dialCoAPFixture(t, fixture)
	request := []byte{0x51, 0x01, 0x31, 0x32, 0xc7}

	for copyNumber := 0; copyNumber < 2; copyNumber++ {
		if written, err := connection.Write(request); err != nil {
			t.Fatalf("write CoAP request copy %d: %v", copyNumber+1, err)
		} else if written != len(request) {
			t.Fatalf("CoAP request copy %d bytes = %d, want %d", copyNumber+1, written, len(request))
		}
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set first CoAP response deadline: %v", err)
	}
	_ = readCoAPDatagram(t, connection, fixture.output)
	if err := connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set duplicate suppression deadline: %v", err)
	}
	buffer := make([]byte, 64)
	if length, err := connection.Read(buffer); err == nil {
		t.Fatalf("active duplicate produced an extra %d-byte response", length)
	} else if timeoutError, ok := err.(net.Error); !ok || !timeoutError.Timeout() {
		t.Fatalf("read after active duplicate: %v", err)
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_request_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_response_sent",
	}, 1)

	if written, err := connection.Write(request); err != nil {
		t.Fatalf("write CoAP request after finalization: %v", err)
	} else if written != len(request) {
		t.Fatalf("CoAP reused request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set reused-token response deadline: %v", err)
	}
	_ = readCoAPDatagram(t, connection, fixture.output)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_request_received",
	}, 2)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_response_sent",
	}, 2)
}

func TestCoAPJSONProducerExcludesUnsupportedTrafficFromMetricPopulation(t *testing.T) {
	fixture := startCoAPJSONProducerFixture(t)
	connection := dialCoAPFixture(t, fixture)
	requests := [][]byte{
		{0x51, 0x02, 0x40, 0x01, 0xd1},
		{0x51, 0x01, 0x40, 0x02, 0xd2, 0xb1, 'x'},
	}
	for index, request := range requests {
		if written, err := connection.Write(request); err != nil {
			t.Fatalf("write excluded CoAP datagram %d: %v", index+1, err)
		} else if written != len(request) {
			t.Fatalf("excluded CoAP datagram %d bytes = %d, want %d", index+1, written, len(request))
		}
	}
	if err := connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set excluded CoAP response deadline: %v", err)
	}
	buffer := make([]byte, 64)
	if length, err := connection.Read(buffer); err == nil {
		t.Fatalf("excluded CoAP traffic produced a %d-byte response", length)
	} else if timeoutError, ok := err.(net.Error); !ok || !timeoutError.Timeout() {
		t.Fatalf("read after excluded CoAP traffic: %v", err)
	}

	for _, action := range []string{
		"get_request_received",
		"get_response_sent",
		"get_request_terminated",
		"con_response_sent",
		"con_response_retransmitted",
		"con_response_ack_received",
		"con_response_rst_received",
		"con_response_retry_exhausted",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "coap",
			"action":   action,
		}, 0)
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_request_exchanges", nil, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 0)
}

func TestCoAPJSONProducerFinalizesCONExchangeWithMatchingRST(t *testing.T) {
	fixture := startCoAPJSONProducerFixture(t)
	connection := dialCoAPFixture(t, fixture)
	request := []byte{0x41, 0x01, 0x50, 0x01, 0xe3}
	if written, err := connection.Write(request); err != nil {
		t.Fatalf("write CoAP CON GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("CoAP request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set CoAP response deadline: %v", err)
	}
	_ = readCoAPDatagram(t, connection, fixture.output)
	response := readCoAPDatagram(t, connection, fixture.output)
	if len(response) < 4 {
		t.Fatalf("CoAP response length = %d, want at least 4", len(response))
	}

	rst := []byte{0x70, 0x00, response[2], response[3]}
	if written, err := connection.Write(rst); err != nil {
		t.Fatalf("write matching CoAP RST: %v", err)
	} else if written != len(rst) {
		t.Fatalf("CoAP RST bytes = %d, want %d", written, len(rst))
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_rst_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_con_response_exchange_duration_ms", map[string]string{
		"outcome": "rst_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_ack_received",
	}, 0)
}

func TestCoAPJSONProducerFinalizesCONExchangeAtRetryExhaustion(t *testing.T) {
	fixture := startCoAPJSONProducerFixture(t)
	connection := dialCoAPFixture(t, fixture)
	request := []byte{0x41, 0x01, 0x60, 0x01, 0xf4}
	if written, err := connection.Write(request); err != nil {
		t.Fatalf("write CoAP CON GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("CoAP request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set CoAP response deadline: %v", err)
	}
	_ = readCoAPDatagram(t, connection, fixture.output)
	initialResponse := readCoAPDatagram(t, connection, fixture.output)
	retransmission := readCoAPDatagram(t, connection, fixture.output)
	if !bytes.Equal(retransmission, initialResponse) {
		t.Fatalf("CoAP retransmission = %x, want exact initial response %x", retransmission, initialResponse)
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_retry_exhausted",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_retransmitted",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_con_response_exchange_duration_ms", map[string]string{
		"outcome": "retry_exhausted",
	}, 1)
}

func TestCoAPJSONProducerTreatsShortInitialDatagramAsTerminationAndWriteError(t *testing.T) {
	fixture := startCoAPJSONProducerFixtureWithOptions(t, coapJSONProducerOptions{
		delayMilliseconds: "20",
		ackTimeoutMS:      "100",
		maxRetransmit:     "1",
		sendInjection:     "initial_short",
	})
	connection := dialCoAPFixture(t, fixture)
	request := []byte{0x51, 0x01, 0x70, 0x01, 0xa8}
	if written, err := connection.Write(request); err != nil {
		t.Fatalf("write CoAP NON GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("CoAP request bytes = %d, want %d", written, len(request))
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_request_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_request_terminated",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "coap",
		"reason":   "other",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "get_response_sent",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_request_exchanges", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_coap_request_exchange_duration_ms", map[string]string{
		"outcome": "terminated",
	}, 1)
}

func TestCoAPJSONProducerKeepsCONActiveAfterShortRetransmission(t *testing.T) {
	fixture := startCoAPJSONProducerFixtureWithOptions(t, coapJSONProducerOptions{
		delayMilliseconds: "20",
		ackTimeoutMS:      "500",
		maxRetransmit:     "1",
		sendInjection:     "retransmission_short",
	})
	connection := dialCoAPFixture(t, fixture)
	request := []byte{0x41, 0x01, 0x71, 0x01, 0xa9}
	if written, err := connection.Write(request); err != nil {
		t.Fatalf("write CoAP CON GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("CoAP request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set CoAP response deadline: %v", err)
	}
	_ = readCoAPDatagram(t, connection, fixture.output)
	response := readCoAPDatagram(t, connection, fixture.output)

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "coap",
		"reason":   "other",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_retransmitted",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 1)

	ack := []byte{0x60, 0x00, response[2], response[3]}
	if written, err := connection.Write(ack); err != nil {
		t.Fatalf("write matching CoAP ACK: %v", err)
	} else if written != len(ack) {
		t.Fatalf("CoAP ACK bytes = %d, want %d", written, len(ack))
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "coap",
		"action":   "con_response_ack_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_coap_active_con_response_exchanges", nil, 0)
}

func dialCoAPFixture(t *testing.T, fixture *coapJSONProducerFixture) *net.UDPConn {
	t.Helper()
	connection, err := net.DialUDP(
		"udp4",
		nil,
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: fixture.port},
	)
	if err != nil {
		t.Fatalf("dial isolated JSON CoAP producer: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func readCoAPDatagram(t *testing.T, connection *net.UDPConn, output *bytes.Buffer) []byte {
	t.Helper()
	datagram := make([]byte, 64)
	length, err := connection.Read(datagram)
	if err != nil {
		t.Fatalf("read CoAP datagram: %v\n%s", err, output.String())
	}
	return datagram[:length]
}

func reserveUDPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("reserve UDP port: %v", err)
	}
	port := listener.LocalAddr().(*net.UDPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved UDP port: %v", err)
	}
	return port
}

func waitForUDPBind(t *testing.T, port int, producer *exec.Cmd, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		probe, err := net.ListenUDP("udp4", &net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: port,
		})
		if err != nil {
			return
		}
		_ = probe.Close()
		if producer.ProcessState != nil {
			t.Fatalf("CoAP producer exited before binding UDP port\n%s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("CoAP producer did not bind UDP port %d\n%s", port, output.String())
}
