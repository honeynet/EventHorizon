package main

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type upnpJSONProducerFixture struct {
	registry *prometheus.Registry
	httpPort int
	ssdpPort int
	producer *exec.Cmd
	output   *bytes.Buffer
}

type upnpJSONProducerOptions struct {
	delayMilliseconds string
	writeInjection    string
	ssdpInjection     string
	finalTime         string
}

func startUPnPJSONProducerFixture(t *testing.T) *upnpJSONProducerFixture {
	t.Helper()
	return startUPnPJSONProducerFixtureWithOptions(t, upnpJSONProducerOptions{
		delayMilliseconds: "5",
	})
}

func startUPnPJSONProducerFixtureWithOptions(
	t *testing.T,
	options upnpJSONProducerOptions,
) *upnpJSONProducerFixture {
	t.Helper()
	testDirectory := shortTempDir(t)
	binaryPath := filepath.Join(testDirectory, "upnp_pit_json_mvp")
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	compile := exec.Command(
		"gcc",
		"-Wall", "-Wextra", "-g", "-pthread",
		"-DEVENTHORIZON_JSON_METRIC_EVENTS",
		"-DEVENTHORIZON_UPNP_TEST_RUNTIME",
		"-o", binaryPath,
		filepath.Join(repositoryRoot, "servers/upnp_pit.c"),
		filepath.Join(repositoryRoot, "shared/structs.c"),
		filepath.Join(repositoryRoot, "shared/session_events.c"),
		filepath.Join(repositoryRoot, "shared/interaction_depth.c"),
		filepath.Join(repositoryRoot, "shared/metric_events.c"),
	)
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile isolated JSON UPnP producer: %v\n%s", err, output)
	}

	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	metricSocketPath := filepath.Join(testDirectory, "metrics.sock")
	metricServer, err := startMetricEventServer(metricSocketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = metricServer.Close() })

	httpPort := reserveTCPPort(t)
	ssdpPort := reserveUDPPort(t)
	producerOutput := &bytes.Buffer{}
	producer := exec.Command(
		binaryPath,
		strconv.Itoa(httpPort),
		strconv.Itoa(ssdpPort),
		options.delayMilliseconds,
		"64",
	)
	producer.Env = append(
		os.Environ(),
		"EVENTHORIZON_METRIC_SOCKET="+metricSocketPath,
		"EVENTHORIZON_SESSION_LOG="+filepath.Join(testDirectory, "sessions.jsonl"),
	)
	for name, value := range map[string]string{
		"EVENTHORIZON_UPNP_TEST_WRITE_INJECTION": options.writeInjection,
		"EVENTHORIZON_UPNP_TEST_SSDP_INJECTION":  options.ssdpInjection,
		"EVENTHORIZON_UPNP_TEST_FINAL_TIME":      options.finalTime,
	} {
		if value != "" {
			producer.Env = append(producer.Env, name+"="+value)
		}
	}
	producer.Stdout = producerOutput
	producer.Stderr = producerOutput
	if err := producer.Start(); err != nil {
		t.Fatalf("start isolated JSON UPnP producer: %v", err)
	}
	t.Cleanup(func() {
		if producer.ProcessState == nil {
			_ = producer.Process.Kill()
			_ = producer.Wait()
		}
	})
	waitForTCPBind(t, httpPort, producer, producerOutput)
	waitForUDPBind(t, ssdpPort, producer, producerOutput)

	return &upnpJSONProducerFixture{
		registry: registry,
		httpPort: httpPort,
		ssdpPort: ssdpPort,
		producer: producer,
		output:   producerOutput,
	}
}

func TestUPnPJSONProducerCompletesFiniteDeviceDescriptionResponse(t *testing.T) {
	fixture := startUPnPJSONProducerFixture(t)
	connection, err := net.DialTimeout(
		"tcp4",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(fixture.httpPort)),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("connect to isolated JSON UPnP producer: %v\n%s", err, fixture.output.String())
	}
	t.Cleanup(func() { _ = connection.Close() })
	request := fmt.Sprintf(
		"GET /hue-device.xml HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nConnection: close\r\n\r\n",
		fixture.httpPort,
	)
	if written, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write UPnP description GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("UPnP request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set UPnP response deadline: %v", err)
	}

	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		t.Fatalf("read UPnP description response headers: %v\n%s", err, fixture.output.String())
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read UPnP description response body: %v\n%s", err, fixture.output.String())
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("UPnP response status = %d, want 200", response.StatusCode)
	}
	if response.Proto != "HTTP/1.1" {
		t.Fatalf("UPnP response protocol = %q, want HTTP/1.1", response.Proto)
	}
	if got := response.Header.Get("Content-Type"); got != "text/xml" {
		t.Fatalf("UPnP Content-Type = %q, want text/xml", got)
	}
	if response.ContentLength != int64(len(body)) {
		t.Fatalf("UPnP Content-Length = %d, actual body = %d", response.ContentLength, len(body))
	}
	if response.ContentLength <= 0 {
		t.Fatalf("UPnP Content-Length = %d, want positive", response.ContentLength)
	}
	if got := response.Header.Get("Transfer-Encoding"); got != "" || len(response.TransferEncoding) != 0 {
		t.Fatalf("UPnP response uses Transfer-Encoding %q/%v", got, response.TransferEncoding)
	}
	if !strings.HasPrefix(string(body), "<?xml") {
		t.Fatalf("UPnP response body is not an XML declaration: %.32q", body)
	}
	var document struct {
		XMLName xml.Name `xml:"root"`
	}
	if err := xml.Unmarshal(body, &document); err != nil {
		t.Fatalf("UPnP device description is not valid XML: %v", err)
	}

	for _, action := range []string{
		"description_get_received",
		"description_response_started",
		"description_response_completed",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "upnp",
			"action":   action,
		}, 1)
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "description_response_terminated",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_upnp_active_description_responses", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_upnp_description_stream_duration_ms", map[string]string{
		"outcome": "completed",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_upnp_description_stream_duration_ms", map[string]string{
		"outcome": "terminated",
	}, 0)
	for _, reason := range []string{"timeout", "reset", "closed", "other"} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
			"protocol": "upnp",
			"reason":   reason,
		}, 0)
	}
}

func TestUPnPJSONProducerExportsValidSSDPDiscoveryExchange(t *testing.T) {
	fixture := startUPnPJSONProducerFixture(t)
	connection, err := net.DialUDP(
		"udp4",
		nil,
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: fixture.ssdpPort},
	)
	if err != nil {
		t.Fatalf("dial isolated JSON SSDP listener: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	request := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		"MAN: \"ssdp:discover\"",
		"MX: 1",
		"ST: urn:Philips:device:Basic:1",
		"",
		"",
	}, "\r\n")
	if written, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write valid M-SEARCH: %v", err)
	} else if written != len(request) {
		t.Fatalf("M-SEARCH bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set SSDP response deadline: %v", err)
	}
	response := make([]byte, 1024)
	responseLength, err := connection.Read(response)
	if err != nil {
		t.Fatalf("read SSDP discovery response: %v\n%s", err, fixture.output.String())
	}
	responseText := string(response[:responseLength])
	if !strings.HasPrefix(responseText, "HTTP/1.1 200 OK\r\n") {
		t.Fatalf("SSDP response status line = %.32q", responseText)
	}
	if !strings.Contains(responseText, fmt.Sprintf(":%d/hue-device.xml\r\n", fixture.httpPort)) {
		t.Fatalf("SSDP LOCATION does not advertise the configured description endpoint:\n%s", responseText)
	}
	if !strings.Contains(responseText, "ST: urn:Philips:device:Basic:1\r\n") {
		t.Fatalf("SSDP response does not contain the matching ST:\n%s", responseText)
	}
	if !strings.HasSuffix(responseText, "\r\n\r\n") {
		t.Fatalf("SSDP response is not a complete header datagram:\n%s", responseText)
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "ssdp_msearch_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "ssdp_discovery_response_sent",
	}, 1)
}

func TestUPnPJSONProducerSeparatesUnmatchedAndMalformedSSDP(t *testing.T) {
	fixture := startUPnPJSONProducerFixture(t)
	connection, err := net.DialUDP(
		"udp4",
		nil,
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: fixture.ssdpPort},
	)
	if err != nil {
		t.Fatalf("dial isolated JSON SSDP listener: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	unmatched := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		"MAN: \"ssdp:discover\"",
		"MX: 1",
		"ST: urn:example:device:unsupported:1",
		"",
		"",
	}, "\r\n")
	malformed := "M-SEARCH without required SSDP headers\r\n\r\n"
	for index, request := range []string{unmatched, malformed} {
		if written, err := io.WriteString(connection, request); err != nil {
			t.Fatalf("write SSDP request %d: %v", index+1, err)
		} else if written != len(request) {
			t.Fatalf("SSDP request %d bytes = %d, want %d", index+1, written, len(request))
		}
	}
	if err := connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set unmatched SSDP response deadline: %v", err)
	}
	response := make([]byte, 1024)
	if length, err := connection.Read(response); err == nil {
		t.Fatalf("unmatched or malformed SSDP request produced %d response bytes", length)
	} else if timeoutError, ok := err.(net.Error); !ok || !timeoutError.Timeout() {
		t.Fatalf("read after unmatched/malformed SSDP requests: %v", err)
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "ssdp_msearch_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "ssdp_discovery_response_sent",
	}, 0)
}

func TestUPnPJSONProducerKeepsPreStartWriteFailureOutOfResponsePopulation(t *testing.T) {
	fixture := startUPnPJSONProducerFixtureWithOptions(t, upnpJSONProducerOptions{
		delayMilliseconds: "5",
		writeInjection:    "prestart_failure",
	})
	responseBytes := performRawDescriptionGET(t, fixture)
	if len(responseBytes) != 0 {
		t.Fatalf("pre-start failure wrote %d bytes, want 0", len(responseBytes))
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "description_get_received",
	}, 1)
	for _, action := range []string{
		"description_response_started",
		"description_response_completed",
		"description_response_terminated",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "upnp",
			"action":   action,
		}, 0)
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "upnp",
		"reason":   "other",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_upnp_active_description_responses", nil, 0)
	for _, outcome := range []string{"completed", "terminated"} {
		assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_upnp_description_stream_duration_ms", map[string]string{
			"outcome": outcome,
		}, 0)
	}
}

func TestUPnPJSONProducerTerminatesStartedResponseBeforeWriteError(t *testing.T) {
	fixture := startUPnPJSONProducerFixtureWithOptions(t, upnpJSONProducerOptions{
		delayMilliseconds: "5",
		writeInjection:    "poststart_failure",
	})
	responseBytes := performRawDescriptionGET(t, fixture)
	if len(responseBytes) == 0 {
		t.Fatal("post-start failure produced no positive response bytes")
	}

	for _, action := range []string{
		"description_get_received",
		"description_response_started",
		"description_response_terminated",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "upnp",
			"action":   action,
		}, 1)
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "description_response_completed",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "upnp",
		"reason":   "closed",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_upnp_active_description_responses", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_upnp_description_stream_duration_ms", map[string]string{
		"outcome": "terminated",
	}, 1)
}

func TestUPnPJSONProducerClassifiesDeadlineBeforeMillisecondTruncation(t *testing.T) {
	tests := []struct {
		name         string
		finalTime    string
		outcome      string
		otherOutcome string
	}{
		{
			name:         "exact deadline completes",
			finalTime:    "deadline",
			outcome:      "completed",
			otherOutcome: "terminated",
		},
		{
			name:         "half millisecond beyond deadline terminates",
			finalTime:    "just_over",
			outcome:      "terminated",
			otherOutcome: "completed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := startUPnPJSONProducerFixtureWithOptions(t, upnpJSONProducerOptions{
				delayMilliseconds: "0",
				finalTime:         test.finalTime,
			})
			responseBytes := performRawDescriptionGET(t, fixture)
			if len(responseBytes) == 0 {
				t.Fatal("deadline fixture produced no response bytes")
			}

			assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
				"protocol": "upnp",
				"action":   "description_response_" + test.outcome,
			}, 1)
			assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
				"protocol": "upnp",
				"action":   "description_response_" + test.otherOutcome,
			}, 0)
			assertGatheredHistogramEventually(t, fixture.registry, "eventhorizon_upnp_description_stream_duration_ms", map[string]string{
				"outcome": test.outcome,
			}, 1, 30000)
		})
	}
}

func TestUPnPJSONProducerRequiresExactSSDPDatagramTransmission(t *testing.T) {
	fixture := startUPnPJSONProducerFixtureWithOptions(t, upnpJSONProducerOptions{
		delayMilliseconds: "5",
		ssdpInjection:     "short",
	})
	connection, err := net.DialUDP(
		"udp4",
		nil,
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: fixture.ssdpPort},
	)
	if err != nil {
		t.Fatalf("dial isolated JSON SSDP listener: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	request := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		"MAN: \"ssdp:discover\"",
		"MX: 1",
		"ST: urn:Philips:device:Basic:1",
		"",
		"",
	}, "\r\n")
	if written, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write valid M-SEARCH: %v", err)
	} else if written != len(request) {
		t.Fatalf("M-SEARCH bytes = %d, want %d", written, len(request))
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "ssdp_msearch_received",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "ssdp_discovery_response_sent",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
		"protocol": "upnp",
		"reason":   "other",
	}, 1)
}

func TestUPnPJSONProducerExcludesInvalidDescriptionRequests(t *testing.T) {
	fixture := startUPnPJSONProducerFixture(t)
	requests := []string{
		fmt.Sprintf(
			"GET /not-advertised.xml HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n\r\n",
			fixture.httpPort,
		),
		"GET /hue-device.xml HTTP/1.1\r\nConnection: close\r\n\r\n",
	}
	for index, request := range requests {
		connection, err := net.DialTimeout(
			"tcp4",
			net.JoinHostPort("127.0.0.1", strconv.Itoa(fixture.httpPort)),
			2*time.Second,
		)
		if err != nil {
			t.Fatalf("connect for invalid UPnP request %d: %v", index+1, err)
		}
		if written, err := io.WriteString(connection, request); err != nil {
			_ = connection.Close()
			t.Fatalf("write invalid UPnP request %d: %v", index+1, err)
		} else if written != len(request) {
			_ = connection.Close()
			t.Fatalf("invalid UPnP request %d bytes = %d, want %d", index+1, written, len(request))
		}
		if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			_ = connection.Close()
			t.Fatalf("set invalid UPnP response deadline: %v", err)
		}
		response, err := io.ReadAll(connection)
		_ = connection.Close()
		if err != nil {
			t.Fatalf("read invalid UPnP response %d: %v", index+1, err)
		}
		if len(response) != 0 {
			t.Fatalf("invalid UPnP request %d produced %d response bytes", index+1, len(response))
		}
	}

	for _, action := range []string{
		"description_get_received",
		"description_response_started",
		"description_response_completed",
		"description_response_terminated",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "upnp",
			"action":   action,
		}, 0)
	}
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_upnp_active_description_responses", nil, 0)
}

func TestUPnPJSONProducerKeepsStartedResponseActiveBeforeDeadline(t *testing.T) {
	fixture := startUPnPJSONProducerFixtureWithOptions(t, upnpJSONProducerOptions{
		delayMilliseconds: "1000",
	})
	connection, err := net.DialTimeout(
		"tcp4",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(fixture.httpPort)),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("connect to isolated JSON UPnP producer: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	request := fmt.Sprintf(
		"GET /hue-device.xml HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nConnection: close\r\n\r\n",
		fixture.httpPort,
	)
	if written, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write UPnP description GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("UPnP request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set first UPnP response-byte deadline: %v", err)
	}
	firstBytes := make([]byte, 16)
	if read, err := connection.Read(firstBytes); err != nil {
		t.Fatalf("read first positive UPnP response bytes: %v\n%s", err, fixture.output.String())
	} else if read <= 0 {
		t.Fatalf("first UPnP response read = %d, want positive", read)
	}

	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
		"protocol": "upnp",
		"action":   "description_response_started",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_upnp_active_description_responses", nil, 1)
	for _, action := range []string{
		"description_response_completed",
		"description_response_terminated",
	} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_protocol_actions_total", map[string]string{
			"protocol": "upnp",
			"action":   action,
		}, 0)
	}
	for _, outcome := range []string{"completed", "terminated"} {
		assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_upnp_description_stream_duration_ms", map[string]string{
			"outcome": outcome,
		}, 0)
	}
}

func performRawDescriptionGET(t *testing.T, fixture *upnpJSONProducerFixture) []byte {
	t.Helper()
	connection, err := net.DialTimeout(
		"tcp4",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(fixture.httpPort)),
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("connect to isolated JSON UPnP producer: %v\n%s", err, fixture.output.String())
	}
	t.Cleanup(func() { _ = connection.Close() })
	request := fmt.Sprintf(
		"GET /hue-device.xml HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nConnection: close\r\n\r\n",
		fixture.httpPort,
	)
	if written, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write UPnP description GET: %v", err)
	} else if written != len(request) {
		t.Fatalf("UPnP request bytes = %d, want %d", written, len(request))
	}
	if err := connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set UPnP response deadline: %v", err)
	}
	response, err := io.ReadAll(connection)
	if err != nil {
		t.Fatalf("read raw UPnP response: %v\n%s", err, fixture.output.String())
	}
	return response
}

func waitForTCPBind(t *testing.T, port int, producer *exec.Cmd, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		probe, err := net.ListenTCP("tcp4", &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: port,
		})
		if err != nil {
			return
		}
		_ = probe.Close()
		if producer.ProcessState != nil {
			t.Fatalf("UPnP producer exited before binding TCP port\n%s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("UPnP producer did not bind TCP port %d\n%s", port, output.String())
}
