package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type telnetJSONProducerFixture struct {
	registry *prometheus.Registry
	port     int
	producer *exec.Cmd
	output   *bytes.Buffer
}

func startTelnetJSONProducerFixture(t *testing.T, delayMilliseconds string) *telnetJSONProducerFixture {
	t.Helper()
	testDirectory := shortTempDir(t)
	binaryPath := filepath.Join(testDirectory, "telnet_pit_json_mvp")
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	compile := exec.Command(
		"gcc",
		"-Wall", "-Wextra", "-g", "-pthread",
		"-DEVENTHORIZON_JSON_METRIC_EVENTS",
		"-o", binaryPath,
		filepath.Join(repositoryRoot, "servers/telnet_pit.c"),
		filepath.Join(repositoryRoot, "shared/structs.c"),
		filepath.Join(repositoryRoot, "shared/session_events.c"),
		filepath.Join(repositoryRoot, "shared/interaction_depth.c"),
		filepath.Join(repositoryRoot, "shared/metric_events.c"),
	)
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile isolated JSON Telnet producer: %v\n%s", err, output)
	}

	registry := prometheus.NewRegistry()
	metricState := newMetricEventMetrics(registry)
	metricSocketPath := filepath.Join(testDirectory, "metrics.sock")
	metricServer, err := startMetricEventServer(metricSocketPath, metricState)
	if err != nil {
		t.Fatalf("start metric event server: %v", err)
	}
	t.Cleanup(func() { _ = metricServer.Close() })

	port := reserveTCPPort(t)
	producerOutput := &bytes.Buffer{}
	producer := exec.Command(binaryPath, strconv.Itoa(port), delayMilliseconds, "64")
	producer.Env = append(
		os.Environ(),
		"EVENTHORIZON_METRIC_SOCKET="+metricSocketPath,
		"EVENTHORIZON_SESSION_LOG="+filepath.Join(testDirectory, "sessions.jsonl"),
	)
	producer.Stdout = producerOutput
	producer.Stderr = producerOutput
	if err := producer.Start(); err != nil {
		t.Fatalf("start isolated JSON Telnet producer: %v", err)
	}
	t.Cleanup(func() {
		if producer.ProcessState == nil {
			_ = producer.Process.Kill()
			_ = producer.Wait()
		}
	})

	return &telnetJSONProducerFixture{
		registry: registry,
		port:     port,
		producer: producer,
		output:   producerOutput,
	}
}

func (fixture *telnetJSONProducerFixture) connect(t *testing.T) *net.TCPConn {
	t.Helper()
	connection, err := dialTCPEventually(fixture.port, 2*time.Second)
	if err != nil {
		_ = fixture.producer.Process.Kill()
		_ = fixture.producer.Wait()
		t.Fatalf("connect to isolated JSON Telnet producer: %v\n%s", err, fixture.output.String())
	}
	return connection
}

func TestTelnetJSONProducerExportsRealConnectionLifecycle(t *testing.T) {
	fixture := startTelnetJSONProducerFixture(t, "20")
	connection := fixture.connect(t)
	t.Cleanup(func() { _ = connection.Close() })

	firstInput := []byte("hello\r\n")
	if written, err := connection.Write(firstInput); err != nil {
		t.Fatalf("write Telnet input: %v", err)
	} else if written != len(firstInput) {
		t.Fatalf("Telnet input bytes = %d, want %d", written, len(firstInput))
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set Telnet read deadline: %v", err)
	}
	firstWrite := readTelnetOption(t, connection)

	continuation := []byte("more")
	if written, err := connection.Write(continuation); err != nil {
		t.Fatalf("write Telnet continuation: %v", err)
	} else if written != len(continuation) {
		t.Fatalf("Telnet continuation bytes = %d, want %d", written, len(continuation))
	}
	secondWrite := readTelnetOption(t, connection)
	if err := connection.CloseWrite(); err != nil {
		t.Fatalf("close Telnet client write side: %v", err)
	}
	remainingWireBytes, err := io.ReadAll(connection)
	if err != nil {
		t.Fatalf("read Telnet wire response: %v", err)
	}
	wireBytes := append(firstWrite, secondWrite...)
	wireBytes = append(wireBytes, remainingWireBytes...)
	if len(wireBytes) == 0 {
		t.Fatal("Telnet producer wrote no positive response bytes")
	}
	inputBytes := len(firstInput) + len(continuation)

	assertGatheredValueEventually(t, fixture.registry, "total_connects", map[string]string{
		"server": "Telnet",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_bytes_received_total", map[string]string{
		"protocol": "telnet",
	}, float64(inputBytes))
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_bytes_sent_total", map[string]string{
		"protocol": "telnet",
	}, float64(len(wireBytes)))
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_telnet_first_write_delay_ms", nil, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_telnet_inter_write_interval_ms", nil, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_completed_sessions_total", map[string]string{
		"protocol":          "telnet",
		"disconnect_reason": "peer_closed",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_session_duration_ms", map[string]string{
		"protocol": "telnet",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_session_interaction_depth_total", map[string]string{
		"protocol":    "telnet",
		"depth_level": "3",
	}, 1)
}

func readTelnetOption(t *testing.T, connection net.Conn) []byte {
	t.Helper()
	option := make([]byte, 2, 3)
	if _, err := io.ReadFull(connection, option); err != nil {
		t.Fatalf("read Telnet option prefix: %v", err)
	}
	if option[0] != 255 {
		t.Fatalf("Telnet option prefix = %d, want IAC 255", option[0])
	}
	switch option[1] {
	case 251, 252, 253, 254:
		third := make([]byte, 1)
		if _, err := io.ReadFull(connection, third); err != nil {
			t.Fatalf("read Telnet option value: %v", err)
		}
		option = append(option, third[0])
	case 241, 249:
	default:
		t.Fatalf("unexpected Telnet command %d", option[1])
	}
	return option
}

func TestTelnetJSONProducerFinalizesCleanEOFBeforePositiveWrite(t *testing.T) {
	fixture := startTelnetJSONProducerFixture(t, "100")
	connection := fixture.connect(t)
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.CloseWrite(); err != nil {
		t.Fatalf("close Telnet client write side: %v", err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set Telnet read deadline: %v", err)
	}
	wireBytes, err := io.ReadAll(connection)
	if err != nil {
		t.Fatalf("read Telnet EOF response: %v", err)
	}
	if len(wireBytes) != 0 {
		t.Fatalf("Telnet bytes before clean EOF finalization = %d, want 0", len(wireBytes))
	}

	assertGatheredValueEventually(t, fixture.registry, "total_connects", map[string]string{
		"server": "Telnet",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_completed_sessions_total", map[string]string{
		"protocol":          "telnet",
		"disconnect_reason": "peer_closed",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_session_interaction_depth_total", map[string]string{
		"protocol":    "telnet",
		"depth_level": "0",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_session_duration_ms", map[string]string{
		"protocol": "telnet",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_telnet_first_write_delay_ms", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_telnet_inter_write_interval_ms", nil, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_bytes_sent_total", map[string]string{
		"protocol": "telnet",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_bytes_received_total", map[string]string{
		"protocol": "telnet",
	}, 0)
	for _, reason := range []string{"timeout", "reset", "closed", "other"} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_read_errors_total", map[string]string{
			"protocol": "telnet",
			"reason":   reason,
		}, 0)
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
			"protocol": "telnet",
			"reason":   reason,
		}, 0)
	}
}

func TestTelnetJSONProducerFinalizesResetBeforePositiveWrite(t *testing.T) {
	fixture := startTelnetJSONProducerFixture(t, "500")
	connection := fixture.connect(t)
	assertGatheredValueEventually(t, fixture.registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 1)
	if err := connection.SetLinger(0); err != nil {
		_ = connection.Close()
		t.Fatalf("configure reset-on-close: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("reset Telnet client connection: %v", err)
	}

	assertGatheredValueEventually(t, fixture.registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_completed_sessions_total", map[string]string{
		"protocol":          "telnet",
		"disconnect_reason": "read_error",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_read_errors_total", map[string]string{
		"protocol": "telnet",
		"reason":   "reset",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_session_interaction_depth_total", map[string]string{
		"protocol":    "telnet",
		"depth_level": "0",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_session_duration_ms", map[string]string{
		"protocol": "telnet",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_telnet_first_write_delay_ms", nil, 0)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_telnet_inter_write_interval_ms", nil, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_bytes_sent_total", map[string]string{
		"protocol": "telnet",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_bytes_received_total", map[string]string{
		"protocol": "telnet",
	}, 0)
	for _, reason := range []string{"timeout", "reset", "closed", "other"} {
		assertGatheredValueEventually(t, fixture.registry, "eventhorizon_write_errors_total", map[string]string{
			"protocol": "telnet",
			"reason":   reason,
		}, 0)
	}
}

func TestTelnetJSONProducerFinalizesActiveConnectionOnShutdown(t *testing.T) {
	fixture := startTelnetJSONProducerFixture(t, "1000")
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- fixture.producer.Wait()
		close(waitDone)
	}()

	connection := fixture.connect(t)
	t.Cleanup(func() { _ = connection.Close() })
	assertGatheredValueEventually(t, fixture.registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 1)

	if err := fixture.producer.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal isolated JSON Telnet producer: %v", err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("isolated JSON Telnet producer shutdown: %v\n%s", err, fixture.output.String())
		}
	case <-time.After(2 * time.Second):
		_ = fixture.producer.Process.Kill()
		<-waitDone
		t.Fatal("isolated JSON Telnet producer did not stop after SIGTERM")
	}

	assertGatheredValueEventually(t, fixture.registry, "current_connected_clients", map[string]string{
		"server": "Telnet",
	}, 0)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_completed_sessions_total", map[string]string{
		"protocol":          "telnet",
		"disconnect_reason": "server_shutdown",
	}, 1)
	assertGatheredValueEventually(t, fixture.registry, "eventhorizon_session_interaction_depth_total", map[string]string{
		"protocol":    "telnet",
		"depth_level": "0",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_session_duration_ms", map[string]string{
		"protocol": "telnet",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_telnet_first_write_delay_ms", nil, 0)
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved TCP port: %v", err)
	}
	return port
}

func dialTCPEventually(port int, timeout time.Duration) (*net.TCPConn, error) {
	deadline := time.Now().Add(timeout)
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for {
		connection, err := net.DialTimeout("tcp4", address, 50*time.Millisecond)
		if err == nil {
			return connection.(*net.TCPConn), nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("dial %s before deadline: %w", address, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertGatheredHistogramCountEventually(
	t *testing.T,
	registry *prometheus.Registry,
	familyName string,
	wantLabels map[string]string,
	wantCount uint64,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		count, _, found, err := gatheredHistogram(registry, familyName, wantLabels)
		if err != nil {
			t.Fatalf("gather %s: %v", familyName, err)
		}
		if found && count == wantCount {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s%v count = %d (found=%v), want %d", familyName, wantLabels, count, found, wantCount)
		}
		time.Sleep(time.Millisecond)
	}
}
