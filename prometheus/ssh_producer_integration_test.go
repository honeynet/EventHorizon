package main

import (
	"bytes"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type sshJSONProducerFixture struct {
	registry *prometheus.Registry
	port     int
	producer *exec.Cmd
	output   *bytes.Buffer
}

func startSSHJSONProducerFixture(t *testing.T, delayMilliseconds string) *sshJSONProducerFixture {
	t.Helper()
	testDirectory := shortTempDir(t)
	binaryPath := filepath.Join(testDirectory, "endlessh_json_mvp")
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	// endlessh.c includes "metric_events.h" relative to its own directory,
	// which resolves in the container because the Dockerfile copies both
	// endlessh.c and shared/ into /. Outside the container, point at shared/.
	compile := exec.Command(
		"gcc",
		"-std=c99", "-Wall", "-Wextra", "-Wno-missing-field-initializers", "-Os", "-pthread",
		"-I", filepath.Join(repositoryRoot, "shared"),
		"-o", binaryPath,
		filepath.Join(repositoryRoot, "endlessh/endlessh.c"),
		filepath.Join(repositoryRoot, "shared/metric_events.c"),
	)
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile isolated JSON Endlessh producer: %v\n%s", err, output)
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
	producer := exec.Command(
		binaryPath,
		"-p", strconv.Itoa(port),
		"-d", delayMilliseconds,
		"-v",
	)
	producer.Env = append(
		os.Environ(),
		"EVENTHORIZON_METRIC_SOCKET="+metricSocketPath,
	)
	producer.Stdout = producerOutput
	producer.Stderr = producerOutput
	if err := producer.Start(); err != nil {
		t.Fatalf("start isolated JSON Endlessh producer: %v", err)
	}
	t.Cleanup(func() {
		if producer.ProcessState == nil {
			_ = producer.Process.Kill()
			_ = producer.Wait()
		}
	})

	return &sshJSONProducerFixture{
		registry: registry,
		port:     port,
		producer: producer,
		output:   producerOutput,
	}
}

func (fixture *sshJSONProducerFixture) connect(t *testing.T) *net.TCPConn {
	t.Helper()
	connection, err := dialTCPEventually(fixture.port, 2*time.Second)
	if err != nil {
		_ = fixture.producer.Process.Kill()
		_ = fixture.producer.Wait()
		t.Fatalf("connect to isolated JSON Endlessh producer: %v\n%s", err, fixture.output.String())
	}
	return connection
}

// An accepted TCP connection is the one boundary Endlessh observes exactly, so
// total_connects{server="SSH"} must rise immediately on accept, before any
// write has been scheduled.
func TestSSHJSONProducerCountsAcceptedConnection(t *testing.T) {
	fixture := startSSHJSONProducerFixture(t, "60000")
	connection := fixture.connect(t)
	t.Cleanup(func() { _ = connection.Close() })

	assertGatheredValueEventually(t, fixture.registry, "total_connects", map[string]string{
		"server": "SSH",
	}, 1)
}

// A tracked client is removed only when some later scheduled write to it fails,
// so the lifetime observation must land under write_failed. How many write
// intervals that takes is not fixed and is not bounded to one interval, and the
// recorded value is not equivalent to peer-disconnect time.
func TestSSHJSONProducerObservesWriteFailedTrackedClientLifetime(t *testing.T) {
	fixture := startSSHJSONProducerFixture(t, "300")
	connection := fixture.connect(t)

	assertGatheredValueEventually(t, fixture.registry, "total_connects", map[string]string{
		"server": "SSH",
	}, 1)

	// Close the client. Endlessh removes the tracked client only when some
	// later scheduled write to it fails, not at this moment.
	if err := connection.Close(); err != nil {
		t.Fatalf("close SSH client connection: %v", err)
	}

	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_ssh_tracked_client_lifetime_ms", map[string]string{
		"observation_end_reason": "write_failed",
	}, 1)
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_ssh_tracked_client_lifetime_ms", map[string]string{
		"observation_end_reason": "server_shutdown",
	}, 0)
}

// SSH owns no active gauge and contributes no interaction depth, protocol
// action, or byte evidence. This asserts the deliberate absence, so a future
// change that quietly widens the SSH surface fails here.
func TestSSHJSONProducerPublishesNoActiveGaugeOrRichEvidence(t *testing.T) {
	fixture := startSSHJSONProducerFixture(t, "60000")
	connection := fixture.connect(t)
	t.Cleanup(func() { _ = connection.Close() })

	assertGatheredValueEventually(t, fixture.registry, "total_connects", map[string]string{
		"server": "SSH",
	}, 1)

	families, err := fixture.registry.Gather()
	if err != nil {
		t.Fatalf("gather SSH metric families: %v", err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				isSSHLabel := (label.GetName() == "server" && label.GetValue() == "SSH") ||
					(label.GetName() == "protocol" && label.GetValue() == "ssh")
				if !isSSHLabel {
					continue
				}
				switch family.GetName() {
				case "total_connects":
					// The one counter SSH is allowed to populate.
				default:
					t.Errorf("SSH must not populate %s", family.GetName())
				}
			}
		}
	}

	// current_connected_clients must have no SSH population at all.
	for _, family := range families {
		if family.GetName() != "current_connected_clients" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "server" && label.GetValue() == "SSH" {
					t.Error("current_connected_clients must not carry server=SSH")
				}
			}
		}
	}
}

func TestSSHJSONProducerPreservesPaperEraStdoutEvidence(t *testing.T) {
	fixture := startSSHJSONProducerFixture(t, "300")
	connection := fixture.connect(t)

	assertGatheredValueEventually(t, fixture.registry, "total_connects", map[string]string{
		"server": "SSH",
	}, 1)
	if err := connection.Close(); err != nil {
		t.Fatalf("close SSH client connection: %v", err)
	}
	assertGatheredHistogramCountEventually(t, fixture.registry, "eventhorizon_ssh_tracked_client_lifetime_ms", map[string]string{
		"observation_end_reason": "write_failed",
	}, 1)

	output := fixture.output.String()
	if !strings.Contains(output, "SSH connect ") {
		t.Errorf("missing paper-era SSH connect line:\n%s", output)
	}
	if !strings.Contains(output, "SSH disconnect ") {
		t.Errorf("missing paper-era SSH disconnect line:\n%s", output)
	}
}
