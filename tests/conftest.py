"""
Shared fixtures for EventHorizon integration tests.

These fixtures handle:
- One-time compilation of C tarpit binaries (session-scoped)
- Starting/stopping individual protocol emulators on ephemeral ports
- Port allocation to avoid conflicts between parallel tests
"""

import os
import signal
import socket
import subprocess
import time

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN_DIR = os.path.join(PROJECT_ROOT, "bin")


def find_free_port(protocol="tcp"):
    """Find an available port on localhost."""
    sock_type = socket.SOCK_STREAM if protocol == "tcp" else socket.SOCK_DGRAM
    with socket.socket(socket.AF_INET, sock_type) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_tcp_port(port, host="127.0.0.1", timeout=5.0):
    """Block until a TCP port is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.05)
    return False


def wait_for_udp_port(port, host="127.0.0.1", timeout=5.0):
    """
    Best-effort check that a UDP server is listening.
    Sends a minimal probe and waits briefly for a response or for the
    process to be alive.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(0.3)
            # Send a minimal CoAP-like GET request (version=1, type=CON, code=0.01 GET)
            probe = bytes([0x40, 0x01, 0x00, 0x01])
            sock.sendto(probe, (host, port))
            try:
                sock.recvfrom(1024)
                sock.close()
                return True
            except socket.timeout:
                sock.close()
                # Process might be up but not responding to our probe type
                time.sleep(0.05)
        except OSError:
            time.sleep(0.05)
    return False


@pytest.fixture(scope="session")
def compile_binaries():
    """
    Compile all C tarpit binaries once per test session.
    Requires gcc to be available on the system.
    """
    result = subprocess.run(
        ["make", "telnet_pit", "upnp_pit", "mqtt_pit", "coap_pit"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        pytest.skip(
            f"Compilation failed (is gcc installed?):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    # Verify all binaries exist
    for name in ("telnet_pit", "upnp_pit", "mqtt_pit", "coap_pit"):
        binary = os.path.join(BIN_DIR, name)
        if not os.path.isfile(binary):
            pytest.skip(f"Binary not found after compilation: {binary}")
    return BIN_DIR


class EmulatorProcess:
    """Wrapper around a tarpit subprocess for clean start/stop lifecycle."""

    def __init__(self, binary_path, args, wait_port=None, wait_protocol="tcp"):
        self.binary_path = binary_path
        self.args = args
        self.wait_port = wait_port
        self.wait_protocol = wait_protocol
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            [self.binary_path] + [str(a) for a in self.args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if self.wait_port is not None:
            if self.wait_protocol == "tcp":
                ready = wait_for_tcp_port(self.wait_port)
            else:
                ready = wait_for_udp_port(self.wait_port)
            if not ready:
                self.stop()
                raise RuntimeError(
                    f"Emulator {self.binary_path} did not start on port {self.wait_port}"
                )
        return self

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)

    @property
    def is_running(self):
        return self.process is not None and self.process.poll() is None


@pytest.fixture
def telnet_server(compile_binaries):
    """
    Start a telnet tarpit on an ephemeral port.

    Args used: <port> <delay_ms> <max_clients>
    Using a small delay (50ms) so tests don't wait too long.
    """
    port = find_free_port()
    binary = os.path.join(compile_binaries, "telnet_pit")
    emulator = EmulatorProcess(
        binary, [port, 50, 64], wait_port=port, wait_protocol="tcp"
    )
    emulator.start()
    yield {"process": emulator, "port": port, "host": "127.0.0.1"}
    emulator.stop()


@pytest.fixture
def mqtt_server(compile_binaries):
    """
    Start an MQTT tarpit on an ephemeral port.

    Args: <port> <max_events> <epoll_timeout_ms> <pubrel_interval_ms>
          <max_packets_per_client> <max_clients>
    """
    port = find_free_port()
    binary = os.path.join(compile_binaries, "mqtt_pit")
    emulator = EmulatorProcess(
        binary, [port, 64, 5000, 10000, 50, 64], wait_port=port, wait_protocol="tcp"
    )
    emulator.start()
    yield {"process": emulator, "port": port, "host": "127.0.0.1"}
    emulator.stop()


@pytest.fixture
def coap_server(compile_binaries):
    """
    Start a CoAP tarpit on an ephemeral port.

    Args: <port> <delay_ms> <ack_timeout_ms> <max_retransmit> <max_clients>
    """
    port = find_free_port(protocol="udp")
    binary = os.path.join(compile_binaries, "coap_pit")
    emulator = EmulatorProcess(
        binary, [port, 100, 2000, 4, 64], wait_port=port, wait_protocol="udp"
    )
    emulator.start()
    yield {"process": emulator, "port": port, "host": "127.0.0.1"}
    emulator.stop()


@pytest.fixture
def upnp_server(compile_binaries):
    """
    Start a UPnP tarpit with HTTP + SSDP on ephemeral ports.

    Args: <http_port> <ssdp_port> <delay_ms> <max_clients>
    """
    http_port = find_free_port()
    ssdp_port = find_free_port(protocol="udp")
    binary = os.path.join(compile_binaries, "upnp_pit")
    emulator = EmulatorProcess(
        binary, [http_port, ssdp_port, 200, 64],
        wait_port=http_port, wait_protocol="tcp"
    )
    emulator.start()
    yield {
        "process": emulator,
        "http_port": http_port,
        "ssdp_port": ssdp_port,
        "host": "127.0.0.1",
    }
    emulator.stop()
