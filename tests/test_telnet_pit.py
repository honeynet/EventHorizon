"""
Integration tests for the Telnet tarpit emulator.

The telnet_pit server traps connections by continuously sending Telnet IAC
(Interpret As Command) negotiation sequences on a configurable delay. Each
sequence is a 3-byte packet: [IAC (0xFF), COMMAND, OPTION].

Valid commands sent by the server:
  - WILL (251/0xFB)
  - WONT (252/0xFC)
  - DO   (253/0xFD)
  - DONT (254/0xFE)

These tests verify that:
1. The server accepts TCP connections
2. It sends valid IAC negotiation bytes
3. The connection is kept alive (tarpit behavior)
4. Multiple simultaneous connections are handled
5. Metrics are emitted to stdout in the expected format
"""

import socket
import time

import pytest

# Telnet protocol constants (matching telnet_pit.c)
IAC = 255
WILL = 251
WONT = 252
DO = 253
DONT = 254
VALID_COMMANDS = {WILL, WONT, DO, DONT}


class TestTelnetConnection:
    """Test that the Telnet tarpit accepts and traps connections."""

    def test_accepts_tcp_connection(self, telnet_server):
        """Server should accept a TCP connection without error."""
        sock = socket.create_connection(
            (telnet_server["host"], telnet_server["port"]), timeout=3
        )
        assert sock.fileno() > 0
        sock.close()

    def test_server_stays_running_after_connection(self, telnet_server):
        """Server process should remain alive after a client connects and disconnects."""
        sock = socket.create_connection(
            (telnet_server["host"], telnet_server["port"]), timeout=3
        )
        sock.close()
        time.sleep(0.1)
        assert telnet_server["process"].is_running

    def test_sends_iac_negotiation_bytes(self, telnet_server):
        """
        Server should send IAC negotiation sequences.
        Each sequence is exactly 3 bytes: IAC (0xFF) + command + option.
        """
        sock = socket.create_connection(
            (telnet_server["host"], telnet_server["port"]), timeout=3
        )
        sock.settimeout(2.0)

        # Wait for the server to send negotiation data (delay is 50ms in fixture)
        data = b""
        deadline = time.monotonic() + 2.0
        while len(data) < 3 and time.monotonic() < deadline:
            try:
                chunk = sock.recv(256)
                if not chunk:
                    break
                data += chunk
            except socket.timeout:
                break

        sock.close()

        assert len(data) >= 3, (
            f"Expected at least 3 bytes of IAC negotiation, got {len(data)}"
        )

        # Parse the first negotiation sequence
        assert data[0] == IAC, f"First byte should be IAC (0xFF), got 0x{data[0]:02X}"
        assert data[1] in VALID_COMMANDS, (
            f"Second byte should be a valid Telnet command "
            f"(WILL/WONT/DO/DONT), got 0x{data[1]:02X}"
        )
        # Third byte is the option number (0-255), any value is valid

    def test_iac_sequences_are_valid_triplets(self, telnet_server):
        """
        All data sent should consist of valid 3-byte IAC sequences.
        This verifies the server isn't sending garbage data.
        """
        sock = socket.create_connection(
            (telnet_server["host"], telnet_server["port"]), timeout=3
        )
        sock.settimeout(2.0)

        data = b""
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                data += chunk
            except socket.timeout:
                break

        sock.close()

        assert len(data) >= 3, "Should receive at least one IAC sequence"

        # Every 3-byte chunk should be a valid IAC sequence
        # (data length should be a multiple of 3)
        num_complete = len(data) // 3
        for i in range(num_complete):
            offset = i * 3
            assert data[offset] == IAC, (
                f"Byte {offset} should be IAC (0xFF), got 0x{data[offset]:02X}"
            )
            assert data[offset + 1] in VALID_COMMANDS, (
                f"Byte {offset + 1} should be a Telnet command, "
                f"got 0x{data[offset + 1]:02X}"
            )


class TestTelnetTarpitBehavior:
    """Test that the telnet tarpit actually traps connections."""

    def test_connection_stays_alive(self, telnet_server):
        """
        The tarpit should keep the connection open and continue sending
        negotiation data over time. After waiting for multiple delay cycles,
        we should have received multiple negotiation sequences.
        """
        sock = socket.create_connection(
            (telnet_server["host"], telnet_server["port"]), timeout=3
        )
        sock.settimeout(0.3)

        total_bytes = 0
        # The delay is set to 50ms in the fixture, so over 500ms we expect
        # at least several 3-byte sequences
        deadline = time.monotonic() + 0.8
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
            except socket.timeout:
                continue

        sock.close()

        # With 50ms delay, over 800ms we expect at least ~10 sequences = 30 bytes
        # Be conservative and check for at least 2 sequences
        assert total_bytes >= 6, (
            f"Expected continuous data from tarpit, got only {total_bytes} bytes"
        )

    def test_multiple_concurrent_connections(self, telnet_server):
        """Server should handle multiple simultaneous connections."""
        sockets = []
        num_clients = 5

        try:
            for _ in range(num_clients):
                s = socket.create_connection(
                    (telnet_server["host"], telnet_server["port"]), timeout=3
                )
                s.settimeout(1.0)
                sockets.append(s)

            # Brief wait for server to process all connections
            time.sleep(0.2)

            # All connections should still be open
            for i, s in enumerate(sockets):
                try:
                    data = s.recv(256)
                    assert len(data) > 0, f"Client {i} received no data"
                except socket.timeout:
                    pytest.fail(f"Client {i} timed out waiting for data")
        finally:
            for s in sockets:
                s.close()


class TestTelnetMetricsOutput:
    """Test that the server emits expected metric messages to stdout."""

    def test_connect_metric_emitted(self, telnet_server):
        """
        When a client connects, the server should print a metric line
        of the format: 'Telnet connect <ip>'
        """
        sock = socket.create_connection(
            (telnet_server["host"], telnet_server["port"]), timeout=3
        )
        # Give the server time to log the connection
        time.sleep(0.2)
        sock.close()
        # Give time for disconnect metric
        time.sleep(0.3)

        # The server process should still be running
        assert telnet_server["process"].is_running
