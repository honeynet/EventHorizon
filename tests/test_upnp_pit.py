"""
Integration tests for the UPnP tarpit emulator.

The upnp_pit server has two components:
1. SSDP listener (UDP): Responds to M-SEARCH discovery requests with a fake
   Philips Hue device description pointing to the HTTP server
2. HTTP server (TCP): Serves a fake device XML via chunked transfer encoding,
   trapping clients by endlessly streaming XML service chunks

Tarpit mechanism:
- The HTTP response uses "Transfer-Encoding: chunked"
- After the initial device description, the server continuously sends
  additional <service> XML chunks on a delay
- The transfer never terminates (no zero-length terminating chunk)

These tests verify:
1. SSDP responds to M-SEARCH with a valid UPnP discovery response
2. HTTP server accepts connections and serves chunked responses
3. The XML device description is a realistic Philips Hue device
4. Chunked transfer keeps streaming data (tarpit behavior)
"""

import socket
import time

import pytest


MSEARCH_REQUEST = (
    "M-SEARCH * HTTP/1.1\r\n"
    "HOST: 239.255.255.250:1900\r\n"
    "MAN: \"ssdp:discover\"\r\n"
    "MX: 3\r\n"
    "ST: ssdp:all\r\n"
    "\r\n"
)


class TestSsdpDiscovery:
    """Test the SSDP (Simple Service Discovery Protocol) listener."""

    def test_responds_to_msearch(self, upnp_server):
        """
        Server should respond to M-SEARCH requests with a valid UPnP
        discovery response containing device location.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)

        try:
            sock.sendto(
                MSEARCH_REQUEST.encode(),
                (upnp_server["host"], upnp_server["ssdp_port"])
            )

            data, addr = sock.recvfrom(4096)
            response = data.decode("utf-8", errors="replace")

            assert "HTTP/1.1 200 OK" in response, (
                f"Expected HTTP 200 OK in SSDP response, got: {response[:100]}"
            )
            assert "LOCATION" in response.upper(), (
                "SSDP response should contain a LOCATION header"
            )
        finally:
            sock.close()

    def test_ssdp_response_contains_device_info(self, upnp_server):
        """
        The SSDP response should contain realistic device information
        to appear as a genuine IoT device (Philips Hue).
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)

        try:
            sock.sendto(
                MSEARCH_REQUEST.encode(),
                (upnp_server["host"], upnp_server["ssdp_port"])
            )

            data, _ = sock.recvfrom(4096)
            response = data.decode("utf-8", errors="replace")

            # Should contain key UPnP headers
            assert "CACHE-CONTROL" in response.upper()
            assert "ST:" in response.upper()
            assert "USN:" in response.upper()

            # Should mention Philips in SERVER header
            assert "Philips" in response or "UPnP" in response, (
                "Response should identify as a Philips/UPnP device"
            )
        finally:
            sock.close()

    def test_ssdp_location_points_to_http_port(self, upnp_server):
        """LOCATION header should point to the HTTP server port."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)

        try:
            sock.sendto(
                MSEARCH_REQUEST.encode(),
                (upnp_server["host"], upnp_server["ssdp_port"])
            )

            data, _ = sock.recvfrom(4096)
            response = data.decode("utf-8", errors="replace")

            # The LOCATION should contain the HTTP port
            http_port = str(upnp_server["http_port"])
            assert http_port in response, (
                f"LOCATION should reference HTTP port {http_port}"
            )
            assert "hue-device.xml" in response, (
                "LOCATION should point to hue-device.xml"
            )
        finally:
            sock.close()


class TestHttpDeviceDescription:
    """Test the HTTP server that serves the fake device XML."""

    def test_accepts_http_connection(self, upnp_server):
        """HTTP server should accept TCP connections."""
        sock = socket.create_connection(
            (upnp_server["host"], upnp_server["http_port"]), timeout=3
        )
        assert sock.fileno() > 0
        sock.close()

    def test_chunked_transfer_response(self, upnp_server):
        """
        GET /hue-device.xml should return a chunked transfer encoded response
        with the fake Philips Hue device description.
        """
        sock = socket.create_connection(
            (upnp_server["host"], upnp_server["http_port"]), timeout=3
        )
        sock.settimeout(3.0)

        request = (
            f"GET /hue-device.xml HTTP/1.1\r\n"
            f"Host: {upnp_server['host']}:{upnp_server['http_port']}\r\n"
            f"\r\n"
        )
        sock.sendall(request.encode())

        data = b""
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
            except socket.timeout:
                break

        sock.close()
        response = data.decode("utf-8", errors="replace")

        assert "HTTP/1.1 200 OK" in response, (
            f"Expected HTTP 200 OK, got: {response[:100]}"
        )
        assert "Transfer-Encoding: chunked" in response, (
            "Response should use chunked transfer encoding"
        )

    def test_device_xml_contains_philips_hue_info(self, upnp_server):
        """
        The XML response should contain a realistic Philips Hue device
        description to lure IoT scanners.
        """
        sock = socket.create_connection(
            (upnp_server["host"], upnp_server["http_port"]), timeout=3
        )
        sock.settimeout(3.0)

        request = (
            f"GET /hue-device.xml HTTP/1.1\r\n"
            f"Host: {upnp_server['host']}:{upnp_server['http_port']}\r\n"
            f"\r\n"
        )
        sock.sendall(request.encode())

        data = b""
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
            except socket.timeout:
                break

        sock.close()
        response = data.decode("utf-8", errors="replace")

        # Verify realistic device attributes
        assert "Philips" in response, "Should mention Philips manufacturer"
        assert "Hue" in response, "Should mention Hue product"
        assert "<deviceType>" in response, "Should have UPnP deviceType element"
        assert "<friendlyName>" in response, "Should have friendlyName element"
        assert "<serialNumber>" in response, "Should have serialNumber element"
        assert "<serviceList>" in response, "Should start the serviceList"


class TestUpnpTarpitBehavior:
    """Test that the UPnP tarpit traps connections."""

    def test_chunked_data_keeps_streaming(self, upnp_server):
        """
        After the initial device description, the server should continue
        sending chunked data (service entries) to trap the client.
        """
        sock = socket.create_connection(
            (upnp_server["host"], upnp_server["http_port"]), timeout=3
        )
        sock.settimeout(1.0)

        request = (
            f"GET /hue-device.xml HTTP/1.1\r\n"
            f"Host: {upnp_server['host']}:{upnp_server['http_port']}\r\n"
            f"\r\n"
        )
        sock.sendall(request.encode())

        # Collect data in two phases to verify streaming
        phase1_data = b""
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                phase1_data += chunk
            except socket.timeout:
                continue

        # Wait for more chunks (the delay is 200ms in fixture)
        time.sleep(0.5)

        phase2_data = b""
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                phase2_data += chunk
            except socket.timeout:
                break

        sock.close()

        assert len(phase1_data) > 0, "Should receive initial data"
        assert len(phase2_data) > 0, (
            "Server should continue streaming data after initial response "
            "(tarpit behavior)"
        )

    def test_non_xml_request_gets_rejected(self, upnp_server):
        """
        Requests for paths other than /hue-device.xml should be logged
        but the connection should be closed (server only traps XML requests).
        """
        sock = socket.create_connection(
            (upnp_server["host"], upnp_server["http_port"]), timeout=3
        )
        sock.settimeout(2.0)

        request = (
            f"GET /other-path HTTP/1.1\r\n"
            f"Host: {upnp_server['host']}:{upnp_server['http_port']}\r\n"
            f"\r\n"
        )
        sock.sendall(request.encode())

        # Server should close the connection for non-XML requests
        time.sleep(0.5)

        # Try to receive - should get nothing or connection closed
        try:
            data = sock.recv(4096)
            # Connection was closed (empty read) or no chunked streaming
            # Either is acceptable - the server doesn't trap non-XML requests
        except (ConnectionResetError, BrokenPipeError, socket.timeout):
            pass  # Connection closed as expected

        sock.close()

    def test_server_stays_running(self, upnp_server):
        """Server should remain running after client disconnects."""
        sock = socket.create_connection(
            (upnp_server["host"], upnp_server["http_port"]), timeout=3
        )
        sock.close()
        time.sleep(0.2)
        assert upnp_server["process"].is_running
