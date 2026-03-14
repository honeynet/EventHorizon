"""
Integration tests for the MQTT tarpit emulator.

The mqtt_pit server implements a fake MQTT broker that:
1. Accepts MQTT CONNECT packets and responds with CONNACK
2. Immediately sends a QoS 2 PUBLISH with fake credentials as bait
3. Traps clients in an infinite PUBREL cycle (QoS 2 handshake never completes)
4. Handles SUBSCRIBE, PUBLISH, PING, and DISCONNECT packets
5. Supports MQTT v3.1, v3.1.1, and v5.0

These tests verify protocol-level behavior by constructing raw MQTT packets
and validating server responses byte-by-byte.
"""

import socket
import struct
import time

import pytest


def build_mqtt_connect(client_id="test-client", version=4, keepalive=60,
                       username=None, password=None):
    """
    Build a raw MQTT CONNECT packet.

    Args:
        client_id: Client identifier string
        version: Protocol level (3=v3.1, 4=v3.1.1, 5=v5.0)
        keepalive: Keep-alive interval in seconds
        username: Optional username
        password: Optional password

    Returns:
        bytes: Complete MQTT CONNECT packet
    """
    # Variable header
    if version == 3:
        protocol_name = b"\x00\x06MQIsdp"
    else:
        protocol_name = b"\x00\x04MQTT"

    protocol_level = bytes([version])

    connect_flags = 0x02  # Clean session
    if username:
        connect_flags |= 0x80
    if password:
        connect_flags |= 0x40
    connect_flags_byte = bytes([connect_flags])

    keepalive_bytes = struct.pack("!H", keepalive)

    # Properties (v5 only)
    properties = b""
    if version == 5:
        properties = b"\x00"  # Properties length = 0

    # Payload
    client_id_bytes = struct.pack("!H", len(client_id)) + client_id.encode()

    payload = client_id_bytes
    if username:
        payload += struct.pack("!H", len(username)) + username.encode()
    if password:
        payload += struct.pack("!H", len(password)) + password.encode()

    variable_header = (
        protocol_name + protocol_level + connect_flags_byte + keepalive_bytes + properties
    )

    remaining = variable_header + payload
    remaining_length = _encode_remaining_length(len(remaining))

    # Fixed header: packet type 1 (CONNECT) = 0x10
    return bytes([0x10]) + remaining_length + remaining


def build_mqtt_pingreq():
    """Build an MQTT PINGREQ packet."""
    return bytes([0xC0, 0x00])


def build_mqtt_disconnect():
    """Build an MQTT DISCONNECT packet."""
    return bytes([0xE0, 0x00])


def build_mqtt_subscribe(topic, qos=0, packet_id=1):
    """
    Build an MQTT SUBSCRIBE packet.

    Args:
        topic: Topic filter string
        qos: Requested QoS level (0, 1, or 2)
        packet_id: Packet identifier

    Returns:
        bytes: Complete MQTT SUBSCRIBE packet
    """
    packet_id_bytes = struct.pack("!H", packet_id)
    topic_bytes = struct.pack("!H", len(topic)) + topic.encode()
    qos_byte = bytes([qos])

    payload = packet_id_bytes + topic_bytes + qos_byte
    remaining_length = _encode_remaining_length(len(payload))

    # Fixed header: packet type 8 (SUBSCRIBE) = 0x82 (with required bit 1)
    return bytes([0x82]) + remaining_length + payload


def build_mqtt_pubrec(packet_id=1234):
    """Build an MQTT PUBREC packet for a given packet ID."""
    return bytes([0x50, 0x02]) + struct.pack("!H", packet_id)


def _encode_remaining_length(length):
    """Encode MQTT remaining length as variable-byte integer."""
    encoded = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 0x80
        encoded.append(byte)
        if length == 0:
            break
    return bytes(encoded)


def recv_all(sock, timeout=2.0):
    """Read all available data from socket within timeout."""
    sock.settimeout(timeout)
    data = b""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        except socket.timeout:
            break
    return data


class TestMqttConnection:
    """Test basic MQTT connection handling."""

    def test_accepts_tcp_connection(self, mqtt_server):
        """MQTT tarpit should accept TCP connections."""
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )
        assert sock.fileno() > 0
        sock.close()

    def test_connack_response_v311(self, mqtt_server):
        """
        Server should respond to a valid MQTT v3.1.1 CONNECT with a CONNACK.
        CONNACK format (v3.1.1):
          Byte 0: 0x20 (CONNACK packet type)
          Byte 1: 0x02 (Remaining length)
          Byte 2: 0x00 (Connect Acknowledge Flags)
          Byte 3: Return code (0x00 = success)
        """
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )
        connect_packet = build_mqtt_connect(version=4)
        sock.sendall(connect_packet)

        data = recv_all(sock, timeout=2.0)
        sock.close()

        assert len(data) >= 4, f"Expected at least 4 bytes for CONNACK, got {len(data)}"

        # Validate CONNACK
        assert data[0] == 0x20, (
            f"First byte should be CONNACK type (0x20), got 0x{data[0]:02X}"
        )
        assert data[1] == 0x02, (
            f"Remaining length should be 2, got {data[1]}"
        )
        assert data[2] == 0x00, (
            f"Connect Acknowledge Flags should be 0, got {data[2]}"
        )
        assert data[3] == 0x00, (
            f"Return code should be 0 (success), got 0x{data[3]:02X}"
        )

    def test_connack_response_v5(self, mqtt_server):
        """
        Server should respond to an MQTT v5.0 CONNECT with a v5 CONNACK.
        V5 CONNACK includes properties (Receive Maximum = 1).
        Format:
          Byte 0: 0x20
          Byte 1: 0x06 (Remaining length)
          Byte 2: 0x00 (Flags)
          Byte 3: Reason code
          Byte 4: 0x03 (Properties length)
          Byte 5: 0x21 (Receive Maximum property ID)
          Bytes 6-7: Receive Maximum value
        """
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )
        connect_packet = build_mqtt_connect(version=5)
        sock.sendall(connect_packet)

        data = recv_all(sock, timeout=2.0)
        sock.close()

        assert len(data) >= 8, (
            f"Expected at least 8 bytes for v5 CONNACK, got {len(data)}"
        )

        # Validate v5 CONNACK structure
        assert data[0] == 0x20, f"Should be CONNACK type, got 0x{data[0]:02X}"
        assert data[1] == 0x06, f"Remaining length should be 6, got {data[1]}"
        assert data[4] == 0x03, f"Properties length should be 3, got {data[4]}"
        assert data[5] == 0x21, (
            f"Property ID should be Receive Maximum (0x21), got 0x{data[5]:02X}"
        )

    def test_connack_response_v31(self, mqtt_server):
        """Server should accept MQTT v3.1 connections (MQIsdp protocol name)."""
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )
        connect_packet = build_mqtt_connect(version=3)
        sock.sendall(connect_packet)

        data = recv_all(sock, timeout=2.0)
        sock.close()

        assert len(data) >= 4, f"Expected CONNACK, got {len(data)} bytes"
        assert data[0] == 0x20, f"Should be CONNACK, got 0x{data[0]:02X}"


class TestMqttTarpitBehavior:
    """Test the tarpit's trapping mechanism."""

    def test_publish_bait_after_connack(self, mqtt_server):
        """
        After CONNACK, the server should immediately send a QoS 2 PUBLISH
        with fake credentials to bait the client.

        PUBLISH fixed header for QoS 2: 0x34 (0011 0100)
          - Type = 3 (PUBLISH)
          - QoS = 2 (bits 2-1 = 10)
        """
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )
        connect_packet = build_mqtt_connect(version=4)
        sock.sendall(connect_packet)

        data = recv_all(sock, timeout=2.0)
        sock.close()

        # Should receive CONNACK (4 bytes) + PUBLISH
        assert len(data) > 4, (
            f"Expected CONNACK + PUBLISH, only got {len(data)} bytes"
        )

        # The PUBLISH packet starts after the 4-byte CONNACK
        publish_start = 4
        assert data[publish_start] == 0x34, (
            f"Expected QoS 2 PUBLISH (0x34) after CONNACK, "
            f"got 0x{data[publish_start]:02X}"
        )

    def test_publish_contains_credential_bait(self, mqtt_server):
        """
        The bait PUBLISH should contain the topic '$SYS/credentials'
        and fake credentials in the payload.
        """
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )
        connect_packet = build_mqtt_connect(version=4)
        sock.sendall(connect_packet)

        data = recv_all(sock, timeout=2.0)
        sock.close()

        # After CONNACK (4 bytes), the PUBLISH contains the topic
        publish_data = data[4:]
        assert b"$SYS/credentials" in publish_data, (
            "PUBLISH bait should contain '$SYS/credentials' topic"
        )
        assert b"admin" in publish_data, (
            "PUBLISH bait should contain fake admin credentials"
        )

    def test_credentials_captured(self, mqtt_server):
        """
        When CONNECT includes username/password, the server should
        accept them and still respond with CONNACK.
        """
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )
        connect_packet = build_mqtt_connect(
            version=4, username="attacker", password="p@ssw0rd"
        )
        sock.sendall(connect_packet)

        data = recv_all(sock, timeout=2.0)
        sock.close()

        # Should still get a valid CONNACK
        assert len(data) >= 4
        assert data[0] == 0x20
        assert data[3] == 0x00  # Success return code

    def test_pingresp_on_pingreq(self, mqtt_server):
        """
        Server should respond to PINGREQ with PINGRESP (0xD0 0x00).
        This keeps alive the illusion of a real broker.
        """
        sock = socket.create_connection(
            (mqtt_server["host"], mqtt_server["port"]), timeout=3
        )

        # First establish connection
        sock.sendall(build_mqtt_connect(version=4))
        recv_all(sock, timeout=1.0)  # Consume CONNACK + PUBLISH

        # Send PINGREQ
        sock.sendall(build_mqtt_pingreq())
        data = recv_all(sock, timeout=2.0)
        sock.close()

        # Look for PINGRESP in the response
        # PINGRESP = 0xD0 0x00
        assert b"\xd0\x00" in data, (
            f"Expected PINGRESP (0xD0 0x00) in response, got {data.hex()}"
        )


class TestMqttMultipleClients:
    """Test that the MQTT tarpit handles multiple concurrent clients."""

    def test_concurrent_mqtt_clients(self, mqtt_server):
        """Multiple clients should all receive valid CONNACKs."""
        sockets = []
        num_clients = 3

        try:
            for i in range(num_clients):
                s = socket.create_connection(
                    (mqtt_server["host"], mqtt_server["port"]), timeout=3
                )
                sockets.append(s)

            # Send CONNECT from all clients
            for s in sockets:
                s.sendall(build_mqtt_connect(
                    client_id=f"client-{id(s)}", version=4
                ))

            # All should receive CONNACK
            for i, s in enumerate(sockets):
                data = recv_all(s, timeout=2.0)
                assert len(data) >= 4, f"Client {i} got no CONNACK"
                assert data[0] == 0x20, (
                    f"Client {i}: expected CONNACK, got 0x{data[0]:02X}"
                )
        finally:
            for s in sockets:
                s.close()
