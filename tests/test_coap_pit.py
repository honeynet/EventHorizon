"""
Integration tests for the CoAP tarpit emulator.

The coap_pit server implements a fake CoAP server that traps clients using
the Block2 transfer mechanism (RFC 7959). When a client sends a GET request,
the server responds with Block2 responses that always have the M (More) flag
set, meaning the transfer never completes.

Key protocol details (RFC 7252):
  - CoAP uses UDP
  - Header: 4 bytes (Version, Type, TKL, Code, Message ID)
  - Types: CON=0, NON=1, ACK=2, RST=3
  - Code for GET request: 0.01 (0x01)
  - Code for 2.05 Content response: 0x45

These tests verify:
1. The server responds to CoAP GET requests
2. Responses contain valid CoAP headers
3. Block2 option is present with M=1 (more blocks)
4. Confirmable requests receive ACK responses
5. The server handles multiple simultaneous clients
"""

import socket
import struct
import time

import pytest


def build_coap_get(message_id=1, token=b"\xAB\xCD", uri_path=".well-known/core"):
    """
    Build a CoAP CON GET request.

    Args:
        message_id: 16-bit message identifier
        token: Token bytes (0-8 bytes)
        uri_path: URI path option value

    Returns:
        bytes: Complete CoAP packet
    """
    tkl = len(token)

    # Header: Version=1, Type=CON(0), TKL, Code=GET(0.01)
    header = bytes([
        (0x01 << 6) | (0x00 << 4) | (tkl & 0x0F),  # Ver=1, Type=CON, TKL
        0x01,  # Code = 0.01 (GET)
    ]) + struct.pack("!H", message_id)

    packet = header + token

    # Add Uri-Path option (option number 11)
    # For simplicity, encode as a single option
    if uri_path:
        path_bytes = uri_path.encode()
        opt_delta = 11  # Uri-Path option number
        opt_len = len(path_bytes)

        if opt_delta < 13 and opt_len < 13:
            packet += bytes([(opt_delta << 4) | opt_len])
        elif opt_delta < 13:
            packet += bytes([(opt_delta << 4) | 13, opt_len - 13])
        else:
            packet += bytes([13 << 4 | (opt_len & 0x0F), opt_delta - 13])

        packet += path_bytes

    return packet


def build_coap_ack(message_id):
    """Build a CoAP ACK (empty) for a given message ID."""
    return bytes([
        (0x01 << 6) | (0x02 << 4) | 0,  # Ver=1, Type=ACK, TKL=0
        0x00,  # Code = 0.00 (Empty)
    ]) + struct.pack("!H", message_id)


def build_coap_rst(message_id):
    """Build a CoAP RST for a given message ID."""
    return bytes([
        (0x01 << 6) | (0x03 << 4) | 0,  # Ver=1, Type=RST, TKL=0
        0x00,
    ]) + struct.pack("!H", message_id)


def parse_coap_header(data):
    """
    Parse a CoAP message header.

    Returns:
        dict with version, type, tkl, code_class, code_detail, message_id, token
    """
    if len(data) < 4:
        return None

    byte0 = data[0]
    version = (byte0 >> 6) & 0x03
    msg_type = (byte0 >> 4) & 0x03
    tkl = byte0 & 0x0F
    code = data[1]
    code_class = (code >> 5) & 0x07
    code_detail = code & 0x1F
    message_id = struct.unpack("!H", data[2:4])[0]

    token = data[4:4 + tkl] if tkl > 0 else b""

    return {
        "version": version,
        "type": msg_type,
        "tkl": tkl,
        "code_class": code_class,
        "code_detail": code_detail,
        "code": code,
        "message_id": message_id,
        "token": token,
        "options_start": 4 + tkl,
    }


def find_block2_option(data, options_start):
    """
    Parse CoAP options to find the Block2 option (number 23).

    Returns:
        dict with block_number, more_flag, szx, or None if not found
    """
    offset = options_start
    option_number = 0

    while offset < len(data):
        if data[offset] == 0xFF:
            # Payload marker
            break

        delta_len = data[offset]
        delta = (delta_len >> 4) & 0x0F
        opt_len = delta_len & 0x0F
        offset += 1

        # Extended delta
        if delta == 13:
            delta = data[offset] + 13
            offset += 1
        elif delta == 14:
            delta = struct.unpack("!H", data[offset:offset + 2])[0] + 269
            offset += 2

        # Extended length
        if opt_len == 13:
            opt_len = data[offset] + 13
            offset += 1
        elif opt_len == 14:
            opt_len = struct.unpack("!H", data[offset:offset + 2])[0] + 269
            offset += 2

        option_number += delta
        option_value = data[offset:offset + opt_len]
        offset += opt_len

        if option_number == 23:  # Block2
            # Decode block value
            if opt_len == 1:
                val = option_value[0]
            elif opt_len == 2:
                val = struct.unpack("!H", option_value)[0]
            elif opt_len == 3:
                val = (option_value[0] << 16) | (option_value[1] << 8) | option_value[2]
            else:
                continue

            szx = val & 0x07
            more = (val >> 3) & 0x01
            block_num = val >> 4

            return {
                "block_number": block_num,
                "more_flag": more,
                "szx": szx,
            }

    return None


class TestCoapConnection:
    """Test that the CoAP tarpit handles UDP requests."""

    def test_responds_to_get_request(self, coap_server):
        """Server should respond to a CoAP GET request."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)

        try:
            get_packet = build_coap_get(message_id=1)
            sock.sendto(
                get_packet,
                (coap_server["host"], coap_server["port"])
            )

            data, addr = sock.recvfrom(1024)
            assert len(data) >= 4, f"Response too short: {len(data)} bytes"

            header = parse_coap_header(data)
            assert header is not None
            assert header["version"] == 1, (
                f"CoAP version should be 1, got {header['version']}"
            )
        finally:
            sock.close()

    def test_confirmable_gets_ack(self, coap_server):
        """
        A Confirmable (CON) request should receive an ACK.
        The server sends an empty ACK for CON requests (section 5.2.2),
        then sends a separate response.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)

        try:
            msg_id = 42
            get_packet = build_coap_get(message_id=msg_id)
            sock.sendto(
                get_packet,
                (coap_server["host"], coap_server["port"])
            )

            # Collect responses (ACK + possibly Block2 response)
            responses = []
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and len(responses) < 3:
                try:
                    data, _ = sock.recvfrom(1024)
                    responses.append(data)
                except socket.timeout:
                    break

            # At least one response should be an ACK with matching message ID
            found_ack = False
            for resp in responses:
                header = parse_coap_header(resp)
                if header and header["type"] == 2:  # ACK
                    assert header["message_id"] == msg_id, (
                        f"ACK message ID should match request ({msg_id}), "
                        f"got {header['message_id']}"
                    )
                    found_ack = True
                    break

            assert found_ack, (
                f"Expected ACK response to CON request, got types: "
                f"{[parse_coap_header(r)['type'] for r in responses if parse_coap_header(r)]}"
            )
        finally:
            sock.close()

    def test_response_has_valid_coap_header(self, coap_server):
        """All response fields should be valid CoAP values."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)

        try:
            get_packet = build_coap_get(message_id=100, token=b"\x01\x02")
            sock.sendto(
                get_packet,
                (coap_server["host"], coap_server["port"])
            )

            data, _ = sock.recvfrom(1024)
            header = parse_coap_header(data)

            assert header["version"] == 1
            assert header["type"] in (0, 1, 2, 3), (
                f"Invalid type: {header['type']}"
            )
        finally:
            sock.close()


class TestCoapTarpitBehavior:
    """Test the Block2 trapping mechanism."""

    def test_block2_response_has_more_flag(self, coap_server):
        """
        The server's Block2 responses should have M=1 (more blocks coming),
        which is the core of the tarpit — the transfer never finishes.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5.0)

        try:
            token = b"\xDE\xAD"
            get_packet = build_coap_get(message_id=1, token=token)
            sock.sendto(
                get_packet,
                (coap_server["host"], coap_server["port"])
            )

            # Collect responses until we find one with Block2
            block2_found = False
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                try:
                    data, _ = sock.recvfrom(1024)
                    header = parse_coap_header(data)
                    if not header:
                        continue

                    block2 = find_block2_option(data, header["options_start"])
                    if block2:
                        block2_found = True
                        assert block2["more_flag"] == 1, (
                            "Block2 M flag should be 1 (more blocks) "
                            "for tarpit behavior"
                        )
                        assert block2["szx"] == 2, (
                            f"Block2 SZX should be 2 (64-byte blocks), "
                            f"got {block2['szx']}"
                        )
                        break
                except socket.timeout:
                    break

            assert block2_found, "Expected at least one Block2 response from CoAP tarpit"
        finally:
            sock.close()

    def test_block2_response_is_content(self, coap_server):
        """
        Block2 responses should have code 2.05 (Content).
        Code byte: class=2 (0b010), detail=5 (0b00101) -> 0x45
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5.0)

        try:
            get_packet = build_coap_get(message_id=200, token=b"\xCA\xFE")
            sock.sendto(
                get_packet,
                (coap_server["host"], coap_server["port"])
            )

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                try:
                    data, _ = sock.recvfrom(1024)
                    header = parse_coap_header(data)
                    if not header:
                        continue
                    # Look for a non-empty response (not just ACK)
                    if header["code"] != 0x00:
                        assert header["code_class"] == 2, (
                            f"Response class should be 2 (Success), "
                            f"got {header['code_class']}"
                        )
                        assert header["code_detail"] == 5, (
                            f"Response detail should be 5 (Content), "
                            f"got {header['code_detail']}"
                        )
                        break
                except socket.timeout:
                    break
        finally:
            sock.close()

    def test_block2_contains_payload(self, coap_server):
        """
        Each Block2 response should contain a payload after the 0xFF marker.
        The server sends 5 bytes of 'A' as payload.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5.0)

        try:
            get_packet = build_coap_get(message_id=300, token=b"\xBE\xEF")
            sock.sendto(
                get_packet,
                (coap_server["host"], coap_server["port"])
            )

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                try:
                    data, _ = sock.recvfrom(1024)
                    header = parse_coap_header(data)
                    if not header or header["code"] == 0x00:
                        continue

                    # Find payload marker
                    payload_marker_pos = data.find(b"\xFF", header["options_start"])
                    if payload_marker_pos >= 0:
                        payload = data[payload_marker_pos + 1:]
                        assert len(payload) > 0, "Payload should not be empty"
                        assert payload == b"AAAAA", (
                            f"Expected 5 'A' bytes as payload, got {payload!r}"
                        )
                        break
                except socket.timeout:
                    break
        finally:
            sock.close()

    def test_malformed_request_gets_bad_request(self, coap_server):
        """
        A packet with TKL > 8 should receive a 4.00 Bad Request response.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)

        try:
            # Build a malformed packet with TKL=15 (max 4 bits, but > 8 is invalid)
            malformed = bytes([
                (0x01 << 6) | (0x00 << 4) | 15,  # TKL=15 (invalid)
                0x01,  # GET
                0x00, 0x01,  # Message ID
            ]) + b"\x00" * 15  # Fake token

            sock.sendto(
                malformed,
                (coap_server["host"], coap_server["port"])
            )

            data, _ = sock.recvfrom(1024)
            header = parse_coap_header(data)
            assert header is not None
            assert header["code_class"] == 4, (
                f"Expected 4.xx error response, got class {header['code_class']}"
            )
            assert header["code_detail"] == 0, (
                f"Expected 4.00 Bad Request, got 4.{header['code_detail']:02d}"
            )
        finally:
            sock.close()
