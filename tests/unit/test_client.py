"""
Unit tests for NVMeoFClient class

Tests the main client functionality including connection management,
command sending, and error handling.
"""

import unittest
from unittest.mock import Mock, patch
import socket
import struct

from nvmeof_client.client import NVMeoFClient
from nvmeof_client.exceptions import (
    NVMeoFConnectionError,
    NVMeoFTimeoutError,
    CommandError,
)
from nvmeof_client.protocol import PDUType, NVMeOpcode
from nvmeof_client.parsers.response import ResponseParser


class TestNVMeoFClient(unittest.TestCase):
    """Test cases for NVMeoFClient class."""

    def setUp(self):
        """Set up test fixtures."""
        self.host = "192.168.1.100"
        self.port = 4420
        self.timeout = 10.0
        self.client = NVMeoFClient(self.host, port=self.port, timeout=self.timeout)

    def test_init(self):
        """Test client initialization."""
        self.assertEqual(self.client.host, self.host)
        self.assertEqual(self.client.port, self.port)
        self.assertEqual(self.client.timeout, self.timeout)
        self.assertFalse(self.client.is_connected)
        self.assertIsNone(self.client._socket)

    @patch('socket.socket')
    def test_connect_success(self, mock_socket_class):
        """Test successful connection establishment."""
        mock_socket = Mock()
        mock_socket_class.return_value = mock_socket

        # Mock all responses needed during connection:
        # 1. ICRESP (header + data)
        icresp_header = struct.pack('<BBBBI', PDUType.ICRESP, 0, 128, 0, 128)
        icresp_data = b'\x00' * 120

        # 2. Fabric Connect response (header + data)
        connect_rsp_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        connect_rsp_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 1, 0)  # Success response

        # 3. Property Set CC configure response (header + data)
        prop_set1_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_set1_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 2, 0)  # Success response

        # 4. Property Set CC enable response (header + data)
        prop_set2_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_set2_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 3, 0)  # Success response

        # 5. Property Get CSTS response (header + data)
        prop_get_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_get_data = struct.pack('<LLHHHH', 1, 0, 0, 0, 4, 0)  # Ready status

        # 6. Property Get VS response (header + data)
        prop_get_vs_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_get_vs_data = struct.pack('<LLHHHH', 0x010300, 0, 0, 0, 5, 0)  # Version 1.3

        # Combine all responses into a single stream that recv will consume byte by byte
        all_data = (
            icresp_header + icresp_data +
            connect_rsp_header + connect_rsp_data +
            prop_set1_header + prop_set1_data +
            prop_set2_header + prop_set2_data +
            prop_get_header + prop_get_data +
            prop_get_vs_header + prop_get_vs_data
        )

        data_position = [0]

        def mock_recv(size):
            if data_position[0] < len(all_data):
                # Return up to 'size' bytes from current position
                start = data_position[0]
                end = min(start + size, len(all_data))
                chunk = all_data[start:end]
                data_position[0] = end
                return chunk
            return b''  # No more data

        mock_socket.recv.side_effect = mock_recv

        self.client.connect()

        mock_socket.connect.assert_called_once_with((self.host, self.port))
        mock_socket.settimeout.assert_called_once_with(self.timeout)
        self.assertTrue(self.client.is_connected)

    @patch('socket.socket')
    def test_connect_timeout(self, mock_socket_class):
        """Test connection timeout handling."""
        mock_socket = Mock()
        mock_socket_class.return_value = mock_socket
        mock_socket.connect.side_effect = socket.timeout()

        with self.assertRaises(NVMeoFTimeoutError):
            self.client.connect()

        self.assertFalse(self.client.is_connected)
        mock_socket.close.assert_called_once()

    @patch('socket.socket')
    def test_connect_socket_error(self, mock_socket_class):
        """Test connection socket error handling."""
        mock_socket = Mock()
        mock_socket_class.return_value = mock_socket
        mock_socket.connect.side_effect = socket.error("Connection refused")

        with self.assertRaises(NVMeoFConnectionError):
            self.client.connect()

        self.assertFalse(self.client.is_connected)

    def test_connect_already_connected(self):
        """Test connecting when already connected."""
        self.client._connected = True

        with self.assertRaises(NVMeoFConnectionError):
            self.client.connect()

    @patch('socket.socket')
    def test_disconnect(self, mock_socket_class):
        """Test disconnection."""
        mock_socket = Mock()
        self.client._socket = mock_socket
        self.client._connected = True

        # Mock the cleanup_io_queues method to avoid issues
        self.client.cleanup_io_queues = Mock()

        self.client.disconnect()

        # Should have sent termination PDU (sendall called at least once)
        mock_socket.sendall.assert_called()
        mock_socket.close.assert_called_once()
        self.assertFalse(self.client.is_connected)
        self.assertIsNone(self.client._socket)

    def test_disconnect_not_connected(self):
        """Test disconnection when not connected."""
        self.client.disconnect()  # Should not raise exception
        self.assertFalse(self.client.is_connected)

    @patch('socket.socket')
    def test_send_command_success(self, mock_socket_class):
        """Test successful command sending."""
        mock_socket = Mock()
        self.client._socket = mock_socket
        self.client._connected = True

        # Mock response PDU
        response_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        response_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 1, 0)  # Success response
        mock_socket.recv.side_effect = [response_header, response_data]

        result = self.client.send_command(NVMeOpcode.IDENTIFY)

        self.assertEqual(result['command_id'], 1)
        self.assertEqual(result['status'], 0)
        mock_socket.sendall.assert_called()

    def test_send_command_not_connected(self):
        """Test sending command when not connected."""
        with self.assertRaises(NVMeoFConnectionError):
            self.client.send_command(NVMeOpcode.IDENTIFY)

    @patch('socket.socket')
    def test_send_command_error_response(self, mock_socket_class):
        """Test command with error response."""
        mock_socket = Mock()
        self.client._socket = mock_socket
        self.client._connected = True

        # Mock error response PDU
        response_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        response_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 1, 0x04)  # Error status (0x02 << 1)
        mock_socket.recv.side_effect = [response_header, response_data]

        with self.assertRaises(CommandError) as cm:
            self.client.send_command(NVMeOpcode.IDENTIFY)

        self.assertEqual(cm.exception.status_code, 0x02)
        self.assertEqual(cm.exception.command_id, 1)

    @patch('socket.socket')
    def test_send_command_timeout(self, mock_socket_class):
        """Test command timeout."""
        mock_socket = Mock()
        self.client._socket = mock_socket
        self.client._connected = True
        mock_socket.recv.side_effect = socket.timeout()

        with self.assertRaises(NVMeoFTimeoutError):
            self.client.send_command(NVMeOpcode.IDENTIFY, timeout=1.0)

    @patch('socket.socket')
    def test_identify_controller(self, mock_socket_class):
        """Test identify controller method."""
        mock_socket = Mock()
        mock_socket_class.return_value = mock_socket

        # Mock responses for identify controller command
        response_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        response_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 1, 0)  # Success response
        mock_socket.recv.side_effect = [response_header, response_data]

        # Test that identify_controller method exists and can be called
        with self.assertRaises(NVMeoFConnectionError):
            # Will fail because we're not actually connected, but proves method exists
            self.client.identify_controller()

    def test_get_next_command_id(self):
        """Test command ID generation."""
        id1 = self.client._get_next_command_id()
        id2 = self.client._get_next_command_id()

        self.assertEqual(id1, 1)
        self.assertEqual(id2, 2)

    def test_get_next_command_id_wraparound(self):
        """Test admin command ID wraparound."""
        self.client._admin_command_id_counter = 0xFFFF
        id1 = self.client._get_next_command_id()
        id2 = self.client._get_next_command_id()

        self.assertEqual(id1, 0xFFFF)
        self.assertEqual(id2, 0)

    @patch('socket.socket')
    def test_context_manager(self, mock_socket_class):
        """Test context manager interface."""
        mock_socket = Mock()
        mock_socket_class.return_value = mock_socket

        # Mock all responses needed during connection:
        # 1. ICRESP (header + data)
        icresp_header = struct.pack('<BBBBI', PDUType.ICRESP, 0, 128, 0, 128)
        icresp_data = b'\x00' * 120

        # 2. Fabric Connect response (header + data)
        connect_rsp_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        connect_rsp_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 1, 0)  # Success response

        # 3. Property Set CC configure response (header + data)
        prop_set1_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_set1_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 2, 0)  # Success response

        # 4. Property Set CC enable response (header + data)
        prop_set2_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_set2_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 3, 0)  # Success response

        # 5. Property Get CSTS response (header + data)
        prop_get_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_get_data = struct.pack('<LLHHHH', 1, 0, 0, 0, 4, 0)  # Ready status

        # 6. Property Get VS response (header + data)
        prop_get_vs_header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
        prop_get_vs_data = struct.pack('<LLHHHH', 0x010300, 0, 0, 0, 5, 0)  # Version 1.3

        # Combine all responses into a single stream that recv will consume byte by byte
        all_data = (
            icresp_header + icresp_data +
            connect_rsp_header + connect_rsp_data +
            prop_set1_header + prop_set1_data +
            prop_set2_header + prop_set2_data +
            prop_get_header + prop_get_data +
            prop_get_vs_header + prop_get_vs_data
        )

        data_position = [0]

        def mock_recv(size):
            if data_position[0] < len(all_data):
                # Return up to 'size' bytes from current position
                start = data_position[0]
                end = min(start + size, len(all_data))
                chunk = all_data[start:end]
                data_position[0] = end
                return chunk
            return b''  # No more data

        mock_socket.recv.side_effect = mock_recv

        with self.client:
            self.assertTrue(self.client.is_connected)

        self.assertFalse(self.client.is_connected)
        mock_socket.close.assert_called()


class TestProtocolParsing(unittest.TestCase):
    """Test protocol parsing and building functions."""

    def setUp(self):
        self.client = NVMeoFClient("localhost")

    def test_parse_response_success(self):
        """Test parsing successful response."""
        # Build response data
        response_data = struct.pack('<LLHHHH', 0x12345678, 0x9ABCDEF0, 0, 0, 123, 0)

        result = ResponseParser.parse_response(response_data, 123)

        self.assertEqual(result['command_id'], 123)
        self.assertEqual(result['status'], 0)
        self.assertEqual(result['dw0'], 0x12345678)
        self.assertEqual(result['dw1'], 0x9ABCDEF0)

    def test_parse_response_command_id_mismatch(self):
        """Test response with wrong command ID."""
        response_data = struct.pack('<LLHHHH', 0, 0, 0, 0, 456, 0)

        with self.assertRaises(ValueError):
            ResponseParser.parse_response(response_data, 123)

    def test_parse_response_invalid_length(self):
        """Test response with invalid length."""
        with self.assertRaises(ValueError):
            ResponseParser.parse_response(b'short', 123)


if __name__ == '__main__':
    unittest.main()
