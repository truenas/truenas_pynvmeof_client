"""
Test Helper Functions

Common utilities and helpers used across multiple test modules.
"""

import os
import socket
import struct
from unittest.mock import (
    MagicMock,
    Mock,
)
from nvmeof_client.client import NVMeoFClient
from nvmeof_client.models import (
    ReservationAction,
    ReservationType,
)


def get_test_target_config():
    """Get test target configuration from environment variables."""
    return {
        'host': os.getenv('NVMEOF_TARGET_HOST', 'localhost'),
        'port': int(os.getenv('NVMEOF_TARGET_PORT', '4420')),
        'nqn': os.getenv('NVMEOF_TARGET_NQN', 'nqn.2019-05.io.spdk:target'),
        'transport': os.getenv('NVMEOF_TARGET_TRANSPORT', 'tcp'),
        'timeout': float(os.getenv('NVMEOF_TARGET_TIMEOUT', '10.0'))
    }


def should_skip_integration_tests():
    """Check if integration tests should be skipped."""
    return os.getenv('NVMEOF_SKIP_INTEGRATION', '').lower() in ('1', 'true', 'yes')


def create_mock_client(connected: bool = True, discovery_mode: bool = False):
    """Create a mock NVMeoFClient for testing."""
    client = Mock(spec=NVMeoFClient)
    client._connected = connected
    client._is_discovery_subsystem = discovery_mode
    client._command_id_counter = 0

    def mock_get_next_command_id():
        client._command_id_counter += 1
        return client._command_id_counter & 0xFFFF

    client._get_next_command_id = mock_get_next_command_id
    return client


def create_mock_socket_with_responses(responses: list[bytes]):
    """Create a mock socket that returns the given responses in sequence."""
    mock_socket = MagicMock()
    mock_socket.recv.side_effect = responses
    mock_socket.connect.return_value = None
    mock_socket.settimeout.return_value = None
    mock_socket.sendall.return_value = None
    mock_socket.close.return_value = None
    return mock_socket


def assert_command_structure(test_case, command_bytes: bytes, expected_opcode: int,
                             expected_nsid: int = 0, expected_size: int = 64):
    """Assert that a command has the expected structure."""
    test_case.assertEqual(len(command_bytes), expected_size)

    # Check opcode (byte 0)
    test_case.assertEqual(command_bytes[0], expected_opcode)

    # Check namespace ID (bytes 4-7)
    nsid = struct.unpack('<L', command_bytes[4:8])[0]
    test_case.assertEqual(nsid, expected_nsid)


def assert_reservation_data_structure(test_case, data_bytes: bytes,
                                      expected_key: int, expected_new_key: int = 0):
    """Assert that reservation data has the expected structure."""
    test_case.assertEqual(len(data_bytes), 16)  # Reservation data is 16 bytes

    key, new_key = struct.unpack('<QQ', data_bytes)
    test_case.assertEqual(key, expected_key)
    test_case.assertEqual(new_key, expected_new_key)


def check_target_availability(host: str = 'localhost', port: int = 4420, timeout: float = 5.0) -> bool:
    """Check if an NVMe-oF target is available for testing."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


class MockNVMeoFTarget:
    """Mock NVMe-oF target for integration-like tests without requiring real hardware."""

    def __init__(self, host: str = 'localhost', port: int = 4420):
        self.host = host
        self.port = port
        self.server_socket = None
        self.client_socket = None
        self.is_running = False

    def start(self):
        """Start the mock target server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)
        self.is_running = True

    def stop(self):
        """Stop the mock target server."""
        if self.server_socket:
            self.server_socket.close()
        if self.client_socket:
            self.client_socket.close()
        self.is_running = False

    def accept_connection(self):
        """Accept a client connection."""
        if not self.is_running:
            raise RuntimeError("Target not running")
        self.client_socket, addr = self.server_socket.accept()
        return addr

    def send_response(self, data: bytes):
        """Send a response to the connected client."""
        if not self.client_socket:
            raise RuntimeError("No client connected")
        self.client_socket.sendall(data)

    def receive_data(self, size: int) -> bytes:
        """Receive data from the connected client."""
        if not self.client_socket:
            raise RuntimeError("No client connected")
        return self.client_socket.recv(size)


def reservation_test_scenario(test_case, client_mock, nsid: int = 1,
                              reservation_key: int = 0x123456789ABCDEF0):
    """Execute a complete reservation test scenario."""

    # 1. Register a reservation key
    result = client_mock.reservation_register(nsid, ReservationAction.REGISTER, reservation_key)
    test_case.assertTrue(result.success)

    # 2. Acquire a reservation
    result = client_mock.reservation_acquire(
        nsid, ReservationAction.ACQUIRE, ReservationType.WRITE_EXCLUSIVE, reservation_key)
    test_case.assertTrue(result.success)

    # 3. Get reservation status
    status = client_mock.reservation_report(nsid)
    test_case.assertTrue(status.is_reserved)

    # 4. Release the reservation
    result = client_mock.reservation_release(
        nsid, ReservationAction.RELEASE, ReservationType.WRITE_EXCLUSIVE, reservation_key)
    test_case.assertTrue(result.success)

    # 5. Unregister the key
    result = client_mock.reservation_register(nsid, ReservationAction.UNREGISTER, reservation_key)
    test_case.assertTrue(result.success)
