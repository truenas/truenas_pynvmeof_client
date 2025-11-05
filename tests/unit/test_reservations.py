"""
Unit tests for NVMe Reservation functionality

Tests the reservation methods without requiring a live target.
Uses mocking to simulate protocol responses and validate command generation.
"""

import unittest
import struct
from unittest.mock import Mock, patch

from nvmeof_client.client import NVMeoFClient
from nvmeof_client.exceptions import (
    NVMeoFConnectionError,
    CommandError,
    ProtocolError
)
from nvmeof_client.models import (
    ReservationType,
    ReservationAction,
    ReservationStatus,
    ReservationInfo
)
from nvmeof_client.parsers import ReservationDataParser
from nvmeof_client.protocol import PDUType, NVMeOpcode
from nvmeof_client.protocol.io_commands import (
    pack_nvme_reservation_register_command,
    pack_nvme_reservation_report_command,
    pack_nvme_reservation_acquire_command,
    pack_nvme_reservation_release_command
)

import sys
import os
# Add the fixtures directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'fixtures'))

# Import test fixtures (must come after sys.path manipulation)
from mock_responses import create_reservation_report_data  # noqa: E402
from test_helpers import assert_command_structure  # noqa: E402


class TestReservationCommandGeneration(unittest.TestCase):
    """Test reservation command generation and structure."""

    def test_pack_reservation_register_command(self):
        """Test reservation register command packing."""
        cmd = pack_nvme_reservation_register_command(
            command_id=123, nsid=1, reservation_action=0
        )

        assert_command_structure(self, cmd, NVMeOpcode.RESERVATION_REGISTER, expected_nsid=1)

        # Check action field (DW10)
        action = struct.unpack('<L', cmd[40:44])[0]
        self.assertEqual(action & 0x7, 0)  # Register action

    def test_pack_reservation_report_command(self):
        """Test reservation report command packing."""
        cmd = pack_nvme_reservation_report_command(
            command_id=456, nsid=2, data_length=4096
        )

        assert_command_structure(self, cmd, NVMeOpcode.RESERVATION_REPORT, expected_nsid=2)

        # Check NUMD field (DW10) - number of dwords minus 1
        numd = struct.unpack('<L', cmd[40:44])[0]
        self.assertEqual(numd, (4096 // 4) - 1)

    def test_pack_reservation_acquire_command(self):
        """Test reservation acquire command packing."""
        cmd = pack_nvme_reservation_acquire_command(
            command_id=789, nsid=3, reservation_action=ReservationAction.ACQUIRE,
            reservation_type=ReservationType.WRITE_EXCLUSIVE
        )

        assert_command_structure(self, cmd, NVMeOpcode.RESERVATION_ACQUIRE, expected_nsid=3)

        # Check action and type fields (DW10)
        dw10 = struct.unpack('<L', cmd[40:44])[0]
        action = dw10 & 0x7
        rtype = (dw10 >> 8) & 0xFF
        self.assertEqual(action, ReservationAction.ACQUIRE.value)  # Acquire action
        self.assertEqual(rtype, ReservationType.WRITE_EXCLUSIVE.value)   # Write Exclusive

    def test_pack_reservation_release_command(self):
        """Test reservation release command packing."""
        cmd = pack_nvme_reservation_release_command(
            command_id=1234, nsid=4, reservation_action=ReservationAction.RELEASE,
            reservation_type=ReservationType.EXCLUSIVE_ACCESS
        )

        assert_command_structure(self, cmd, NVMeOpcode.RESERVATION_RELEASE, expected_nsid=4)

        # Check action and type fields (DW10)
        dw10 = struct.unpack('<L', cmd[40:44])[0]
        action = dw10 & 0x7
        rtype = (dw10 >> 8) & 0xFF
        self.assertEqual(action, ReservationAction.RELEASE.value)  # Release action
        self.assertEqual(rtype, ReservationType.EXCLUSIVE_ACCESS.value)   # Exclusive Access


class TestReservationModels(unittest.TestCase):
    """Test reservation data models and validation."""

    def test_reservation_type_enum(self):
        """Test ReservationType enum values."""
        self.assertEqual(ReservationType.WRITE_EXCLUSIVE.value, 1)
        self.assertEqual(ReservationType.EXCLUSIVE_ACCESS.value, 2)
        self.assertEqual(ReservationType.WRITE_EXCLUSIVE_REGISTRANTS_ONLY.value, 3)
        self.assertEqual(ReservationType.EXCLUSIVE_ACCESS_REGISTRANTS_ONLY.value, 4)
        self.assertEqual(ReservationType.WRITE_EXCLUSIVE_ALL_REGISTRANTS.value, 5)
        self.assertEqual(ReservationType.EXCLUSIVE_ACCESS_ALL_REGISTRANTS.value, 6)

    def test_reservation_action_enum(self):
        """Test ReservationAction enum values."""
        # Register actions
        self.assertEqual(ReservationAction.REGISTER.value, 0)
        self.assertEqual(ReservationAction.UNREGISTER.value, 1)
        self.assertEqual(ReservationAction.REPLACE.value, 2)

        # Acquire actions (reuse same values with different context)
        self.assertEqual(ReservationAction.ACQUIRE.value, 0)
        self.assertEqual(ReservationAction.PREEMPT.value, 1)
        self.assertEqual(ReservationAction.PREEMPT_AND_ABORT.value, 2)

        # Release actions
        self.assertEqual(ReservationAction.RELEASE.value, 0)
        self.assertEqual(ReservationAction.CLEAR.value, 1)

    def test_reservation_status_model(self):
        """Test ReservationStatus model."""
        status = ReservationStatus(
            generation=123,
            reservation_type=ReservationType.WRITE_EXCLUSIVE,
            reservation_holder=42,
            registered_controllers=[1, 2, 3],
            reservation_keys={1: 0x1111, 2: 0x2222, 3: 0x3333}
        )

        self.assertEqual(status.generation, 123)
        self.assertTrue(status.is_reserved)  # holder != 0
        self.assertEqual(status.num_registered_controllers, 3)
        self.assertEqual(status.reservation_keys[2], 0x2222)

    def test_reservation_status_not_reserved(self):
        """Test ReservationStatus when not reserved."""
        status = ReservationStatus(
            generation=456,
            reservation_type=ReservationType.WRITE_EXCLUSIVE,
            reservation_holder=0,  # No holder = not reserved
            registered_controllers=[1, 2],
            reservation_keys={1: 0x1111, 2: 0x2222}
        )

        self.assertFalse(status.is_reserved)
        self.assertEqual(status.num_registered_controllers, 2)

    def test_reservation_info_model(self):
        """Test ReservationInfo model."""
        info = ReservationInfo(
            success=True,
            reservation_key=0x123456789ABCDEF0,
            generation=789,
            status_code=0
        )

        self.assertTrue(info.success)
        self.assertEqual(info.reservation_key, 0x123456789ABCDEF0)
        self.assertEqual(info.generation, 789)
        self.assertEqual(info.status_code, 0)


class TestReservationMethodParameterValidation(unittest.TestCase):
    """Test parameter validation for reservation methods."""

    def setUp(self):
        """Set up test client."""
        self.client = NVMeoFClient("localhost", port=4420)
        self.client._connected = True
        self.client._is_discovery_subsystem = False

    def test_reservation_register_parameter_validation(self):
        """Test reservation_register parameter validation."""
        # Invalid namespace ID
        with self.assertRaises(ValueError):
            self.client.reservation_register(0, ReservationAction.REGISTER, 0x123)

        # Invalid action
        with self.assertRaises(ValueError):
            self.client.reservation_register(1, 99, 0x123)

        # Replace action without new key
        with self.assertRaises(ValueError):
            self.client.reservation_register(1, ReservationAction.REPLACE, 0x123, 0)

    def test_reservation_report_parameter_validation(self):
        """Test reservation_report parameter validation."""
        # Invalid namespace ID
        with self.assertRaises(ValueError):
            self.client.reservation_report(-1)

    def test_reservation_acquire_parameter_validation(self):
        """Test reservation_acquire parameter validation."""
        # Invalid namespace ID
        with self.assertRaises(ValueError):
            self.client.reservation_acquire(0, ReservationAction.ACQUIRE,
                                            ReservationType.WRITE_EXCLUSIVE, 0x123)

        # Invalid action
        with self.assertRaises(ValueError):
            self.client.reservation_acquire(1, 99, ReservationType.WRITE_EXCLUSIVE, 0x123)

        # Invalid reservation type
        with self.assertRaises(ValueError):
            self.client.reservation_acquire(1, ReservationAction.ACQUIRE, 99, 0x123)

        # Preempt action without preempt key
        with self.assertRaises(ValueError):
            self.client.reservation_acquire(1, ReservationAction.PREEMPT,
                                            ReservationType.WRITE_EXCLUSIVE, 0x123, 0)

    def test_reservation_release_parameter_validation(self):
        """Test reservation_release parameter validation."""
        # Invalid namespace ID
        with self.assertRaises(ValueError):
            self.client.reservation_release(0, ReservationAction.RELEASE,
                                            ReservationType.WRITE_EXCLUSIVE, 0x123)

        # Invalid action
        with self.assertRaises(ValueError):
            self.client.reservation_release(1, 99, ReservationType.WRITE_EXCLUSIVE, 0x123)

        # Invalid reservation type
        with self.assertRaises(ValueError):
            self.client.reservation_release(1, ReservationAction.RELEASE, 99, 0x123)

    def test_connection_state_validation(self):
        """Test that reservation methods check connection state."""
        # Not connected
        disconnected_client = NVMeoFClient("localhost", port=4420)

        with self.assertRaises(NVMeoFConnectionError):
            disconnected_client.reservation_register(1, ReservationAction.REGISTER, 0x123)

        with self.assertRaises(NVMeoFConnectionError):
            disconnected_client.reservation_report(1)

        with self.assertRaises(NVMeoFConnectionError):
            disconnected_client.reservation_acquire(1, ReservationAction.ACQUIRE,
                                                    ReservationType.WRITE_EXCLUSIVE, 0x123)

        with self.assertRaises(NVMeoFConnectionError):
            disconnected_client.reservation_release(1, ReservationAction.RELEASE,
                                                    ReservationType.WRITE_EXCLUSIVE, 0x123)

    def test_discovery_subsystem_validation(self):
        """Test that reservation methods reject discovery subsystem connections."""
        discovery_client = NVMeoFClient("localhost", port=4420)
        discovery_client._connected = True
        discovery_client._is_discovery_subsystem = True

        with self.assertRaises(NVMeoFConnectionError):
            discovery_client.reservation_register(1, ReservationAction.REGISTER, 0x123)

        with self.assertRaises(NVMeoFConnectionError):
            discovery_client.reservation_report(1)

        with self.assertRaises(NVMeoFConnectionError):
            discovery_client.reservation_acquire(1, ReservationAction.ACQUIRE,
                                                 ReservationType.WRITE_EXCLUSIVE, 0x123)

        with self.assertRaises(NVMeoFConnectionError):
            discovery_client.reservation_release(1, ReservationAction.RELEASE,
                                                 ReservationType.WRITE_EXCLUSIVE, 0x123)


class TestReservationMethodMockedExecution(unittest.TestCase):
    """Test reservation methods with mocked socket responses."""

    def setUp(self):
        """Set up test client with mocked dependencies."""
        self.client = NVMeoFClient("localhost", port=4420)
        self.client._connected = True
        self.client._is_discovery_subsystem = False
        self.client._command_id_counter = 0

        # Mock I/O socket and queue setup
        self.client._io_socket = Mock()
        self.client._io_queues_setup = True
        self.client.setup_io_queues = Mock()

    @patch('nvmeof_client.client.NVMeoFClient._send_nvme_reservation_command_pdu')
    @patch('nvmeof_client.client.NVMeoFClient._receive_pdu_on_socket')
    @patch('nvmeof_client.parsers.response.ResponseParser.parse_response')
    def test_reservation_register_success(self, mock_parse, mock_receive, mock_send):
        """Test successful reservation register operation."""
        # Mock successful response
        mock_parse.return_value = {'status': 0}
        mock_receive.return_value = (Mock(pdu_type=PDUType.RSP), b'response_data')

        result = self.client.reservation_register(
            1, ReservationAction.REGISTER, 0x123456789ABCDEF0)

        # Verify method calls
        mock_send.assert_called_once()
        mock_receive.assert_called_once()
        mock_parse.assert_called_once()

        # Verify result
        self.assertIsInstance(result, ReservationInfo)
        self.assertTrue(result.success)
        self.assertEqual(result.reservation_key, 0x123456789ABCDEF0)
        self.assertEqual(result.status_code, 0)

    @patch('nvmeof_client.client.NVMeoFClient._send_nvme_io_command_pdu')
    @patch('nvmeof_client.client.NVMeoFClient._receive_pdu_on_socket')
    @patch('nvmeof_client.parsers.response.ResponseParser.parse_response')
    def test_reservation_report_success(self, mock_parse, mock_receive, mock_send):
        """Test successful reservation report operation."""
        # Create mock reservation report data (extended format)
        report_data = create_reservation_report_data(
            generation=123,
            reservation_type=ReservationType.WRITE_EXCLUSIVE.value,
            reservation_holder=1,  # Controller 1 holds the reservation
            registered_controllers=[(1, 0x1111), (2, 0x2222)],
            extended_format=True
        )

        # Mock responses: data first, then completion
        mock_receive.side_effect = [
            (Mock(pdu_type=PDUType.C2H_DATA), report_data),
            (Mock(pdu_type=PDUType.RSP), b'response_data')
        ]
        mock_parse.return_value = {'status': 0}

        result = self.client.reservation_report(1)

        # Verify method calls
        mock_send.assert_called_once()
        self.assertEqual(mock_receive.call_count, 2)
        mock_parse.assert_called_once()

        # Verify result
        self.assertIsInstance(result, ReservationStatus)
        self.assertEqual(result.generation, 123)
        self.assertEqual(result.reservation_type, ReservationType.WRITE_EXCLUSIVE)
        self.assertEqual(result.reservation_holder, 1)  # Controller 1 holds the reservation
        self.assertEqual(len(result.registered_controllers), 2)
        self.assertEqual(result.reservation_keys[1], 0x1111)

    @patch('nvmeof_client.client.NVMeoFClient._send_nvme_reservation_pdu')
    @patch('nvmeof_client.client.NVMeoFClient._receive_pdu_on_socket')
    @patch('nvmeof_client.parsers.response.ResponseParser.parse_response')
    def test_reservation_acquire_success(self, mock_parse, mock_receive, mock_send):
        """Test successful reservation acquire operation."""
        # Mock successful response
        mock_parse.return_value = {'status': 0}
        mock_receive.return_value = (Mock(pdu_type=PDUType.RSP), b'response_data')

        result = self.client.reservation_acquire(
            1, ReservationAction.ACQUIRE, ReservationType.WRITE_EXCLUSIVE,
            0x123456789ABCDEF0)

        # Verify method calls
        mock_send.assert_called_once()
        mock_receive.assert_called_once()
        mock_parse.assert_called_once()

        # Verify result
        self.assertIsInstance(result, ReservationInfo)
        self.assertTrue(result.success)
        self.assertEqual(result.reservation_key, 0x123456789ABCDEF0)

    @patch('nvmeof_client.client.NVMeoFClient._send_nvme_reservation_pdu')
    @patch('nvmeof_client.client.NVMeoFClient._receive_pdu_on_socket')
    @patch('nvmeof_client.parsers.response.ResponseParser.parse_response')
    def test_reservation_release_success(self, mock_parse, mock_receive, mock_send):
        """Test successful reservation release operation."""
        # Mock successful response
        mock_parse.return_value = {'status': 0}
        mock_receive.return_value = (Mock(pdu_type=PDUType.RSP), b'response_data')

        result = self.client.reservation_release(
            1, ReservationAction.RELEASE, ReservationType.WRITE_EXCLUSIVE,
            0x123456789ABCDEF0)

        # Verify method calls
        mock_send.assert_called_once()
        mock_receive.assert_called_once()
        mock_parse.assert_called_once()

        # Verify result
        self.assertIsInstance(result, ReservationInfo)
        self.assertTrue(result.success)
        self.assertEqual(result.reservation_key, 0x123456789ABCDEF0)

    @patch('nvmeof_client.client.NVMeoFClient._send_nvme_reservation_command_pdu')
    @patch('nvmeof_client.client.NVMeoFClient._receive_pdu_on_socket')
    @patch('nvmeof_client.parsers.response.ResponseParser.parse_response')
    def test_reservation_command_failure(self, mock_parse, mock_receive, mock_send):
        """Test reservation command failure handling."""
        # Mock command failure
        mock_parse.return_value = {'status': 0x18}  # Reservation conflict
        mock_receive.return_value = (Mock(pdu_type=PDUType.RSP), b'response_data')

        with self.assertRaises(CommandError) as cm:
            self.client.reservation_register(1, ReservationAction.REGISTER, 0x123)

        self.assertIn('failed with status 18', str(cm.exception))

    @patch('nvmeof_client.client.NVMeoFClient._send_nvme_io_command_pdu')
    @patch('nvmeof_client.client.NVMeoFClient._receive_pdu_on_socket')
    def test_reservation_report_protocol_error(self, mock_receive, mock_send):
        """Test reservation report protocol error handling."""
        # Mock unexpected PDU type (e.g., H2C_DATA when we expect C2H_DATA)
        mock_receive.return_value = (Mock(pdu_type=PDUType.H2C_DATA), b'unexpected_data')

        with self.assertRaises(ProtocolError) as cm:
            self.client.reservation_report(1)

        self.assertIn('Expected C2H_DATA or RSP PDU', str(cm.exception))


class TestReservationDataParser(unittest.TestCase):
    """Test ReservationDataParser functionality."""

    def test_parse_extended_format(self):
        """Test parsing extended format reservation data."""

        # Create test data with extended format (64-byte entries)
        test_data = create_reservation_report_data(
            generation=456,
            reservation_type=2,  # Exclusive Access
            reservation_holder=1,
            registered_controllers=[(1, 0xAABBCCDD), (2, 0x11223344)],
            extended_format=True
        )

        # Parse the data
        parsed = ReservationDataParser.parse_reservation_report(test_data, extended_format=True)

        # Verify results
        self.assertEqual(parsed['generation'], 456)
        self.assertEqual(parsed['reservation_type'], 2)
        self.assertEqual(parsed['num_registered_controllers'], 2)
        self.assertTrue(parsed['extended_format'])
        self.assertEqual(parsed['entry_size'], 64)

        # Verify registrants
        self.assertEqual(len(parsed['registrants']), 2)

        registrant1 = parsed['registrants'][0]
        self.assertEqual(registrant1['controller_id'], 1)
        self.assertEqual(registrant1['reservation_key'], 0xAABBCCDD)
        self.assertTrue(registrant1['holds_reservation'])  # Controller 1 is the holder
        self.assertEqual(registrant1['host_identifier_size'], 128)

        registrant2 = parsed['registrants'][1]
        self.assertEqual(registrant2['controller_id'], 2)
        self.assertEqual(registrant2['reservation_key'], 0x11223344)
        self.assertFalse(registrant2['holds_reservation'])  # Controller 2 is not the holder
        self.assertEqual(registrant2['host_identifier_size'], 128)

    def test_parse_standard_format(self):
        """Test parsing standard format reservation data."""

        # Create test data with standard format (24-byte entries)
        test_data = create_reservation_report_data(
            generation=789,
            reservation_type=3,  # Write Exclusive Registrants Only
            reservation_holder=2,
            registered_controllers=[(1, 0x1111), (2, 0x2222)],
            extended_format=False
        )

        # Parse the data
        parsed = ReservationDataParser.parse_reservation_report(test_data, extended_format=False)

        # Verify results
        self.assertEqual(parsed['generation'], 789)
        self.assertEqual(parsed['reservation_type'], 3)
        self.assertEqual(parsed['num_registered_controllers'], 2)
        self.assertFalse(parsed['extended_format'])
        self.assertEqual(parsed['entry_size'], 24)

        # Verify registrants
        self.assertEqual(len(parsed['registrants']), 2)

        registrant1 = parsed['registrants'][0]
        self.assertEqual(registrant1['controller_id'], 1)
        self.assertEqual(registrant1['reservation_key'], 0x1111)
        self.assertFalse(registrant1['holds_reservation'])  # Controller 1 is not the holder
        self.assertEqual(registrant1['host_identifier_size'], 64)

        registrant2 = parsed['registrants'][1]
        self.assertEqual(registrant2['controller_id'], 2)
        self.assertEqual(registrant2['reservation_key'], 0x2222)
        self.assertTrue(registrant2['holds_reservation'])  # Controller 2 is the holder
        self.assertEqual(registrant2['host_identifier_size'], 64)


if __name__ == '__main__':
    unittest.main()
