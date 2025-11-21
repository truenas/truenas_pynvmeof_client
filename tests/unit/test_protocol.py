"""
Unit tests for protocol module

Tests protocol constants, structures, and utility functions.
"""

import unittest
import struct

from nvmeof_client.protocol import (
    PDUType,
    NVMeOpcode,
    pack_pdu_header,
    unpack_pdu_header,
    PDUHeader,
    parse_controller_capabilities,
    parse_discovery_log_page,
    format_discovery_entry
)
from nvmeof_client.protocol.utils import pack_nvme_command


class TestProtocolConstants(unittest.TestCase):
    """Test protocol constants and enums."""

    def test_pdu_types(self):
        """Test PDU type constants."""
        self.assertEqual(PDUType.ICREQ, 0x00)
        self.assertEqual(PDUType.ICRESP, 0x01)
        self.assertEqual(PDUType.CMD, 0x04)
        self.assertEqual(PDUType.RSP, 0x05)
        self.assertEqual(PDUType.H2C_DATA, 0x06)
        self.assertEqual(PDUType.R2T, 0x09)

    def test_nvme_opcodes(self):
        """Test NVMe opcode constants."""
        self.assertEqual(NVMeOpcode.IDENTIFY, 0x06)
        self.assertEqual(NVMeOpcode.CREATE_IO_SQ, 0x01)
        self.assertEqual(NVMeOpcode.CREATE_IO_CQ, 0x05)
        self.assertEqual(NVMeOpcode.READ, 0x02)
        self.assertEqual(NVMeOpcode.WRITE, 0x01)
        self.assertEqual(NVMeOpcode.FLUSH, 0x00)


class TestPDUHeader(unittest.TestCase):
    """Test PDU header packing and unpacking."""

    def test_pack_pdu_header(self):
        """Test PDU header packing."""
        header = pack_pdu_header(PDUType.CMD, 0x01, 8, 8, 72)
        expected = struct.pack('<BBBBI', PDUType.CMD, 0x01, 8, 8, 72)
        self.assertEqual(header, expected)

    def test_unpack_pdu_header(self):
        """Test PDU header unpacking."""
        data = struct.pack('<BBBBI', PDUType.RSP, 0x02, 8, 8, 24)
        header = unpack_pdu_header(data)

        self.assertEqual(header.pdu_type, PDUType.RSP)
        self.assertEqual(header.flags, 0x02)
        self.assertEqual(header.hlen, 8)
        self.assertEqual(header.pdo, 8)
        self.assertEqual(header.plen, 24)

    def test_unpack_pdu_header_short_data(self):
        """Test PDU header unpacking with insufficient data."""
        with self.assertRaises(ValueError):
            unpack_pdu_header(b'short')

    def test_pdu_header_roundtrip(self):
        """Test PDU header pack/unpack roundtrip."""
        original = PDUHeader(PDUType.H2C_DATA, 0x04, 16, 16, 1024)
        packed = pack_pdu_header(original.pdu_type, original.flags,
                                 original.hlen, original.pdo, original.plen)
        unpacked = unpack_pdu_header(packed)

        self.assertEqual(original, unpacked)


class TestNVMeCommand(unittest.TestCase):
    """Test NVMe command packing."""

    def test_pack_nvme_command(self):
        """Test NVMe command packing."""
        cmd = pack_nvme_command(NVMeOpcode.IDENTIFY, 0x00, 123, 1)

        # Verify command structure
        self.assertEqual(len(cmd), 64)  # NVMe command is 64 bytes

        # Check first few fields
        opcode, flags, command_id, nsid = struct.unpack('<BBHI', cmd[:8])
        self.assertEqual(opcode, NVMeOpcode.IDENTIFY)
        self.assertEqual(flags, 0x00)
        self.assertEqual(command_id, 123)
        self.assertEqual(nsid, 1)

    def test_pack_nvme_command_defaults(self):
        """Test NVMe command packing with defaults."""
        cmd = pack_nvme_command(NVMeOpcode.FLUSH, 0x01, 456)

        opcode, flags, command_id, nsid = struct.unpack('<BBHI', cmd[:8])
        self.assertEqual(opcode, NVMeOpcode.FLUSH)
        self.assertEqual(flags, 0x01)
        self.assertEqual(command_id, 456)
        self.assertEqual(nsid, 0)  # Default namespace


class TestUtilityFunctions(unittest.TestCase):
    """Test utility functions."""

    def test_parse_controller_capabilities(self):
        """Test CAP register parsing."""
        # Create sample CAP data: MQES=1023, TO=30, DSTRD=0
        cap_value = (1023 | (30 << 24))  # MQES + timeout
        cap_data = struct.pack('<Q', cap_value)

        result = parse_controller_capabilities(cap_data)

        self.assertEqual(result['max_queue_entries_supported'], 1024)  # MQES is 0-based
        self.assertEqual(result['timeout'], 15000)  # 30 * 500ms
        self.assertEqual(result['doorbell_stride'], 4)  # 4 << 0
        self.assertFalse(result['contiguous_queues_required'])

    def test_parse_controller_capabilities_short_data(self):
        """Test CAP parsing with insufficient data."""
        with self.assertRaises(ValueError):
            parse_controller_capabilities(b'short')

    def test_parse_discovery_log_page(self):
        """Test discovery log page parsing."""
        # Create minimal discovery log with no entries
        header = struct.pack('<QQ', 123, 0)  # generation=123, num_records=0
        log_data = header + b'\x00' * 1008  # Pad to 1024 bytes

        result = parse_discovery_log_page(log_data)

        self.assertIsInstance(result, dict)
        self.assertEqual(result['generation'], 123)
        self.assertEqual(result['num_records'], 0)
        self.assertIsInstance(result['entries'], list)
        self.assertEqual(len(result['entries']), 0)

    def test_parse_discovery_log_page_short_data(self):
        """Test discovery log parsing with insufficient data."""
        with self.assertRaises(ValueError):
            parse_discovery_log_page(b'short')

    def test_format_discovery_entry(self):
        """Test discovery entry formatting with human-readable strings."""
        # Create raw entry with numeric codes
        raw_entry = {
            'transport_type': 3,  # TCP
            'address_family': 1,  # IPv4
            'subsystem_type': 2,  # NVMe
            'port_id': 4420,
            'controller_id': 1,
            'transport_address': '192.168.1.100',
            'transport_service_id': '4420',
            'subsystem_nqn': 'nqn.2019-05.io.spdk:target'
        }

        formatted = format_discovery_entry(raw_entry)

        # Check human-readable fields
        self.assertEqual(formatted['transport_type'], 'TCP')
        self.assertEqual(formatted['address_family'], 'IPv4')
        self.assertEqual(formatted['subsystem_type'], 'NVMe')

        # Check raw values are preserved
        self.assertEqual(formatted['raw_transport_type'], 3)
        self.assertEqual(formatted['raw_address_family'], 1)
        self.assertEqual(formatted['raw_subsystem_type'], 2)

        # Check other fields passed through
        self.assertEqual(formatted['port_id'], 4420)
        self.assertEqual(formatted['controller_id'], 1)
        self.assertEqual(formatted['transport_address'], '192.168.1.100')
        self.assertEqual(formatted['transport_service_id'], '4420')
        self.assertEqual(formatted['subsystem_nqn'], 'nqn.2019-05.io.spdk:target')

    def test_format_discovery_entry_unknown_values(self):
        """Test discovery entry formatting with unknown codes."""
        raw_entry = {
            'transport_type': 99,  # Unknown
            'address_family': 88,  # Unknown
            'subsystem_type': 77,  # Unknown
            'port_id': 0,
            'controller_id': 0,
            'transport_address': '',
            'transport_service_id': '',
            'subsystem_nqn': ''
        }

        formatted = format_discovery_entry(raw_entry)

        # Check unknown values are formatted with numeric code
        self.assertEqual(formatted['transport_type'], 'Unknown(99)')
        self.assertEqual(formatted['address_family'], 'Unknown(88)')
        self.assertEqual(formatted['subsystem_type'], 'Unknown(77)')

    def test_format_discovery_entry_discovery_subsystem(self):
        """Test discovery entry formatting for discovery subsystem."""
        raw_entry = {
            'transport_type': 3,  # TCP
            'address_family': 1,  # IPv4
            'subsystem_type': 1,  # Discovery
            'port_id': 8009,
            'controller_id': 0,
            'transport_address': '192.168.1.1',
            'transport_service_id': '8009',
            'subsystem_nqn': 'nqn.2014-08.org.nvmexpress.discovery'
        }

        formatted = format_discovery_entry(raw_entry)

        self.assertEqual(formatted['subsystem_type'], 'Discovery')
        self.assertEqual(formatted['raw_subsystem_type'], 1)


class TestNamedTuples(unittest.TestCase):
    """Test named tuple structures."""

    def test_pdu_header_namedtuple(self):
        """Test PDUHeader named tuple."""
        header = PDUHeader(PDUType.CMD, 0x01, 8, 8, 72)

        self.assertEqual(header.pdu_type, PDUType.CMD)
        self.assertEqual(header.flags, 0x01)
        self.assertEqual(header.hlen, 8)
        self.assertEqual(header.pdo, 8)
        self.assertEqual(header.plen, 72)

        # Test immutability
        with self.assertRaises(AttributeError):
            header.pdu_type = PDUType.RSP


if __name__ == '__main__':
    unittest.main()
