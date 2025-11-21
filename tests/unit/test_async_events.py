"""
Unit tests for Asynchronous Event functionality.

Tests async event models, parsing, and protocol commands.
"""

import unittest
import struct
from nvmeof_client.models import (
    AsyncEvent,
    AsyncEventType,
    AsyncEventInfoNotice,
    AsyncEventInfoImmediate
)
from nvmeof_client.parsers import AsyncEventParser
from nvmeof_client.protocol import (
    pack_async_event_request_command,
    pack_set_features_command,
    NVMeOpcode,
    FeatureIdentifier
)


class TestAsyncEventModels(unittest.TestCase):
    """Test AsyncEvent data models."""

    def test_async_event_creation(self):
        """Test creating AsyncEvent objects."""
        event = AsyncEvent(
            event_type=AsyncEventType.NOTICE,
            event_info=AsyncEventInfoNotice.ANA_CHANGE,
            log_page_id=0x0C,
            description="ANA state change",
            raw_dword0=0x0C030200,
            event_specific_param=None
        )

        self.assertEqual(event.event_type, AsyncEventType.NOTICE)
        self.assertEqual(event.event_info, AsyncEventInfoNotice.ANA_CHANGE)
        self.assertEqual(event.log_page_id, 0x0C)
        self.assertIsNone(event.event_specific_param)

    def test_async_event_type_enum(self):
        """Test AsyncEventType enum values."""
        self.assertEqual(AsyncEventType.ERROR_STATUS, 0x00)
        self.assertEqual(AsyncEventType.SMART_HEALTH_STATUS, 0x01)
        self.assertEqual(AsyncEventType.NOTICE, 0x02)
        self.assertEqual(AsyncEventType.IMMEDIATE, 0x03)
        self.assertEqual(AsyncEventType.ONE_SHOT, 0x04)
        self.assertEqual(AsyncEventType.IO_COMMAND_SPECIFIC, 0x06)
        self.assertEqual(AsyncEventType.VENDOR_SPECIFIC, 0x07)

    def test_async_event_info_notice_enum(self):
        """Test AsyncEventInfoNotice enum values."""
        self.assertEqual(AsyncEventInfoNotice.NAMESPACE_ATTRIBUTE_CHANGED, 0x00)
        self.assertEqual(AsyncEventInfoNotice.FIRMWARE_ACTIVATION_STARTING, 0x01)
        self.assertEqual(AsyncEventInfoNotice.ANA_CHANGE, 0x03)
        self.assertEqual(AsyncEventInfoNotice.LBA_STATUS_INFO_ALERT, 0x05)

    def test_async_event_info_immediate_enum(self):
        """Test AsyncEventInfoImmediate enum values."""
        self.assertEqual(AsyncEventInfoImmediate.NORMAL_SUBSYSTEM_SHUTDOWN, 0x00)
        self.assertEqual(AsyncEventInfoImmediate.TEMPERATURE_THRESHOLD_HYSTERESIS, 0x01)

    def test_async_event_properties(self):
        """Test AsyncEvent helper properties."""
        event = AsyncEvent(
            event_type=AsyncEventType.NOTICE,
            event_info=AsyncEventInfoNotice.NAMESPACE_ATTRIBUTE_CHANGED,
            log_page_id=0x04,
            description="Namespace changed",
            raw_dword0=0x04000200
        )

        self.assertTrue(event.is_notice)
        self.assertFalse(event.is_error)
        self.assertFalse(event.is_smart_health)

    def test_async_event_error_type(self):
        """Test AsyncEvent for error type."""
        event = AsyncEvent(
            event_type=AsyncEventType.ERROR_STATUS,
            event_info=0x00,
            log_page_id=0x01,
            description="Error",
            raw_dword0=0x01000000
        )

        self.assertTrue(event.is_error)
        self.assertFalse(event.is_notice)

    def test_async_event_smart_health_type(self):
        """Test AsyncEvent for SMART/Health type."""
        event = AsyncEvent(
            event_type=AsyncEventType.SMART_HEALTH_STATUS,
            event_info=0x01,
            log_page_id=0x02,
            description="Temperature",
            raw_dword0=0x02010100
        )

        self.assertTrue(event.is_smart_health)
        self.assertFalse(event.is_error)


class TestAsyncEventParser(unittest.TestCase):
    """Test AsyncEventParser functionality."""

    def test_parse_ana_change_event(self):
        """Test parsing ANA change notice event."""
        # DW0: Event Type=0x02 (Notice), Event Info=0x03 (ANA Change), Log Page=0x0C
        # Bits: 2:0=010b (Notice), 15:8=0x03, 23:16=0x0C
        dw0 = 0x02 | (0x03 << 8) | (0x0C << 16)
        dw1 = 0  # No event specific parameter

        result = AsyncEventParser.parse_async_event_completion(dw0, dw1)

        self.assertEqual(result['event_type'], AsyncEventType.NOTICE)
        self.assertEqual(result['event_info'], 0x03)
        self.assertEqual(result['log_page_id'], 0x0C)
        self.assertIsNone(result['event_specific_param'])
        self.assertIn("NOTICE", result['description'].upper())

    def test_parse_firmware_activation_event(self):
        """Test parsing firmware activation notice event."""
        # DW0: Event Type=0x02 (Notice), Event Info=0x01 (Firmware), Log Page=0x03
        dw0 = 0x02 | (0x01 << 8) | (0x03 << 16)
        dw1 = 0

        result = AsyncEventParser.parse_async_event_completion(dw0, dw1)

        self.assertEqual(result['event_type'], AsyncEventType.NOTICE)
        self.assertEqual(result['event_info'], 0x01)
        self.assertEqual(result['log_page_id'], 0x03)
        self.assertIn("FIRMWARE", result['description'].upper())

    def test_parse_namespace_attribute_changed(self):
        """Test parsing namespace attribute changed event."""
        # DW0: Event Type=0x02 (Notice), Event Info=0x00 (Namespace), Log Page=0x04
        dw0 = 0x02 | (0x00 << 8) | (0x04 << 16)
        dw1 = 0

        result = AsyncEventParser.parse_async_event_completion(dw0, dw1)

        self.assertEqual(result['event_type'], AsyncEventType.NOTICE)
        self.assertEqual(result['event_info'], 0x00)
        self.assertEqual(result['log_page_id'], 0x04)

    def test_parse_smart_health_event(self):
        """Test parsing SMART/Health status event."""
        # DW0: Event Type=0x01 (SMART), Event Info=0x01 (Temperature), Log Page=0x02
        dw0 = 0x01 | (0x01 << 8) | (0x02 << 16)
        dw1 = 0

        result = AsyncEventParser.parse_async_event_completion(dw0, dw1)

        self.assertEqual(result['event_type'], AsyncEventType.SMART_HEALTH_STATUS)
        self.assertEqual(result['event_info'], 0x01)
        self.assertEqual(result['log_page_id'], 0x02)
        self.assertIn("SMART", result['description'].upper())

    def test_parse_error_event(self):
        """Test parsing error status event."""
        # DW0: Event Type=0x00 (Error), Event Info=0x00, Log Page=0x01
        dw0 = 0x00 | (0x00 << 8) | (0x01 << 16)
        dw1 = 0

        result = AsyncEventParser.parse_async_event_completion(dw0, dw1)

        self.assertEqual(result['event_type'], AsyncEventType.ERROR_STATUS)
        self.assertEqual(result['event_info'], 0x00)
        self.assertEqual(result['log_page_id'], 0x01)

    def test_parse_immediate_event(self):
        """Test parsing immediate event."""
        # DW0: Event Type=0x03 (Immediate), Event Info=0x00 (Shutdown), Log Page=0x00
        dw0 = 0x03 | (0x00 << 8) | (0x00 << 16)
        dw1 = 0

        result = AsyncEventParser.parse_async_event_completion(dw0, dw1)

        self.assertEqual(result['event_type'], AsyncEventType.IMMEDIATE)
        self.assertEqual(result['event_info'], 0x00)

    def test_parse_with_event_specific_param(self):
        """Test parsing event with event specific parameter."""
        dw0 = 0x02 | (0x03 << 8) | (0x0C << 16)
        dw1 = 0x12345678  # Event specific parameter

        result = AsyncEventParser.parse_async_event_completion(dw0, dw1)

        self.assertEqual(result['event_specific_param'], 0x12345678)

    def test_parse_to_object(self):
        """Test parsing to AsyncEvent object."""
        dw0 = 0x02 | (0x03 << 8) | (0x0C << 16)
        dw1 = 0

        event = AsyncEventParser.parse_async_event_to_object(dw0, dw1)

        self.assertIsInstance(event, AsyncEvent)
        self.assertEqual(event.event_type, AsyncEventType.NOTICE)
        self.assertEqual(event.event_info, 0x03)
        self.assertEqual(event.log_page_id, 0x0C)


class TestAsyncEventCommands(unittest.TestCase):
    """Test async event command packing."""

    def test_pack_set_features_command(self):
        """Test packing Set Features command for Async Event Configuration."""
        cmd_id = 10
        feature_id = FeatureIdentifier.ASYNCHRONOUS_EVENT_CONFIG  # 0x0B
        value = 0xFFFFFFFF

        # Test without save bit
        cmd = pack_set_features_command(cmd_id, feature_id, value, nsid=0, save=False)

        # Should be 64 bytes
        self.assertEqual(len(cmd), 64)

        # Check opcode (byte 0)
        opcode = cmd[0]
        self.assertEqual(opcode, NVMeOpcode.SET_FEATURES)

        # Check command ID (bytes 2-3)
        parsed_cmd_id = struct.unpack('<H', cmd[2:4])[0]
        self.assertEqual(parsed_cmd_id, cmd_id)

        # Check namespace ID (bytes 4-7) should be 0
        nsid = struct.unpack('<L', cmd[4:8])[0]
        self.assertEqual(nsid, 0)

        # Check DW10 (bytes 40-43): FID in bits 7:0, SV in bit 31
        dw10 = struct.unpack('<L', cmd[40:44])[0]
        fid_extracted = dw10 & 0xFF
        sv_bit = (dw10 >> 31) & 1
        reserved_bits = (dw10 >> 8) & 0x7FFFFF  # Bits 30:8 should be 0

        self.assertEqual(fid_extracted, 0x0B, "FID should be 0x0B (ASYNCHRONOUS_EVENT_CONFIG)")
        self.assertEqual(sv_bit, 0, "Save bit should be 0")
        self.assertEqual(reserved_bits, 0, "Reserved bits 30:8 should be 0")

        # Check DW11 (bytes 44-47): Feature-specific value
        dw11 = struct.unpack('<L', cmd[44:48])[0]
        self.assertEqual(dw11, value)

    def test_pack_set_features_command_with_save(self):
        """Test packing Set Features command with Save bit set."""
        cmd_id = 11
        feature_id = FeatureIdentifier.ASYNCHRONOUS_EVENT_CONFIG
        value = 0x12345678

        cmd = pack_set_features_command(cmd_id, feature_id, value, save=True)

        # Check DW10: SV bit should be 1
        dw10 = struct.unpack('<L', cmd[40:44])[0]
        sv_bit = (dw10 >> 31) & 1
        self.assertEqual(sv_bit, 1, "Save bit should be 1")

    def test_pack_async_event_request_command(self):
        """Test packing Asynchronous Event Request command."""
        cmd_id = 42
        cmd = pack_async_event_request_command(cmd_id)

        # Should be 64 bytes
        self.assertEqual(len(cmd), 64)

        # Check opcode (byte 0)
        opcode = cmd[0]
        self.assertEqual(opcode, NVMeOpcode.ASYNC_EVENT_REQUEST)

        # Check command ID (bytes 2-3)
        parsed_cmd_id = struct.unpack('<H', cmd[2:4])[0]
        self.assertEqual(parsed_cmd_id, cmd_id)

        # All other fields should be zero (reserved)
        # Check namespace ID (bytes 4-7) should be 0
        nsid = struct.unpack('<L', cmd[4:8])[0]
        self.assertEqual(nsid, 0)

        # Check DW10-15 are zero (bytes 40-63)
        for i in range(40, 64, 4):
            dword = struct.unpack('<L', cmd[i: i + 4])[0]
            self.assertEqual(dword, 0, f"Dword at offset {i} should be 0")

    def test_pack_async_event_request_different_ids(self):
        """Test packing with different command IDs."""
        for cmd_id in [1, 100, 1000, 65535]:
            cmd = pack_async_event_request_command(cmd_id)
            parsed_cmd_id = struct.unpack('<H', cmd[2:4])[0]
            self.assertEqual(parsed_cmd_id, cmd_id)


if __name__ == '__main__':
    unittest.main()
