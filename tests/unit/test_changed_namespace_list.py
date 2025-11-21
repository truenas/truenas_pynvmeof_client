"""
Unit tests for Changed Namespace List log page parsing.
"""

import unittest
import struct
from nvmeof_client.parsers import ChangedNamespaceListParser


class TestChangedNamespaceListParser(unittest.TestCase):
    """Test Changed Namespace List parsing."""

    def test_parse_empty_list(self):
        """Test parsing empty namespace list (all zeros)."""
        data = b'\x00' * 4096
        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        self.assertEqual(result, [])

    def test_parse_single_namespace(self):
        """Test parsing list with single namespace."""
        # NSID 1, followed by zeros
        data = struct.pack('<L', 1) + b'\x00' * 4092
        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        self.assertEqual(result, [1])

    def test_parse_multiple_namespaces(self):
        """Test parsing list with multiple namespaces."""
        # NSIDs 1, 2, 5, 10 (in ascending order), followed by zeros
        nsids = [1, 2, 5, 10]
        data = b''.join(struct.pack('<L', nsid) for nsid in nsids) + b'\x00' * (4096 - len(nsids) * 4)
        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        self.assertEqual(result, nsids)

    def test_parse_overflow_indicator(self):
        """Test parsing overflow (more than 1024 namespaces changed)."""
        # First entry is FFFFFFFFh to indicate overflow
        data = struct.pack('<L', 0xFFFFFFFF) + b'\x00' * 4092
        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        self.assertEqual(result, [0xFFFFFFFF])

    def test_parse_max_namespaces(self):
        """Test parsing maximum number of namespaces (1024)."""
        # Create list of 1024 namespace IDs
        nsids = list(range(1, 1025))
        data = b''.join(struct.pack('<L', nsid) for nsid in nsids)
        self.assertEqual(len(data), 4096)  # Should be exactly 4096 bytes

        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        self.assertEqual(result, nsids)
        self.assertEqual(len(result), 1024)

    def test_parse_terminated_by_zero(self):
        """Test that list is terminated by zero entry."""
        # NSIDs 1, 2, 3, then zero (end), followed by more data that should be ignored
        data = (struct.pack('<L', 1) + struct.pack('<L', 2) +
                struct.pack('<L', 3) + struct.pack('<L', 0) +
                struct.pack('<L', 99) + struct.pack('<L', 100) + b'\x00' * (4096 - 24))
        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        # Should only get [1, 2, 3], stopping at zero
        self.assertEqual(result, [1, 2, 3])

    def test_parse_short_data(self):
        """Test parsing data shorter than 4096 bytes."""
        # Only 3 entries
        data = struct.pack('<L', 1) + struct.pack('<L', 5) + struct.pack('<L', 10)
        self.assertEqual(len(data), 12)

        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        self.assertEqual(result, [1, 5, 10])

    def test_parse_non_multiple_of_4(self):
        """Test parsing data that's not a multiple of 4 bytes (should pad)."""
        # 10 bytes (2.5 entries) - should pad to 12
        # bytes: 01 00 00 00 | 02 00 00 00 | 03 00 (padded with 00 00)
        # After padding: 01 00 00 00 | 02 00 00 00 | 03 00 00 00
        # Which gives: 1, 2, 3
        data = b'\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00'
        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        # Padding turns the partial third entry into a valid entry
        self.assertEqual(result, [1, 2, 3])

    def test_parse_empty_data(self):
        """Test parsing empty data."""
        data = b''
        result = ChangedNamespaceListParser.parse_changed_namespace_list(data)
        self.assertEqual(result, [])


class TestChangedNamespaceListFormatting(unittest.TestCase):
    """Test Changed Namespace List formatting."""

    def test_format_empty_list(self):
        """Test formatting empty list."""
        result = ChangedNamespaceListParser.format_changed_namespace_list([])
        self.assertEqual(result, "No namespace changes detected")

    def test_format_single_namespace(self):
        """Test formatting single namespace."""
        result = ChangedNamespaceListParser.format_changed_namespace_list([1])
        self.assertEqual(result, "1 namespace changed: NSID 1")

    def test_format_multiple_namespaces(self):
        """Test formatting multiple namespaces."""
        result = ChangedNamespaceListParser.format_changed_namespace_list([1, 2, 5, 10])
        self.assertEqual(result, "4 namespaces changed: 1, 2, 5, 10")

    def test_format_overflow(self):
        """Test formatting overflow indicator."""
        result = ChangedNamespaceListParser.format_changed_namespace_list([0xFFFFFFFF])
        self.assertEqual(result, "More than 1,024 namespaces changed (overflow)")

    def test_format_many_namespaces(self):
        """Test formatting truncates long lists."""
        # Create list of 20 namespaces
        nsids = list(range(1, 21))
        result = ChangedNamespaceListParser.format_changed_namespace_list(nsids)
        # Should show first 10 and indicate there are more
        self.assertIn("20 namespaces changed", result)
        self.assertIn("1, 2, 3, 4, 5, 6, 7, 8, 9, 10", result)
        self.assertIn("... (10 more)", result)


if __name__ == '__main__':
    unittest.main()
