"""
Unit tests for ANA (Asymmetric Namespace Access) functionality

Tests ANA models, enums, and parser implementation.
"""

import struct
import unittest
from nvmeof_client.models import (
    ANAGroupDescriptor,
    ANALogPage,
    ANAState,
)
from nvmeof_client.parsers.ana import ANALogPageParser


class TestANAState(unittest.TestCase):
    """Test ANA state enum values."""

    def test_ana_state_values(self):
        """Test that ANA state enum values match NVMe specification."""
        self.assertEqual(ANAState.OPTIMIZED, 0x01)
        self.assertEqual(ANAState.NON_OPTIMIZED, 0x02)
        self.assertEqual(ANAState.INACCESSIBLE, 0x03)
        self.assertEqual(ANAState.PERSISTENT_LOSS, 0x04)
        self.assertEqual(ANAState.CHANGE, 0x0F)

    def test_ana_state_names(self):
        """Test ANA state enum names."""
        self.assertEqual(ANAState.OPTIMIZED.name, 'OPTIMIZED')
        self.assertEqual(ANAState.NON_OPTIMIZED.name, 'NON_OPTIMIZED')
        self.assertEqual(ANAState.INACCESSIBLE.name, 'INACCESSIBLE')
        self.assertEqual(ANAState.PERSISTENT_LOSS.name, 'PERSISTENT_LOSS')
        self.assertEqual(ANAState.CHANGE.name, 'CHANGE')


class TestANAGroupDescriptor(unittest.TestCase):
    """Test ANA Group Descriptor dataclass."""

    def test_descriptor_creation(self):
        """Test creating an ANA Group Descriptor."""
        descriptor = ANAGroupDescriptor(
            ana_group_id=1,
            num_namespaces=2,
            change_count=100,
            ana_state=ANAState.OPTIMIZED,
            namespace_ids=[1, 2]
        )

        self.assertEqual(descriptor.ana_group_id, 1)
        self.assertEqual(descriptor.num_namespaces, 2)
        self.assertEqual(descriptor.change_count, 100)
        self.assertEqual(descriptor.ana_state, ANAState.OPTIMIZED)
        self.assertEqual(descriptor.namespace_ids, [1, 2])

    def test_descriptor_is_accessible(self):
        """Test is_accessible property for different states."""
        # Accessible states
        for state in [ANAState.OPTIMIZED, ANAState.NON_OPTIMIZED]:
            descriptor = ANAGroupDescriptor(
                ana_group_id=1,
                num_namespaces=0,
                change_count=0,
                ana_state=state,
                namespace_ids=[]
            )
            self.assertTrue(descriptor.is_accessible,
                            f"State {state.name} should be accessible")

        # Inaccessible states
        for state in [ANAState.INACCESSIBLE, ANAState.PERSISTENT_LOSS, ANAState.CHANGE]:
            descriptor = ANAGroupDescriptor(
                ana_group_id=1,
                num_namespaces=0,
                change_count=0,
                ana_state=state,
                namespace_ids=[]
            )
            self.assertFalse(descriptor.is_accessible,
                             f"State {state.name} should not be accessible")

    def test_descriptor_is_optimized(self):
        """Test is_optimized property."""
        # Only OPTIMIZED state
        descriptor = ANAGroupDescriptor(
            ana_group_id=1,
            num_namespaces=0,
            change_count=0,
            ana_state=ANAState.OPTIMIZED,
            namespace_ids=[]
        )
        self.assertTrue(descriptor.is_optimized)

        # All other states
        for state in [ANAState.NON_OPTIMIZED, ANAState.INACCESSIBLE,
                      ANAState.PERSISTENT_LOSS, ANAState.CHANGE]:
            descriptor = ANAGroupDescriptor(
                ana_group_id=1,
                num_namespaces=0,
                change_count=0,
                ana_state=state,
                namespace_ids=[]
            )
            self.assertFalse(descriptor.is_optimized,
                             f"State {state.name} should not be optimized")


class TestANALogPage(unittest.TestCase):
    """Test ANA Log Page dataclass and helper methods."""

    def test_log_page_creation(self):
        """Test creating an ANA Log Page."""
        groups = [
            ANAGroupDescriptor(1, 2, 100, ANAState.OPTIMIZED, [1, 2]),
            ANAGroupDescriptor(2, 1, 50, ANAState.INACCESSIBLE, [3])
        ]

        log_page = ANALogPage(
            change_count=200,
            num_ana_group_descriptors=2,
            groups=groups
        )

        self.assertEqual(log_page.change_count, 200)
        self.assertEqual(log_page.num_ana_group_descriptors, 2)
        self.assertEqual(len(log_page.groups), 2)

    def test_get_group(self):
        """Test retrieving a specific ANA group by ID."""
        groups = [
            ANAGroupDescriptor(1, 2, 100, ANAState.OPTIMIZED, [1, 2]),
            ANAGroupDescriptor(2, 1, 50, ANAState.INACCESSIBLE, [3])
        ]

        log_page = ANALogPage(
            change_count=200,
            num_ana_group_descriptors=2,
            groups=groups
        )

        # Find existing group
        group = log_page.get_group(1)
        self.assertIsNotNone(group)
        self.assertEqual(group.ana_group_id, 1)

        # Non-existent group
        group = log_page.get_group(99)
        self.assertIsNone(group)

    def test_get_namespace_state(self):
        """Test retrieving ANA state for a specific namespace."""
        groups = [
            ANAGroupDescriptor(1, 2, 100, ANAState.OPTIMIZED, [1, 2]),
            ANAGroupDescriptor(2, 1, 50, ANAState.INACCESSIBLE, [3])
        ]

        log_page = ANALogPage(
            change_count=200,
            num_ana_group_descriptors=2,
            groups=groups
        )

        # Namespace in first group
        state = log_page.get_namespace_state(1)
        self.assertEqual(state, ANAState.OPTIMIZED)

        # Namespace in second group
        state = log_page.get_namespace_state(3)
        self.assertEqual(state, ANAState.INACCESSIBLE)

        # Non-existent namespace
        state = log_page.get_namespace_state(99)
        self.assertIsNone(state)

    def test_optimized_groups_property(self):
        """Test optimized_groups property."""
        groups = [
            ANAGroupDescriptor(1, 2, 100, ANAState.OPTIMIZED, [1, 2]),
            ANAGroupDescriptor(2, 1, 50, ANAState.NON_OPTIMIZED, [3]),
            ANAGroupDescriptor(3, 1, 75, ANAState.OPTIMIZED, [4]),
            ANAGroupDescriptor(4, 1, 25, ANAState.INACCESSIBLE, [5])
        ]

        log_page = ANALogPage(
            change_count=200,
            num_ana_group_descriptors=4,
            groups=groups
        )

        optimized = log_page.optimized_groups
        self.assertEqual(len(optimized), 2)
        self.assertEqual(optimized[0].ana_group_id, 1)
        self.assertEqual(optimized[1].ana_group_id, 3)

    def test_accessible_groups_property(self):
        """Test accessible_groups property."""
        groups = [
            ANAGroupDescriptor(1, 2, 100, ANAState.OPTIMIZED, [1, 2]),
            ANAGroupDescriptor(2, 1, 50, ANAState.NON_OPTIMIZED, [3]),
            ANAGroupDescriptor(3, 1, 75, ANAState.INACCESSIBLE, [4]),
            ANAGroupDescriptor(4, 1, 25, ANAState.CHANGE, [5])
        ]

        log_page = ANALogPage(
            change_count=200,
            num_ana_group_descriptors=4,
            groups=groups
        )

        accessible = log_page.accessible_groups
        self.assertEqual(len(accessible), 2)
        self.assertEqual(accessible[0].ana_group_id, 1)
        self.assertEqual(accessible[1].ana_group_id, 2)


class TestANALogPageParser(unittest.TestCase):
    """Test ANA Log Page parser."""

    def test_parse_empty_log_page(self):
        """Test parsing ANA log page with no groups."""
        # Build log page: change_count=100, num_descriptors=0
        log_data = struct.pack('<Q', 100)  # Change count (8 bytes)
        log_data += struct.pack('<H', 0)   # Number of descriptors (2 bytes)
        log_data += b'\x00' * 6            # Reserved (6 bytes)

        result = ANALogPageParser.parse_ana_log_page(log_data)

        self.assertIsInstance(result, ANALogPage)
        self.assertEqual(result.change_count, 100)
        self.assertEqual(result.num_ana_group_descriptors, 0)
        self.assertEqual(len(result.groups), 0)

    def test_parse_single_group_no_namespaces(self):
        """Test parsing single ANA group with no namespaces."""
        # Build log page header
        log_data = struct.pack('<Q', 200)  # Change count
        log_data += struct.pack('<H', 1)   # Number of descriptors
        log_data += b'\x00' * 6            # Reserved

        # Build ANA Group Descriptor (32 bytes)
        log_data += struct.pack('<L', 1)   # ANA Group ID
        log_data += struct.pack('<L', 0)   # Number of namespaces
        log_data += struct.pack('<Q', 50)  # Change count
        log_data += struct.pack('B', 0x01)  # ANA State (OPTIMIZED)
        log_data += b'\x00' * 15           # Reserved

        result = ANALogPageParser.parse_ana_log_page(log_data)

        self.assertEqual(result.change_count, 200)
        self.assertEqual(result.num_ana_group_descriptors, 1)
        self.assertEqual(len(result.groups), 1)

        group = result.groups[0]
        self.assertEqual(group.ana_group_id, 1)
        self.assertEqual(group.num_namespaces, 0)
        self.assertEqual(group.change_count, 50)
        self.assertEqual(group.ana_state, ANAState.OPTIMIZED)
        self.assertEqual(len(group.namespace_ids), 0)

    def test_parse_single_group_with_namespaces(self):
        """Test parsing single ANA group with namespaces."""
        # Build log page header
        log_data = struct.pack('<Q', 300)  # Change count
        log_data += struct.pack('<H', 1)   # Number of descriptors
        log_data += b'\x00' * 6            # Reserved

        # Build ANA Group Descriptor with 3 namespaces
        log_data += struct.pack('<L', 2)   # ANA Group ID
        log_data += struct.pack('<L', 3)   # Number of namespaces
        log_data += struct.pack('<Q', 75)  # Change count
        log_data += struct.pack('B', 0x02)  # ANA State (NON_OPTIMIZED)
        log_data += b'\x00' * 15           # Reserved
        log_data += struct.pack('<L', 1)   # Namespace ID 1
        log_data += struct.pack('<L', 2)   # Namespace ID 2
        log_data += struct.pack('<L', 3)   # Namespace ID 3

        result = ANALogPageParser.parse_ana_log_page(log_data)

        self.assertEqual(result.num_ana_group_descriptors, 1)
        self.assertEqual(len(result.groups), 1)

        group = result.groups[0]
        self.assertEqual(group.ana_group_id, 2)
        self.assertEqual(group.num_namespaces, 3)
        self.assertEqual(group.change_count, 75)
        self.assertEqual(group.ana_state, ANAState.NON_OPTIMIZED)
        self.assertEqual(group.namespace_ids, [1, 2, 3])

    def test_parse_multiple_groups(self):
        """Test parsing multiple ANA groups."""
        # Build log page header
        log_data = struct.pack('<Q', 400)  # Change count
        log_data += struct.pack('<H', 2)   # Number of descriptors
        log_data += b'\x00' * 6            # Reserved

        # First ANA Group Descriptor (2 namespaces)
        log_data += struct.pack('<L', 1)   # ANA Group ID
        log_data += struct.pack('<L', 2)   # Number of namespaces
        log_data += struct.pack('<Q', 100)  # Change count
        log_data += struct.pack('B', 0x01)  # ANA State (OPTIMIZED)
        log_data += b'\x00' * 15           # Reserved
        log_data += struct.pack('<L', 1)   # Namespace ID 1
        log_data += struct.pack('<L', 2)   # Namespace ID 2

        # Second ANA Group Descriptor (1 namespace)
        log_data += struct.pack('<L', 2)   # ANA Group ID
        log_data += struct.pack('<L', 1)   # Number of namespaces
        log_data += struct.pack('<Q', 50)  # Change count
        log_data += struct.pack('B', 0x03)  # ANA State (INACCESSIBLE)
        log_data += b'\x00' * 15           # Reserved
        log_data += struct.pack('<L', 3)   # Namespace ID 3

        result = ANALogPageParser.parse_ana_log_page(log_data)

        self.assertEqual(result.change_count, 400)
        self.assertEqual(result.num_ana_group_descriptors, 2)
        self.assertEqual(len(result.groups), 2)

        # Check first group
        group1 = result.groups[0]
        self.assertEqual(group1.ana_group_id, 1)
        self.assertEqual(group1.num_namespaces, 2)
        self.assertEqual(group1.ana_state, ANAState.OPTIMIZED)
        self.assertEqual(group1.namespace_ids, [1, 2])

        # Check second group
        group2 = result.groups[1]
        self.assertEqual(group2.ana_group_id, 2)
        self.assertEqual(group2.num_namespaces, 1)
        self.assertEqual(group2.ana_state, ANAState.INACCESSIBLE)
        self.assertEqual(group2.namespace_ids, [3])

    def test_parse_unknown_ana_state(self):
        """Test parsing with unknown ANA state defaults to CHANGE."""
        # Build log page header
        log_data = struct.pack('<Q', 500)  # Change count
        log_data += struct.pack('<H', 1)   # Number of descriptors
        log_data += b'\x00' * 6            # Reserved

        # Build descriptor with invalid state value
        log_data += struct.pack('<L', 1)   # ANA Group ID
        log_data += struct.pack('<L', 0)   # Number of namespaces
        log_data += struct.pack('<Q', 25)  # Change count
        log_data += struct.pack('B', 0xFF)  # Invalid ANA State (only lower 4 bits used)
        log_data += b'\x00' * 15           # Reserved

        result = ANALogPageParser.parse_ana_log_page(log_data)

        group = result.groups[0]
        # Invalid state (0x0F after masking) should map to CHANGE
        self.assertEqual(group.ana_state, ANAState.CHANGE)

    def test_parse_truncated_header(self):
        """Test parsing with truncated header raises error."""
        log_data = b'\x00' * 10  # Only 10 bytes, need 16

        with self.assertRaises(ValueError) as context:
            ANALogPageParser.parse_ana_log_page(log_data)

        self.assertIn("ANA log page", str(context.exception))

    def test_parse_truncated_descriptor_header(self):
        """Test parsing with truncated descriptor header raises error."""
        # Valid log page header
        log_data = struct.pack('<Q', 100)  # Change count
        log_data += struct.pack('<H', 1)   # Number of descriptors
        log_data += b'\x00' * 6            # Reserved

        # Truncated descriptor (only 20 bytes, need 32)
        log_data += b'\x00' * 20

        with self.assertRaises(ValueError) as context:
            ANALogPageParser.parse_ana_log_page(log_data)

        self.assertIn("Insufficient data", str(context.exception))
        self.assertIn("ANA Group Descriptor", str(context.exception))

    def test_parse_truncated_namespace_list(self):
        """Test parsing with truncated namespace list raises error."""
        # Build log page header
        log_data = struct.pack('<Q', 100)  # Change count
        log_data += struct.pack('<H', 1)   # Number of descriptors
        log_data += b'\x00' * 6            # Reserved

        # Build descriptor header claiming 5 namespaces
        log_data += struct.pack('<L', 1)   # ANA Group ID
        log_data += struct.pack('<L', 5)   # Number of namespaces (claims 5)
        log_data += struct.pack('<Q', 50)  # Change count
        log_data += struct.pack('B', 0x01)  # ANA State
        log_data += b'\x00' * 15           # Reserved

        # Only provide 2 namespace IDs (need 5 * 4 = 20 bytes, provide 8)
        log_data += struct.pack('<L', 1)   # Namespace ID 1
        log_data += struct.pack('<L', 2)   # Namespace ID 2

        with self.assertRaises(ValueError) as context:
            ANALogPageParser.parse_ana_log_page(log_data)

        self.assertIn("Insufficient data", str(context.exception))
        self.assertIn("namespace ID list", str(context.exception))


if __name__ == '__main__':
    unittest.main()
