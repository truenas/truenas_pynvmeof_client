"""
Integration tests for basic NVMe-oF operations

Tests basic connectivity, discovery, and administrative operations
against a live NVMe-oF target.
"""

import pytest
from nvmeof_client.client import NVMeoFClient
from nvmeof_client.exceptions import NVMeoFConnectionError
from nvmeof_client.models import (
    AddressFamily,
    ControllerInfo,
    DiscoveryEntry,
    NamespaceInfo,
    TransportType,
)


@pytest.mark.integration
class TestBasicConnectivity:
    """Test basic connection operations."""

    def test_connect_disconnect(self, target_config):
        """Test basic connection and disconnection."""

        client = NVMeoFClient(target_config['host'], port=target_config['port'])

        # Should not be connected initially
        assert not client.is_connected

        # Connect
        client.connect()
        assert client.is_connected

        # Disconnect
        client.disconnect()
        assert not client.is_connected

    def test_double_connect_error(self, client):
        """Test that connecting when already connected raises error."""
        assert client.is_connected

        with pytest.raises(NVMeoFConnectionError):
            client.connect()

    def test_disconnect_when_not_connected(self, target_config):
        """Test disconnecting when not connected."""

        client = NVMeoFClient(target_config['host'], port=target_config['port'])

        # Should not raise error
        client.disconnect()
        assert not client.is_connected


@pytest.mark.integration
class TestDiscoveryOperations:
    """Test discovery subsystem operations."""

    def test_discovery_connection(self, discovery_client):
        """Test connection to discovery subsystem."""
        assert discovery_client.is_connected
        # Additional discovery-specific assertions could go here

    def test_get_discovery_log(self, discovery_client):
        """Test retrieving discovery log."""
        entries = discovery_client.discover_subsystems()

        assert isinstance(entries, list)
        # Should have at least one entry (the target itself)
        assert len(entries) >= 1

        for entry in entries:
            assert isinstance(entry, dict)
            assert entry.get('transport_address')
            assert entry.get('subsystem_nqn')
            assert entry.get('transport_service_id')

    def test_get_discovery_entries(self, discovery_client):
        """Test high-level discovery method that returns DiscoveryEntry objects."""
        entries = discovery_client.get_discovery_entries()

        assert isinstance(entries, list)
        # Should have at least one entry (the target itself)
        assert len(entries) >= 1

        for entry in entries:
            # Verify it's a DiscoveryEntry object, not a dict
            assert isinstance(entry, DiscoveryEntry)

            # Test typed attributes (not dict access)
            assert entry.transport_address
            assert entry.subsystem_nqn
            assert entry.transport_service_id

            # Test enum types
            assert isinstance(entry.transport_type, TransportType)
            assert isinstance(entry.address_family, AddressFamily)

            # Test helper properties
            assert isinstance(entry.is_nvme_subsystem, bool)
            assert isinstance(entry.is_discovery_subsystem, bool)

            # At least one should be true (either NVMe or Discovery subsystem)
            assert entry.is_nvme_subsystem or entry.is_discovery_subsystem


@pytest.mark.integration
class TestControllerOperations:
    """Test controller identification and properties."""

    def test_identify_controller(self, nvme_client):
        """Test controller identification."""
        controller_data = nvme_client.identify_controller()

        assert isinstance(controller_data, dict)
        # Vendor ID can be 0 for some targets, so just check it exists
        assert 'vid' in controller_data
        assert controller_data.get('sn')  # Serial Number
        assert controller_data.get('mn')  # Model Number
        assert controller_data.get('fr')  # Firmware Revision
        # mdts can be 0 for unlimited, so check for presence
        assert 'mdts' in controller_data

    def test_get_controller_capabilities(self, nvme_client):
        """Test getting controller capabilities."""
        capabilities = nvme_client.get_controller_capabilities()

        assert isinstance(capabilities, dict)
        assert 'max_queue_entries_supported' in capabilities
        assert 'timeout' in capabilities
        assert 'doorbell_stride' in capabilities
        assert capabilities['max_queue_entries_supported'] > 0
        assert capabilities['timeout'] > 0

    def test_get_controller_info(self, nvme_client):
        """Test high-level controller info method that returns ControllerInfo object."""
        controller_info = nvme_client.get_controller_info()

        assert isinstance(controller_info, ControllerInfo)
        assert hasattr(controller_info, 'vendor_id')
        assert hasattr(controller_info, 'serial_number')
        assert hasattr(controller_info, 'model_number')
        assert hasattr(controller_info, 'firmware_revision')
        # Verify actual values
        assert controller_info.serial_number
        assert controller_info.model_number


@pytest.mark.integration
class TestNamespaceOperations:
    """Test namespace identification."""

    def test_identify_namespace(self, nvme_client, test_namespace_id):
        """Test namespace identification."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)

        assert isinstance(ns_data, dict)
        assert ns_data.get('nsze', 0) > 0  # Namespace Size
        # Use logical_block_size field instead of lbaf0_lbads for TrueNAS targets
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            assert logical_block_size in [512, 4096]  # Common block sizes
        else:
            # Fallback to lbaf0_lbads if available
            lbads = ns_data.get('lbaf0_lbads', 0)
            if lbads > 0:
                assert lbads in [9, 12]  # 2^9=512, 2^12=4096

    def test_list_namespaces(self, nvme_client):
        """Test listing namespaces."""
        namespaces = nvme_client.list_namespaces()

        assert isinstance(namespaces, list)
        assert len(namespaces) >= 1  # Should have at least one namespace

        for nsid in namespaces:
            assert isinstance(nsid, int)
            assert nsid > 0

    def test_get_namespace_info(self, nvme_client, test_namespace_id):
        """Test high-level namespace info method that returns NamespaceInfo object."""
        ns_info = nvme_client.get_namespace_info(test_namespace_id)

        assert isinstance(ns_info, NamespaceInfo)
        assert hasattr(ns_info, 'namespace_id')
        assert hasattr(ns_info, 'namespace_size')
        assert hasattr(ns_info, 'logical_block_size')
        # Verify actual values
        assert ns_info.namespace_id == test_namespace_id
        assert ns_info.namespace_size > 0
        assert ns_info.logical_block_size in [512, 4096]  # Common block sizes


@pytest.mark.integration
@pytest.mark.slow
class TestQueueManagement:
    """Test I/O queue creation and management."""

    def test_setup_io_queues(self, nvme_client):
        """Test I/O queue setup."""
        # This should work without errors
        nvme_client.setup_io_queues()

        # Verify queues are set up (implementation dependent)
        # Could check internal state if exposed

    def test_queue_creation_parameters(self, nvme_client):
        """Test queue creation with specific parameters."""
        # Test with different queue sizes
        nvme_client.setup_io_queues(queue_size=32)
        nvme_client.setup_io_queues(queue_size=64)
        nvme_client.setup_io_queues(queue_size=128)


@pytest.mark.integration
@pytest.mark.slow
class TestIOOperations:
    """Test basic I/O operations (read/write)."""

    def test_basic_write_read(self, nvme_client, test_namespace_id):
        """Test basic write and read operations."""
        # Get namespace info to determine block size
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        # Test data - fill with pattern
        test_pattern = b'NVMeoF_TEST_DATA_PATTERN_'
        # Pad or truncate to exact block size
        if len(test_pattern) > block_size:
            test_data = test_pattern[:block_size]
        else:
            # Repeat pattern to fill block
            repeats = (block_size + len(test_pattern) - 1) // len(test_pattern)
            test_data = (test_pattern * repeats)[:block_size]

        lba = 0  # Start at LBA 0
        num_blocks = 1

        # Write data
        nvme_client.write_data(test_namespace_id, lba, test_data)

        # Read back the data
        read_data = nvme_client.read_data(test_namespace_id, lba, num_blocks)

        # Verify data matches
        assert len(read_data) == block_size
        assert read_data == test_data

    def test_multi_block_write_read(self, nvme_client, test_namespace_id):
        """Test multi-block write and read operations."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        num_blocks = 4
        lba = 10  # Use a different LBA than single block test

        # Create test data for multiple blocks
        test_data = b''
        for i in range(num_blocks):
            block_pattern = f'BLOCK_{i:02d}_TEST_DATA_'.encode()
            # Pad each block to block_size
            if len(block_pattern) > block_size:
                block_data = block_pattern[:block_size]
            else:
                repeats = (block_size + len(block_pattern) - 1) // len(block_pattern)
                block_data = (block_pattern * repeats)[:block_size]
            test_data += block_data

        # Write multiple blocks
        nvme_client.write_data(test_namespace_id, lba, test_data)

        # Read back multiple blocks
        read_data = nvme_client.read_data(test_namespace_id, lba, num_blocks)

        # Verify data
        assert len(read_data) == block_size * num_blocks
        assert read_data == test_data

    def test_read_write_different_lbas(self, nvme_client, test_namespace_id):
        """Test read/write at different LBA addresses."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        # Test different LBAs
        test_lbas = [0, 1, 100, 1000]

        namespace_size = ns_data.get('nsze', 0)

        for lba in test_lbas:
            # Skip if LBA exceeds namespace size
            if lba >= namespace_size:
                continue

            # Create unique test data for each LBA
            test_pattern = f'LBA_{lba}_DATA_'.encode()
            if len(test_pattern) > block_size:
                test_data = test_pattern[:block_size]
            else:
                repeats = (block_size + len(test_pattern) - 1) // len(test_pattern)
                test_data = (test_pattern * repeats)[:block_size]

            # Write and read back
            nvme_client.write_data(test_namespace_id, lba, test_data)
            read_data = nvme_client.read_data(test_namespace_id, lba, 1)

            assert read_data == test_data

    def test_read_without_prior_write(self, nvme_client, test_namespace_id):
        """Test reading from unwritten areas (should not fail)."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads
        namespace_size = ns_data.get('nsze', 0)

        # Read from a high LBA that likely hasn't been written
        high_lba = min(50000, namespace_size - 1)

        # This should succeed (data content is undefined but operation should work)
        data = nvme_client.read_data(test_namespace_id, high_lba, 1)
        assert len(data) == block_size

    def test_flush_operation(self, nvme_client, test_namespace_id):
        """Test flush operation."""
        # Write some data first
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads
        test_data = b'FLUSH_TEST_' + b'X' * (block_size - 11)

        nvme_client.write_data(test_namespace_id, 0, test_data)

        # Flush should complete without error
        nvme_client.flush_namespace(test_namespace_id)

        # Verify data is still there after flush
        read_data = nvme_client.read_data(test_namespace_id, 0, 1)
        assert read_data == test_data

    def test_large_io_operation(self, nvme_client, test_namespace_id):
        """Test larger I/O operation (multiple blocks)."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads
        namespace_size = ns_data.get('nsze', 0)

        # Use more blocks for larger I/O
        num_blocks = min(16, namespace_size // 2)  # Don't exceed namespace
        lba = 1000

        if lba + num_blocks > namespace_size:
            pytest.skip(f"Namespace too small for large I/O test (size: {namespace_size})")

        # Create larger test data
        test_data = b''
        for i in range(num_blocks):
            block_data = f'LARGE_IO_BLOCK_{i:04d}_'.encode()
            block_data += b'X' * (block_size - len(block_data))
            test_data += block_data

        # Perform large write
        nvme_client.write_data(test_namespace_id, lba, test_data)

        # Perform large read
        read_data = nvme_client.read_data(test_namespace_id, lba, num_blocks)

        assert len(read_data) == block_size * num_blocks
        assert read_data == test_data


@pytest.mark.integration
class TestIOErrorHandling:
    """Test I/O error conditions."""

    def test_write_beyond_namespace_size(self, nvme_client, test_namespace_id):
        """Test writing beyond namespace boundaries."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads
        namespace_size = ns_data.get('nsze', 0)

        # Try to write beyond the namespace
        invalid_lba = namespace_size  # One past the end
        test_data = b'X' * block_size

        with pytest.raises(Exception):  # Should raise some form of error
            nvme_client.write_data(test_namespace_id, invalid_lba, test_data)

    def test_read_beyond_namespace_size(self, nvme_client, test_namespace_id):
        """Test reading beyond namespace boundaries."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        namespace_size = ns_data.get('nsze', 0)

        # Try to read beyond the namespace
        invalid_lba = namespace_size  # One past the end

        with pytest.raises(Exception):  # Should raise some form of error
            nvme_client.read_data(test_namespace_id, invalid_lba, 1)

    def test_zero_block_io(self, nvme_client, test_namespace_id):
        """Test I/O with zero blocks (should fail)."""

        with pytest.raises(Exception):  # Should raise validation error
            nvme_client.write_data(test_namespace_id, 0, b'')

        with pytest.raises(Exception):  # Should raise validation error
            nvme_client.read_data(test_namespace_id, 0, 0)

    def test_mismatched_data_size(self, nvme_client, test_namespace_id):
        """Test write with incorrect data size."""
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        # Provide data that doesn't match the number of blocks
        wrong_size_data = b'X' * (block_size // 2)  # Half a block

        with pytest.raises(Exception):  # Should raise validation error
            nvme_client.write_data(test_namespace_id, 0, wrong_size_data)


@pytest.mark.integration
class TestErrorHandling:
    """Test error handling with live target."""

    def test_invalid_namespace_id(self, nvme_client):
        """Test operations with invalid namespace ID."""
        with pytest.raises(Exception):  # Should raise some form of error
            nvme_client.identify_namespace(0xFFFFFFFF)  # Invalid NSID

    def test_connection_timeout(self, target_config):
        """Test connection timeout behavior."""

        # Use very short timeout
        client = NVMeoFClient(target_config['host'], port=target_config['port'], timeout=0.001)

        # This might succeed if target is very fast, or timeout
        # Exact behavior depends on network conditions
        try:
            client.connect()
            client.disconnect()
        except Exception:
            # Timeout or other connection error expected
            pass
