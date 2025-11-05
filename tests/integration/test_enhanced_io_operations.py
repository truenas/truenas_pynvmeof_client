"""
Enhanced I/O integration tests with rigorous data verification

Tests I/O operations with proper data reset and verification to ensure
no false positives from historical data. Each test follows a strict
reset-write-verify-cleanup cycle.
"""

import pytest
from nvmeof_client.client import NVMeoFClient


@pytest.mark.integration
class TestEnhancedIOOperations:
    """Enhanced I/O tests with rigorous data verification."""

    def test_write_read_with_data_reset(self, nvme_client, test_namespace_id):
        """Test write/read with proper data reset to prevent false positives."""
        # Get namespace info
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        lba = 42  # Use a specific LBA for this test

        # Step 1: Reset the LBA to zeros
        zero_data = b'\x00' * block_size
        nvme_client.write_data(test_namespace_id, lba, zero_data)

        # Step 2: Verify it's actually zeros (ensures WRITE works for reset)
        reset_data = nvme_client.read_data(test_namespace_id, lba, 1)
        assert len(reset_data) == block_size
        assert reset_data == zero_data, "Failed to reset LBA to zeros"

        # Step 3: Write unique test pattern
        test_pattern = f"ENHANCED_TEST_LBA_{lba}_".encode()
        repeats = (block_size + len(test_pattern) - 1) // len(test_pattern)
        test_data = (test_pattern * repeats)[:block_size]

        nvme_client.write_data(test_namespace_id, lba, test_data)

        # Step 4: Read back and verify (ensures READ correctly retrieves written data)
        read_data = nvme_client.read_data(test_namespace_id, lba, 1)
        assert len(read_data) == block_size
        assert read_data == test_data, "Data verification failed - written data doesn't match read data"

        # Step 5: Clean up - reset to zeros again
        nvme_client.write_data(test_namespace_id, lba, zero_data)

        # Step 6: Verify cleanup (ensures subsequent tests start clean)
        cleanup_data = nvme_client.read_data(test_namespace_id, lba, 1)
        assert cleanup_data == zero_data, "Failed to clean up LBA"

    def test_multiple_lba_data_integrity(self, nvme_client, test_namespace_id):
        """Test data integrity across multiple LBAs with unique patterns."""
        # Get namespace info
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        namespace_size = ns_data.get('nsze', 0)

        # Test various LBAs
        test_lbas = [0, 1, 10, 100, 500, 1000]

        # Filter out LBAs that exceed namespace size
        valid_lbas = [lba for lba in test_lbas if lba < namespace_size]
        if not valid_lbas:
            pytest.skip("No valid LBAs available for testing")

        zero_data = b'\x00' * block_size
        written_data = {}

        try:
            # Phase 1: Reset all test LBAs to zeros
            for lba in valid_lbas:
                nvme_client.write_data(test_namespace_id, lba, zero_data)

                # Verify reset
                reset_data = nvme_client.read_data(test_namespace_id, lba, 1)
                assert reset_data == zero_data, f"Failed to reset LBA {lba}"

            # Phase 2: Write unique patterns to each LBA
            for lba in valid_lbas:
                test_pattern = f"MULTI_LBA_TEST_{lba}_DATA_".encode()
                repeats = (block_size + len(test_pattern) - 1) // len(test_pattern)
                test_data = (test_pattern * repeats)[:block_size]
                written_data[lba] = test_data

                nvme_client.write_data(test_namespace_id, lba, test_data)

            # Phase 3: Verify all written data in random order
            import random
            verification_order = valid_lbas.copy()
            random.shuffle(verification_order)

            for lba in verification_order:
                read_data = nvme_client.read_data(test_namespace_id, lba, 1)
                expected_data = written_data[lba]
                assert len(read_data) == block_size
                assert read_data == expected_data, f"Data verification failed for LBA {lba}"

        finally:
            # Phase 4: Clean up all test LBAs
            for lba in valid_lbas:
                try:
                    nvme_client.write_data(test_namespace_id, lba, zero_data)
                except Exception:
                    pass  # Best effort cleanup

    def test_write_read_edge_patterns(self, nvme_client, test_namespace_id):
        """Test write/read with edge case data patterns."""
        # Get namespace info
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        lba = 777  # Use a specific LBA for edge pattern testing
        zero_data = b'\x00' * block_size

        # Test patterns: all zeros, all ones, alternating, incrementing
        test_patterns = [
            ("all_zeros", b'\x00' * block_size),
            ("all_ones", b'\xFF' * block_size),
            ("alternating_55", b'\x55' * block_size),
            ("alternating_AA", b'\xAA' * block_size),
            ("incrementing", bytes(i % 256 for i in range(block_size))),
        ]

        try:
            for pattern_name, pattern_data in test_patterns:
                # Reset LBA
                nvme_client.write_data(test_namespace_id, lba, zero_data)
                reset_data = nvme_client.read_data(test_namespace_id, lba, 1)
                assert reset_data == zero_data, f"Failed to reset LBA for {pattern_name} test"

                # Write pattern
                nvme_client.write_data(test_namespace_id, lba, pattern_data)

                # Read and verify
                read_data = nvme_client.read_data(test_namespace_id, lba, 1)
                assert len(read_data) == block_size
                assert read_data == pattern_data, f"Pattern verification failed for {pattern_name}"

        finally:
            # Clean up
            try:
                nvme_client.write_data(test_namespace_id, lba, zero_data)
            except Exception:
                pass  # Best effort cleanup

    def test_multi_block_write_read_with_reset(self, nvme_client, test_namespace_id):
        """Test multi-block operations with proper reset and verification."""
        # Get namespace info
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)  # Default to 512 bytes (2^9)
            block_size = 2 ** lbads

        start_lba = 2000
        num_blocks = 4
        total_size = block_size * num_blocks

        zero_data = b'\x00' * total_size

        try:
            # Step 1: Reset all blocks to zeros
            nvme_client.write_data(test_namespace_id, start_lba, zero_data)

            # Step 2: Verify reset
            reset_data = nvme_client.read_data(test_namespace_id, start_lba, num_blocks)
            assert len(reset_data) == total_size
            assert reset_data == zero_data, "Failed to reset multi-block range"

            # Step 3: Create unique test data for each block
            test_data = b''
            for i in range(num_blocks):
                block_pattern = f'MULTIBLOCK_{i}_TEST_'.encode()
                repeats = (block_size + len(block_pattern) - 1) // len(block_pattern)
                block_data = (block_pattern * repeats)[:block_size]
                test_data += block_data

            # Step 4: Write multi-block data
            nvme_client.write_data(test_namespace_id, start_lba, test_data)

            # Step 5: Read back multi-block data
            read_data = nvme_client.read_data(test_namespace_id, start_lba, num_blocks)
            assert len(read_data) == total_size
            assert read_data == test_data, "Multi-block data verification failed"

            # Step 6: Verify individual blocks
            for i in range(num_blocks):
                block_start = i * block_size
                block_end = block_start + block_size
                expected_block = test_data[block_start:block_end]

                single_block_data = nvme_client.read_data(test_namespace_id, start_lba + i, 1)
                assert len(single_block_data) == block_size
                assert single_block_data == expected_block, f"Individual block {i} verification failed"

        finally:
            # Clean up
            try:
                nvme_client.write_data(test_namespace_id, start_lba, zero_data)
            except Exception:
                pass  # Best effort cleanup


@pytest.mark.integration
class TestDataIntegrityValidation:
    """Additional data integrity validation tests."""

    def test_write_read_consistency_across_sessions(self, target_config, test_namespace_id):
        """Test that data persists correctly across client sessions."""

        # Get block size from first connection
        client1 = NVMeoFClient(target_config['host'], target_config['nqn'],
                               target_config['port'], target_config['timeout'])
        client1.connect()

        try:
            ns_data = client1.identify_namespace(test_namespace_id)
            logical_block_size = ns_data.get('logical_block_size')
            if logical_block_size:
                block_size = logical_block_size
            else:
                lbads = ns_data.get('lbaf0_lbads', 9)
                block_size = 2 ** lbads

            lba = 9999
            zero_data = b'\x00' * block_size

            # Reset and write data in first session
            client1.write_data(test_namespace_id, lba, zero_data)

            test_pattern = b"PERSISTENCE_TEST_DATA_"
            repeats = (block_size + len(test_pattern) - 1) // len(test_pattern)
            test_data = (test_pattern * repeats)[:block_size]

            client1.write_data(test_namespace_id, lba, test_data)

            # Verify in first session
            read_data1 = client1.read_data(test_namespace_id, lba, 1)
            assert read_data1 == test_data, "Data verification failed in first session"

        finally:
            client1.disconnect()

        # Create new session and verify data persists
        client2 = NVMeoFClient(target_config['host'], target_config['nqn'],
                               target_config['port'], target_config['timeout'])
        client2.connect()

        try:
            # Read data in second session
            read_data2 = client2.read_data(test_namespace_id, lba, 1)
            assert len(read_data2) == block_size
            assert read_data2 == test_data, "Data persistence failed across sessions"

        finally:
            # Clean up in second session
            try:
                zero_data = b'\x00' * block_size
                client2.write_data(test_namespace_id, lba, zero_data)
            except Exception:
                pass
            client2.disconnect()

    def test_concurrent_lba_isolation(self, nvme_client, test_namespace_id):
        """Test that writes to different LBAs don't interfere with each other."""
        # Get namespace info
        ns_data = nvme_client.identify_namespace(test_namespace_id)
        logical_block_size = ns_data.get('logical_block_size')
        if logical_block_size:
            block_size = logical_block_size
        else:
            lbads = ns_data.get('lbaf0_lbads', 9)
            block_size = 2 ** lbads

        # Use well-separated LBAs
        lba1, lba2, lba3 = 1111, 2222, 3333
        zero_data = b'\x00' * block_size

        try:
            # Reset all test LBAs
            for lba in [lba1, lba2, lba3]:
                nvme_client.write_data(test_namespace_id, lba, zero_data)

            # Write different patterns to each LBA
            pattern1 = b'LBA1_PATTERN_' + b'A' * (block_size - 13)
            pattern2 = b'LBA2_PATTERN_' + b'B' * (block_size - 13)
            pattern3 = b'LBA3_PATTERN_' + b'C' * (block_size - 13)

            nvme_client.write_data(test_namespace_id, lba1, pattern1)
            nvme_client.write_data(test_namespace_id, lba2, pattern2)
            nvme_client.write_data(test_namespace_id, lba3, pattern3)

            # Verify each LBA independently
            read1 = nvme_client.read_data(test_namespace_id, lba1, 1)
            read2 = nvme_client.read_data(test_namespace_id, lba2, 1)
            read3 = nvme_client.read_data(test_namespace_id, lba3, 1)

            assert read1 == pattern1, "LBA1 data corrupted"
            assert read2 == pattern2, "LBA2 data corrupted"
            assert read3 == pattern3, "LBA3 data corrupted"

            # Verify patterns are actually different
            assert pattern1 != pattern2 != pattern3, "Test patterns should be different"
            assert read1 != read2 != read3, "Read data should be different"

        finally:
            # Clean up all test LBAs
            for lba in [lba1, lba2, lba3]:
                try:
                    nvme_client.write_data(test_namespace_id, lba, zero_data)
                except Exception:
                    pass
