"""
Integration tests for ANA (Asymmetric Namespace Access) functionality

Tests ANA log page retrieval and state querying against a live NVMe-oF target.
Requires a target with ANA support enabled.
"""

import pytest
from nvmeof_client.models import ANAState, ANALogPage, ANAGroupDescriptor
from nvmeof_client.exceptions import CommandError


@pytest.mark.integration
class TestANACapabilities:
    """Test ANA capability detection."""

    def test_controller_has_ana_capabilities(self, nvme_client):
        """Test that controller reports ANA capabilities."""
        controller_data = nvme_client.identify_controller()

        assert isinstance(controller_data, dict)
        assert 'anacap' in controller_data
        assert 'anatt' in controller_data
        assert 'anagrpmax' in controller_data
        assert 'nanagrpid' in controller_data

        # If anacap is non-zero, ANA is supported
        ana_supported = controller_data['anacap'] != 0

        if not ana_supported:
            pytest.skip("Target does not support ANA")

        # Verify reasonable values
        assert controller_data['anagrpmax'] > 0, "ANAGRPMAX should be > 0 if ANA supported"
        assert controller_data['nanagrpid'] >= 0, "NANAGRPID should be >= 0"


@pytest.mark.integration
class TestGenericLogPageRetrieval:
    """Test generic log page retrieval method."""

    def test_get_log_page_ana(self, nvme_client):
        """Test retrieving ANA log page via generic get_log_page() method."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Get ANA log page (Log Page ID 0x0C)
        # Start with 4KB which should be sufficient for most configurations
        log_data = nvme_client.get_log_page(log_page_id=0x0C, data_length=4096, nsid=0)

        assert isinstance(log_data, bytes)
        assert len(log_data) > 0
        # ANA log page should be at least 16 bytes (header)
        assert len(log_data) >= 16

    def test_get_log_page_discovery_not_allowed(self, discovery_client):
        """Test that get_log_page() correctly rejects calls on discovery subsystem."""
        from nvmeof_client.exceptions import NVMeoFConnectionError

        # get_log_page() should not work on discovery subsystem
        # Discovery has its own dedicated discover_subsystems() method
        with pytest.raises(NVMeoFConnectionError, match="discovery subsystem"):
            discovery_client.get_log_page(log_page_id=0x70, data_length=4096, nsid=0)


@pytest.mark.integration
class TestANALogPageRetrieval:
    """Test ANA-specific log page retrieval methods."""

    def test_get_ana_log_page(self, nvme_client):
        """Test retrieving ANA log page with parsing."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Get parsed ANA log page
        ana_log = nvme_client.get_ana_log_page()

        # Verify structure
        assert isinstance(ana_log, ANALogPage)
        assert hasattr(ana_log, 'change_count')
        assert hasattr(ana_log, 'num_ana_group_descriptors')
        assert hasattr(ana_log, 'groups')

        # Change count should be non-negative
        assert ana_log.change_count >= 0

        # Number of groups should match list length
        assert ana_log.num_ana_group_descriptors == len(ana_log.groups)

        # If there are groups, verify their structure
        for group in ana_log.groups:
            assert isinstance(group, ANAGroupDescriptor)
            assert hasattr(group, 'ana_group_id')
            assert hasattr(group, 'ana_state')
            assert hasattr(group, 'num_namespaces')
            assert hasattr(group, 'namespace_ids')

            # Verify ANA state is valid
            assert isinstance(group.ana_state, ANAState)
            assert group.ana_state in [
                ANAState.OPTIMIZED,
                ANAState.NON_OPTIMIZED,
                ANAState.INACCESSIBLE,
                ANAState.PERSISTENT_LOSS,
                ANAState.CHANGE
            ]

            # Number of namespaces should match list length
            assert group.num_namespaces == len(group.namespace_ids)

            # All namespace IDs should be positive
            for nsid in group.namespace_ids:
                assert nsid > 0

    def test_ana_log_page_change_count(self, nvme_client):
        """Test that ANA log page has a change count."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Get ANA log page twice
        ana_log1 = nvme_client.get_ana_log_page()
        ana_log2 = nvme_client.get_ana_log_page()

        # Both should have change counts
        assert hasattr(ana_log1, 'change_count')
        assert hasattr(ana_log2, 'change_count')

        # Change counts should be the same (no failover occurred)
        # or second is greater (failover occurred between calls)
        assert ana_log2.change_count >= ana_log1.change_count


@pytest.mark.integration
class TestANAStateQuery:
    """Test simplified ANA state query methods."""

    def test_get_ana_state(self, nvme_client):
        """Test simplified ANA state query."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Get simplified state dict
        ana_states = nvme_client.get_ana_state()

        assert isinstance(ana_states, dict)

        # Each key should be a group ID, each value should be an ANAState
        for group_id, state in ana_states.items():
            assert isinstance(group_id, int)
            assert group_id > 0
            assert isinstance(state, ANAState)

    def test_ana_state_consistency(self, nvme_client):
        """Test that get_ana_state() matches get_ana_log_page() results."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Get both forms
        ana_log = nvme_client.get_ana_log_page()
        ana_states = nvme_client.get_ana_state()

        # Number of groups should match
        assert len(ana_states) == len(ana_log.groups)

        # States should match for each group
        for group in ana_log.groups:
            assert group.ana_group_id in ana_states
            assert ana_states[group.ana_group_id] == group.ana_state


@pytest.mark.integration
class TestANAGroupMatching:
    """Test that ANA group IDs match namespace identification data."""

    def test_namespace_ana_group_id(self, nvme_client, test_namespace_id):
        """Test that namespace reports valid ANA group ID."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Identify the namespace
        ns_data = nvme_client.identify_namespace(test_namespace_id)

        assert isinstance(ns_data, dict)
        assert 'anagrpid' in ns_data

        # ANA group ID should be valid (0 means not supported, > 0 means valid)
        ana_group_id = ns_data['anagrpid']

        if ana_group_id == 0:
            pytest.skip("Namespace does not report ANA group membership")

        # Get ANA log page
        ana_log = nvme_client.get_ana_log_page()

        # Find the group for this namespace
        found_group = None
        for group in ana_log.groups:
            if test_namespace_id in group.namespace_ids:
                found_group = group
                break

        # Namespace should be in ANA log
        assert found_group is not None, f"Namespace {test_namespace_id} not found in any ANA group"

        # Group ID from namespace identify should match group in log
        assert found_group.ana_group_id == ana_group_id, \
            f"Namespace reports group {ana_group_id} but found in group {found_group.ana_group_id}"

    def test_all_namespaces_in_ana_groups(self, nvme_client):
        """Test that all active namespaces are in ANA groups."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Get list of namespaces
        namespace_list = nvme_client.list_namespaces()

        if not namespace_list:
            pytest.skip("No namespaces available")

        # Get ANA log page
        ana_log = nvme_client.get_ana_log_page()

        # Collect all NSIDs from ANA groups
        ana_nsids = set()
        for group in ana_log.groups:
            ana_nsids.update(group.namespace_ids)

        # Check each namespace
        for nsid in namespace_list:
            ns_data = nvme_client.identify_namespace(nsid)
            ana_group_id = ns_data.get('anagrpid', 0)

            if ana_group_id > 0:
                # Namespace claims ANA group membership
                assert nsid in ana_nsids, \
                    f"Namespace {nsid} reports ANA group {ana_group_id} but not in ANA log"


@pytest.mark.integration
class TestANAHelperMethods:
    """Test ANA log page helper methods."""

    def test_ana_log_page_get_group(self, nvme_client):
        """Test get_group() helper method."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        ana_log = nvme_client.get_ana_log_page()

        if len(ana_log.groups) == 0:
            pytest.skip("No ANA groups configured")

        # Get first group by ID
        first_group = ana_log.groups[0]
        found_group = ana_log.get_group(first_group.ana_group_id)

        assert found_group is not None
        assert found_group.ana_group_id == first_group.ana_group_id

        # Try to get non-existent group
        invalid_group = ana_log.get_group(99999)
        assert invalid_group is None

    def test_ana_log_page_get_namespace_state(self, nvme_client, test_namespace_id):
        """Test get_namespace_state() helper method."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        ana_log = nvme_client.get_ana_log_page()

        # Check if test namespace is in any group
        state = ana_log.get_namespace_state(test_namespace_id)

        # State might be None if namespace not in ANA groups
        if state is not None:
            assert isinstance(state, ANAState)

    def test_ana_log_page_optimized_groups(self, nvme_client):
        """Test optimized_groups property."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        ana_log = nvme_client.get_ana_log_page()

        optimized = ana_log.optimized_groups

        assert isinstance(optimized, list)

        # All returned groups should be in OPTIMIZED state
        for group in optimized:
            assert group.ana_state == ANAState.OPTIMIZED

    def test_ana_log_page_accessible_groups(self, nvme_client):
        """Test accessible_groups property."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        ana_log = nvme_client.get_ana_log_page()

        accessible = ana_log.accessible_groups

        assert isinstance(accessible, list)

        # All returned groups should be accessible
        for group in accessible:
            assert group.is_accessible
            assert group.ana_state in [ANAState.OPTIMIZED, ANAState.NON_OPTIMIZED]


@pytest.mark.integration
@pytest.mark.slow
class TestANAStateTransitions:
    """Test ANA state monitoring for failover detection."""

    def test_monitor_ana_change_count(self, nvme_client):
        """Test monitoring ANA change count for state transitions."""
        controller_data = nvme_client.identify_controller()

        if controller_data['anacap'] == 0:
            pytest.skip("Target does not support ANA")

        # Get initial state
        initial_log = nvme_client.get_ana_log_page()
        initial_count = initial_log.change_count

        # In a real failover test, we would trigger a path change here
        # For now, just verify the change count is stable
        import time
        time.sleep(0.5)

        # Get state again
        second_log = nvme_client.get_ana_log_page()
        second_count = second_log.change_count

        # Change count should be the same (no transition) or higher (transition occurred)
        assert second_count >= initial_count

        # If count increased, states may have changed
        if second_count > initial_count:
            # This would indicate a failover occurred during the test
            # Verify both logs have valid structure
            assert len(initial_log.groups) > 0
            assert len(second_log.groups) > 0


@pytest.mark.integration
class TestANAErrorHandling:
    """Test error handling in ANA operations."""

    def test_invalid_log_page_id(self, nvme_client):
        """Test requesting invalid log page ID."""
        # Try to get a reserved/invalid log page ID
        # Using 0xFF which is typically reserved
        invalid_log_page_id = 0xFF

        with pytest.raises(CommandError):
            nvme_client.get_log_page(log_page_id=invalid_log_page_id, data_length=512, nsid=0)
