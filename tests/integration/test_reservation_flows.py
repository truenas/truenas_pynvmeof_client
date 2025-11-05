"""
Integration tests for NVMe Reservation workflows

Tests complete reservation workflows against a live NVMe-oF target.
These tests require a target that supports reservations.
"""

import pytest
import time
from nvmeof_client.models import (
    ReservationType,
    ReservationAction,
    ReservationStatus,
    ReservationInfo
)
from nvmeof_client.exceptions import CommandError


@pytest.mark.integration
@pytest.mark.slow
class TestBasicReservationWorkflow:
    """Test basic reservation registration and reporting."""

    def test_reservation_register_unregister(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test basic key registration and unregistration."""
        # Get initial status
        initial_status = nvme_client.reservation_report(test_namespace_id)
        assert isinstance(initial_status, ReservationStatus)
        initial_count = initial_status.num_registered_controllers

        # Register a key
        result = nvme_client.reservation_register(
            test_namespace_id, ReservationAction.REGISTER, test_reservation_key)
        assert isinstance(result, ReservationInfo)
        assert result.success
        assert result.reservation_key == test_reservation_key

        # Check status after registration
        status_after_register = nvme_client.reservation_report(test_namespace_id)
        assert status_after_register.num_registered_controllers == initial_count + 1
        # Our key should be in the registered keys
        controller_ids = list(status_after_register.reservation_keys.keys())
        assert len(controller_ids) >= 1

        # Find our controller ID (the one with our key)
        our_controller_id = None
        for cid, key in status_after_register.reservation_keys.items():
            if key == test_reservation_key:
                our_controller_id = cid
                break
        assert our_controller_id is not None

        # Unregister the key
        result = nvme_client.reservation_register(
            test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
        assert result.success

        # Check status after unregistration
        status_after_unregister = nvme_client.reservation_report(test_namespace_id)
        assert status_after_unregister.num_registered_controllers == initial_count
        # Our key should no longer be present
        assert test_reservation_key not in status_after_unregister.reservation_keys.values()

    def test_reservation_report_structure(self, nvme_client, test_namespace_id):
        """Test reservation report data structure."""
        status = nvme_client.reservation_report(test_namespace_id)

        assert isinstance(status, ReservationStatus)
        assert isinstance(status.generation, int)
        assert status.reservation_type is None or isinstance(status.reservation_type, ReservationType)
        assert isinstance(status.reservation_holder, int)
        assert isinstance(status.registered_controllers, list)
        assert isinstance(status.reservation_keys, dict)

        # Test properties
        assert isinstance(status.is_reserved, bool)
        assert isinstance(status.num_registered_controllers, int)
        assert status.num_registered_controllers == len(status.registered_controllers)
        assert status.num_registered_controllers == len(status.reservation_keys)


@pytest.mark.integration
@pytest.mark.slow
class TestReservationAcquisition:
    """Test reservation acquisition and release."""

    def test_acquire_release_write_exclusive(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test acquiring and releasing a write exclusive reservation."""
        try:
            # Register key first
            result = nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key)
            assert result.success

            # Acquire write exclusive reservation
            result = nvme_client.reservation_acquire(
                test_namespace_id, ReservationAction.ACQUIRE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
            assert result.success

            # Check that reservation is active
            status = nvme_client.reservation_report(test_namespace_id)
            assert status.is_reserved
            assert status.reservation_type == ReservationType.WRITE_EXCLUSIVE

            # Release the reservation
            result = nvme_client.reservation_release(
                test_namespace_id, ReservationAction.RELEASE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
            assert result.success

            # Check that reservation is released
            status_after_release = nvme_client.reservation_report(test_namespace_id)
            assert not status_after_release.is_reserved

        finally:
            # Cleanup: unregister key
            try:
                nvme_client.reservation_register(
                    test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
            except Exception:
                pass  # Best effort cleanup

    def test_acquire_exclusive_access(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test acquiring exclusive access reservation."""
        try:
            # Register key
            result = nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key)
            assert result.success

            # Acquire exclusive access reservation
            result = nvme_client.reservation_acquire(
                test_namespace_id, ReservationAction.ACQUIRE,
                ReservationType.EXCLUSIVE_ACCESS, test_reservation_key)
            assert result.success

            # Verify reservation type
            status = nvme_client.reservation_report(test_namespace_id)
            assert status.is_reserved
            assert status.reservation_type == ReservationType.EXCLUSIVE_ACCESS

            # Release
            result = nvme_client.reservation_release(
                test_namespace_id, ReservationAction.RELEASE,
                ReservationType.EXCLUSIVE_ACCESS, test_reservation_key)
            assert result.success

        finally:
            # Cleanup
            try:
                nvme_client.reservation_register(
                    test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.slow
class TestReservationKeyReplacement:
    """Test reservation key replacement."""

    def test_replace_reservation_key(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test replacing a reservation key."""
        new_key = test_reservation_key + 1

        try:
            # Register initial key
            result = nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key)
            assert result.success

            # Replace with new key
            result = nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REPLACE,
                test_reservation_key, new_key)
            assert result.success

            # Verify old key is gone and new key is present
            status = nvme_client.reservation_report(test_namespace_id)
            registered_keys = list(status.reservation_keys.values())
            assert test_reservation_key not in registered_keys
            assert new_key in registered_keys

        finally:
            # Cleanup with new key
            try:
                nvme_client.reservation_register(
                    test_namespace_id, ReservationAction.UNREGISTER, new_key)
            except Exception:
                # Try with old key as fallback
                try:
                    nvme_client.reservation_register(
                        test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
                except Exception:
                    pass


@pytest.mark.integration
@pytest.mark.slow
class TestReservationConflicts:
    """Test reservation conflict scenarios."""

    def test_double_acquire_same_key(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test acquiring a reservation twice with the same key."""
        try:
            # Register and acquire
            nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key)

            result1 = nvme_client.reservation_acquire(
                test_namespace_id, ReservationAction.ACQUIRE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
            assert result1.success

            # Try to acquire again - should succeed (same key)
            result2 = nvme_client.reservation_acquire(
                test_namespace_id, ReservationAction.ACQUIRE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
            assert result2.success

            # Release
            nvme_client.reservation_release(
                test_namespace_id, ReservationAction.RELEASE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)

        finally:
            # Cleanup
            try:
                nvme_client.reservation_register(
                    test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
            except Exception:
                pass

    def test_acquire_without_registration(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test acquiring reservation without registering key first."""
        # This should fail
        with pytest.raises(CommandError):
            nvme_client.reservation_acquire(
                test_namespace_id, ReservationAction.ACQUIRE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)

    def test_release_without_reservation(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test releasing reservation that we don't hold.

        Per NVMe Base Specification Section 8.1.22.6 "Releasing a Reservation":
        "An attempt by a registrant to release a reservation using the Reservation Release
        command in the absence of a reservation held on the namespace or when the host is
        not the reservation holder shall cause the command to complete successfully, but
        shall have no effect on the controller or namespace."

        This test verifies that releasing a non-existent reservation succeeds as a no-op.
        """
        try:
            # Register key but don't acquire reservation
            nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key)

            # Try to release - should succeed per NVMe spec (no-op)
            result = nvme_client.reservation_release(
                test_namespace_id, ReservationAction.RELEASE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
            assert result.success  # Per NVMe spec: succeeds but has no effect

        finally:
            # Cleanup
            try:
                nvme_client.reservation_register(
                    test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.slow
class TestReservationClear:
    """Test reservation clear operations."""

    def test_clear_reservation(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test clearing a reservation."""
        try:
            # Register, acquire reservation
            nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key)
            nvme_client.reservation_acquire(
                test_namespace_id, ReservationAction.ACQUIRE,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)

            # Verify reservation is active
            status = nvme_client.reservation_report(test_namespace_id)
            assert status.is_reserved

            # Clear the reservation
            result = nvme_client.reservation_release(
                test_namespace_id, ReservationAction.CLEAR,
                ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
            assert result.success

            # Verify reservation is cleared
            status_after_clear = nvme_client.reservation_report(test_namespace_id)
            assert not status_after_clear.is_reserved

        finally:
            # Cleanup
            try:
                nvme_client.reservation_register(
                    test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.slow
class TestReservationStress:
    """Stress test reservation operations."""

    def test_rapid_register_unregister(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test rapid registration and unregistration."""
        iterations = 10

        for i in range(iterations):
            # Register
            result = nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key + i)
            assert result.success

            # Verify registration
            status = nvme_client.reservation_report(test_namespace_id)
            assert (test_reservation_key + i) in status.reservation_keys.values()

            # Unregister
            result = nvme_client.reservation_register(
                test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key + i)
            assert result.success

            # Brief pause to avoid overwhelming target
            time.sleep(0.1)

    def test_acquire_release_cycle(self, nvme_client, test_namespace_id, test_reservation_key):
        """Test repeated acquire/release cycles."""
        try:
            # Register key once
            nvme_client.reservation_register(
                test_namespace_id, ReservationAction.REGISTER, test_reservation_key)

            iterations = 5
            for i in range(iterations):
                # Acquire
                result = nvme_client.reservation_acquire(
                    test_namespace_id, ReservationAction.ACQUIRE,
                    ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
                assert result.success

                # Verify acquired
                status = nvme_client.reservation_report(test_namespace_id)
                assert status.is_reserved

                # Release
                result = nvme_client.reservation_release(
                    test_namespace_id, ReservationAction.RELEASE,
                    ReservationType.WRITE_EXCLUSIVE, test_reservation_key)
                assert result.success

                # Verify released
                status = nvme_client.reservation_report(test_namespace_id)
                assert not status.is_reserved

                time.sleep(0.1)

        finally:
            # Cleanup
            try:
                nvme_client.reservation_register(
                    test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
            except Exception:
                pass
