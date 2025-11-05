"""
Integration tests for Asynchronous Event functionality.

These tests require a live NVMe-oF target and may require manual intervention
to trigger certain events (e.g., ANA state changes, namespace modifications).
"""

import pytest
import time
from nvmeof_client.models import AsyncEvent, AsyncEventType


@pytest.mark.integration
@pytest.mark.slow
class TestAsyncEventBasic:
    """Test basic async event functionality."""

    def test_enable_async_events(self, nvme_client):
        """Test enabling async event notifications."""
        # Enable async events with all event types
        nvme_client.enable_async_events()

        # Should succeed without exceptions
        assert nvme_client._async_events_enabled

    def test_request_async_events(self, nvme_client):
        """Test submitting async event requests."""
        # Enable async events first
        nvme_client.enable_async_events()

        # Get controller info to check AERL
        controller_info = nvme_client.get_controller_info()
        max_requests = controller_info.aerl + 1  # AERL is 0-based

        # Submit async event requests (up to AERL limit)
        count = min(3, max_requests)
        nvme_client.request_async_events(count=count)

        # Should have outstanding requests
        assert len(nvme_client._outstanding_async_requests) == count

    def test_request_async_events_exceeds_aerl(self, nvme_client):
        """Test that requesting too many async events raises error."""
        # Enable async events
        nvme_client.enable_async_events()

        # Get AERL
        controller_info = nvme_client.get_controller_info()
        max_requests = controller_info.aerl + 1

        # Try to submit more than AERL + 1 requests
        with pytest.raises(ValueError, match="would exceed AERL limit"):
            nvme_client.request_async_events(count=max_requests + 1)

    def test_poll_async_events_no_events(self, nvme_client):
        """Test polling when no events have occurred."""
        # Enable async events and submit requests
        nvme_client.enable_async_events()
        nvme_client.request_async_events(count=1)

        # Poll with very short timeout (no events expected)
        events = nvme_client.poll_async_events(timeout=0.1)

        # Should return list of AsyncEvent objects
        assert isinstance(events, list)
        # If any events were returned, verify they are AsyncEvent instances
        for event in events:
            assert isinstance(event, AsyncEvent)
            assert isinstance(event.event_type, AsyncEventType)
            assert isinstance(event.event_info, int)
            assert isinstance(event.log_page_id, int)
            assert isinstance(event.description, str)
            assert event.description  # Should not be empty


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.manual
class TestAsyncEventManual:
    """
    Manual tests for async events that require external triggers.

    These tests require manual intervention to trigger events and are marked
    with @pytest.mark.manual to skip them in automated test runs.

    To run these tests:
    1. Start the test
    2. When prompted, perform the required action on the target (e.g., trigger ANA change)
    3. The test will poll for the event and verify it was received
    """

    def test_ana_change_event(self, nvme_client):
        """
        Test receiving ANA state change event.

        Manual steps:
        1. This test will enable async events and wait
        2. On the TrueNAS target, trigger an ANA state change (e.g., via API or UI)
        3. The test will poll for 30 seconds to receive the event
        """
        print("\n" + "=" * 70)
        print("MANUAL TEST: ANA Change Event")
        print("=" * 70)
        print("1. This test will enable async events and submit requests")
        print("2. You have 30 seconds to trigger an ANA state change on the target")
        print("3. The test will poll for the ANA change event")
        print("=" * 70)

        # Enable async events
        nvme_client.enable_async_events()

        # Submit async event requests
        nvme_client.request_async_events(count=3)

        print("\nAsync events enabled. Waiting for ANA state change...")
        print("Please trigger an ANA state change on the target NOW.")

        # Poll for 30 seconds
        max_wait = 30
        poll_interval = 1
        ana_event_received = False

        for i in range(max_wait):
            print(f"Polling... ({i+1}/{max_wait})")
            events = nvme_client.poll_async_events(timeout=poll_interval)

            if events:
                print(f"\nReceived {len(events)} event(s):")
                for event in events:
                    # Verify event structure
                    assert isinstance(event, AsyncEvent)
                    assert isinstance(event.event_type, AsyncEventType)
                    assert isinstance(event.event_info, int)
                    assert isinstance(event.log_page_id, int)
                    assert isinstance(event.description, str)

                    print(f"  - Type: {event.event_type.name}")
                    print(f"    Description: {event.description}")
                    print(f"    Log Page ID: {event.log_page_id:#x}")

                    # Check if this is an ANA change event
                    if event.is_notice and event.event_info == 0x03:
                        ana_event_received = True
                        assert event.event_type == AsyncEventType.NOTICE
                        assert event.log_page_id == 0x0C  # ANA log page
                        print("\n*** ANA change event received successfully! ***")

            if ana_event_received:
                break

            time.sleep(0.5)

        if not ana_event_received:
            pytest.skip("No ANA change event received during test period. "
                        "This is expected if no ANA state change was triggered.")

    def test_namespace_attribute_change_event(self, nvme_client):
        """
        Test receiving namespace attribute change event.

        Manual steps:
        1. This test will enable async events and wait
        2. On the target, modify a namespace attribute (e.g., resize namespace)
        3. The test will poll for 30 seconds to receive the event
        """
        print("\n" + "=" * 70)
        print("MANUAL TEST: Namespace Attribute Change Event")
        print("=" * 70)
        print("1. This test will enable async events and submit requests")
        print("2. You have 30 seconds to modify a namespace on the target")
        print("3. The test will poll for the namespace change event")
        print("=" * 70)

        # Enable async events
        nvme_client.enable_async_events()

        # Submit async event requests
        nvme_client.request_async_events(count=3)

        print("\nAsync events enabled. Waiting for namespace attribute change...")
        print("Please modify a namespace on the target NOW.")

        # Poll for 30 seconds
        max_wait = 30
        poll_interval = 1
        namespace_event_received = False

        for i in range(max_wait):
            print(f"Polling... ({i+1}/{max_wait})")
            events = nvme_client.poll_async_events(timeout=poll_interval)

            if events:
                print(f"\nReceived {len(events)} event(s):")
                for event in events:
                    # Verify event structure
                    assert isinstance(event, AsyncEvent)
                    assert isinstance(event.event_type, AsyncEventType)
                    assert isinstance(event.event_info, int)
                    assert isinstance(event.log_page_id, int)
                    assert isinstance(event.description, str)

                    print(f"  - Type: {event.event_type.name}")
                    print(f"    Description: {event.description}")
                    print(f"    Log Page ID: {event.log_page_id:#x}")

                    # Check if this is a namespace attribute change event
                    if event.is_notice and event.event_info == 0x00:
                        namespace_event_received = True
                        assert event.event_type == AsyncEventType.NOTICE
                        assert event.log_page_id == 0x04  # Changed namespace list log page
                        print("\n*** Namespace attribute change event received successfully! ***")

            if namespace_event_received:
                break

            time.sleep(0.5)

        if not namespace_event_received:
            pytest.skip("No namespace change event received during test period. "
                        "This is expected if no namespace was modified.")

    def test_firmware_activation_event(self, nvme_client):
        """
        Test receiving firmware activation event.

        Manual steps:
        1. This test will enable async events and wait
        2. On the target, initiate a firmware activation
        3. The test will poll for 60 seconds to receive the event

        Note: This test may not be practical for most test environments.
        """
        pytest.skip("Firmware activation test requires firmware update - skip by default")


@pytest.mark.integration
@pytest.mark.slow
class TestAsyncEventWorkflow:
    """Test complete async event workflows."""

    def test_enable_request_poll_workflow(self, nvme_client):
        """Test complete workflow: enable, request, poll."""
        # Step 1: Enable async events
        nvme_client.enable_async_events()
        assert nvme_client._async_events_enabled

        # Step 2: Submit async event requests
        nvme_client.request_async_events(count=2)
        assert len(nvme_client._outstanding_async_requests) == 2

        # Step 3: Poll for events (should return empty if no events)
        events = nvme_client.poll_async_events(timeout=0.1)
        assert isinstance(events, list)
        # Verify any returned events are properly typed
        for event in events:
            assert isinstance(event, AsyncEvent)
            assert isinstance(event.event_type, AsyncEventType)

        # Step 4: Submit more requests to replenish
        controller_info = nvme_client.get_controller_info()
        max_requests = controller_info.aerl + 1
        current = len(nvme_client._outstanding_async_requests)

        if current < max_requests:
            nvme_client.request_async_events(count=1)

    def test_multiple_poll_cycles(self, nvme_client):
        """Test multiple poll cycles without events."""
        nvme_client.enable_async_events()
        nvme_client.request_async_events(count=2)

        # Poll multiple times
        for _ in range(5):
            events = nvme_client.poll_async_events(timeout=0.05)
            assert isinstance(events, list)
            # Verify any returned events are properly typed
            for event in events:
                assert isinstance(event, AsyncEvent)
                assert isinstance(event.event_type, AsyncEventType)
            time.sleep(0.1)

    def test_controller_info_has_aerl(self, nvme_client):
        """Verify controller info includes AERL field."""
        controller_info = nvme_client.get_controller_info()

        assert hasattr(controller_info, 'aerl')
        assert isinstance(controller_info.aerl, int)
        assert controller_info.aerl >= 0

        print(f"\nController AERL (Async Event Request Limit): {controller_info.aerl}")
        print(f"Maximum outstanding async requests: {controller_info.aerl + 1}")

    def test_controller_info_has_oaes_fields(self, nvme_client):
        """Verify controller info includes OAES fields."""
        controller_info = nvme_client.get_controller_info()

        # Check that OAES fields exist
        assert hasattr(controller_info, 'oaes_namespace_attribute_notices')
        assert hasattr(controller_info, 'oaes_firmware_activation_notices')
        assert hasattr(controller_info, 'oaes_ana_change_notices')

        print("\nController OAES (Optional Async Events Supported):")
        print(f"  Namespace Attribute Notices: {controller_info.oaes_namespace_attribute_notices}")
        print(f"  Firmware Activation Notices: {controller_info.oaes_firmware_activation_notices}")
        print(f"  ANA Change Notices: {controller_info.oaes_ana_change_notices}")
