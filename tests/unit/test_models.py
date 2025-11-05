"""
Unit tests for data models

Tests the data model classes, enums, and validation logic.
"""

import unittest
from nvmeof_client.models import (
    TransportType,
    AddressFamily,
    ReservationType,
    ReservationAction,
    ControllerInfo,
    NamespaceInfo,
    DiscoveryEntry,
    ConnectionInfo,
    QueueInfo,
    ControllerCapabilities,
    ControllerStatus,
    ReservationStatus,
    ReservationInfo
)


class TestEnums(unittest.TestCase):
    """Test enum definitions and values."""

    def test_transport_type_enum(self):
        """Test TransportType enum values."""
        self.assertEqual(TransportType.RDMA.value, 1)
        self.assertEqual(TransportType.FC.value, 2)
        self.assertEqual(TransportType.TCP.value, 3)
        self.assertEqual(TransportType.LOOP.value, 4)

    def test_address_family_enum(self):
        """Test AddressFamily enum values."""
        self.assertEqual(AddressFamily.IPV4.value, 1)
        self.assertEqual(AddressFamily.IPV6.value, 2)
        self.assertEqual(AddressFamily.FC.value, 3)
        self.assertEqual(AddressFamily.IB.value, 4)

    def test_reservation_type_enum(self):
        """Test ReservationType enum values match NVMe spec."""
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

        # Acquire actions (same values, different context)
        self.assertEqual(ReservationAction.ACQUIRE.value, 0)
        self.assertEqual(ReservationAction.PREEMPT.value, 1)
        self.assertEqual(ReservationAction.PREEMPT_AND_ABORT.value, 2)

        # Release actions
        self.assertEqual(ReservationAction.RELEASE.value, 0)
        self.assertEqual(ReservationAction.CLEAR.value, 1)


class TestControllerInfo(unittest.TestCase):
    """Test ControllerInfo data model."""

    def test_controller_info_creation(self):
        """Test ControllerInfo model creation."""
        info = ControllerInfo(
            vendor_id=0x1234,
            subsystem_vendor_id=0x5678,
            serial_number="TEST123456789",
            model_number="Test NVMe Controller",
            firmware_revision="1.0.0",
            controller_id=1,
            max_data_transfer_size=131072,
            controller_multipath_io_capabilities=0,
            optional_admin_command_support=0x1FF,
            optional_nvm_command_support=0x3F,
            oaes_namespace_attribute_notices=True,
            oaes_firmware_activation_notices=True,
            oaes_ana_change_notices=True,
            oaes_predictable_latency_event_notices=False,
            oaes_lba_status_information_notices=False,
            oaes_endurance_group_event_notices=False,
            oaes_normal_subsystem_shutdown_notices=False,
            oaes_temperature_threshold_hysteresis=False,
            oaes_reachability_groups_change_notices=False,
            oaes_allocated_namespace_attribute_notices=False,
            oaes_cross_controller_reset_notices=False,
            oaes_lost_host_communication_notices=False,
            oaes_zone_descriptor_changed_notices=False,
            oaes_discovery_log_change_notices=False,
            aerl=3,
            max_submission_queue_entries=64,
            max_completion_queue_entries=64,
            number_of_namespaces=256,
            max_power_consumption=25,
            warning_composite_temp_threshold=70,
            critical_composite_temp_threshold=85
        )

        self.assertEqual(info.vendor_id, 0x1234)
        self.assertEqual(info.serial_number, "TEST123456789")
        self.assertEqual(info.model_number, "Test NVMe Controller")
        self.assertEqual(info.max_data_transfer_size, 131072)
        self.assertEqual(info.controller_id, 1)
        self.assertEqual(info.number_of_namespaces, 256)
        self.assertTrue(info.oaes_namespace_attribute_notices)
        self.assertTrue(info.oaes_ana_change_notices)

    def test_controller_info_optional_fields(self):
        """Test ControllerInfo with optional fields."""
        info = ControllerInfo(
            vendor_id=0x1234,
            subsystem_vendor_id=0x5678,
            serial_number="TEST123456789",
            model_number="Test NVMe Controller",
            firmware_revision="1.0.0",
            controller_id=1,
            max_data_transfer_size=131072,
            controller_multipath_io_capabilities=0,
            optional_admin_command_support=0x1FF,
            optional_nvm_command_support=0x3F,
            oaes_namespace_attribute_notices=False,
            oaes_firmware_activation_notices=True,
            oaes_ana_change_notices=False,
            oaes_predictable_latency_event_notices=False,
            oaes_lba_status_information_notices=False,
            oaes_endurance_group_event_notices=False,
            oaes_normal_subsystem_shutdown_notices=True,
            oaes_temperature_threshold_hysteresis=False,
            oaes_reachability_groups_change_notices=False,
            oaes_allocated_namespace_attribute_notices=False,
            oaes_cross_controller_reset_notices=False,
            oaes_lost_host_communication_notices=False,
            oaes_zone_descriptor_changed_notices=False,
            oaes_discovery_log_change_notices=True,
            aerl=2,
            max_submission_queue_entries=64,
            max_completion_queue_entries=64,
            number_of_namespaces=256,
            max_power_consumption=25,
            warning_composite_temp_threshold=70,
            critical_composite_temp_threshold=85,
            nvmeof_attributes=0x01,
            nvme_version="1.4",
            raw_data=b'raw_controller_data'
        )

        self.assertEqual(info.nvmeof_attributes, 0x01)
        self.assertEqual(info.nvme_version, "1.4")
        self.assertEqual(info.raw_data, b'raw_controller_data')


class TestNamespaceInfo(unittest.TestCase):
    """Test NamespaceInfo data model."""

    def test_namespace_info_creation(self):
        """Test NamespaceInfo model creation."""
        info = NamespaceInfo(
            namespace_id=1,
            namespace_size=2097152,  # 1GB in 512-byte blocks
            namespace_capacity=2097152,
            namespace_utilization=1048576,  # 50% utilized
            logical_block_size=512,
            metadata_size=0,
            relative_performance=0,  # Best
            thin_provisioning_supported=True,
            deallocate_supported=True,
            write_zeros_supported=True,
            protection_type=0,
            protection_info_location=0,
            preferred_write_granularity=1,
            preferred_write_alignment=1
        )

        self.assertEqual(info.namespace_id, 1)
        self.assertEqual(info.namespace_size, 2097152)
        self.assertEqual(info.logical_block_size, 512)
        self.assertTrue(info.thin_provisioning_supported)
        self.assertTrue(info.deallocate_supported)
        self.assertTrue(info.write_zeros_supported)


class TestDiscoveryEntry(unittest.TestCase):
    """Test DiscoveryEntry data model."""

    def test_discovery_entry_creation(self):
        """Test DiscoveryEntry model creation."""
        entry = DiscoveryEntry(
            transport_type=TransportType.TCP,
            address_family=AddressFamily.IPV4,
            subsystem_type=2,  # NVMe subsystem
            port_id=1,
            controller_id=1,
            transport_address="192.168.1.100",
            transport_service_id="4420",
            subsystem_nqn="nqn.2019-05.io.spdk:target"
        )

        self.assertEqual(entry.transport_type, TransportType.TCP)
        self.assertEqual(entry.address_family, AddressFamily.IPV4)
        self.assertEqual(entry.subsystem_type, 2)
        self.assertEqual(entry.transport_address, "192.168.1.100")
        self.assertEqual(entry.subsystem_nqn, "nqn.2019-05.io.spdk:target")

    def test_discovery_entry_properties(self):
        """Test DiscoveryEntry property methods."""
        # Discovery subsystem
        discovery_entry = DiscoveryEntry(
            transport_type=TransportType.TCP,
            address_family=AddressFamily.IPV4,
            subsystem_type=1,  # Discovery
            port_id=1,
            controller_id=1,
            transport_address="192.168.1.100",
            transport_service_id="4420",
            subsystem_nqn="nqn.2014-08.org.nvmexpress.discovery"
        )

        self.assertTrue(discovery_entry.is_discovery_subsystem)
        self.assertFalse(discovery_entry.is_nvme_subsystem)

        # NVMe subsystem
        nvme_entry = DiscoveryEntry(
            transport_type=TransportType.TCP,
            address_family=AddressFamily.IPV4,
            subsystem_type=2,  # NVMe
            port_id=1,
            controller_id=1,
            transport_address="192.168.1.100",
            transport_service_id="4420",
            subsystem_nqn="nqn.2019-05.io.spdk:target"
        )

        self.assertFalse(nvme_entry.is_discovery_subsystem)
        self.assertTrue(nvme_entry.is_nvme_subsystem)


class TestControllerCapabilities(unittest.TestCase):
    """Test ControllerCapabilities data model."""

    def test_controller_capabilities_creation(self):
        """Test ControllerCapabilities model creation."""
        caps = ControllerCapabilities(
            max_queue_entries_supported=1024,
            contiguous_queues_required=False,
            arbitration_mechanism_supported=0x3,
            timeout=15000,
            doorbell_stride=4,
            nvm_subsystem_reset_supported=True,
            command_sets_supported=0x41,
            boot_partition_support=False,
            memory_page_size_minimum=4096,
            memory_page_size_maximum=65536
        )

        self.assertEqual(caps.max_queue_entries_supported, 1024)
        self.assertFalse(caps.contiguous_queues_required)
        self.assertEqual(caps.arbitration_mechanism_supported, 0x3)
        self.assertEqual(caps.timeout, 15000)
        self.assertEqual(caps.doorbell_stride, 4)
        self.assertTrue(caps.nvm_subsystem_reset_supported)
        self.assertEqual(caps.command_sets_supported, 0x41)
        self.assertFalse(caps.boot_partition_support)
        self.assertEqual(caps.memory_page_size_minimum, 4096)
        self.assertEqual(caps.memory_page_size_maximum, 65536)

    def test_controller_capabilities_minimum_values(self):
        """Test ControllerCapabilities with minimum valid values."""
        caps = ControllerCapabilities(
            max_queue_entries_supported=2,
            contiguous_queues_required=True,
            arbitration_mechanism_supported=0,
            timeout=500,
            doorbell_stride=4,
            nvm_subsystem_reset_supported=False,
            command_sets_supported=0x01,
            boot_partition_support=False,
            memory_page_size_minimum=4096,
            memory_page_size_maximum=4096
        )

        self.assertEqual(caps.max_queue_entries_supported, 2)
        self.assertTrue(caps.contiguous_queues_required)
        self.assertEqual(caps.timeout, 500)

    def test_controller_capabilities_maximum_values(self):
        """Test ControllerCapabilities with maximum typical values."""
        caps = ControllerCapabilities(
            max_queue_entries_supported=65536,
            contiguous_queues_required=False,
            arbitration_mechanism_supported=0x3,
            timeout=255000,
            doorbell_stride=256,
            nvm_subsystem_reset_supported=True,
            command_sets_supported=0xFF,
            boot_partition_support=True,
            memory_page_size_minimum=4096,
            memory_page_size_maximum=16777216
        )

        self.assertEqual(caps.max_queue_entries_supported, 65536)
        self.assertEqual(caps.timeout, 255000)
        self.assertEqual(caps.doorbell_stride, 256)
        self.assertTrue(caps.boot_partition_support)
        self.assertEqual(caps.memory_page_size_maximum, 16777216)


class TestControllerStatus(unittest.TestCase):
    """Test ControllerStatus data model."""

    def test_controller_status_ready(self):
        """Test ControllerStatus ready state."""
        status = ControllerStatus(
            ready=True,
            controller_fatal_status=False,
            shutdown_status=0,
            nvm_subsystem_reset_occurred=False,
            processing_paused=False
        )

        self.assertTrue(status.ready)
        self.assertFalse(status.controller_fatal_status)
        self.assertTrue(status.is_ready)  # ready=True and no fatal status

    def test_controller_status_not_ready(self):
        """Test ControllerStatus not ready states."""
        # Not ready
        status1 = ControllerStatus(
            ready=False,
            controller_fatal_status=False,
            shutdown_status=0,
            nvm_subsystem_reset_occurred=False,
            processing_paused=False
        )
        self.assertFalse(status1.is_ready)

        # Fatal status
        status2 = ControllerStatus(
            ready=True,
            controller_fatal_status=True,  # Fatal error
            shutdown_status=0,
            nvm_subsystem_reset_occurred=False,
            processing_paused=False
        )
        self.assertFalse(status2.is_ready)


class TestReservationStatus(unittest.TestCase):
    """Test ReservationStatus data model."""

    def test_reservation_status_reserved(self):
        """Test ReservationStatus when namespace is reserved."""
        status = ReservationStatus(
            generation=123,
            reservation_type=ReservationType.WRITE_EXCLUSIVE,
            reservation_holder=42,  # Non-zero = reserved
            registered_controllers=[1, 2, 42],
            reservation_keys={1: 0x1111, 2: 0x2222, 42: 0x4242}
        )

        self.assertEqual(status.generation, 123)
        self.assertEqual(status.reservation_type, ReservationType.WRITE_EXCLUSIVE)
        self.assertEqual(status.reservation_holder, 42)
        self.assertTrue(status.is_reserved)  # holder != 0
        self.assertEqual(status.num_registered_controllers, 3)
        self.assertEqual(status.reservation_keys[42], 0x4242)

    def test_reservation_status_not_reserved(self):
        """Test ReservationStatus when namespace is not reserved."""
        status = ReservationStatus(
            generation=456,
            reservation_type=ReservationType.WRITE_EXCLUSIVE,
            reservation_holder=0,  # Zero = not reserved
            registered_controllers=[1, 2],
            reservation_keys={1: 0x1111, 2: 0x2222}
        )

        self.assertEqual(status.generation, 456)
        self.assertFalse(status.is_reserved)  # holder == 0
        self.assertEqual(status.num_registered_controllers, 2)

    def test_reservation_status_empty(self):
        """Test ReservationStatus with no registered controllers."""
        status = ReservationStatus(
            generation=0,
            reservation_type=ReservationType.WRITE_EXCLUSIVE,
            reservation_holder=0,
            registered_controllers=[],
            reservation_keys={}
        )

        self.assertFalse(status.is_reserved)
        self.assertEqual(status.num_registered_controllers, 0)
        self.assertEqual(len(status.reservation_keys), 0)


class TestReservationInfo(unittest.TestCase):
    """Test ReservationInfo data model."""

    def test_reservation_info_success(self):
        """Test ReservationInfo for successful operation."""
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
        self.assertIsNone(info.reservation_status)  # Not provided

    def test_reservation_info_with_status(self):
        """Test ReservationInfo with detailed reservation status."""
        detailed_status = ReservationStatus(
            generation=789,
            reservation_type=ReservationType.EXCLUSIVE_ACCESS,
            reservation_holder=1,
            registered_controllers=[1],
            reservation_keys={1: 0x123456789ABCDEF0}
        )

        info = ReservationInfo(
            success=True,
            reservation_key=0x123456789ABCDEF0,
            generation=789,
            status_code=0,
            reservation_status=detailed_status
        )

        self.assertTrue(info.success)
        self.assertIsNotNone(info.reservation_status)
        self.assertEqual(info.reservation_status.generation, 789)
        self.assertTrue(info.reservation_status.is_reserved)

    def test_reservation_info_failure(self):
        """Test ReservationInfo for failed operation."""
        info = ReservationInfo(
            success=False,
            reservation_key=0x123456789ABCDEF0,
            generation=0,
            status_code=0x18  # Reservation conflict
        )

        self.assertFalse(info.success)
        self.assertEqual(info.status_code, 0x18)


class TestConnectionInfo(unittest.TestCase):
    """Test ConnectionInfo data model."""

    def test_connection_info_creation(self):
        """Test ConnectionInfo model creation."""
        info = ConnectionInfo(
            host_nqn="nqn.2014-08.org.nvmexpress:uuid:12345678-1234-1234-1234-123456789abc",
            subsystem_nqn="nqn.2019-05.io.spdk:target",
            transport_address="192.168.1.100",
            transport_port=4420,
            max_data_size=131072,
            queue_depth=64,
            digest_types=0,
            is_connected=True,
            is_discovery_connection=False
        )

        self.assertTrue(info.is_connected)
        self.assertFalse(info.is_discovery_connection)
        self.assertEqual(info.transport_port, 4420)
        self.assertEqual(info.max_data_size, 131072)


class TestQueueInfo(unittest.TestCase):
    """Test QueueInfo data model."""

    def test_queue_info_creation(self):
        """Test QueueInfo model creation."""
        admin_queue = QueueInfo(
            queue_id=0,
            queue_size=32,
            queue_type="admin",
            is_created=True,
            commands_processed=100
        )

        self.assertEqual(admin_queue.queue_id, 0)
        self.assertEqual(admin_queue.queue_type, "admin")
        self.assertTrue(admin_queue.is_created)
        self.assertEqual(admin_queue.commands_processed, 100)

        io_queue = QueueInfo(
            queue_id=1,
            queue_size=64,
            queue_type="io_submission"
        )

        self.assertEqual(io_queue.queue_id, 1)
        self.assertEqual(io_queue.queue_type, "io_submission")
        self.assertFalse(io_queue.is_created)  # Default
        self.assertIsNone(io_queue.commands_processed)  # Default


if __name__ == '__main__':
    unittest.main()
