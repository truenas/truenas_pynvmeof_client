"""
NVMe-oF Data Models

Structured data classes for NVMe-oF responses and information.
Provides type safety and clear interfaces instead of generic dictionaries.

References:
- NVM Express Base Specification 2.0c
- NVM Express over Fabrics 1.1a
- NVM Command Set Specification 1.0c
"""

from dataclasses import dataclass
from typing import Any
from enum import IntEnum


class TransportType(IntEnum):
    """
    NVMe-oF Transport Types.

    Reference: NVM Express over Fabrics 1.1a, Section 5.1.2 "Transport Type (TRTYPE)"
    """
    RDMA = 1    # RDMA (InfiniBand, RoCE, iWARP)
    FC = 2      # Fibre Channel
    TCP = 3     # TCP/IP
    LOOP = 4    # Local loopback (for testing)


class AddressFamily(IntEnum):
    """
    Address Family Types.

    Reference: NVM Express over Fabrics 1.1a, Section 5.1.3 "Address Family (ADRFAM)"
    """
    IPV4 = 1    # IPv4 address family
    IPV6 = 2    # IPv6 address family
    FC = 3      # Fibre Channel address family
    IB = 4      # InfiniBand address family


class ReservationType(IntEnum):
    """
    NVMe Reservation Types.

    Reference: NVM Command Set Specification 1.0c, Section 6.2.2 "Reservation Type (RTYPE)"
    """
    WRITE_EXCLUSIVE = 1                      # Write Exclusive
    EXCLUSIVE_ACCESS = 2                     # Exclusive Access
    WRITE_EXCLUSIVE_REGISTRANTS_ONLY = 3     # Write Exclusive - Registrants Only
    EXCLUSIVE_ACCESS_REGISTRANTS_ONLY = 4    # Exclusive Access - Registrants Only
    WRITE_EXCLUSIVE_ALL_REGISTRANTS = 5      # Write Exclusive - All Registrants
    EXCLUSIVE_ACCESS_ALL_REGISTRANTS = 6     # Exclusive Access - All Registrants


class ReservationAction(IntEnum):
    """
    NVMe Reservation Actions.

    Note: These values are context-dependent based on the command.

    References:
    - NVM Command Set Specification 1.0c, Section 6.1 "Reservation Register command"
    - NVM Command Set Specification 1.0c, Section 6.2 "Reservation Acquire command"
    - NVM Command Set Specification 1.0c, Section 6.3 "Reservation Release command"
    """
    # Register actions (Reservation Register command)
    REGISTER = 0      # Register reservation key
    UNREGISTER = 1    # Unregister reservation key
    REPLACE = 2       # Replace reservation key

    # Acquire actions (Reservation Acquire command)
    ACQUIRE = 0            # Acquire reservation
    PREEMPT = 1            # Preempt reservation
    PREEMPT_AND_ABORT = 2  # Preempt and abort reservation

    # Release actions (Reservation Release command)
    RELEASE = 0  # Release reservation
    CLEAR = 1    # Clear reservation


class ANAState(IntEnum):
    """
    Asymmetric Namespace Access (ANA) States.

    Indicates the accessibility state of namespaces in an ANA Group when accessed
    through a specific controller in a multi-controller subsystem.

    Reference: NVM Express Base Specification 2.3, Figure 228 "ANA Group Descriptor format"
    """
    OPTIMIZED = 0x01        # ANA Optimized state - Active, preferred path
    NON_OPTIMIZED = 0x02    # ANA Non-Optimized state - Active, non-preferred path
    INACCESSIBLE = 0x03     # ANA Inaccessible state - Passive, not accessible
    PERSISTENT_LOSS = 0x04  # ANA Persistent Loss state - Permanent path failure
    CHANGE = 0x0F           # ANA Change state - Transitioning between states


@dataclass
class ControllerInfo:
    """
    NVMe Controller Information from Identify Controller command.

    Reference: NVM Express Base Specification 2.0c, Figure 275 "Identify Controller Data Structure"
    """

    # Basic Controller Information (bytes 0-255)
    vendor_id: int                    # VID: bytes 0-1, IEEE OUI vendor identifier
    subsystem_vendor_id: int          # SSVID: bytes 2-3, subsystem vendor identifier
    serial_number: str                # SN: bytes 4-23, serial number (ASCII)
    model_number: str                 # MN: bytes 24-63, model number (ASCII)
    firmware_revision: str            # FR: bytes 64-71, firmware revision (ASCII)
    controller_id: int                # CNTLID: bytes 78-79, controller identifier

    # Capabilities (bytes 256-511)
    max_data_transfer_size: int       # MDTS: byte 77, maximum data transfer size
    controller_multipath_io_capabilities: int  # CMIC: byte 76, controller multipath I/O capabilities
    optional_admin_command_support: int        # OACS: bytes 256-257, optional admin command support
    optional_nvm_command_support: int          # ONCS: bytes 520-521, optional NVM command support

    # Optional Asynchronous Events Supported (bytes 92-95, OAES field - 32 bits)
    # Bits 7:0 - Reserved (used by Asynchronous Event Configuration)
    oaes_namespace_attribute_notices: bool           # Bit 8: NSAN - Attached Namespace Attribute Notices
    oaes_firmware_activation_notices: bool           # Bit 9: FAN - Firmware Activation Notices
    # Bit 10 - Reserved
    oaes_ana_change_notices: bool                    # Bit 11: ANACN - Asymmetric Namespace Access Change Notices
    oaes_predictable_latency_event_notices: bool     # Bit 12: PLEAN - Predictable Latency Event Aggregate Log Change
    oaes_lba_status_information_notices: bool        # Bit 13: LSIAN - LBA Status Information Alert Notices
    oaes_endurance_group_event_notices: bool         # Bit 14: EGEAN - Endurance Group Event Aggregate Log Change
    oaes_normal_subsystem_shutdown_notices: bool     # Bit 15: NNSS - Normal NVM Subsystem Shutdown
    oaes_temperature_threshold_hysteresis: bool      # Bit 16: TTHR - Temperature Threshold Hysteresis Recovery
    oaes_reachability_groups_change_notices: bool    # Bit 17: RGCNS - Reachability Groups Change Notices Support
    # Bit 18 - Reserved (used by AEC)
    oaes_allocated_namespace_attribute_notices: bool  # Bit 19: ANSAN - Allocated Namespace Attribute Notices
    oaes_cross_controller_reset_notices: bool        # Bit 20: CCRCN - Cross-Controller Reset Completed Notices
    oaes_lost_host_communication_notices: bool       # Bit 21: LHCN - Lost Host Communication Notices
    # Bits 26:22 - Reserved
    oaes_zone_descriptor_changed_notices: bool       # Bit 27: ZDCN - Zone Descriptor Changed Notices
    # Bits 30:28 - Reserved
    oaes_discovery_log_change_notices: bool          # Bit 31: DLPCN - Discovery Log Page Change Notification (NVMe-oF)

    # Asynchronous Event Configuration (byte 259)
    aerl: int                              # AERL: Asynchronous Event Request Limit (0-based, 0 = 1 outstanding max)

    # Queue Limits (bytes 512-515)
    max_submission_queue_entries: int  # MAXCMD: bytes 514-515, maximum outstanding commands
    max_completion_queue_entries: int  # MAXCMD: bytes 514-515, maximum outstanding commands

    # Namespace Information (bytes 516-519)
    number_of_namespaces: int          # NN: bytes 516-519, number of namespaces

    # Power and Thermal (bytes 266-271)
    max_power_consumption: int         # MPC: bytes 266-267, maximum power consumption
    warning_composite_temp_threshold: int   # WCTEMP: bytes 266-267, warning composite temperature threshold
    critical_composite_temp_threshold: int  # CCTEMP: bytes 268-269, critical composite temperature threshold

    # NVMe-oF Specific
    nvmeof_attributes: int | None = None  # NVMe-oF specific attributes if applicable

    # Version Information
    nvme_version: str | None = None  # VER: bytes 80-83, NVMe version

    # Additional capabilities (can be extended)
    raw_data: bytes | None = None    # Raw identify controller data for debugging


@dataclass
class NamespaceInfo:
    """
    NVMe Namespace Information from Identify Namespace command.

    Reference: NVM Express Base Specification 2.0c, Figure 276 "Identify Namespace Data Structure"
    """

    namespace_id: int                   # NSID: namespace identifier
    namespace_size: int                 # NSZE: bytes 0-7, namespace size in logical blocks
    namespace_capacity: int             # NCAP: bytes 8-15, namespace capacity in logical blocks
    namespace_utilization: int          # NUSE: bytes 16-23, namespace utilization in logical blocks

    # LBA Format Information (bytes 128-191)
    logical_block_size: int             # LBADS: LBA data size, 2^n bytes
    metadata_size: int                  # MS: metadata size per logical block
    relative_performance: int           # RP: 0=Best, 1=Better, 2=Good, 3=Degraded

    # Features (bytes 24-29)
    thin_provisioning_supported: bool   # NSFEAT: bit 0, thin provisioning support
    deallocate_supported: bool          # NSFEAT: deallocate support
    write_zeros_supported: bool         # From controller ONCS capabilities

    # Protection Information (bytes 29-30)
    protection_type: int                # DPS: data protection settings
    protection_info_location: int       # DPS: protection information location

    # Additional info (bytes 32-39)
    preferred_write_granularity: int    # NPWG: preferred write granularity
    preferred_write_alignment: int      # NPWA: preferred write alignment

    raw_data: bytes | None = None    # Raw identify namespace data for debugging


@dataclass
class DiscoveryEntry:
    """
    Discovery Log Entry for NVMe-oF subsystems.

    Reference: NVM Express over Fabrics 1.1a, Section 5.3 "Discovery Log Page"
    """

    transport_type: TransportType       # TRTYPE: bytes 0, transport type
    address_family: AddressFamily       # ADRFAM: byte 1, address family
    subsystem_type: int                 # SUBTYPE: byte 2, subsystem type (1=Discovery, 2=NVMe)
    port_id: int                        # PORTID: bytes 4-5, port identifier
    controller_id: int                  # CNTLID: bytes 6-7, controller identifier

    # Network Information (bytes 8-263)
    transport_address: str              # TRADDR: bytes 8-263, transport address (IP/FC address)
    transport_service_id: str           # TRSVCID: bytes 264-295, transport service ID (port/FC service)

    # NQN Information (bytes 296-551)
    subsystem_nqn: str                  # SUBNQN: bytes 296-551, subsystem NQN

    # Optional Fields
    transport_requirements: int | None = None  # TREQ: byte 3, transport requirements

    @property
    def is_discovery_subsystem(self) -> bool:
        """Check if this is a discovery subsystem (SUBTYPE=1 referral or 3 current)."""
        return self.subsystem_type in (1, 3)

    @property
    def is_nvme_subsystem(self) -> bool:
        """Check if this is an NVMe subsystem (SUBTYPE=2)."""
        return self.subsystem_type == 2


@dataclass
class ConnectionInfo:
    """
    Information about an active NVMe-oF connection.

    This is client-side connection tracking information, not part of the NVMe specification.
    """

    host_nqn: str                      # Host NQN used for connection
    subsystem_nqn: str                 # Target subsystem NQN
    transport_address: str             # Target IP address or transport address
    transport_port: int                # Target port number

    # Connection Parameters
    max_data_size: int                 # Maximum data transfer size negotiated
    queue_depth: int                   # Queue depth for I/O operations
    digest_types: int                  # Digest types supported/enabled

    # State
    is_connected: bool                 # Connection is currently active
    is_discovery_connection: bool      # Connection is to discovery subsystem

    # Statistics (optional)
    commands_sent: int | None = None      # Number of commands sent
    bytes_transferred: int | None = None  # Total bytes transferred


@dataclass
class QueueInfo:
    """
    Information about NVMe queues.

    This is client-side queue tracking information, not part of the NVMe specification.
    Relates to NVMe queue concepts from NVM Express Base Specification 2.0c, Section 4 "Queues".
    """

    queue_id: int                      # Queue identifier (0=admin, 1+=I/O)
    queue_size: int                    # Number of entries in queue
    queue_type: str                    # Queue type: "admin", "io_submission", "io_completion"

    # State
    is_created: bool = False           # Queue has been created on controller

    # Statistics (optional)
    commands_processed: int | None = None  # Number of commands processed through this queue


@dataclass
class ControllerCapabilities:
    """
    Parsed Controller Capabilities Register (CAP).

    Reference: NVM Express Base Specification 2.0c, Section 3.1.1 "Controller Capabilities (CAP)"
    """

    max_queue_entries_supported: int   # MQES: bits 15:0, max queue entries supported (0-based)
    contiguous_queues_required: bool   # CQR: bit 16, contiguous queues required
    arbitration_mechanism_supported: int  # AMS: bits 18:17, arbitration mechanism supported
    timeout: int                       # TO: bits 31:24, timeout (in 500ms units)
    doorbell_stride: int               # DSTRD: bits 35:32, doorbell stride (4 << DSTRD bytes)
    nvm_subsystem_reset_supported: bool  # NSSRS: bit 36, NVM subsystem reset supported
    command_sets_supported: int        # CSS: bits 44:37, command sets supported
    boot_partition_support: bool       # BPS: bit 45, boot partition support
    memory_page_size_minimum: int      # MPSMIN: bits 51:48, memory page size minimum (2^(12+MPSMIN) bytes)
    memory_page_size_maximum: int      # MPSMAX: bits 55:52, memory page size maximum (2^(12+MPSMAX) bytes)


@dataclass
class ControllerStatus:
    """
    Parsed Controller Status Register (CSTS).

    Reference: NVM Express Base Specification 2.0c, Section 3.1.4 "Controller Status (CSTS)"
    """

    ready: bool                         # RDY: bit 0, controller ready
    controller_fatal_status: bool       # CFS: bit 1, controller fatal status
    shutdown_status: int                # SHST: bits 3:2, shutdown status (0=normal, 1=in_progress, 2=complete)
    nvm_subsystem_reset_occurred: bool  # NSSRO: bit 4, NVM subsystem reset occurred
    processing_paused: bool             # PP: bit 5, processing paused

    @property
    def is_ready(self) -> bool:
        """Check if controller is ready and not in fatal state."""
        return self.ready and not self.controller_fatal_status


@dataclass
class ReservationStatus:
    """
    NVMe Reservation Status Information from Reservation Report command.

    Reference: NVM Command Set Specification 1.0c, Section 6.4 "Reservation Report command"
    """

    generation: int                    # REGCTL: bytes 0-3, reservation generation counter
    reservation_type: ReservationType  # RTYPE: byte 22, current reservation type
    reservation_holder: int            # Controller ID of current reservation holder
    registered_controllers: list[int]  # List of registered controller IDs

    # Reservation keys for registered controllers (from Registered Controller Data)
    reservation_keys: dict[int, int]   # controller_id -> reservation_key mapping

    @property
    def is_reserved(self) -> bool:
        """Check if namespace is currently reserved (has a reservation holder)."""
        return self.reservation_holder != 0

    @property
    def num_registered_controllers(self) -> int:
        """Get number of registered controllers."""
        return len(self.registered_controllers)


@dataclass
class ReservationInfo:
    """
    Reservation operation result information.

    Returned by Reservation Register, Acquire, and Release commands.
    """

    success: bool                      # Operation completed successfully
    reservation_key: int               # Reservation key used in operation
    generation: int                    # Current reservation generation counter
    status_code: int                   # NVMe status code from command completion

    # Optional detailed status
    reservation_status: ReservationStatus | None = None  # Full reservation status if requested


class AsyncEventType(IntEnum):
    """
    Asynchronous Event Types.

    Reference: NVM Express Base Specification 2.3, Figure 150 (Dword 0, bits 2:0)
    """
    ERROR_STATUS = 0x00              # Error status (Figure 152)
    SMART_HEALTH_STATUS = 0x01       # SMART / Health status (Figure 153)
    NOTICE = 0x02                    # Notice (Figure 154)
    IMMEDIATE = 0x03                 # Immediate (Figure 156)
    ONE_SHOT = 0x04                  # One-Shot (Figure 157)
    IO_COMMAND_SPECIFIC = 0x06       # I/O Command Specific status (Figure 155)
    VENDOR_SPECIFIC = 0x07           # Vendor specific


class AsyncEventInfoNotice(IntEnum):
    """
    Asynchronous Event Information codes for Notice events (Type 0x02).

    Reference: NVM Express Base Specification 2.3, Figure 154
    """
    NAMESPACE_ATTRIBUTE_CHANGED = 0x00        # Attached Namespace Attribute Changed (NSAN)
    FIRMWARE_ACTIVATION_STARTING = 0x01       # Firmware Activation Starting (FAN)
    TELEMETRY_LOG_CHANGED = 0x02              # Telemetry Log Changed
    ANA_CHANGE = 0x03                         # Asymmetric Namespace Access Change (ANACN)
    PREDICTABLE_LATENCY_AGGR_CHANGED = 0x04   # Predictable Latency Event Aggregate Log Change (PLEAN)
    LBA_STATUS_INFO_ALERT = 0x05              # LBA Status Information Alert (LSIAN)
    ENDURANCE_GROUP_AGGR_CHANGED = 0x06       # Endurance Group Event Aggregate Log Change (EGEAN)
    REACHABILITY_GROUP_CHANGE = 0x07          # Reachability Group Change
    REACHABILITY_ASSOCIATION_CHANGE = 0x08    # Reachability Association Change
    ALLOCATED_NAMESPACE_ATTRIBUTE_CHANGED = 0x09  # Allocated Namespace Attribute Changed (ANSAN)
    ZONE_DESCRIPTOR_CHANGED = 0xEF            # Zone Descriptor Changed (ZDCN)
    DISCOVERY_LOG_CHANGED = 0xF0              # Discovery Log Page Change (DLPCN) - NVMe-oF
    HOST_DISCOVERY_LOG_CHANGED = 0xF1         # Host Discovery Log Page Change
    CROSS_CONTROLLER_RESET_COMPLETED = 0xF3   # Cross-Controller Reset Completed (CCRCN)
    LOST_HOST_COMMUNICATION = 0xF4            # Lost Host Communication (LHCN)


class AsyncEventInfoImmediate(IntEnum):
    """
    Asynchronous Event Information codes for Immediate events (Type 0x03).

    Reference: NVM Express Base Specification 2.3, Figure 156
    """
    NORMAL_SUBSYSTEM_SHUTDOWN = 0x00          # Normal NVM Subsystem Shutdown (NNSS)
    TEMPERATURE_THRESHOLD_HYSTERESIS = 0x01   # Temperature Threshold Hysteresis Recovery (TTHR)


@dataclass
class AsyncEvent:
    """
    Asynchronous Event notification from controller.

    Represents a parsed asynchronous event completion from the controller.
    Events are requested via Async Event Request commands and are sent
    unsolicited when configured events occur.

    Reference: NVM Express Base Specification 2.3, Figures 150-151
    """

    event_type: AsyncEventType              # Bits 2:0 of Dword 0 - Event type category
    event_info: int                         # Bits 15:08 of Dword 0 - Specific event within category
    log_page_id: int                        # Bits 23:16 of Dword 0 - Associated log page (0-255)

    # Human-readable description
    description: str

    # Raw completion dword 0 for debugging
    raw_dword0: int

    # Optional: Event Specific Parameter from Dword 1 (for some events)
    event_specific_param: int | None = None

    @property
    def is_notice(self) -> bool:
        """Check if this is a Notice event (type 0x02)."""
        return self.event_type == AsyncEventType.NOTICE

    @property
    def is_error(self) -> bool:
        """Check if this is an Error event (type 0x00)."""
        return self.event_type == AsyncEventType.ERROR_STATUS

    @property
    def is_smart_health(self) -> bool:
        """Check if this is a SMART/Health event (type 0x01)."""
        return self.event_type == AsyncEventType.SMART_HEALTH_STATUS

    @property
    def is_immediate(self) -> bool:
        """Check if this is an Immediate event (type 0x03)."""
        return self.event_type == AsyncEventType.IMMEDIATE


@dataclass
class ANAGroupDescriptor:
    """
    ANA Group Descriptor containing state and namespace information for an ANA Group.

    Each descriptor contains the ANA state and list of namespaces that belong to a
    specific ANA Group as seen from a particular controller.

    Reference: NVM Express Base Specification 2.3, Figure 228 "ANA Group Descriptor format"
    """

    ana_group_id: int           # AGID: bytes 03:00, ANA Group Identifier
    num_namespaces: int         # NNV: bytes 07:04, Number of NSID values in this descriptor
    change_count: int           # CHGC: bytes 15:08, Change count for this ANA Group (0 = not reported)
    ana_state: ANAState         # ANAS: byte 16 bits 03:00, Current ANA state for this group
    namespace_ids: list[int]    # Bytes 35:32+, List of NSIDs in this ANA Group (ascending order)

    @property
    def is_accessible(self) -> bool:
        """Check if namespaces in this group are accessible (Optimized or Non-Optimized state)."""
        return self.ana_state in (ANAState.OPTIMIZED, ANAState.NON_OPTIMIZED)

    @property
    def is_optimized(self) -> bool:
        """Check if this is the preferred/optimized path for these namespaces."""
        return self.ana_state == ANAState.OPTIMIZED


@dataclass
class ANALogPage:
    """
    Asymmetric Namespace Access Log Page (Log Page ID 0x0C).

    Contains the ANA state information for all ANA Groups with namespaces attached
    to the controller. Used to determine optimal paths in multi-controller configurations.

    Reference: NVM Express Base Specification 2.3, Figure 227 "Asymmetric Namespace Access Log Page"
    """

    change_count: int                      # Bytes 07:00, Log-level change count (increments on any change)
    num_ana_group_descriptors: int         # Bytes 15:08, Number of ANA Group Descriptors in this log
    groups: list[ANAGroupDescriptor]       # List of ANA Group Descriptors (one per ANA Group)

    def get_group(self, ana_group_id: int) -> ANAGroupDescriptor | None:
        """Get descriptor for a specific ANA Group ID, or None if not found."""
        for group in self.groups:
            if group.ana_group_id == ana_group_id:
                return group
        return None

    def get_namespace_state(self, nsid: int) -> ANAState | None:
        """Get the ANA state for a specific namespace ID, or None if not found."""
        for group in self.groups:
            if nsid in group.namespace_ids:
                return group.ana_state
        return None

    @property
    def optimized_groups(self) -> list[ANAGroupDescriptor]:
        """Get list of ANA Groups in Optimized state."""
        return [g for g in self.groups if g.ana_state == ANAState.OPTIMIZED]

    @property
    def accessible_groups(self) -> list[ANAGroupDescriptor]:
        """Get list of ANA Groups in accessible states (Optimized or Non-Optimized)."""
        return [g for g in self.groups if g.is_accessible]


# Type aliases for backward compatibility with legacy dictionary-based APIs
# These should be deprecated in favor of the typed dataclass models above
ControllerData = dict[str, Any]  # Legacy controller information format
NamespaceData = dict[str, Any]   # Legacy namespace information format
DiscoveryData = dict[str, Any]   # Legacy discovery information format
