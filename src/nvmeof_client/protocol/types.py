"""
NVMe-oF Protocol Types and Enums

Type definitions, enums, and data structures for NVMe-oF protocol.
"""

from enum import IntEnum
from typing import NamedTuple


class PDUType(IntEnum):
    """NVMe-oF TCP PDU Types."""
    ICREQ = 0x00       # Initialize Connection Request
    ICRESP = 0x01      # Initialize Connection Response
    H2C_TERM = 0x02    # Host to Controller Terminate
    C2H_TERM = 0x03    # Controller to Host Terminate
    CMD = 0x04         # Command
    RSP = 0x05         # Response
    H2C_DATA = 0x06    # Host to Controller Data
    C2H_DATA = 0x07    # Controller to Host Data
    R2T = 0x09         # Ready to Transfer


class NVMeOpcode(IntEnum):
    """NVMe Admin and I/O Command Opcodes."""
    # Admin Commands
    DELETE_IO_SQ = 0x00
    CREATE_IO_SQ = 0x01
    GET_LOG_PAGE = 0x02
    DELETE_IO_CQ = 0x04
    CREATE_IO_CQ = 0x05
    IDENTIFY = 0x06
    ABORT = 0x08
    SET_FEATURES = 0x09
    GET_FEATURES = 0x0A
    ASYNC_EVENT_REQUEST = 0x0C
    NAMESPACE_MANAGEMENT = 0x0D
    FIRMWARE_COMMIT = 0x10
    FIRMWARE_IMAGE_DOWNLOAD = 0x11
    DEVICE_SELF_TEST = 0x14
    NAMESPACE_ATTACHMENT = 0x15
    KEEP_ALIVE = 0x18
    DIRECTIVE_SEND = 0x19
    DIRECTIVE_RECEIVE = 0x1A
    VIRTUALIZATION_MANAGEMENT = 0x1C
    NVMI_SEND = 0x1D
    NVMI_RECEIVE = 0x1E
    DOORBELL_BUFFER_CONFIG = 0x7C
    FABRICS = 0x7F

    # NVM I/O Commands
    FLUSH = 0x00
    WRITE = 0x01
    READ = 0x02
    WRITE_UNCORRECTABLE = 0x04
    COMPARE = 0x05
    WRITE_ZEROES = 0x08
    DATASET_MANAGEMENT = 0x09
    VERIFY = 0x0C
    RESERVATION_REGISTER = 0x0D
    RESERVATION_REPORT = 0x0E
    RESERVATION_ACQUIRE = 0x11
    RESERVATION_RELEASE = 0x15


class FabricCommandType(IntEnum):
    """NVMe-oF Fabric Command Types."""
    PROPERTY_SET = 0x00
    CONNECT = 0x01
    PROPERTY_GET = 0x04
    AUTHENTICATION_SEND = 0x05
    AUTHENTICATION_RECEIVE = 0x06
    DISCONNECT = 0x08


class NVMeProperty(IntEnum):
    """NVMe Controller Property Offsets."""
    CAP = 0x00     # Controller Capabilities
    VS = 0x08      # Version
    INTMS = 0x0C   # Interrupt Mask Set
    INTMC = 0x10   # Interrupt Mask Clear
    CC = 0x14      # Controller Configuration
    CSTS = 0x1C    # Controller Status
    NSSR = 0x20    # NVM Subsystem Reset
    AQA = 0x24     # Admin Queue Attributes
    ASQ = 0x28     # Admin Submission Queue Base Address
    ACQ = 0x30     # Admin Completion Queue Base Address
    CMBLOC = 0x38  # Controller Memory Buffer Location
    CMBSZ = 0x3C   # Controller Memory Buffer Size


class LogPageIdentifier(IntEnum):
    """NVMe Log Page Identifiers."""
    ERROR_INFORMATION = 0x01
    SMART_HEALTH_INFORMATION = 0x02
    FIRMWARE_SLOT_INFORMATION = 0x03
    CHANGED_NAMESPACE_LIST = 0x04
    COMMANDS_SUPPORTED_AND_EFFECTS = 0x05
    DEVICE_SELF_TEST = 0x06
    TELEMETRY_HOST_INITIATED = 0x07
    TELEMETRY_CONTROLLER_INITIATED = 0x08
    ENDURANCE_GROUP_INFORMATION = 0x09
    PREDICTABLE_LATENCY_PER_NVM_SET = 0x0A
    PREDICTABLE_LATENCY_EVENT_AGGREGATE = 0x0B
    ASYMMETRIC_NAMESPACE_ACCESS = 0x0C
    PERSISTENT_EVENT_LOG = 0x0D
    LBA_STATUS_INFORMATION = 0x0E
    ENDURANCE_GROUP_EVENT_AGGREGATE = 0x0F
    DISCOVERY_LOG = 0x70


class IdentifyDataStructure(IntEnum):
    """Identify Command CNS Values."""
    NAMESPACE = 0x00
    CONTROLLER = 0x01
    NAMESPACE_LIST = 0x02
    NAMESPACE_DESCRIPTOR_LIST = 0x03
    NVM_SET_LIST = 0x04
    CONTROLLER_LIST = 0x10
    CONTROLLER_LIST_ATTACHED_TO_NSID = 0x11
    CONTROLLER_LIST_SUBSYSTEM = 0x12
    PRIMARY_CONTROLLER_CAPABILITIES = 0x13
    SECONDARY_CONTROLLER_LIST = 0x14
    NAMESPACE_GRANULARITY_LIST = 0x15
    UUID_LIST = 0x17


class FeatureIdentifier(IntEnum):
    """NVMe Feature Identifiers for Get/Set Features commands."""
    ARBITRATION = 0x01
    POWER_MANAGEMENT = 0x02
    LBA_RANGE_TYPE = 0x03
    TEMPERATURE_THRESHOLD = 0x04
    ERROR_RECOVERY = 0x05
    VOLATILE_WRITE_CACHE = 0x06
    NUMBER_OF_QUEUES = 0x07
    INTERRUPT_COALESCING = 0x08
    INTERRUPT_VECTOR_CONFIG = 0x09
    WRITE_ATOMICITY_NORMAL = 0x0A
    ASYNCHRONOUS_EVENT_CONFIG = 0x0B
    AUTONOMOUS_POWER_STATE_TRANSITION = 0x0C
    HOST_MEMORY_BUFFER = 0x0D
    TIMESTAMP = 0x0E
    KEEP_ALIVE_TIMER = 0x0F
    HOST_CONTROLLED_THERMAL_MANAGEMENT = 0x10
    NON_OPERATIONAL_POWER_STATE_CONFIG = 0x11


class TransportType(IntEnum):
    """NVMe-oF Transport Types."""
    RDMA = 1
    FIBRE_CHANNEL = 2
    TCP = 3
    INTRA_HOST = 254


class AddressFamily(IntEnum):
    """Address Family Types for NVMe-oF."""
    IPV4 = 1
    IPV6 = 2
    INFINIBAND = 3
    FIBRE_CHANNEL = 4
    LOOP = 254


class ControllerStatus:
    """Controller Status Register Bit Definitions."""
    RDY = 0x1      # Ready
    CFS = 0x2      # Controller Fatal Status
    SHST_MASK = 0xC  # Shutdown Status mask
    NSSRO = 0x10   # NVM Subsystem Reset Occurred
    PP = 0x20      # Processing Paused


class ControllerConfiguration:
    """Controller Configuration Register Values."""
    # Enable values
    EN_DISABLED = 0
    EN_ENABLED = 1

    # Command Set Selection values
    CSS_NVM_COMMAND_SET = 0
    CSS_ALL_SUPPORTED = 6
    CSS_ADMIN_ONLY = 7

    # Arbitration Mechanism values
    AMS_ROUND_ROBIN = 0
    AMS_WEIGHTED_ROUND_ROBIN_URGENT = 1
    AMS_VENDOR_SPECIFIC = 7

    @staticmethod
    def build_cc_register(en: int, css: int, ams: int, iosqes: int, iocqes: int) -> int:
        """Build Controller Configuration register value."""
        return (
            (en & 0x1) |
            ((css & 0x7) << 4) |
            ((ams & 0x7) << 11) |
            ((iosqes & 0xF) << 16) |
            ((iocqes & 0xF) << 20)
        )


class PDUFlags:
    """
    NVMe-oF TCP PDU FLAGS field bit definitions.

    Reference: NVM Express NVMe-oF TCP Transport Specification Rev 1.2,
    Section 3.6.2 (PDU Definitions), Figure 33 (C2HData PDU)
    """
    # Common flags for multiple PDU types
    HDGSTF = 0x01  # Bit 0: PDU Header Digest Flag
    DDGSTF = 0x02  # Bit 1: PDU Data Digest Flag

    # C2HData PDU specific flags (Figure 33, lines 2423-2438)
    LAST_PDU = 0x04     # Bit 2: Last PDU in data transfer
    SCSS = 0x08         # Bit 3: SUCCESS - Command completed successfully, no CapsuleResp follows

    # H2CData PDU specific flags
    H2C_DATA_LAST = 0x04  # Bit 2: Last PDU in data transfer


class PDUHeader(NamedTuple):
    """NVMe-oF TCP PDU Header structure."""
    pdu_type: int
    flags: int
    hlen: int
    pdo: int
    plen: int


class DiscoveryLogEntry(NamedTuple):
    """Discovery Log Entry structure."""
    transport_type: int
    address_family: int
    subsystem_type: int
    port_id: int
    controller_id: int
    transport_address: str
    transport_service_id: str
    subsystem_nqn: str
