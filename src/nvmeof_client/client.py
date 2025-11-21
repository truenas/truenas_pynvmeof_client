"""
NVMe-oF TCP Client Implementation

Main client class for establishing connections and communicating with NVMe-oF targets
via TCP transport. Implements synchronous operations with optional timeout support.

References:
- NVMe-oF TCP Transport Specification Revision 1.1
- NVMe Base Specification Revision 2.2
"""

import select
import socket
import struct
import time
import logging
import uuid
from typing import Any

from .exceptions import (
    CommandError,
    NVMeoFConnectionError,
    NVMeoFTimeoutError,
    ProtocolError,
)
from .models import (
    AddressFamily,
    ANALogPage,
    ANAState,
    AsyncEvent,
    ControllerInfo,
    DiscoveryEntry,
    NamespaceInfo,
    ReservationAction,
    ReservationInfo,
    ReservationStatus,
    ReservationType,
    TransportType,
)
from .parsers import (
    ANALogPageParser,
    AsyncEventParser,
    ChangedNamespaceListParser,
    ControllerDataParser,
    NamespaceDataParser,
    ReservationDataParser,
    ResponseParser,
)
from .protocol import (
    # Types and Enums
    ControllerConfiguration,
    ControllerStatus,
    FeatureIdentifier,
    IdentifyDataStructure,
    LogPageIdentifier,
    NVMeProperty,
    PDUFlags,
    PDUHeader,
    PDUType,
    # Constants
    NVME_COMMAND_ID_MASK,
    NVME_COMMAND_SIZE,
    NVME_DEFAULT_MAX_ENTRIES,
    NVME_DISCOVERY_LOG_SIZE,
    NVME_IOCQES_16_BYTES,
    NVME_IOSQES_64_BYTES,
    NVME_MAX_IO_SIZE,
    NVME_SECTOR_SIZE,
    NVME_TCP_PFV_1_0,
    NVMEOF_TCP_CMD_HEADER_LEN,
    NVMEOF_TCP_CMD_PDO,
    NVMEOF_TCP_ICREQ_HEADER_LEN,
    NVMEOF_TCP_ICREQ_TOTAL_LEN,
    NVMEOF_TCP_PDU_BASIC_HEADER_LEN,
    NVMEOF_TCP_PORT,
    # Functions
    format_discovery_entry,
    pack_async_event_request_command,
    pack_fabric_connect_command,
    pack_fabric_connect_data,
    pack_fabric_property_get_command,
    pack_fabric_property_set_command,
    pack_get_features_command,
    pack_get_log_page_command,
    pack_identify_command,
    pack_keep_alive_command,
    pack_nvme_command,
    pack_nvme_compare_command,
    pack_nvme_flush_command,
    pack_nvme_read_command,
    pack_nvme_reservation_acquire_command,
    pack_nvme_reservation_register_command,
    pack_nvme_reservation_release_command,
    pack_nvme_reservation_report_command,
    pack_nvme_write_command,
    pack_nvme_write_command_host_data,
    pack_nvme_write_uncorrectable_command,
    pack_nvme_write_zeroes_command,
    pack_pdu_header,
    pack_set_features_command,
    parse_controller_capabilities,
    parse_discovery_log_page,
    unpack_pdu_header,
)


class NVMeoFClient:
    """
    NVMe over Fabrics TCP Client

    Provides synchronous interface for connecting to and communicating with
    NVMe-oF targets using TCP transport protocol.

    Example:
        # Use automatically generated Host NQN
        client = NVMeoFClient("192.168.1.100", timeout=30.0)

        # Or provide a specific Host NQN
        client = NVMeoFClient("192.168.1.100",
                             host_nqn="nqn.2014-08.org.nvmexpress:uuid:12345678-1234-1234-1234-123456789abc")

        try:
            client.connect()
            response = client.send_identify_command()
            print(f"Controller ID: {response.get('controller_id')}")
        finally:
            client.disconnect()
    """

    def __init__(self, host: str, subsystem_nqn: str | None = None,
                 port: int = NVMEOF_TCP_PORT,
                 timeout: float | None = None, host_nqn: str | None = None,
                 kato: int = 0):
        """
        Initialize NVMe-oF TCP client.

        Args:
            host: Target hostname or IP address
            subsystem_nqn: Subsystem NQN to connect to (if None, connects to discovery service)
            port: Target port (default: 4420 per NVMe-oF specification)
            timeout: Default timeout for operations in seconds
            host_nqn: Host NQN to use (if None, one will be generated automatically)
            kato: Keep Alive Timeout in milliseconds (0 = disabled, default)

        Reference: NVMe-oF TCP Transport Specification Section 2.1
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._subsystem_nqn = subsystem_nqn  # Subsystem NQN to connect to
        self._host_nqn = host_nqn  # Store user-provided Host NQN or None
        self._kato = kato  # Keep Alive Timeout in milliseconds
        self._socket: socket.socket | None = None
        self._io_socket: socket.socket | None = None  # Separate socket for I/O queue
        self._connected = False
        self._admin_command_id_counter = 1  # Separate command ID sequence for admin queue
        self._io_command_id_counter = 1     # Separate command ID sequence for I/O queue
        self._logger = logging.getLogger(__name__)

        # Connection parameters - negotiated during connection setup
        self._max_data_size = 4096  # Maximum data transfer size
        self._queue_depth = 32      # Maximum outstanding commands
        self._controller_pda = 0    # Controller PDU Data Alignment
        self._digest_types = 0      # Digest types supported

        # R2T (Ready to Transfer) flow parameters
        # IOCCSZ: I/O Command Capsule Supported Size in 16-byte units (bytes 1792-1795 from Identify Controller)
        # Reference: NVMe Base Specification Section 5.2.13.2.1, Figure 328
        # Note: Value is in 16-byte units. Minimum is 4 (= 64 bytes). Multiply by 16 to get bytes.
        self._ioccsz = 0
        # MAXH2CDATA: Maximum H2C_DATA transfer length per PDU in bytes (from ICRESP)
        # Reference: NVMe-oF TCP Transport Specification Rev 1.2, Section 3.6.2.3, Figure 27
        self._maxh2cdata = 0

        # Connection type tracking
        self._connected_subsystem_nqn = None
        self._is_discovery_subsystem = False

        # Namespace information cache for performance optimization
        self._namespace_info_cache: dict[int, dict[str, Any]] = {}  # Cache namespace info by NSID

        # I/O queue tracking
        self._io_queues_setup = False  # Track if I/O queues are established
        self._io_queue_size = 64       # Default I/O queue size (1-based)
        self._controller_id = None     # Controller ID assigned by target

        # Asynchronous event tracking
        self._outstanding_async_requests: list[int] = []  # Command IDs of outstanding async event requests
        self._async_events_enabled = False  # Track if async events are enabled via Set Features
        self._aerl: int | None = None    # Asynchronous Event Request Limit from controller

    def connect(self, subsystem_nqn: str = None) -> None:
        """
        Establish TCP connection to NVMe-oF target and perform initialization.

        Args:
            subsystem_nqn: Target subsystem NQN to connect to. If None, uses subsystem_nqn
                          from constructor. If both are None, connects to discovery subsystem.

        Raises:
            NVMeoFConnectionError: If connection establishment fails
            NVMeoFTimeoutError: If connection times out
            ProtocolError: If protocol negotiation fails

        Reference: NVMe-oF TCP Transport Specification Section 3.1
        """
        if self._connected:
            raise NVMeoFConnectionError("Already connected to target")

        # Use subsystem_nqn from constructor if not provided
        if subsystem_nqn is None:
            subsystem_nqn = self._subsystem_nqn

        try:
            self._logger.info(f"Connecting to NVMe-oF target at {self.host}:{self.port}")

            # Create TCP socket
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.timeout:
                self._socket.settimeout(self.timeout)

            # Establish TCP connection
            start_time = time.time()
            self._socket.connect((self.host, self.port))

            # Perform NVMe-oF connection initialization
            self._initialize_connection(subsystem_nqn)

            self._connected = True

            # Configure controller if needed (must be done after _connected is set)
            if subsystem_nqn is None or subsystem_nqn == "nqn.2014-08.org.nvmexpress.discovery":
                self._logger.debug("Configuring controller for discovery operations...")
                self.configure_controller()
            else:
                # For NVM subsystems, also try controller configuration
                # Some targets require this even for NVM subsystems
                self._logger.debug("Configuring controller for NVM subsystem operations...")
                try:
                    self.configure_controller()
                except Exception as e:
                    self._logger.warning(f"Controller configuration failed (may not be required): {e}")

            elapsed = time.time() - start_time
            self._logger.info(f"Connected to target in {elapsed:.2f} seconds")

        except socket.timeout:
            self._cleanup_socket()
            raise NVMeoFTimeoutError(f"Connection timeout after {self.timeout} seconds")
        except socket.error as e:
            self._cleanup_socket()
            raise NVMeoFConnectionError(f"TCP connection failed: {e}")
        except Exception as e:
            self._cleanup_socket()
            raise NVMeoFConnectionError(f"Connection initialization failed: {e}")

    def disconnect(self) -> None:
        """
        Close connection to NVMe-oF target.

        Performs graceful shutdown by deleting I/O queues via admin commands,
        then closing the TCP connection. Per NVMe-oF TCP spec Section 3.5
        "Error Handling Model", H2CTermReq PDU is reserved for fatal error
        scenarios only, not normal graceful disconnection.

        Reference: NVMe-oF TCP Transport Specification Rev 1.2, Section 3.5
        """
        if not self._connected:
            return

        try:
            self._logger.info("Disconnecting from NVMe-oF target")

            # Clean up I/O queues before disconnecting
            try:
                self.cleanup_io_queues()
            except Exception as e:
                self._logger.warning(f"Failed to clean up I/O queues: {e}")

            # For normal graceful disconnect, simply close the TCP connection
            # H2CTermReq PDU is only for fatal transport errors (see Section 3.5)

        finally:
            self._cleanup_socket()
            self._cleanup_io_socket()
            self._connected = False
            self._connected_subsystem_nqn = None
            self._is_discovery_subsystem = False
            self._io_queues_setup = False  # Ensure queue state is reset

            # Clear namespace info cache on disconnect to ensure fresh data on reconnection
            self._namespace_info_cache.clear()

            self._logger.info("Disconnected from target")

    def send_command(self, opcode: int, nsid: int = 0, data: bytes | None = None,
                     timeout: float | None = None) -> dict[str, Any]:
        """
        Send NVMe command to target and receive response.

        Args:
            opcode: NVMe command opcode
            nsid: Namespace identifier
            data: Optional command data payload
            timeout: Command-specific timeout (overrides default)

        Returns:
            Dictionary containing response data and status

        Raises:
            CommandError: If command execution fails
            NVMeoFTimeoutError: If command times out
            ProtocolError: If protocol error occurs

        Reference: NVMe Base Specification Section 4 (Command Processing)
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        command_id = self._get_next_command_id()
        cmd_timeout = timeout or self.timeout

        try:
            self._logger.debug(f"Sending command {command_id}: opcode={opcode:02x}, nsid={nsid}")

            # Build and send command PDU
            self._send_command_pdu(opcode, command_id, nsid, data)

            # Receive and parse response
            response = self._receive_response(command_id, cmd_timeout)

            self._logger.debug(f"Command {command_id} completed successfully")
            return response

        except Exception as e:
            self._logger.error(f"Command {command_id} failed: {e}")
            raise

    def get_controller_info(self) -> ControllerInfo:
        """
        Get structured controller information using new data models.

        This is a high-level method that returns a ControllerInfo object instead of
        a dictionary. It internally calls identify_controller() and parses the result.

        Returns:
            ControllerInfo object with parsed controller data

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If Identify Controller command fails
        """
        # Call low-level method to get raw dict
        controller_dict = self.identify_controller()

        # Convert VER field to version string (major.minor.tertiary)
        ver_raw = controller_dict.get('ver', 0)
        if ver_raw > 0:
            major = (ver_raw >> 16) & 0xFFFF
            minor = (ver_raw >> 8) & 0xFF
            tertiary = ver_raw & 0xFF
            nvme_version_str = f"{major}.{minor}.{tertiary}"
        else:
            nvme_version_str = None

        # Parse OAES (Optional Asynchronous Events Supported) bitfield
        oaes = controller_dict.get('oaes', 0)

        # Convert to ControllerInfo object
        controller_info = ControllerInfo(
            vendor_id=controller_dict.get('vid', 0),
            subsystem_vendor_id=controller_dict.get('ssvid', 0),
            serial_number=controller_dict.get('sn', ''),
            model_number=controller_dict.get('mn', ''),
            firmware_revision=controller_dict.get('fr', ''),
            controller_id=controller_dict.get('cntlid', 0),
            max_data_transfer_size=controller_dict.get('mdts', 0),
            controller_multipath_io_capabilities=controller_dict.get('cmic', 0),
            optional_admin_command_support=controller_dict.get('oacs', 0),
            optional_nvm_command_support=controller_dict.get('oncs', 0),
            # OAES bits (bytes 92-95)
            oaes_namespace_attribute_notices=bool(oaes & (1 << 8)),
            oaes_firmware_activation_notices=bool(oaes & (1 << 9)),
            oaes_ana_change_notices=bool(oaes & (1 << 11)),
            oaes_predictable_latency_event_notices=bool(oaes & (1 << 12)),
            oaes_lba_status_information_notices=bool(oaes & (1 << 13)),
            oaes_endurance_group_event_notices=bool(oaes & (1 << 14)),
            oaes_normal_subsystem_shutdown_notices=bool(oaes & (1 << 15)),
            oaes_temperature_threshold_hysteresis=bool(oaes & (1 << 16)),
            oaes_reachability_groups_change_notices=bool(oaes & (1 << 17)),
            oaes_allocated_namespace_attribute_notices=bool(oaes & (1 << 19)),
            oaes_cross_controller_reset_notices=bool(oaes & (1 << 20)),
            oaes_lost_host_communication_notices=bool(oaes & (1 << 21)),
            oaes_zone_descriptor_changed_notices=bool(oaes & (1 << 27)),
            oaes_discovery_log_change_notices=bool(oaes & (1 << 31)),
            aerl=controller_dict.get('aerl', 0),
            max_submission_queue_entries=controller_dict.get('maxcmd', 0),
            max_completion_queue_entries=controller_dict.get('maxcmd', 0),
            number_of_namespaces=controller_dict.get('nn', 0),
            max_power_consumption=0,  # Not parsed in current controller parser
            warning_composite_temp_threshold=controller_dict.get('wctemp', 0),
            critical_composite_temp_threshold=controller_dict.get('cctemp', 0),
            nvme_version=nvme_version_str
        )

        return controller_info

    def identify_controller(self) -> dict[str, Any]:
        """
        Send Identify Controller command to retrieve controller information.

        Returns:
            Dictionary containing parsed controller information:
            - vid: Vendor ID
            - ssvid: Subsystem Vendor ID
            - sn: Serial Number
            - mn: Model Number
            - fr: Firmware Revision
            - rab: Recommended Arbitration Burst
            - ieee: IEEE OUI Identifier
            - cmic: Controller Multi-Path I/O and Namespace Sharing Capabilities
            - mdts: Maximum Data Transfer Size
            - cntlid: Controller ID
            - ver: Version
            - rtd3r: RTD3 Resume Latency
            - rtd3e: RTD3 Entry Latency
            - oaes: Optional Asynchronous Events Supported
            - ctratt: Controller Attributes
            - rrls: Read Recovery Levels Supported
            - cntrltype: Controller Type
            - fguid: FRU GUID
            - crdt1: Command Retry Delay Time 1
            - crdt2: Command Retry Delay Time 2
            - crdt3: Command Retry Delay Time 3
            - oacs: Optional Admin Command Support
            - acl: Abort Command Limit
            - aerl: Asynchronous Event Request Limit
            - frmw: Firmware Updates
            - lpa: Log Page Attributes
            - elpe: Error Log Page Entries
            - npss: Number of Power States Support
            - avscc: Admin Vendor Specific Command Configuration
            - apsta: Autonomous Power State Transition Attributes
            - wctemp: Warning Composite Temperature Threshold
            - cctemp: Critical Composite Temperature Threshold
            - mtfa: Maximum Time for Firmware Activation
            - hmpre: Host Memory Buffer Preferred Size
            - hmmin: Host Memory Buffer Minimum Size
            - tnvmcap: Total NVM Capacity
            - unvmcap: Unallocated NVM Capacity
            - rpmbs: Replay Protected Memory Block Support
            - edstt: Extended Device Self-test Time
            - dsto: Device Self-test Options
            - fwug: Firmware Update Granularity
            - kas: Keep Alive Support
            - hctma: Host Controlled Thermal Management Attributes
            - mntmt: Minimum Thermal Management Temperature
            - mxtmt: Maximum Thermal Management Temperature
            - sanicap: Sanitize Capabilities
            - hmminds: Host Memory Buffer Minimum Descriptor Entry Size
            - hmmaxd: Host Memory Buffer Maximum Descriptors
            - nsetidmax: NVM Set Identifier Maximum
            - endgidmax: Endurance Group Identifier Maximum
            - anatt: ANA Transition Time
            - anacap: Asymmetric Namespace Access Capabilities
            - anagrpmax: ANA Group Identifier Maximum
            - nanagrpid: Number of ANA Group Identifiers
            - pels: Persistent Event Log Size
            - sqes: Submission Queue Entry Size
            - cqes: Completion Queue Entry Size
            - maxcmd: Maximum Outstanding Commands
            - nn: Number of Namespaces
            - oncs: Optional NVM Command Support
            - fuses: Fused Operation Support
            - fna: Format NVM Attributes
            - vwc: Volatile Write Cache
            - awun: Atomic Write Unit Normal
            - awupf: Atomic Write Unit Power Fail
            - nvscc: NVM Vendor Specific Command Configuration
            - nwpc: Namespace Write Protection Capabilities
            - acwu: Atomic Compare & Write Unit
            - sgls: SGL Support
            - mnan: Maximum Number of Allowed Namespaces
            - subnqn: NVM Subsystem NVMe Qualified Name
            - ioccsz: I/O Queue Command Capsule Supported Size
            - iorcsz: I/O Queue Response Capsule Supported Size
            - icdoff: In Capsule Data Offset
            - ctrattr: Controller Attributes
            - msdbd: Maximum SGL Data Block Descriptors

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If identify command fails

        Reference: NVMe Base Specification Section 5.15 (Identify Command)
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError(
                "Identify Controller not available on discovery subsystem. "
                "Connect to an NVM subsystem to use this command.")

        command_id = self._get_next_command_id()

        try:
            self._logger.debug(f"Sending Identify Controller with command {command_id}")

            # Build and send Identify Controller command
            self._send_identify_controller_pdu(command_id)

            # Receive response - data comes first, then completion
            first_header, first_data = self._receive_pdu()
            self._logger.debug(
                f"Received first PDU: type={first_header.pdu_type}, len={len(first_data)}")

            identify_data = b''

            if first_header.pdu_type == PDUType.C2H_DATA:
                # Data PDU comes first (typical for multi-PDU responses)
                identify_data = first_data
                self._logger.debug(f"Received identify controller data: {len(identify_data)} bytes")

                # Check for C2H SUCCESS flag (C2H success optimization)
                # Reference: NVMe-oF TCP Transport Specification Rev 1.2,
                # Section 3.3.2.1 (lines 888-900), Figure 33 (lines 2423-2427)
                if first_header.flags & PDUFlags.SCSS:
                    # SUCCESS flag set - command completed successfully, no CapsuleResp follows
                    # Validate: SUCCESS requires LAST_PDU (protocol requirement)
                    if not (first_header.flags & PDUFlags.LAST_PDU):
                        # Fatal protocol error per spec (lines 898-900)
                        raise ProtocolError(
                            f"C2HData PDU has SUCCESS flag without LAST_PDU flag - "
                            f"protocol violation (flags=0x{first_header.flags:02x})")

                    self._logger.debug("C2HData PDU has SUCCESS flag - command completed successfully")
                    # Synthesize successful completion (status=0) per spec
                    response = {'status': 0}
                else:
                    # SUCCESS flag not set - wait for CapsuleResp with actual status
                    try:
                        response_header, response_data = self._receive_pdu()
                        self._logger.debug(
                            f"Received completion PDU: type={response_header.pdu_type}, len={len(response_data)}")

                        if response_header.pdu_type != PDUType.RSP:
                            raise ProtocolError(
                                f"Expected response PDU, got type {response_header.pdu_type}")

                        # Parse the completion entry to check status
                        response = ResponseParser.parse_response(response_data, command_id)
                    except Exception as e:
                        self._logger.warning(f"Failed to receive completion PDU: {e}")
                        # Some targets may close connection after sending data
                        response = {'status': 0}

            elif first_header.pdu_type == PDUType.RSP:
                # Completion first (less common)
                response = ResponseParser.parse_response(first_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Identify Controller command failed with status 0x{response['status']:04x}",
                        response['status'], command_id)

                # Try to receive data PDU
                try:
                    data_header, identify_data = self._receive_pdu()
                    if data_header.pdu_type != PDUType.C2H_DATA:
                        self._logger.warning(f"Expected data PDU, got type {data_header.pdu_type}")
                except Exception as e:
                    self._logger.warning(f"Failed to receive data PDU: {e}")
                    identify_data = b''
            else:
                raise ProtocolError(
                    f"Expected DATA or RSP PDU, got type {first_header.pdu_type}")

            if not identify_data:
                self._logger.warning("No identify controller data received")
                return {}

            self._logger.debug(f"Received identify controller data: {len(identify_data)} bytes")

            # Parse the identify controller data structure
            controller_info = ControllerDataParser.parse(identify_data)

            self._logger.debug("Identify Controller completed successfully")
            return controller_info

        except Exception as e:
            self._logger.error(f"Identify Controller failed: {e}")
            raise

    def get_namespace_info(self, nsid: int) -> NamespaceInfo:
        """
        Get structured namespace information using new data models.

        This is a high-level method that returns a NamespaceInfo object instead of
        a dictionary. It internally calls identify_namespace() and parses the result.

        Args:
            nsid: Namespace identifier (1-based, must be > 0)

        Returns:
            NamespaceInfo object with parsed namespace data

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If Identify Namespace command fails
            ValueError: If nsid is invalid
        """
        # Call low-level method to get raw dict
        ns_dict = self.identify_namespace(nsid)

        # Parse DPS (Data Protection Settings) field
        dps = ns_dict.get('dps', 0)
        protection_type = dps & 0x07  # Bits 2-0
        protection_info_location = (dps >> 3) & 0x01  # Bit 3

        # Convert to NamespaceInfo object
        namespace_info = NamespaceInfo(
            namespace_id=nsid,
            namespace_size=ns_dict.get('nsze', 0),
            namespace_capacity=ns_dict.get('ncap', 0),
            namespace_utilization=ns_dict.get('nuse', 0),
            logical_block_size=ns_dict.get('logical_block_size', 512),
            metadata_size=ns_dict.get('lbaf0_ms', 0),
            relative_performance=ns_dict.get('lbaf0_rp', 0),
            thin_provisioning_supported=bool(ns_dict.get('nsfeat', 0) & 0x01),
            deallocate_supported=bool(ns_dict.get('nsfeat', 0) & 0x04),
            write_zeros_supported=False,  # This comes from controller caps, not namespace
            protection_type=protection_type,
            protection_info_location=protection_info_location,
            preferred_write_granularity=ns_dict.get('npwg', 0),
            preferred_write_alignment=ns_dict.get('npwa', 0)
        )

        return namespace_info

    def identify_namespace(self, nsid: int) -> dict[str, Any]:
        """
        Send Identify Namespace command to retrieve namespace information.

        Args:
            nsid: Namespace identifier (1-based, must be > 0)

        Returns:
            Dictionary containing parsed namespace information:
            - nsze: Namespace Size (total number of logical blocks)
            - ncap: Namespace Capacity (total number of logical blocks that may be allocated)
            - nuse: Namespace Utilization (number of logical blocks currently allocated)
            - nsfeat: Namespace Features
            - nlbaf: Number of LBA Formats
            - flbas: Formatted LBA Size
            - mc: Metadata Capabilities
            - dpc: End-to-end Data Protection Capabilities
            - dps: End-to-end Data Protection Type Settings
            - nmic: Namespace Multi-path I/O and Namespace Sharing Capabilities
            - rescap: Reservation Capabilities
            - fpi: Format Progress Indicator
            - dlfeat: Deallocate Logical Block Features
            - nawun: Namespace Atomic Write Unit Normal
            - nawupf: Namespace Atomic Write Unit Power Fail
            - nacwu: Namespace Atomic Compare & Write Unit
            - nabsn: Namespace Atomic Boundary Size Normal
            - nabo: Namespace Atomic Boundary Offset
            - nabspf: Namespace Atomic Boundary Size Power Fail
            - noiob: Namespace Optimal IO Boundary
            - nvmcap: NVM Capacity
            - npwg: Namespace Preferred Write Granularity
            - npwa: Namespace Preferred Write Alignment
            - npdg: Namespace Preferred Deallocate Granularity
            - npda: Namespace Preferred Deallocate Alignment
            - nows: Namespace Optimal Write Size
            - mssrl: Maximum Single Source Range Length
            - mcl: Maximum Copy Length
            - msrc: Maximum Source Range Count
            - nulbaf: Number of Unique LBA Formats
            - anagrpid: ANA Group Identifier
            - nsattr: Namespace Attributes
            - nvmsetid: NVM Set Identifier
            - endgid: Endurance Group Identifier
            - nguid: Namespace Globally Unique Identifier
            - eui64: IEEE Extended Unique Identifier (64-bit)
            - lbaf: LBA Format Support (array of formats)
            - vs: Vendor Specific data

        Raises:
            NVMeoFConnectionError: If not connected to target or connected to discovery subsystem
            CommandError: If identify namespace command fails
            ValueError: If nsid is invalid (must be > 0)

        Reference: NVMe Base Specification Section 5.15 (Identify Command)
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError(
                "Identify Namespace not available on discovery subsystem. "
                "Connect to an NVM subsystem to use this command.")

        if nsid <= 0:
            raise ValueError("Namespace ID must be greater than 0")

        command_id = self._get_next_command_id()

        try:
            self._logger.debug(f"Sending Identify Namespace for NSID {nsid} with command {command_id}")

            # Build and send Identify Namespace command
            self._send_identify_namespace_pdu(command_id, nsid)

            # Receive response - data comes first, then completion
            first_header, first_data = self._receive_pdu()
            self._logger.debug(
                f"Received first PDU: type={first_header.pdu_type}, len={len(first_data)}")

            identify_data = b''

            if first_header.pdu_type == PDUType.C2H_DATA:
                # Data PDU comes first (typical for multi-PDU responses)
                identify_data = first_data
                self._logger.debug(f"Received identify namespace data: {len(identify_data)} bytes")

                # Check for C2H SUCCESS flag (C2H success optimization)
                # Reference: NVMe-oF TCP Transport Specification Rev 1.2,
                # Section 3.3.2.1 (lines 888-900), Figure 33 (lines 2423-2427)
                if first_header.flags & PDUFlags.SCSS:
                    # SUCCESS flag set - command completed successfully, no CapsuleResp follows
                    # Validate: SUCCESS requires LAST_PDU (protocol requirement)
                    if not (first_header.flags & PDUFlags.LAST_PDU):
                        # Fatal protocol error per spec (lines 898-900)
                        raise ProtocolError(
                            f"C2HData PDU has SUCCESS flag without LAST_PDU flag - "
                            f"protocol violation (flags=0x{first_header.flags:02x})")

                    self._logger.debug("C2HData PDU has SUCCESS flag - command completed successfully")
                    # Synthesize successful completion (status=0) per spec
                    response = {'status': 0}
                else:
                    # SUCCESS flag not set - wait for CapsuleResp with actual status
                    try:
                        response_header, response_data = self._receive_pdu()
                        self._logger.debug(
                            f"Received completion PDU: type={response_header.pdu_type}, len={len(response_data)}")

                        if response_header.pdu_type != PDUType.RSP:
                            raise ProtocolError(
                                f"Expected response PDU, got type {response_header.pdu_type}")

                        # Parse the completion entry to check status
                        response = ResponseParser.parse_response(response_data, command_id)
                        if response['status'] != 0:
                            raise CommandError(
                                f"Identify Namespace failed with status {response['status']:02x}",
                                response['status'], command_id)
                    except Exception as e:
                        self._logger.warning(f"Failed to receive completion PDU: {e}")
                        # Some targets may close connection after sending data
                        response = {'status': 0}

            elif first_header.pdu_type == PDUType.RSP:
                # Completion first (less common)
                response = ResponseParser.parse_response(first_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Identify Namespace failed with status {response['status']:02x}",
                        response['status'], command_id)

                # Try to receive data PDU
                try:
                    data_header, identify_data = self._receive_pdu()
                    if data_header.pdu_type != PDUType.C2H_DATA:
                        self._logger.warning(f"Expected data PDU, got type {data_header.pdu_type}")
                except Exception as e:
                    self._logger.warning(f"Failed to receive data PDU: {e}")
                    identify_data = b''
            else:
                raise ProtocolError(
                    f"Expected DATA or RSP PDU, got type {first_header.pdu_type}")

            if not identify_data:
                self._logger.warning("No identify namespace data received")
                return {}

            self._logger.debug(f"Received identify namespace data: {len(identify_data)} bytes")

            # Parse the identify namespace data structure
            namespace_info = NamespaceDataParser.parse(identify_data)

            self._logger.debug(f"Identify Namespace {nsid} completed successfully")
            return namespace_info

        except Exception as e:
            self._logger.error(f"Identify Namespace {nsid} failed: {e}")
            raise

    def list_namespaces(self) -> list[int]:
        """
        Get list of active namespace IDs.

        Returns:
            List of active namespace IDs

        Raises:
            NVMeoFConnectionError: If not connected to target or connected to discovery subsystem
            CommandError: If identify namespace list command fails

        Reference: NVMe Base Specification Section 5.15 (Identify Command)
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError(
                "Namespace listing not available on discovery subsystem. "
                "Connect to an NVM subsystem to use this command.")

        command_id = self._get_next_command_id()

        try:
            self._logger.debug(f"Sending Identify Namespace List with command {command_id}")

            # Build and send Identify Namespace List command
            self._send_identify_namespace_list_pdu(command_id)

            # Receive response - data comes first, then completion
            first_header, first_data = self._receive_pdu()
            self._logger.debug(
                f"Received first PDU: type={first_header.pdu_type}, len={len(first_data)}")

            identify_data = b''

            if first_header.pdu_type == PDUType.C2H_DATA:
                # Data PDU comes first (typical for multi-PDU responses)
                identify_data = first_data
                self._logger.debug(f"Received namespace list data: {len(identify_data)} bytes")

                # Check for C2H SUCCESS flag (C2H success optimization)
                # Reference: NVMe-oF TCP Transport Specification Rev 1.2,
                # Section 3.3.2.1 (lines 888-900), Figure 33 (lines 2423-2427)
                if first_header.flags & PDUFlags.SCSS:
                    # SUCCESS flag set - command completed successfully, no CapsuleResp follows
                    # Validate: SUCCESS requires LAST_PDU (protocol requirement)
                    if not (first_header.flags & PDUFlags.LAST_PDU):
                        # Fatal protocol error per spec (lines 898-900)
                        raise ProtocolError(
                            f"C2HData PDU has SUCCESS flag without LAST_PDU flag - "
                            f"protocol violation (flags=0x{first_header.flags:02x})")

                    self._logger.debug("C2HData PDU has SUCCESS flag - command completed successfully")
                    # Synthesize successful completion (status=0) per spec
                    response = {'status': 0}
                else:
                    # SUCCESS flag not set - wait for CapsuleResp with actual status
                    try:
                        response_header, response_data = self._receive_pdu()
                        self._logger.debug(
                            f"Received completion PDU: type={response_header.pdu_type}, len={len(response_data)}")

                        if response_header.pdu_type != PDUType.RSP:
                            raise ProtocolError(
                                f"Expected response PDU, got type {response_header.pdu_type}")

                        # Parse the completion entry to check status
                        response = ResponseParser.parse_response(response_data, command_id)
                        if response['status'] != 0:
                            raise CommandError(
                                f"Identify Namespace List failed with status {response['status']:02x}",
                                response['status'], command_id)
                    except Exception as e:
                        self._logger.warning(f"Failed to receive completion PDU: {e}")
                        # Some targets may close connection after sending data
                        response = {'status': 0}

            elif first_header.pdu_type == PDUType.RSP:
                # Completion first (less common)
                response = ResponseParser.parse_response(first_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Identify Namespace List failed with status {response['status']:02x}",
                        response['status'], command_id)

                # Try to receive data PDU
                try:
                    data_header, identify_data = self._receive_pdu()
                    if data_header.pdu_type != PDUType.C2H_DATA:
                        self._logger.warning(f"Expected data PDU, got type {data_header.pdu_type}")
                except Exception as e:
                    self._logger.warning(f"Failed to receive data PDU: {e}")
                    identify_data = b''
            else:
                raise ProtocolError(
                    f"Expected DATA or RSP PDU, got type {first_header.pdu_type}")

            if not identify_data:
                self._logger.warning("No namespace list data received")
                return []

            self._logger.debug(f"Received namespace list data: {len(identify_data)} bytes")

            # Parse namespace list (4096 bytes, each NSID is 4 bytes)
            namespace_ids = []
            for i in range(0, min(len(identify_data), 4096), 4):
                nsid = struct.unpack('<L', identify_data[i: i + 4])[0]
                if nsid != 0:  # Valid namespace ID
                    namespace_ids.append(nsid)
                else:
                    break  # End of list (remaining entries are zero)

            self._logger.debug(f"Found {len(namespace_ids)} active namespaces: {namespace_ids}")
            return namespace_ids

        except Exception as e:
            self._logger.error(f"List namespaces failed: {e}")
            raise

    def set_features(self, feature_id: int, value: int, nsid: int = 0) -> dict:
        """
        Send Set Features command to the controller.

        Args:
            feature_id: Feature identifier (e.g., 0x07 for Number of Queues)
            value: Feature value to set
            nsid: Namespace ID (0 for controller-level features)

        Returns:
            Dictionary with response status and data

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If Set Features command fails

        Reference: NVM Express Base Specification Section 5.27
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        command_id = self._get_next_command_id()

        try:
            self._logger.debug(f"Sending Set Features command (FID=0x{feature_id:02x}, value=0x{value:08x})")

            # Build and send Set Features command
            set_features_cmd = pack_set_features_command(command_id, feature_id, value, nsid)
            self._send_admin_command_pdu(set_features_cmd)

            response_header, response_data = self._receive_pdu()
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected response PDU, got type {response_header.pdu_type}")

            response = ResponseParser.parse_response(response_data, command_id)
            if response['status'] != 0:
                raise CommandError(f"Set Features (FID=0x{feature_id:02x}) failed with status {response['status']:02x}",
                                   response['status'], command_id)

            self._logger.debug(f"Set Features (FID=0x{feature_id:02x}) succeeded")
            return response

        except Exception as e:
            self._logger.error(f"Set Features failed: {e}")
            raise

    def get_features(self, feature_id: int, nsid: int = 0) -> dict:
        """
        Send Get Features command to retrieve feature settings from the controller.

        Args:
            feature_id: Feature identifier (e.g., 0x07 for Number of Queues)
            nsid: Namespace ID (0 for controller-level features)

        Returns:
            Dictionary with response status and feature value

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If Get Features command fails

        Reference: NVM Express Base Specification Section 5.17
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        command_id = self._get_next_command_id()

        try:
            self._logger.debug(f"Sending Get Features command (FID=0x{feature_id:02x})")

            # Build and send Get Features command
            get_features_cmd = pack_get_features_command(command_id, feature_id, nsid)
            self._send_admin_command_pdu(get_features_cmd)

            response_header, response_data = self._receive_pdu()
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected response PDU, got type {response_header.pdu_type}")

            response = ResponseParser.parse_response(response_data, command_id)
            if response['status'] != 0:
                raise CommandError(f"Get Features (FID=0x{feature_id:02x}) failed with status {response['status']:02x}",
                                   response['status'], command_id)

            self._logger.debug(
                "Get Features (FID=0x%02x) succeeded, value=0x%08x",
                feature_id, response.get('result', 0))
            return response

        except Exception as e:
            self._logger.error(f"Get Features failed: {e}")
            raise

    def send_keep_alive(self) -> dict:
        """
        Send Keep Alive command to maintain the NVMe-oF connection.

        Keep Alive commands are used to maintain the connection to NVMe-oF targets
        that require periodic communication to prevent connection timeout. This should
        be called manually by the application at appropriate intervals.

        Returns:
            Dictionary with response status

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If Keep Alive command fails

        Reference: NVM Express Base Specification Section 5.25 "Keep Alive command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        command_id = self._get_next_command_id()

        try:
            self._logger.debug("Sending Keep Alive command")

            # Build and send Keep Alive command
            keep_alive_cmd = pack_keep_alive_command(command_id)
            self._send_admin_command_pdu(keep_alive_cmd)

            response_header, response_data = self._receive_pdu()
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected response PDU, got type {response_header.pdu_type}")

            response = ResponseParser.parse_response(response_data, command_id)
            if response['status'] != 0:
                raise CommandError(f"Keep Alive command failed with status {response['status']:02x}",
                                   response['status'], command_id)

            self._logger.debug("Keep Alive command succeeded")
            return response

        except Exception as e:
            self._logger.error(f"Keep Alive failed: {e}")
            raise

    def get_log_page(self, log_page_id: int, data_length: int, nsid: int = 0) -> bytes:
        """
        Retrieve a generic NVMe log page.

        Args:
            log_page_id: Log Page Identifier (use LogPageIdentifier enum)
            data_length: Number of bytes to retrieve
            nsid: Namespace ID (0 for controller-level log pages)

        Returns:
            Raw bytes of the log page data

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If log page retrieval fails

        Reference:
            NVM Express Base Specification 2.3, Section 5.2.12 "Get Log Page command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError(
                "Log pages not available on discovery subsystem. "
                "Connect to an NVM subsystem to use this command.")

        command_id = self._get_next_command_id()

        try:
            self._logger.debug(
                "Getting log page: id=0x%02x, length=%d, nsid=%d",
                log_page_id, data_length, nsid)

            # Send Get Log Page command
            self._send_get_log_page_pdu(command_id, log_page_id, data_length)

            # Receive response - data comes first, then completion
            first_header, first_data = self._receive_pdu()
            self._logger.debug(
                "Received first PDU: type=%s, len=%d",
                first_header.pdu_type, len(first_data))

            log_data = b''

            if first_header.pdu_type == PDUType.C2H_DATA:
                # Data PDU comes first
                log_data = first_data
                self._logger.debug("Received log page data: %d bytes", len(log_data))

                # Check for C2H SUCCESS flag (C2H success optimization)
                # Reference: NVMe-oF TCP Transport Specification Rev 1.2,
                # Section 3.3.2.1 (lines 888-900), Figure 33 (lines 2423-2427)
                if first_header.flags & PDUFlags.SCSS:
                    # SUCCESS flag set - command completed successfully, no CapsuleResp follows
                    # Validate: SUCCESS requires LAST_PDU (protocol requirement)
                    if not (first_header.flags & PDUFlags.LAST_PDU):
                        # Fatal protocol error per spec (lines 898-900)
                        raise ProtocolError(
                            f"C2HData PDU has SUCCESS flag without LAST_PDU flag - "
                            f"protocol violation (flags=0x{first_header.flags:02x})")

                    self._logger.debug("C2HData PDU has SUCCESS flag - command completed successfully")
                    # Synthesize successful completion (status=0) per spec
                    # No need to wait for CapsuleResp
                else:
                    # SUCCESS flag not set - wait for CapsuleResp with actual status
                    response_header, response_data = self._receive_pdu()
                    self._logger.debug(
                        "Received completion PDU: type=%s, len=%d",
                        response_header.pdu_type, len(response_data))

                    if response_header.pdu_type != PDUType.RSP:
                        raise ProtocolError(
                            f"Expected response PDU, got type {response_header.pdu_type}")

                    # Parse completion to check status
                    response = ResponseParser.parse_response(response_data, command_id)
                    if response['status'] != 0:
                        raise CommandError(
                            f"Get Log Page command failed with status 0x{response['status']:04x}",
                            response['status'], command_id)

            elif first_header.pdu_type == PDUType.RSP:
                # Response with no data
                response = ResponseParser.parse_response(first_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Get Log Page command failed with status 0x{response['status']:04x}",
                        response['status'], command_id)
            else:
                raise ProtocolError(
                    f"Unexpected PDU type {first_header.pdu_type} for Get Log Page")

            self._logger.debug("Get Log Page succeeded, retrieved %d bytes", len(log_data))
            return log_data

        except Exception as e:
            self._logger.error("Get Log Page failed: %s", e)
            raise

    def get_ana_log_page(self) -> ANALogPage:
        """
        Retrieve and parse the ANA (Asymmetric Namespace Access) log page.

        Returns ANA state information for all ANA Groups with namespaces attached
        to this controller. Useful for determining optimal paths in multi-controller
        configurations.

        Returns:
            ANALogPage dataclass with parsed ANA group information

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If log page retrieval fails
            ValueError: If log page parsing fails

        Reference:
            NVM Express Base Specification 2.3, Figure 227 "Asymmetric Namespace Access Log Page"

        Example:
            >>> ana_log = client.get_ana_log_page()
            >>> print(f"Change count: {ana_log.change_count}")
            >>> for group in ana_log.groups:
            ...     print(f"Group {group.ana_group_id}: {group.ana_state.name}")
            ...     print(f"  Namespaces: {group.namespace_ids}")
        """
        # First retrieve small header to determine total size needed
        # ANA log header is 16 bytes
        header_data = self.get_log_page(
            LogPageIdentifier.ASYMMETRIC_NAMESPACE_ACCESS,
            16
        )

        # Parse header to get number of descriptors
        if len(header_data) < 16:
            raise ValueError(
                f"ANA log page header too short: got {len(header_data)} bytes, need 16")

        # Bytes 8-9: Number of ANA Group Descriptors (16-bit LE)
        num_descriptors = struct.unpack('<H', header_data[8:10])[0]

        self._logger.debug("ANA log page has %d group descriptors", num_descriptors)

        # Calculate required size:
        # - 16 byte header
        # - Each descriptor: 32 bytes minimum + 4 bytes per NSID
        # Conservative estimate: assume max 256 NSIDs per group
        estimated_size = 16 + (num_descriptors * (32 + 256 * 4))

        # Retrieve full log page (or use a reasonable max like 4KB)
        max_log_size = min(estimated_size, 4096)

        full_log_data = self.get_log_page(
            LogPageIdentifier.ASYMMETRIC_NAMESPACE_ACCESS,
            max_log_size
        )

        # Parse the ANA log page
        ana_log = ANALogPageParser.parse_ana_log_page(full_log_data)

        self._logger.debug(
            "Parsed ANA log: %d groups, change_count=%d",
            ana_log.num_ana_group_descriptors, ana_log.change_count)

        return ana_log

    def get_ana_state(self) -> dict[int, ANAState]:
        """
        Get simplified ANA state mapping for all ANA Groups.

        Returns a dictionary mapping ANA Group ID to ANA State, providing
        a quick overview of the accessibility state of each ANA Group.

        Returns:
            Dictionary mapping ana_group_id -> ANAState

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If log page retrieval fails

        Example:
            >>> ana_states = client.get_ana_state()
            >>> for group_id, state in ana_states.items():
            ...     print(f"Group {group_id}: {state.name}")
            Group 1: OPTIMIZED
            Group 2: INACCESSIBLE
        """
        ana_log = self.get_ana_log_page()

        state_map = {}
        for group in ana_log.groups:
            state_map[group.ana_group_id] = group.ana_state

        return state_map

    def get_changed_namespace_list(self) -> list[int]:
        """
        Get the list of namespaces that have changed since last query.

        This method retrieves the Changed Attached Namespace List log page, which
        contains namespaces that have:
        - Changed their Identify Namespace data structures
        - Been attached to the controller
        - Been detached from the controller
        - Been deleted

        The list is cleared after each successful read, so this shows changes since
        the last time this log page was queried.

        Returns:
            List of namespace IDs that have changed (in ascending order).
            Returns [0xFFFFFFFF] if more than 1,024 namespaces changed.
            Returns [] if no changes detected.

        Raises:
            NVMeoFConnectionError: If not connected to target
            CommandError: If log page retrieval fails

        Reference:
            NVM Express Base Specification 2.3
            Section 5.2.12.1.5 "Changed Attached Namespace List"

        Example:
            >>> changed = client.get_changed_namespace_list()
            >>> if changed:
            ...     print(f"Namespaces changed: {changed}")
            ...     # Re-identify changed namespaces to get updated info
            ...     for nsid in changed:
            ...         if nsid != 0xFFFFFFFF:
            ...             ns_info = client.identify_namespace(nsid)
        """

        # Retrieve the log page (up to 4096 bytes for 1,024 entries)
        log_data = self.get_log_page(
            LogPageIdentifier.CHANGED_NAMESPACE_LIST,
            4096
        )

        # Parse the namespace list
        changed_nsids = ChangedNamespaceListParser.parse_changed_namespace_list(log_data)

        self._logger.debug(
            "Changed namespace list: %s",
            ChangedNamespaceListParser.format_changed_namespace_list(changed_nsids))

        return changed_nsids

    def setup_io_queues(self, queue_size: int = 127) -> None:
        """
        Set up I/O queues for data operations.

        Creates I/O completion and submission queues required for I/O operations
        like read, write, and flush. Must be called after connecting to an NVM
        subsystem and before performing any I/O operations.

        Args:
            queue_size: Number of entries in the I/O queue (0-based, so 127 = 128 entries)

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If queue creation fails

        Reference: NVM Express Base Specification Sections 5.3-5.4
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O queues not available on discovery subsystem")

        if self._io_queues_setup:
            self._logger.debug("I/O queues already set up")
            return

        try:
            # Get IOCCSZ from Identify Controller data for R2T flow calculations
            # Reference: NVMe Base Specification Section 5.2.13.2.1, Figure 328, bytes 1792-1795
            if self._ioccsz == 0:
                controller_data = self.identify_controller()
                self._ioccsz = controller_data.get('ioccsz', 0)
                self._logger.debug("Retrieved IOCCSZ: %d (in 16-byte units = %d bytes)",
                                   self._ioccsz, self._ioccsz * 16)

            self._logger.debug("Creating I/O queues for NVMe-oF TCP using separate connection...")

            # For NVMe-oF TCP, I/O queues require separate TCP connections
            # Each queue gets its own socket connection with ICREQ/ICRESP + Fabric Connect

            queue_id = 1  # I/O queue ID
            # queue_size parameter is already passed in, use it directly

            if self._controller_id is None:
                raise NVMeoFConnectionError("No controller ID available - admin connection may have failed")
            controller_id = self._controller_id  # Use the controller ID assigned during admin connect

            self._logger.debug(f"Establishing separate TCP connection for I/O queue {queue_id}")

            # Create separate socket for I/O queue
            io_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.timeout:
                io_socket.settimeout(self.timeout)

            # Connect to same target
            io_socket.connect((self.host, self.port))

            # Perform ICREQ/ICRESP handshake on I/O connection
            icreq_data = self._build_icreq()
            self._send_icreq_pdu_on_socket(io_socket, icreq_data)

            # Receive ICRESP on I/O connection
            icresp_header, icresp_data = self._receive_pdu_on_socket(io_socket)
            if icresp_header.pdu_type != PDUType.ICRESP:
                raise ProtocolError(f"Expected ICRESP, got PDU type {icresp_header.pdu_type}")

            self._process_icresp(icresp_data)

            # Send Fabric Connect for I/O queue
            command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence
            connect_cmd = pack_fabric_connect_command(command_id, queue_id=queue_id, queue_size=queue_size, kato=0)

            connect_data = pack_fabric_connect_data(
                host_nqn=self._host_nqn,
                subsys_nqn=self._connected_subsystem_nqn,
                controller_id=controller_id
            )

            self._send_fabric_connect_pdu_on_socket(io_socket, connect_cmd, connect_data)

            # Wait for connect response on I/O connection
            response_header, response_data = self._receive_pdu_on_socket(io_socket)
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected connect response, got PDU type {response_header.pdu_type}")

            # Parse connect response
            connect_response = ResponseParser.parse_response(response_data, command_id)
            if connect_response['status'] != 0:
                io_socket.close()
                raise CommandError(f"I/O queue Fabric Connect failed with status {connect_response['status']:02x}",
                                   connect_response['status'], command_id)

            # Store I/O socket for later use
            self._io_socket = io_socket
            self._logger.debug("I/O queue established via separate connection")

            # Mark I/O queues as set up
            self._io_queues_setup = True
            self._logger.info(f"I/O queues created successfully (Queue ID {queue_id}, {queue_size + 1} entries)")

        except Exception as e:
            self._logger.error(f"I/O queue setup failed: {e}")
            raise

    def cleanup_io_queues(self) -> None:
        """
        Clean up I/O queues.

        For NVMe-oF TCP, I/O queue cleanup is typically handled by the target
        when the connection is terminated. This method marks queues as not set up.

        Reference: NVMe-oF TCP Transport Specification
        """
        if not self._connected:
            return

        if self._is_discovery_subsystem:
            self._logger.debug("No I/O queues to clean up on discovery subsystem")
            return

        if not self._io_queues_setup:
            self._logger.debug("I/O queues not set up, nothing to clean up")
            return

        try:
            self._logger.debug("Cleaning up I/O queues...")

            # For NVMe-oF TCP, I/O queues are cleaned up when the connection terminates
            # No explicit delete commands needed - the target handles this

            self._io_queues_setup = False
            self._logger.debug("I/O queues marked as cleaned up")

        except Exception as e:
            self._logger.error(f"I/O queue cleanup failed: {e}")
            # Don't re-raise - cleanup should be best effort
            # Set flag anyway to avoid repeated cleanup attempts
            self._io_queues_setup = False

    def read_data(self, nsid: int, lba: int, block_count: int) -> bytes:
        """
        Read data from specified namespace.

        Args:
            nsid: Namespace identifier (1-based)
            lba: Starting logical block address (0-based)
            block_count: Number of blocks to read (1-based)

        Returns:
            Read data as bytes

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If read command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 4.3 "Read command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        if block_count <= 0 or block_count > NVME_MAX_IO_SIZE:
            raise ValueError(f"Invalid block_count: {block_count}, must be 1-{NVME_MAX_IO_SIZE}")

        if lba < 0:
            raise ValueError(f"Invalid LBA: {lba}, must be >= 0")

        # Ensure I/O queues are set up before performing I/O operations
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(f"Reading {block_count} blocks from LBA {lba} on namespace {nsid}")

            # Get logical block size for accurate SGL handling
            logical_block_size = self._get_namespace_logical_block_size(nsid)

            # Pack and send the NVMe Read command via I/O queue
            read_cmd = pack_nvme_read_command(command_id, nsid, lba, block_count, logical_block_size)
            self._send_nvme_io_command_pdu(command_id, read_cmd)

            # Receive data response first from I/O socket (like reservation commands)
            data_header, data_payload = self._receive_pdu_on_socket(self._io_socket)

            if data_header.pdu_type == PDUType.C2H_DATA:
                # Data PDU comes first (normal for read operations)
                read_data = data_payload
                self._logger.debug(f"Received read data: {len(read_data)} bytes")

                # Check for C2H SUCCESS flag (C2H success optimization)
                # Reference: NVMe-oF TCP Transport Specification Rev 1.2,
                # Section 3.3.2.1 (lines 888-900), Figure 33 (lines 2423-2427)
                if data_header.flags & PDUFlags.SCSS:
                    # SUCCESS flag set - command completed successfully, no CapsuleResp follows
                    # Validate: SUCCESS requires LAST_PDU (protocol requirement)
                    if not (data_header.flags & PDUFlags.LAST_PDU):
                        # Fatal protocol error per spec (lines 898-900)
                        raise ProtocolError(
                            f"C2HData PDU has SUCCESS flag without LAST_PDU flag - "
                            f"protocol violation (flags=0x{data_header.flags:02x})")

                    self._logger.debug("C2HData PDU has SUCCESS flag - command completed successfully")
                    # Synthesize successful completion (status=0) per spec
                    return read_data
                else:
                    # SUCCESS flag not set - wait for CapsuleResp with actual status
                    try:
                        response_header, response_data = self._receive_pdu_on_socket(self._io_socket)
                        if response_header.pdu_type == PDUType.RSP:
                            response = ResponseParser.parse_response(response_data, command_id)
                            if response['status'] != 0:
                                raise CommandError(
                                    f"Read command failed with status {response['status']:02x}",
                                    response['status'], command_id)
                    except Exception as e:
                        self._logger.warning(f"Failed to receive completion PDU: {e}")
                        # Some targets may close connection after data transfer

                    return read_data

            elif data_header.pdu_type == PDUType.RSP:
                # Completion first, then data (less common)
                response = ResponseParser.parse_response(data_payload, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Read command failed with status {response['status']:02x}",
                        response['status'], command_id)

                # Receive data PDU
                data_header, read_data = self._receive_pdu_on_socket(self._io_socket)
                if data_header.pdu_type != PDUType.C2H_DATA:
                    raise ProtocolError(f"Expected C2H_DATA PDU, got type {data_header.pdu_type}")

                return read_data

            else:
                raise ProtocolError(f"Unexpected PDU type for read response: {data_header.pdu_type}")

        except Exception as e:
            self._logger.error(f"Read operation failed: {e}")
            raise

    def write_data(self, nsid: int, lba: int, data: bytes) -> None:
        """
        Write data to specified namespace.

        For small writes (<= inline_data_size), data is sent inline in the CMD PDU.
        For large writes (> inline_data_size), the R2T (Ready to Transfer) protocol
        is used:
        1. Send CMD PDU with no data (SGL type = HOST_DATA)
        2. Receive R2T PDU from target
        3. Send H2C_DATA PDU(s) with data chunks
        4. Receive CapsuleResp PDU with completion

        Args:
            nsid: Namespace identifier (1-based)
            lba: Starting logical block address (0-based)
            data: Data to write (must be multiple of logical block size)

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If write command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 4.4 "Write command";
        NVMe-oF TCP Transport Spec Rev 1.2, Section 3.3.2.2
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        if not data:
            raise ValueError("Data cannot be empty")

        # Get actual logical block size from namespace (with caching for performance)
        logical_block_size = self._get_namespace_logical_block_size(nsid)

        # Calculate block count based on data size and actual logical block size
        if len(data) % logical_block_size != 0:
            raise ValueError("Data size (%d) must be multiple of logical block size (%d)" %
                             (len(data), logical_block_size))

        block_count = len(data) // logical_block_size

        if block_count > NVME_MAX_IO_SIZE:
            raise ValueError("Data too large: %d blocks, max %d" % (block_count, NVME_MAX_IO_SIZE))

        if lba < 0:
            raise ValueError("Invalid LBA: %d, must be >= 0" % lba)

        # Ensure I/O queues are set up before performing I/O operations
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            # Determine write flow based on data size
            inline_data_size = self._get_inline_data_size()

            if len(data) <= inline_data_size:
                # Small write: use inline data flow
                self._logger.debug(
                    "Writing %d blocks to LBA %d on namespace %d (inline data)",
                    block_count, lba, nsid)
                self._send_nvme_write_pdu(command_id, nsid, lba, data, logical_block_size)
            else:
                # Large write: use R2T flow
                self._logger.debug(
                    "Writing %d blocks to LBA %d on namespace %d (R2T flow, data size %d > inline limit %d)",
                    block_count, lba, nsid, len(data), inline_data_size)

                # Send write command without data (Transport SGL)
                nvme_command = pack_nvme_write_command_host_data(
                    command_id, nsid, lba, block_count, logical_block_size, len(data))

                # Create Command PDU header (no data)
                total_pdu_length = NVMEOF_TCP_CMD_HEADER_LEN  # 72 bytes, no data

                pdu_header = pack_pdu_header(
                    pdu_type=PDUType.CMD,
                    flags=0,
                    hlen=NVMEOF_TCP_CMD_HEADER_LEN,
                    pdo=0,  # No data offset (no data in this PDU)
                    plen=total_pdu_length
                )

                # Send PDU: header + NVMe command (no data)
                pdu_data = pdu_header + nvme_command
                self._io_socket.sendall(pdu_data)

                # Handle R2T PDU and send data via H2C_DATA PDUs
                self._handle_r2t_and_send_data(command_id, data, self._io_socket)

            # Receive completion response from I/O socket
            response_header, response_data = self._receive_pdu_on_socket(self._io_socket)

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        "Write command failed with status %02x" % response['status'],
                        response['status'], command_id)
            else:
                raise ProtocolError("Expected RSP PDU for write response, got type %d" %
                                    response_header.pdu_type)

            self._logger.debug("Write operation completed successfully")

        except Exception as e:
            self._logger.error("Write operation failed: %s", e)
            raise

    def write_zeroes(self, nsid: int, lba: int, block_count: int) -> None:
        """
        Write zeros to specified logical block range without transferring data.

        Args:
            nsid: Namespace identifier (1-based)
            lba: Starting logical block address (0-based)
            block_count: Number of logical blocks to zero (1-based)

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If write zeroes command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 4.5 "Write Zeroes command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        if block_count <= 0:
            raise ValueError(f"Block count must be > 0, got {block_count}")

        if lba < 0:
            raise ValueError(f"Invalid LBA: {lba}, must be >= 0")

        if block_count > NVME_MAX_IO_SIZE:
            raise ValueError(f"Block count too large: {block_count}, max {NVME_MAX_IO_SIZE}")

        # Ensure I/O queues are set up
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(f"Writing zeros to {block_count} blocks at LBA {lba} on namespace {nsid}")

            # Build and send Write Zeroes command
            write_zeroes_cmd = pack_nvme_write_zeroes_command(command_id, nsid,
                                                              lba, block_count - 1)  # NVMe uses 0-based count
            self._send_nvme_io_command_pdu(command_id, write_zeroes_cmd)

            # Receive completion response
            response_header, response_data = self._receive_pdu()

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Write Zeroes command failed with status {response['status']:02x}",
                        response['status'], command_id)
            else:
                raise ProtocolError(f"Expected RSP PDU for write zeroes response, got type {response_header.pdu_type}")

            self._logger.debug("Write Zeroes operation completed successfully")

        except Exception as e:
            self._logger.error(f"Write Zeroes operation failed: {e}")
            raise

    def compare_data(self, nsid: int, lba: int, data: bytes) -> None:
        """
        Compare data in specified logical blocks with provided data.

        Args:
            nsid: Namespace identifier (1-based)
            lba: Starting logical block address (0-based)
            data: Data to compare against (must be multiple of logical block size)

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If compare command fails or data doesn't match
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 4.3 "Compare command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        if not data:
            raise ValueError("Compare data cannot be empty")

        # Get actual logical block size from namespace
        logical_block_size = self._get_namespace_logical_block_size(nsid)

        # Calculate block count based on data size
        if len(data) % logical_block_size != 0:
            raise ValueError(f"Data size ({len(data)}) must be multiple of logical block size ({logical_block_size})")

        block_count = len(data) // logical_block_size

        if block_count > NVME_MAX_IO_SIZE:
            raise ValueError(f"Data too large: {block_count} blocks, max {NVME_MAX_IO_SIZE}")

        if lba < 0:
            raise ValueError(f"Invalid LBA: {lba}, must be >= 0")

        # Ensure I/O queues are set up
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(f"Comparing {block_count} blocks at LBA {lba} on namespace {nsid}")

            # For compare, we need to send the command and data together like write
            self._send_nvme_compare_pdu(command_id, nsid, lba, data, logical_block_size)

            # Receive completion response
            response_header, response_data = self._receive_pdu()

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                if response['status'] != 0:
                    if response['status'] == 0x85:  # Compare failure
                        raise CommandError(
                            f"Compare failed: Data at LBA {lba} does not match",
                            response['status'], command_id)
                    else:
                        raise CommandError(
                            f"Compare command failed with status {response['status']:02x}",
                            response['status'], command_id)
            else:
                raise ProtocolError(f"Expected RSP PDU for compare response, got type {response_header.pdu_type}")

            self._logger.debug("Compare operation completed successfully - data matches")

        except Exception as e:
            self._logger.error(f"Compare operation failed: {e}")
            raise

    def write_uncorrectable(self, nsid: int, lba: int, block_count: int) -> None:
        """
        Mark specified logical blocks as containing uncorrectable data.

        Args:
            nsid: Namespace identifier (1-based)
            lba: Starting logical block address (0-based)
            block_count: Number of logical blocks to mark (1-based)

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If write uncorrectable command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 4.6 "Write Uncorrectable command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        if block_count <= 0:
            raise ValueError(f"Block count must be > 0, got {block_count}")

        if lba < 0:
            raise ValueError(f"Invalid LBA: {lba}, must be >= 0")

        if block_count > NVME_MAX_IO_SIZE:
            raise ValueError(f"Block count too large: {block_count}, max {NVME_MAX_IO_SIZE}")

        # Ensure I/O queues are set up
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(f"Marking {block_count} blocks as uncorrectable at LBA {lba} on namespace {nsid}")

            # Build and send Write Uncorrectable command
            write_uncorrectable_cmd = pack_nvme_write_uncorrectable_command(command_id, nsid, lba,
                                                                            block_count - 1)  # NVMe uses 0-based count
            self._send_nvme_io_command_pdu(command_id, write_uncorrectable_cmd)

            # Receive completion response
            response_header, response_data = self._receive_pdu()

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Write Uncorrectable command failed with status {response['status']:02x}",
                        response['status'], command_id)
            else:
                raise ProtocolError(
                    "Expected RSP PDU for write uncorrectable response, "
                    f"got type {response_header.pdu_type}")

            self._logger.debug("Write Uncorrectable operation completed successfully")

        except Exception as e:
            self._logger.error(f"Write Uncorrectable operation failed: {e}")
            raise

    def flush_namespace(self, nsid: int) -> None:
        """
        Flush (sync) data to persistent storage for the specified namespace.

        Args:
            nsid: Namespace identifier (1-based)

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If flush command fails

        Reference: NVM Command Set Specification Section 4.1 "Flush command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        # Ensure I/O queues are set up before performing I/O operations
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(f"Flushing namespace {nsid}")

            # Pack and send the NVMe Flush command via I/O queue
            flush_cmd = pack_nvme_flush_command(command_id, nsid)
            self._send_nvme_io_command_pdu(command_id, flush_cmd)

            # Receive completion response from I/O socket
            response_header, response_data = self._receive_pdu_on_socket(self._io_socket)

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Flush command failed with status {response['status']:02x}",
                        response['status'], command_id)
            else:
                raise ProtocolError(f"Expected RSP PDU for flush response, got type {response_header.pdu_type}")

            self._logger.debug("Flush operation completed successfully")

        except Exception as e:
            self._logger.error(f"Flush operation failed: {e}")
            raise

    def reservation_register(self, nsid: int, action: int, reservation_key: int,
                             new_reservation_key: int = 0) -> ReservationInfo:
        """
        Register, unregister, or replace a reservation key for the namespace.

        Args:
            nsid: Namespace identifier (1-based)
            action: Registration action (0=Register, 1=Unregister, 2=Replace)
            reservation_key: Current reservation key (64-bit)
            new_reservation_key: New reservation key for Replace action (64-bit)

        Returns:
            ReservationInfo object with operation result

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If reservation register command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 6.1 "Reservation Register command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        # Parameter validation
        if nsid <= 0:
            raise ValueError("Namespace ID must be positive")
        if action not in [ReservationAction.REGISTER, ReservationAction.UNREGISTER, ReservationAction.REPLACE]:
            raise ValueError(f"Invalid reservation action: {action}")
        if action == ReservationAction.REPLACE and new_reservation_key == 0:
            raise ValueError("New reservation key required for Replace action")

        # Ensure I/O queues are set up
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(
                "Reservation register on namespace %d, action=%d, key=0x%016x",
                nsid, action, reservation_key)

            # Build reservation register command
            register_cmd = pack_nvme_reservation_register_command(
                command_id, nsid, action)

            # Build the register data payload according to Figure 574
            # Bytes 0-7: CRKEY, Bytes 8-15: NRKEY
            if action == ReservationAction.REGISTER:
                # For Register: CRKEY=0 (reserved), NRKEY=new key
                register_data = struct.pack('<QQ', 0, reservation_key)
            elif action == ReservationAction.UNREGISTER:
                # For Unregister: CRKEY=current key, NRKEY=0 (reserved)
                register_data = struct.pack('<QQ', reservation_key, 0)
            elif action == ReservationAction.REPLACE:
                # For Replace: CRKEY=current key, NRKEY=new key
                register_data = struct.pack('<QQ', reservation_key, new_reservation_key)
            else:
                raise ValueError(f"Invalid reservation action: {action}")

            self._send_nvme_reservation_command_pdu(register_cmd, register_data)

            # Receive completion response from I/O socket
            response_header, response_data = self._receive_pdu_on_socket(self._io_socket)

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                status_code = response['status']

                if status_code != 0:
                    raise CommandError(
                        f"Reservation register failed with status {status_code:02x}",
                        status_code, command_id)

                self._logger.debug("Reservation register completed successfully")

                # Return reservation info
                return ReservationInfo(
                    success=True,
                    reservation_key=reservation_key,
                    generation=0,  # Will be updated by subsequent report
                    status_code=status_code
                )
            else:
                raise ProtocolError(
                    "Expected RSP PDU for reservation register response, "
                    f"got type {response_header.pdu_type}")

        except Exception as e:
            self._logger.error(f"Reservation register operation failed: {e}")
            raise

    def reservation_report(self, nsid: int, eds: int = 1) -> ReservationStatus:
        """
        Get the current reservation status for the namespace.

        Args:
            nsid: Namespace identifier (1-based)
            eds: Extended Data Structure bit (0=standard format, 1=extended format)

        Returns:
            ReservationStatus object with detailed reservation information

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If reservation report command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 6.2 "Reservation Report command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        # Parameter validation
        if nsid <= 0:
            raise ValueError("Namespace ID must be positive")

        # Ensure I/O queues are set up
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence
        data_length = 4096  # Standard reservation report data size

        try:
            self._logger.debug(f"Getting reservation report for namespace {nsid}")

            # Build reservation report command
            report_cmd = pack_nvme_reservation_report_command(command_id, nsid, data_length, eds)
            self._send_nvme_io_command_pdu(command_id, report_cmd)

            # Receive data response first from I/O socket
            data_header, data_payload = self._receive_pdu_on_socket(self._io_socket)

            if data_header.pdu_type == PDUType.C2H_DATA:
                self._logger.debug(f"Received {len(data_payload)} bytes of reservation data")

                # Check for C2H SUCCESS flag (C2H success optimization)
                # Reference: NVMe-oF TCP Transport Specification Rev 1.2,
                # Section 3.3.2.1 (lines 888-900), Figure 33 (lines 2423-2427)
                if data_header.flags & PDUFlags.SCSS:
                    # SUCCESS flag set - command completed successfully, no CapsuleResp follows
                    # Validate: SUCCESS requires LAST_PDU (protocol requirement)
                    if not (data_header.flags & PDUFlags.LAST_PDU):
                        # Fatal protocol error per spec (lines 898-900)
                        raise ProtocolError(
                            f"C2HData PDU has SUCCESS flag without LAST_PDU flag - "
                            f"protocol violation (flags=0x{data_header.flags:02x})")

                    self._logger.debug("C2HData PDU has SUCCESS flag - command completed successfully")
                    # Synthesize successful completion (status=0) per spec
                    status_code = 0
                else:
                    # SUCCESS flag not set - wait for CapsuleResp with actual status
                    # Receive completion response from I/O socket
                    response_header, response_data = self._receive_pdu_on_socket(self._io_socket)

                    if response_header.pdu_type == PDUType.RSP:
                        response = ResponseParser.parse_response(response_data, command_id)
                        status_code = response['status']

                        if status_code != 0:
                            raise CommandError(
                                f"Reservation report failed with status {status_code:02x}",
                                status_code, command_id)
                    else:
                        raise ProtocolError(
                            f"Expected RSP PDU after C2H_DATA, got type {response_header.pdu_type}")

            elif data_header.pdu_type == PDUType.RSP:
                # No data returned, command failed - this is the response header and data
                response_header, response_data = data_header, data_payload
                response = ResponseParser.parse_response(response_data, command_id)
                status_code = response['status']

                if status_code != 0:
                    raise CommandError(
                        f"Reservation report failed with status {status_code:02x}",
                        status_code, command_id)

                # No data was received
                raise ProtocolError("No reservation data received")
            else:
                raise ProtocolError(
                    "Expected C2H_DATA or RSP PDU for reservation report, "
                    f"got type {data_header.pdu_type}")

            # At this point, we have data_payload with reservation data and status_code = 0
            if data_header.pdu_type == PDUType.C2H_DATA:

                # Parse reservation report data using the new parser
                if len(data_payload) < 24:
                    raise ProtocolError("Reservation report data too short")

                # Use the same EDS value for parsing as we used in the command
                extended_format = bool(eds)
                parsed_data = ReservationDataParser.parse_reservation_report(data_payload, extended_format)

                # Extract parsed information
                generation = parsed_data['generation']
                reservation_type = (
                    ReservationType(parsed_data['reservation_type'])
                    if parsed_data['reservation_type'] != 0 else None)

                # Find reservation holder (controller that holds reservation)
                reservation_holder = 0
                registered_controllers = []
                reservation_keys = {}

                for registrant in parsed_data['registrants']:
                    controller_id = registrant['controller_id']
                    reservation_key = registrant['reservation_key']
                    holds_reservation = registrant['holds_reservation']

                    # All registrants in the parsed data are valid registered controllers
                    # The parser already filtered out unused slots based on controller ID
                    registered_controllers.append(controller_id)
                    reservation_keys[controller_id] = reservation_key

                    if holds_reservation:
                        reservation_holder = controller_id

                self._logger.debug(
                    "Reservation report completed: gen=%d, type=%s, holder=%d, registered=%d",
                    generation, reservation_type, reservation_holder, len(registered_controllers))
                self._logger.debug(f"Extended format: {extended_format}, entry size: {parsed_data['entry_size']} bytes")

                return ReservationStatus(
                    generation=generation,
                    reservation_type=reservation_type,
                    reservation_holder=reservation_holder,
                    registered_controllers=registered_controllers,
                    reservation_keys=reservation_keys
                )
            else:
                raise ProtocolError(
                    "Expected RSP PDU for reservation report response, "
                    f"got type {response_header.pdu_type}")

        except Exception as e:
            self._logger.error(f"Reservation report operation failed: {e}")
            raise

    def reservation_acquire(self, nsid: int, action: int, reservation_type: int,
                            reservation_key: int, preempt_key: int = 0) -> ReservationInfo:
        """
        Acquire a reservation or preempt an existing reservation on the namespace.

        Args:
            nsid: Namespace identifier (1-based)
            action: Acquire action (0=Acquire, 1=Preempt, 2=Preempt and Abort)
            reservation_type: Type of reservation (1-6: Write Exclusive, Exclusive Access, etc.)
            reservation_key: Reservation key to use for the operation (64-bit)
            preempt_key: Key to preempt (for Preempt actions, 64-bit)

        Returns:
            ReservationInfo object with operation result

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If reservation acquire command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 6.3 "Reservation Acquire command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        # Parameter validation
        if nsid <= 0:
            raise ValueError("Namespace ID must be positive")
        if action not in [ReservationAction.ACQUIRE, ReservationAction.PREEMPT, ReservationAction.PREEMPT_AND_ABORT]:
            raise ValueError(f"Invalid reservation acquire action: {action}")
        if reservation_type not in [t.value for t in ReservationType]:
            raise ValueError(f"Invalid reservation type: {reservation_type}")
        if action in [ReservationAction.PREEMPT, ReservationAction.PREEMPT_AND_ABORT] and preempt_key == 0:
            raise ValueError("Preempt key required for Preempt actions")

        # Ensure I/O queues are set up
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(
                "Reservation acquire on namespace %d, action=%d, type=%s, key=0x%016x",
                nsid, action, reservation_type, reservation_key)

            # Build reservation acquire command
            acquire_cmd = pack_nvme_reservation_acquire_command(
                command_id, nsid, action, reservation_type)

            # Create and send the acquire data payload
            acquire_data = struct.pack('<QQ', reservation_key, preempt_key)
            self._send_nvme_reservation_pdu(acquire_cmd, acquire_data)

            # Receive completion response
            response_header, response_data = self._receive_pdu_on_socket(self._io_socket)

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                status_code = response['status']

                if status_code != 0:
                    raise CommandError(
                        f"Reservation acquire failed with status {status_code:02x}",
                        status_code, command_id)

                self._logger.debug("Reservation acquire completed successfully")

                # Return reservation info
                return ReservationInfo(
                    success=True,
                    reservation_key=reservation_key,
                    generation=0,  # Will be updated by subsequent report
                    status_code=status_code
                )
            else:
                raise ProtocolError(
                    "Expected RSP PDU for reservation acquire response, "
                    f"got type {response_header.pdu_type}")

        except Exception as e:
            self._logger.error(f"Reservation acquire operation failed: {e}")
            raise

    def reservation_release(self, nsid: int, action: int, reservation_type: int,
                            reservation_key: int) -> ReservationInfo:
        """
        Release or clear a reservation on the namespace.

        Args:
            nsid: Namespace identifier (1-based)
            action: Release action (0=Release, 1=Clear)
            reservation_type: Type of reservation to release (must match acquired type)
            reservation_key: Reservation key used to acquire the reservation (64-bit)

        Returns:
            ReservationInfo object with operation result

        Raises:
            NVMeoFConnectionError: If not connected or connected to discovery subsystem
            CommandError: If reservation release command fails
            ValueError: If parameters are invalid

        Reference: NVM Command Set Specification Section 6.4 "Reservation Release command"
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if self._is_discovery_subsystem:
            raise NVMeoFConnectionError("I/O operations not available on discovery subsystem")

        # Parameter validation
        if nsid <= 0:
            raise ValueError("Namespace ID must be positive")
        if action not in [ReservationAction.RELEASE, ReservationAction.CLEAR]:
            raise ValueError(f"Invalid reservation release action: {action}")
        if reservation_type not in [t.value for t in ReservationType]:
            raise ValueError(f"Invalid reservation type: {reservation_type}")

        # Ensure I/O queues are set up
        self.setup_io_queues()

        command_id = self._get_next_io_command_id()  # Use I/O queue command ID sequence

        try:
            self._logger.debug(
                "Reservation release on namespace %d, action=%d, type=%s, key=0x%016x",
                nsid, action, reservation_type, reservation_key)

            # Build reservation release command
            release_cmd = pack_nvme_reservation_release_command(
                command_id, nsid, action, reservation_type)

            # Create and send the release data payload
            # Use 8-byte payload (just current key) to match nvme CLI behavior
            release_data = struct.pack('<Q', reservation_key)
            self._send_nvme_reservation_pdu(release_cmd, release_data)

            # Receive completion response
            response_header, response_data = self._receive_pdu_on_socket(self._io_socket)

            if response_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                status_code = response['status']

                if status_code != 0:
                    raise CommandError(
                        f"Reservation release failed with status {status_code:02x}",
                        status_code, command_id)

                self._logger.debug("Reservation release completed successfully")

                # Return reservation info
                return ReservationInfo(
                    success=True,
                    reservation_key=reservation_key,
                    generation=0,  # Will be updated by subsequent report
                    status_code=status_code
                )
            else:
                raise ProtocolError(
                    "Expected RSP PDU for reservation release response, "
                    f"got type {response_header.pdu_type}")

        except Exception as e:
            self._logger.error(f"Reservation release operation failed: {e}")
            raise

    def get_controller_capabilities(self) -> dict[str, Any]:
        """
        Get Controller Capabilities using Property Get command.

        Returns:
            Dictionary containing parsed CAP register fields:
            - mqes: Maximum Queue Entries Supported
            - cqr: Contiguous Queues Required
            - ams: Arbitration Mechanism Supported
            - to: Timeout (milliseconds)
            - dstrd: Doorbell Stride
            - nssrs: NVM Subsystem Reset Supported
            - css: Command Sets Supported
            - bps: Boot Partition Support
            - cps: Controller Power Scope
            - mpsmin/mpsmax: Memory Page Size Min/Max
            - pmrs: Persistent Memory Region Supported
            - cmbs: Controller Memory Buffer Supported
            - nsss: NVM Sets Supported
            - crms: Controller Ready Modes Supported

        Reference: NVMe Base Specification Section 3.1.1
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        command_id = 1  # Use command ID 1 to match working nvme-cli command

        try:
            self._logger.debug(f"Getting Controller Capabilities with command {command_id}")

            # Build and send Property Get command for CAP register (8 bytes)
            self._send_property_get_pdu(command_id, NVMeProperty.CAP, 8)

            # Receive and parse response
            response_header, response_data = self._receive_pdu()
            self._logger.debug(
                f"Received response PDU: type={response_header.pdu_type}, len={len(response_data)}")

            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(
                    f"Expected response PDU, got type {response_header.pdu_type}")

            # Parse completion entry for debug
            if len(response_data) >= 16:
                dw0, dw1, sq_head, sq_id, cmd_id, status = struct.unpack('<LLHHHH', response_data[:16])
                self._logger.debug(f"Completion entry: DW0=0x{dw0:08x}, DW1=0x{dw1:08x}, status=0x{status:04x}")

                # For Property Get, check if the CAP value is in DW0/DW1 regardless of status
                if dw0 != 0 or dw1 != 0:
                    self._logger.debug(f"Found non-zero register data: DW0=0x{dw0:08x}, DW1=0x{dw1:08x}")
                    # Try to extract CAP value even if status is non-zero
                    cap_value = dw0 | (dw1 << 32)
                    if cap_value != 0:
                        cap_data = struct.pack('<Q', cap_value)
                        cap_parsed = parse_controller_capabilities(cap_data)
                        self._logger.debug("Controller Capabilities extracted from completion entry")
                        return cap_parsed

            # Parse Property Get response normally
            prop_response = ResponseParser.parse_response(response_data, command_id)
            if prop_response['status'] != 0:
                raise CommandError(
                    f"Property Get failed with status {prop_response['status']:02x}",
                    prop_response['status'], command_id)

            # For Property Get, the register value is returned in DW0 and DW1 of completion entry
            # CAP register is 64-bit, so it's split across DW0 (low) and DW1 (high)
            cap_low = prop_response.get('dw0', 0)
            cap_high = prop_response.get('dw1', 0)

            # Reconstruct 64-bit CAP value
            cap_value = cap_low | (cap_high << 32)
            cap_data = struct.pack('<Q', cap_value)

            # Parse the CAP register
            cap_parsed = parse_controller_capabilities(cap_data)

            self._logger.debug("Controller Capabilities retrieved successfully")
            return cap_parsed

        except Exception as e:
            self._logger.error(f"Failed to get Controller Capabilities: {e}")
            raise

    def wait_for_controller_ready(self, timeout: float = 10.0) -> bool:
        """
        Wait for controller to be in ready state.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if controller is ready, False if timeout

        Reference: NVMe Base Specification Section 3.1.6
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        self._logger.debug("Checking controller ready status...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Get Controller Status (CSTS) register (4 bytes)
                command_id = self._get_next_command_id()
                self._send_property_get_pdu(command_id, NVMeProperty.CSTS, 4)

                response_header, response_data = self._receive_pdu()
                if response_header.pdu_type != PDUType.RSP:
                    self._logger.warning(f"Expected response PDU, got type {response_header.pdu_type}")
                    continue

                # Parse completion entry for CSTS value
                if len(response_data) >= 16:
                    dw0, dw1, sq_head, sq_id, cmd_id, status = struct.unpack('<LLHHHH', response_data[:16])

                    if status == 0:  # Command succeeded
                        csts_value = dw0  # CSTS is a 32-bit register
                        ready = bool(csts_value & ControllerStatus.RDY)

                        self._logger.debug(f"Controller Status: 0x{csts_value:08x}, Ready: {ready}")

                        if ready:
                            self._logger.debug("Controller is ready")
                            return True
                        else:
                            self._logger.debug("Controller not ready yet, waiting...")
                            time.sleep(0.1)
                            continue
                    else:
                        self._logger.warning(f"Property Get CSTS failed with status {status:02x}")
                        time.sleep(0.1)
                        continue

            except Exception as e:
                self._logger.warning(f"Error checking controller status: {e}")
                time.sleep(0.1)
                continue

        self._logger.warning(f"Controller not ready after {timeout}s timeout")
        return False

    def configure_controller(self) -> None:
        """
        Configure and enable controller for discovery operations.

        Sends the two-step controller configuration sequence:
        1. Property Set CC = 0x00460060 (configure but disabled)
        2. Property Set CC = 0x00460061 (enable controller)

        Reference: Working nvme-cli controller enable sequence
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        self._logger.debug("Configuring controller for discovery...")

        try:
            # Step 1: Configure controller (EN=0)
            # Note: Working nvme-cli uses CSS=6 (ALL_SUPPORTED) and AMS=6 (reserved but accepted)
            # IOSQES=0, IOCQES=0 match working capture (0x00460060)
            cc_configure = ControllerConfiguration.build_cc_register(
                en=ControllerConfiguration.EN_DISABLED,
                css=ControllerConfiguration.CSS_ALL_SUPPORTED,  # 6 = All Supported I/O Command Sets
                ams=0,  # 0 = Round Robin arbitration
                iosqes=NVME_IOSQES_64_BYTES,  # 64-byte submission queue entries (2^6)
                iocqes=NVME_IOCQES_16_BYTES   # 16-byte completion queue entries (2^4)
            )

            command_id = self._get_next_command_id()
            self._logger.debug(f"Property Set CC=0x{cc_configure:08x} (configure) with command {command_id}")
            self._send_property_set_pdu(command_id, NVMeProperty.CC, cc_configure)

            response_header, response_data = self._receive_pdu()
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected response PDU, got type {response_header.pdu_type}")

            response = ResponseParser.parse_response(response_data, command_id)
            if response['status'] != 0:
                raise CommandError(f"Property Set CC configure failed with status {response['status']:02x}",
                                   response['status'], command_id)

            self._logger.debug("Controller configured")

            # Step 2: Enable controller (EN=1)
            cc_enable = ControllerConfiguration.build_cc_register(
                en=ControllerConfiguration.EN_ENABLED,
                css=ControllerConfiguration.CSS_ALL_SUPPORTED,
                ams=0,  # 0 = Round Robin arbitration (same as configure step)
                iosqes=NVME_IOSQES_64_BYTES,  # 64-byte submission queue entries (same as configure step)
                iocqes=NVME_IOCQES_16_BYTES   # 16-byte completion queue entries (2^4)
            )

            command_id = self._get_next_command_id()
            self._logger.debug(f"Property Set CC=0x{cc_enable:08x} (enable) with command {command_id}")
            self._send_property_set_pdu(command_id, NVMeProperty.CC, cc_enable)

            response_header, response_data = self._receive_pdu()
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected response PDU, got type {response_header.pdu_type}")

            response = ResponseParser.parse_response(response_data, command_id)
            if response['status'] != 0:
                raise CommandError(f"Property Set CC enable failed with status {response['status']:02x}",
                                   response['status'], command_id)

            self._logger.debug("Controller enabled")

            # Step 3: Check Controller Status to verify controller is ready
            command_id = self._get_next_command_id()
            self._logger.debug(f"Property Get CSTS (Controller Status) with command {command_id}")
            self._send_property_get_pdu(command_id, NVMeProperty.CSTS, 4)

            response_header, response_data = self._receive_pdu()
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected response PDU, got type {response_header.pdu_type}")

            response = ResponseParser.parse_response(response_data, command_id)
            if response['status'] != 0:
                raise CommandError(f"Property Get CSTS failed with status {response['status']:02x}",
                                   response['status'], command_id)

            # Parse Controller Status register
            if 'result' in response:
                csts_value = response['result']
                rdy = csts_value & ControllerStatus.RDY
                cfs = csts_value & ControllerStatus.CFS
                shst = (csts_value & ControllerStatus.SHST_MASK) >> 2

                self._logger.debug(f"Controller Status: RDY={rdy}, CFS={cfs}, SHST={shst}")

                if not rdy:
                    self._logger.warning("Controller not ready (RDY=0)")
                if cfs:
                    raise CommandError("Controller Fatal Status detected", cfs, command_id)

                self._logger.debug("Controller ready for commands")

            # Step 4: Get Version register
            command_id = self._get_next_command_id()
            self._logger.debug(f"Property Get VS (Version) with command {command_id}")
            self._send_property_get_pdu(command_id, NVMeProperty.VS, 4)

            response_header, response_data = self._receive_pdu()
            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(f"Expected response PDU, got type {response_header.pdu_type}")

            response = ResponseParser.parse_response(response_data, command_id)
            if response['status'] != 0:
                raise CommandError(f"Property Get VS failed with status {response['status']:02x}",
                                   response['status'], command_id)

            # Parse Version register
            if 'result' in response:
                vs_value = response['result']
                major = (vs_value >> 16) & NVME_COMMAND_ID_MASK
                minor = vs_value & NVME_COMMAND_ID_MASK
                self._logger.debug(f"NVMe Version: {major}.{minor}")

            # Discovery subsystem is now ready for Get Log Page commands
            # Note: Discovery subsystem does not support Identify Controller or Set Features commands
            self._logger.debug("Controller configuration complete")

        except Exception as e:
            self._logger.error(f"Failed to configure controller: {e}")
            raise

    def discover_subsystems(self, max_entries: int = NVME_DEFAULT_MAX_ENTRIES) -> list:
        """
        Discover available NVMe-oF subsystems using Get Log Page command.

        This method only works when connected to a discovery subsystem.
        To discover subsystems, connect without specifying a subsystem_nqn.

        Args:
            max_entries: Maximum number of discovery entries to retrieve

        Returns:
            List of formatted discovery entries

        Raises:
            NVMeoFConnectionError: If not connected or connected to non-discovery subsystem

        Reference: NVMe-oF Base Specification Section 5.4
        """
        if not self._connected:
            raise NVMeoFConnectionError("Not connected to target")

        if not self._is_discovery_subsystem:
            raise NVMeoFConnectionError(
                "Discovery only available when connected to discovery subsystem. "
                "Use connect() without subsystem_nqn to connect to discovery subsystem.")

        command_id = 9  # Use command ID 9 to match working nvme-cli command

        try:
            self._logger.debug(f"Starting discovery with command {command_id}")

            # Controller should already be configured before calling this method

            # Phase 1: Get discovery log header to determine actual size
            # Match working client's request size for discovery log
            header_size = NVME_DISCOVERY_LOG_SIZE  # Working client uses this size

            self._logger.debug("Phase 1: Getting discovery log header")
            self._send_get_log_page_pdu(command_id, LogPageIdentifier.DISCOVERY_LOG, header_size)

            # Small delay to allow response to arrive
            time.sleep(0.1)

            # Check connection state before trying to receive
            self._logger.debug(f"Socket info: fd={self._socket.fileno()}, connected={self._connected}")
            ready = select.select([self._socket], [], [], 0)
            if ready[0]:
                self._logger.debug("Data available for reading")
            else:
                self._logger.debug("No data available yet, waiting...")
                # Wait a bit longer for data to arrive
                ready = select.select([self._socket], [], [], 1.0)
                if ready[0]:
                    self._logger.debug("Data became available after waiting")
                else:
                    self._logger.error("No response data received after 1 second")

            # Receive response - data comes first, then completion
            first_header, first_data = self._receive_pdu()
            self._logger.debug(
                f"Received first PDU: type={first_header.pdu_type}, len={len(first_data)}")

            log_data = b''

            if first_header.pdu_type == PDUType.C2H_DATA:
                # Data PDU comes first
                log_data = first_data
                self._logger.debug(f"Received discovery log data: {len(log_data)} bytes")

                # Check for C2H SUCCESS flag (C2H success optimization)
                # Reference: NVMe-oF TCP Transport Specification Rev 1.2,
                # Section 3.3.2.1 (lines 888-900), Figure 33 (lines 2423-2427)
                if first_header.flags & PDUFlags.SCSS:
                    # SUCCESS flag set - command completed successfully, no CapsuleResp follows
                    # Validate: SUCCESS requires LAST_PDU (protocol requirement)
                    if not (first_header.flags & PDUFlags.LAST_PDU):
                        # Fatal protocol error per spec (lines 898-900)
                        raise ProtocolError(
                            f"C2HData PDU has SUCCESS flag without LAST_PDU flag - "
                            f"protocol violation (flags=0x{first_header.flags:02x})")

                    self._logger.debug("C2HData PDU has SUCCESS flag - command completed successfully")
                    # Synthesize successful completion (status=0) per spec
                    response = {'status': 0}
                else:
                    # SUCCESS flag not set - wait for CapsuleResp with actual status
                    try:
                        response_header, response_data = self._receive_pdu()
                        self._logger.debug(
                            f"Received completion PDU: type={response_header.pdu_type}, len={len(response_data)}")

                        if response_header.pdu_type != PDUType.RSP:
                            raise ProtocolError(
                                f"Expected response PDU, got type {response_header.pdu_type}")

                        # Parse the completion entry to check status
                        response = ResponseParser.parse_response(response_data, command_id)
                        if response['status'] != 0:
                            raise CommandError(
                                f"Get Log Page failed with status {response['status']:02x}",
                                response['status'], command_id)
                    except Exception as e:
                        self._logger.warning("Failed to receive completion PDU (target may have closed): %s", e)
                        # Some targets send only data and close connection for discovery
                        # Assume success if we got valid data
                        response = {'status': 0}

            elif first_header.pdu_type == PDUType.RSP:
                # Old order: completion first, then data
                response_header, response_data = first_header, first_data

                # Try to receive data PDU
                try:
                    data_header, log_data = self._receive_pdu()
                    if data_header.pdu_type != PDUType.C2H_DATA:
                        self._logger.warning(f"Expected data PDU, got type {data_header.pdu_type}")
                except Exception as e:
                    self._logger.warning(f"Failed to receive data PDU: {e}")
                    log_data = b''
            else:
                raise ProtocolError(
                    f"Expected DATA or RSP PDU, got type {first_header.pdu_type}")

            # Response validation already handled above for C2H_DATA case
            # For RSP-first case, parse the completion entry
            if first_header.pdu_type == PDUType.RSP:
                response = ResponseParser.parse_response(response_data, command_id)
                if response['status'] != 0:
                    raise CommandError(
                        f"Get Log Page failed with status {response['status']:02x}",
                        response['status'], command_id)

            if not log_data:
                self._logger.warning("No discovery log data received")
                return []

            self._logger.debug(f"Received discovery log data: {len(log_data)} bytes")

            # Parse discovery log page
            parsed_log = parse_discovery_log_page(log_data)
            entries = parsed_log['entries']

            # Format entries for easy consumption
            formatted_entries = [format_discovery_entry(entry) for entry in entries]

            self._logger.debug(
                "Discovery completed: found %d subsystems (generation=%d)",
                len(formatted_entries), parsed_log['generation'])
            return formatted_entries

        except Exception as e:
            self._logger.error(f"Discovery failed: {e}")
            raise

    def get_discovery_entries(self, max_entries: int = NVME_DEFAULT_MAX_ENTRIES):
        """
        Get discovery entries as DiscoveryEntry objects (high-level API).

        This method provides the same discovery functionality as discover_subsystems()
        but returns typed DiscoveryEntry objects instead of dictionaries, following
        the same pattern as get_controller_info() and get_namespace_info().

        Args:
            max_entries: Maximum number of discovery entries to retrieve

        Returns:
            List of DiscoveryEntry objects

        Raises:
            NVMeoFConnectionError: If not connected or connected to non-discovery subsystem

        Example:
            >>> client = NVMeoFClient('192.168.1.100')
            >>> client.connect()  # Connect to discovery subsystem
            >>> entries = client.get_discovery_entries()
            >>> for entry in entries:
            ...     print(f"Subsystem: {entry.subsystem_nqn}")
            ...     print(f"  Address: {entry.transport_address}")
            ...     print(f"  Type: {entry.transport_type.name}")
            ...     if entry.is_nvme_subsystem:
            ...         print("  This is an NVMe subsystem")

        Reference: NVMe-oF Base Specification Section 5.4
        """

        # Get formatted entries from low-level method
        formatted_entries = self.discover_subsystems(max_entries)

        # Convert to DiscoveryEntry objects
        discovery_entries = []
        for entry in formatted_entries:
            discovery_entry = DiscoveryEntry(
                transport_type=TransportType(entry['raw_transport_type']),
                address_family=AddressFamily(entry['raw_address_family']),
                subsystem_type=entry['raw_subsystem_type'],
                port_id=entry['port_id'],
                controller_id=entry['controller_id'],
                transport_address=entry['transport_address'],
                transport_service_id=entry['transport_service_id'],
                subsystem_nqn=entry['subsystem_nqn']
            )
            discovery_entries.append(discovery_entry)

        return discovery_entries

    def perform_discovery(self, max_entries: int = NVME_DEFAULT_MAX_ENTRIES) -> list:
        """
        Alias for discover_subsystems() for compatibility.

        Args:
            max_entries: Maximum number of discovery entries to retrieve

        Returns:
            List of formatted discovery entries
        """
        return self.discover_subsystems(max_entries)

    @property
    def is_connected(self) -> bool:
        """Check if client is connected to target."""
        return self._connected

    @property
    def is_discovery_connection(self) -> bool:
        """Check if connected to discovery subsystem."""
        return self._connected and self._is_discovery_subsystem

    @property
    def connected_subsystem_nqn(self) -> str | None:
        """Get the NQN of the currently connected subsystem."""
        return self._connected_subsystem_nqn if self._connected else None

    @property
    def kato(self) -> int:
        """Get the Keep Alive Timeout value in milliseconds (0 = disabled)."""
        return self._kato

    def _initialize_connection(self, subsystem_nqn: str = None) -> None:
        """
        Perform NVMe-oF specific connection initialization.

        Args:
            subsystem_nqn: Target subsystem NQN to connect to

        This includes sending Initialize Connection Request (ICREQ) and
        processing Initialize Connection Response (ICRESP).

        Reference: NVMe-oF TCP Transport Specification Section 3.3
        """
        # Send ICREQ PDU
        icreq_data = self._build_icreq()
        self._logger.debug(f"Sending ICREQ: {len(icreq_data)} bytes")
        self._send_icreq_pdu(icreq_data)

        # Receive ICRESP PDU
        self._logger.debug("Waiting for ICRESP...")
        icresp_header, icresp_data = self._receive_pdu()
        self._logger.debug(f"Received PDU: type={icresp_header.pdu_type}, len={len(icresp_data)}")
        if icresp_header.pdu_type != PDUType.ICRESP:
            raise ProtocolError(f"Expected ICRESP, got PDU type {icresp_header.pdu_type}")

        self._process_icresp(icresp_data)

        # Send Fabric Connect command to establish admin queue
        self._logger.debug("Sending Fabric Connect command...")
        self._send_fabric_connect(subsystem_nqn)

        # Track connection type
        if subsystem_nqn is None or subsystem_nqn == "nqn.2014-08.org.nvmexpress.discovery":
            self._is_discovery_subsystem = True
            self._connected_subsystem_nqn = "nqn.2014-08.org.nvmexpress.discovery"
        else:
            self._is_discovery_subsystem = False
            self._connected_subsystem_nqn = subsystem_nqn

    def _build_icreq(self) -> bytes:
        """
        Build Initialize Connection Request data according to Linux kernel implementation.

        Returns:
            ICREQ payload bytes - based on Linux kernel nvme-tcp.c

        Reference: Linux kernel drivers/nvme/host/tcp.c nvme_tcp_init_connection()
        """
        # ICREQ structure based on Linux kernel struct nvme_tcp_icreq_pdu:
        # After common PDU header (8 bytes):
        # pfv (2 bytes): Protocol Format Version - little endian 0x0000 for version 1.0
        # hpda (1 byte): Host PDU Data Alignment - 0 (no alignment constraint)
        # digest (1 byte): Digest types - 0 (no digest initially)
        # maxr2t (1 byte): Maximum R2Ts per request - 0 (single inflight R2T)
        # rsvd2 (115 bytes): Reserved - must be zero

        # Based on packet capture: working nvme-cli sends all zeros for ICREQ data
        # This suggests the target doesn't require specific field values in ICREQ
        icreq_data = b'\x00' * 120

        return icreq_data

    def _send_icreq_pdu(self, data: bytes) -> None:
        """
        Send ICREQ PDU with proper header based on Linux kernel implementation.

        Args:
            data: ICREQ payload data (120 bytes)

        Reference: Linux kernel implementation sets:
        - type = nvme_tcp_icreq (0x0)
        - flags = 0
        - hlen = sizeof(icreq) (header length)
        - pdo = 0 (PDU data offset)
        - plen = hlen (total PDU length in little-endian)
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Corrected ICREQ PDU header format based on packet capture analysis
        # nvme-cli uses entire ICREQ as header
        header_len = NVMEOF_TCP_ICREQ_HEADER_LEN
        total_len = NVMEOF_TCP_ICREQ_TOTAL_LEN

        header = pack_pdu_header(PDUType.ICREQ, 0, header_len, 0, total_len)

        self._logger.debug(f"Sending ICREQ PDU: hlen={header_len}, pdo=0, plen={total_len}")
        self._socket.sendall(header + data)

    def _send_icreq_pdu_on_socket(self, sock: socket.socket, data: bytes) -> None:
        """Send ICREQ PDU on specific socket."""
        header_len = NVMEOF_TCP_ICREQ_HEADER_LEN
        total_len = NVMEOF_TCP_ICREQ_TOTAL_LEN
        header = pack_pdu_header(PDUType.ICREQ, 0, header_len, 0, total_len)
        sock.sendall(header + data)

    def _receive_pdu_on_socket(self, sock: socket.socket) -> tuple[PDUHeader, bytes]:
        """Receive PDU on specific socket."""
        # Read PDU header (8 bytes)
        header_data = self._recv_exactly(sock, 8)
        header = unpack_pdu_header(header_data)

        # Read remaining PDU data if any
        remaining_size = header.plen - 8
        if remaining_size > 0:
            remaining_data = self._recv_exactly(sock, remaining_size)
        else:
            remaining_data = b''

        # For PDUs with extended headers (like C2H_DATA), extract the actual payload
        if header.pdu_type == PDUType.C2H_DATA:
            # C2H_DATA PDU structure: basic header (8) + extended header (hlen-8) + data payload
            # Data payload starts at hlen offset and has size (plen - hlen)
            extended_header_size = header.hlen - 8
            payload_data = remaining_data[extended_header_size:]
        else:
            # For other PDU types, the entire remaining data is the payload
            payload_data = remaining_data

        return header, payload_data

    def _send_fabric_connect_pdu_on_socket(self, sock: socket.socket, connect_cmd: bytes, connect_data: bytes) -> None:
        """Send Fabric Connect PDU on specific socket."""
        header_len = 72
        pdo = 72
        total_len = 72 + len(connect_data)
        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)
        sock.sendall(header + connect_cmd + connect_data)

    def _recv_exactly(self, sock: socket.socket, size: int) -> bytes:
        """Receive exactly the specified number of bytes from socket."""
        data = b''
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise NVMeoFConnectionError("Connection closed by target")
            data += chunk
        return data

    def _get_inline_data_size(self) -> int:
        """
        Calculate maximum inline data size for I/O commands.

        For I/O commands, inline data size is the I/O Command Capsule Supported Size
        (IOCCSZ) minus the NVMe command size (64 bytes).

        Returns:
            Maximum bytes that can be sent inline in CMD PDU

        Raises:
            ProtocolError: If IOCCSZ not negotiated

        Reference: NVMe-oF TCP Transport Specification Rev 1.2,
        Section 3.3.2.2 "Host to Controller Command Data Buffer Transfers";
        NVMe Base Specification Section 5.2.13.2.1, Figure 328
        """
        if not self._ioccsz:
            raise ProtocolError("I/O Command Capsule Supported Size not negotiated")
        # IOCCSZ is in 16-byte units per Figure 328
        ioccsz_bytes = self._ioccsz * 16
        return ioccsz_bytes - NVME_COMMAND_SIZE  # 64 bytes

    def _send_h2c_data_pdu(self, command_id: int, ttag: int, data_offset: int,
                           data: bytes, is_last: bool, socket: socket.socket) -> None:
        """
        Send Host to Controller Data Transfer PDU (H2CData).

        H2CData PDUs transfer data from host to controller in response to an
        R2T PDU. Multiple H2CData PDUs may be sent to complete a single
        data transfer.

        Args:
            command_id: Command identifier from original CMD PDU
            ttag: Transfer tag from R2T PDU (not command_id!)
            data_offset: Byte offset within command data buffer
            data: Data chunk to send (up to maxh2cdata bytes)
            is_last: True if this is the last H2C_DATA PDU for this transfer
            socket: Socket to send on (typically I/O socket)

        Raises:
            ValueError: If data chunk exceeds maxh2cdata

        Reference: NVMe-oF TCP Transport Specification Rev 1.2,
        Section 3.6.2.8, Figure 32 (H2CData PDU)

        PDU Structure:
        Bytes 00-07: Common Header (CH)
          - PDU Type: 06h (H2C_DATA)
          - Flags: LAST_PDU (bit 2) if is_last
          - HLEN: 24 (0x18) - Fixed header length
          - PDO: Data offset (24 + header digest if enabled)
          - PLEN: Total PDU length
        Bytes 08-09: Command ID (CCCID)
        Bytes 10-11: Transfer Tag (TTAG) - from R2T PDU
        Bytes 12-15: Data Offset (DATAO)
        Bytes 16-19: Data Length (DATAL)
        Bytes 20-23: Reserved
        Bytes 24+: DATA (if any)
        """
        if len(data) > self._maxh2cdata:
            raise ValueError(
                "Data chunk size %d exceeds maxh2cdata %d" % (len(data), self._maxh2cdata))

        # Build H2C_DATA PDU
        pdu_hlen = 24  # Fixed H2CData header length
        pdu_pdo = pdu_hlen  # No digest support yet
        pdu_plen = pdu_hlen + len(data)

        # Common header (8 bytes)
        flags = PDUFlags.H2C_DATA_LAST if is_last else 0
        pdu_header = pack_pdu_header(
            pdu_type=PDUType.H2C_DATA,
            flags=flags,
            hlen=pdu_hlen,
            pdo=pdu_pdo,
            plen=pdu_plen
        )

        # Protocol Specific Header (16 bytes)
        psh = struct.pack(
            '<HHII4x',  # Little-endian: u16, u16, u32, u32, 4 reserved bytes
            command_id,   # CCCID (bytes 8-9)
            ttag,         # TTAG (bytes 10-11) - MUST use ttag from R2T, not command_id
            data_offset,  # DATAO (bytes 12-15)
            len(data)     # DATAL (bytes 16-19)
            # 4x = 4 reserved bytes (20-23)
        )

        # Send complete PDU
        pdu_data = pdu_header + psh + data
        socket.sendall(pdu_data)

        self._logger.debug(
            "Sent H2C_DATA: cmd_id=%d, ttag=%d, offset=%d, len=%d, last=%s",
            command_id, ttag, data_offset, len(data), is_last)

    def _handle_r2t_and_send_data(self, command_id: int, data: bytes,
                                  socket: socket.socket) -> None:
        """
        Handle R2T (Ready to Transfer) PDU and send data via H2C_DATA PDUs.

        This implements the large write protocol flow where the target sends
        an R2T PDU to request data transfer.

        Args:
            command_id: Command identifier to match with R2T
            data: Complete data buffer to send
            socket: Socket to receive R2T and send H2C_DATA (typically I/O socket)

        Raises:
            ProtocolError: If R2T validation fails
            NVMeoFTimeoutError: If R2T not received in time

        Reference: NVMe-oF TCP Transport Specification Rev 1.2,
        Section 3.3.2.2, Figure 34 (R2T PDU)
        """
        # 1. Receive R2T PDU
        r2t_header, r2t_data = self._receive_pdu_on_socket(socket)

        if r2t_header.pdu_type != PDUType.R2T:
            raise ProtocolError("Expected R2T PDU, got type %d" % r2t_header.pdu_type)

        # 2. Parse R2T PDU (24 bytes total)
        # Reference: Figure 34, Section 3.6.2.10
        # Bytes 00-07: Common Header (CH)
        # Bytes 08-09: Command ID (CCCID)
        # Bytes 10-11: Transfer Tag (TTAG)
        # Bytes 12-15: R2T Offset (R2TO)
        # Bytes 16-19: R2T Length (R2TL)
        # Bytes 20-23: Reserved

        if len(r2t_data) < 16:  # Need PSH: command_id, ttag, offset, length
            raise ProtocolError("R2T PDU too short: %d bytes" % len(r2t_data))

        r2t_command_id = struct.unpack('<H', r2t_data[0:2])[0]
        ttag = struct.unpack('<H', r2t_data[2:4])[0]
        r2t_offset = struct.unpack('<I', r2t_data[4:8])[0]
        r2t_length = struct.unpack('<I', r2t_data[8:12])[0]

        # 3. Validate R2T parameters (per kernel nvme_tcp_handle_r2t logic)
        if r2t_command_id != command_id:
            raise ProtocolError(
                "R2T command_id mismatch: expected %d, got %d" % (command_id, r2t_command_id))

        if r2t_length == 0:
            raise ProtocolError("R2T length is zero")

        if r2t_offset + r2t_length > len(data):
            raise ProtocolError(
                "R2T range [%d:%d] exceeds data length %d" %
                (r2t_offset, r2t_offset + r2t_length, len(data)))

        # 4. Send data in H2C_DATA PDU(s)
        data_sent = 0
        while data_sent < r2t_length:
            chunk_size = min(r2t_length - data_sent, self._maxh2cdata)
            chunk_offset = r2t_offset + data_sent
            chunk_data = data[chunk_offset:chunk_offset + chunk_size]
            is_last = (data_sent + chunk_size >= r2t_length)

            self._send_h2c_data_pdu(
                command_id=command_id,
                ttag=ttag,
                data_offset=chunk_offset,
                data=chunk_data,
                is_last=is_last,
                socket=socket
            )

            data_sent += chunk_size

        self._logger.debug("Sent %d bytes via H2C_DATA for command %d", data_sent, command_id)

    def _process_icresp(self, data: bytes) -> None:
        """
        Process Initialize Connection Response.

        Args:
            data: ICRESP payload data (128 bytes total minus 8 byte header = 120 bytes)

        Reference: NVMe-oF TCP Transport Specification Rev 1.2, Section 3.6.2.3, Figure 27
        """
        if len(data) < 120:
            raise ProtocolError(f"Invalid ICRESP data length: {len(data)}, expected 120")

        # Parse ICRESP fields per Figure 27 (offsets relative to data, not PDU):
        # Bytes 0-1: PFV (PDU Format Version, little-endian)
        # Byte 2: CPDA (Controller PDU Data Alignment)
        # Byte 3: DGST (Digest types)
        # Bytes 4-7: MAXH2CDATA (Maximum Host to Controller Data length)
        # Bytes 8-119: Reserved

        pfv, cpda, digest, maxh2cdata = struct.unpack('<HBBI', data[:8])

        # Check protocol version compatibility - Linux kernel expects version 1.0
        if pfv != NVME_TCP_PFV_1_0:
            raise ProtocolError(f"Unsupported protocol version: 0x{pfv:04x}, expected 0x{NVME_TCP_PFV_1_0:04x}")

        # Store connection parameters
        self._controller_pda = cpda
        self._digest_types = digest
        self._max_data_size = maxh2cdata if maxh2cdata > 0 else self._max_data_size
        self._maxh2cdata = maxh2cdata

        self._logger.debug(
            f"ICRESP: pfv=0x{pfv:04x}, cpda={cpda}, digest={digest}, maxh2cdata={maxh2cdata}")

    def _send_fabric_connect(self, subsys_nqn: str = None) -> None:
        """
        Send Fabric Connect command to establish admin queue.

        Args:
            subsys_nqn: Subsystem NQN to connect to (defaults to discovery)

        Reference: NVMe-oF Base Specification Section 3.3.1
        """
        # Use discovery subsystem by default, or user-specified subsystem
        if subsys_nqn is None:
            subsys_nqn = "nqn.2014-08.org.nvmexpress.discovery"

        # Use provided Host NQN or generate one
        if self._host_nqn is None:
            # Generate host NQN if not provided
            host_nqn = f"nqn.2014-08.org.nvmexpress:uuid:{self._generate_uuid()}"
            self._host_nqn = host_nqn  # Store for later use
        else:
            # Use user-provided Host NQN
            host_nqn = self._host_nqn

        command_id = self._get_next_command_id()
        self._logger.debug(f"Connecting to subsystem: {subsys_nqn}")
        self._logger.debug(f"Using host NQN: {host_nqn}")

        # Build Fabric Connect command for admin queue
        # Admin queue uses smaller size, I/O queues use larger size
        queue_size = 31  # Admin queue size (32 entries)
        connect_cmd = pack_fabric_connect_command(command_id, queue_id=0, queue_size=queue_size, kato=self._kato)

        # Build connect data with NQNs
        connect_data = pack_fabric_connect_data(host_nqn, subsys_nqn)

        # Send command PDU (72 bytes) + data PDU (1024 bytes)
        # Based on capture: PDU type=4, hlen=72, pdo=72, plen=1096
        self._send_fabric_connect_pdu(connect_cmd, connect_data)

        # Wait for connect response with timeout
        try:
            response_header, response_data = self._receive_pdu()
            self._logger.debug(
                f"Received response PDU: type={response_header.pdu_type}, len={len(response_data)}")

            if response_header.pdu_type != PDUType.RSP:
                raise ProtocolError(
                    f"Expected connect response, got PDU type {response_header.pdu_type}")

            # Parse connect response
            connect_response = ResponseParser.parse_response(response_data, command_id)
            if connect_response['status'] != 0:
                raise CommandError(
                    f"Fabric Connect failed with status {connect_response['status']:02x}",
                    connect_response['status'], command_id)

            # Store the controller ID assigned by the target (returned in dw0)
            self._controller_id = connect_response['dw0'] & 0xFFFF
            self._logger.debug(f"Fabric Connect completed successfully, assigned controller ID: {self._controller_id}")

        except NVMeoFConnectionError as e:
            if "Connection closed by target" in str(e):
                self._logger.error(
                    "Target closed connection during Fabric Connect - "
                    "possible authentication/subsystem issue")
                raise ProtocolError("Fabric Connect failed: target closed connection")
            raise

    def _send_fabric_connect_pdu(self, connect_cmd: bytes, connect_data: bytes) -> None:
        """
        Send Fabric Connect with minimal CMD PDU structure.

        Args:
            connect_cmd: 64-byte Fabric Connect command
            connect_data: 1024-byte connect data structure

        Reference: Try the absolute minimal CMD PDU structure
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Use working client values: hlen=72, pdo=72
        # This means 8-byte PDU header + 64-byte command, then data starts at offset 72
        header_len = 72
        pdo = 72
        total_len = 72 + len(connect_data)  # 72 + 1024 = 1096 total

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send everything as one: header + command + data
        full_payload = connect_cmd + connect_data

        # Debug: Show exactly what we're sending
        full_pdu = header + full_payload
        self._logger.debug(
            f"Sending Fabric Connect: hlen={header_len}, pdo={pdo}, "
            f"plen={total_len}, payload_size={len(full_payload)}")
        self._logger.debug(f"PDU header: {header.hex()}")
        self._logger.debug(f"Command start: {connect_cmd[:16].hex()}")
        self._logger.debug(f"Command DW11 (fctype): {struct.unpack('<L', connect_cmd[44:48])[0]}")

        self._socket.sendall(full_pdu)

    def _generate_uuid(self) -> str:
        """Generate a simple UUID for host NQN."""
        return str(uuid.uuid4())

    @property
    def host_nqn(self) -> str:
        """Get the Host NQN used by this client."""
        return getattr(self, '_host_nqn', None)

    def _send_get_log_page_pdu(self, command_id: int, log_page_id: int, data_length: int) -> None:
        """
        Send Get Log Page command PDU.

        Args:
            command_id: Command identifier
            log_page_id: Log Page identifier (LogPageIdentifier enum)
            data_length: Number of bytes to retrieve

        Reference: NVMe Base Specification Section 5.14
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build Get Log Page command
        log_cmd = pack_get_log_page_command(command_id, log_page_id, data_length)

        # Use same PDU format as other commands: hlen=72, pdo=72
        header_len = 72
        pdo = 72
        total_len = 72  # No additional data for Get Log Page command

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command (no additional data needed for Get Log Page)
        full_pdu = header + log_cmd

        self._logger.debug(
            f"Sending Get Log Page: log_id=0x{log_page_id:02x}, "
            f"length={data_length}, hlen={header_len}, pdo={pdo}, plen={total_len}")
        self._logger.debug(f"Command start: {log_cmd[:16].hex()}")
        self._logger.debug(f"Full command: {log_cmd.hex()}")

        self._socket.sendall(full_pdu)

    def _send_identify_controller_pdu(self, command_id: int) -> None:
        """
        Send NVMe Identify Controller command PDU.

        Args:
            command_id: Command identifier

        Reference: NVM Express Base Specification Rev 2.1, Section 5.1.1
        "Identify Controller command"
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build Identify Controller command
        # CNS=0x01 specifies "return controller data structure"
        cmd_data = pack_identify_command(command_id, IdentifyDataStructure.CONTROLLER)

        # Use standard command PDU format
        header_len = NVMEOF_TCP_CMD_HEADER_LEN
        pdo = NVMEOF_TCP_CMD_PDO
        total_len = NVMEOF_TCP_CMD_HEADER_LEN  # No additional data for Identify command

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command (no additional data needed for Identify)
        full_pdu = header + cmd_data

        self._logger.debug(
            f"Sending Identify Controller: command_id={command_id}, cns=0x01, "
            f"hlen={header_len}, pdo={pdo}, plen={total_len}")

        self._socket.sendall(full_pdu)

    def _send_admin_command_pdu(self, nvme_command: bytes) -> None:
        """
        Send a generic admin command PDU.

        Args:
            nvme_command: 64-byte packed NVMe admin command

        Reference: NVMe-oF TCP Transport Specification Section 3.3.2 "Command PDU"
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        if len(nvme_command) != NVME_COMMAND_SIZE:
            raise ValueError(f"Invalid command size: {len(nvme_command)}, expected {NVME_COMMAND_SIZE}")

        # Use standard command PDU format for admin commands
        header_len = NVMEOF_TCP_CMD_HEADER_LEN
        pdo = NVMEOF_TCP_CMD_PDO
        total_len = NVMEOF_TCP_CMD_HEADER_LEN  # No additional data for admin commands

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command PDU
        full_pdu = header + nvme_command
        self._socket.sendall(full_pdu)

    def _send_identify_namespace_pdu(self, command_id: int, nsid: int) -> None:
        """
        Send NVMe Identify Namespace command PDU.

        Args:
            command_id: Command identifier
            nsid: Namespace identifier

        Reference: NVM Express Base Specification Rev 2.1, Section 5.1.1
        "Identify Namespace command"
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build Identify Namespace command
        # CNS=0x00 specifies "return namespace data structure"
        cmd_data = pack_identify_command(command_id, IdentifyDataStructure.NAMESPACE, nsid)

        # Use standard command PDU format
        header_len = NVMEOF_TCP_CMD_HEADER_LEN
        pdo = NVMEOF_TCP_CMD_PDO
        total_len = NVMEOF_TCP_CMD_HEADER_LEN  # No additional data for Identify command

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command (no additional data needed for Identify)
        full_pdu = header + cmd_data

        self._logger.debug(
            f"Sending Identify Namespace: command_id={command_id}, nsid={nsid}, cns=0x00, "
            f"hlen={header_len}, pdo={pdo}, plen={total_len}")

        self._socket.sendall(full_pdu)

    def _send_identify_namespace_list_pdu(self, command_id: int) -> None:
        """
        Send NVMe Identify Namespace List command PDU.

        Args:
            command_id: Command identifier

        Reference: NVM Express Base Specification Rev 2.1, Section 5.1.1
        "Identify Namespace List command"
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build Identify Namespace List command
        # CNS=0x02 specifies "return active namespace ID list"
        cmd_data = pack_identify_command(command_id, IdentifyDataStructure.NAMESPACE_LIST, 0)

        # Use standard command PDU format
        header_len = NVMEOF_TCP_CMD_HEADER_LEN
        pdo = NVMEOF_TCP_CMD_PDO
        total_len = NVMEOF_TCP_CMD_HEADER_LEN  # No additional data for Identify command

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command (no additional data needed for Identify)
        full_pdu = header + cmd_data

        self._logger.debug(
            f"Sending Identify Namespace List: command_id={command_id}, cns=0x02, "
            f"hlen={header_len}, pdo={pdo}, plen={total_len}")

        self._socket.sendall(full_pdu)

    def _send_set_features_pdu(self, command_id: int, feature_id: int, value: int) -> None:
        """
        Send NVMe Set Features command PDU.

        Args:
            command_id: Command identifier
            feature_id: Feature identifier (e.g., 0x0B for Async Event Config)
            value: Feature value to set

        Reference: NVM Express Base Specification Rev 2.1, Section 5.27
        "Set Features Command"
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build Set Features command
        cmd_data = pack_set_features_command(command_id, feature_id, value)

        # Use same PDU format as other commands: hlen=72, pdo=72
        header_len = 72
        pdo = 72
        total_len = 72  # No additional data for Set Features command

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command (no additional data needed for Set Features)
        full_pdu = header + cmd_data

        self._logger.debug(
            f"Sending Set Features: feature_id=0x{feature_id:02x}, value=0x{value:08x}, "
            f"hlen={header_len}, pdo={pdo}, plen={total_len}")

        self._socket.sendall(full_pdu)

    def _send_property_get_pdu(self, command_id: int, property_offset: int, property_size: int = 4) -> None:
        """
        Send Property Get command PDU.

        Args:
            command_id: Command identifier
            property_offset: NVMe property register offset
            property_size: Size of property in bytes (4 or 8)

        Reference: NVMe-oF Base Specification Section 5.2
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build Property Get command with proper size encoding
        prop_cmd = pack_fabric_property_get_command(command_id, property_offset, property_size)

        # Use same PDU format as Fabric Connect: hlen=72, pdo=72
        header_len = 72
        pdo = 72
        total_len = 72  # No additional data for Property Get

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command (no additional data needed for Property Get)
        full_pdu = header + prop_cmd

        self._logger.debug(
            f"Sending Property Get: property=0x{property_offset:02x}, "
            f"hlen={header_len}, pdo={pdo}, plen={total_len}")
        self._logger.debug(f"Command start: {prop_cmd[:16].hex()}")
        self._logger.debug(f"Full command: {prop_cmd.hex()}")

        self._socket.sendall(full_pdu)

    def _send_property_set_pdu(self, command_id: int, property_offset: int, value: int) -> None:
        """
        Send Property Set command PDU.

        Args:
            command_id: Command identifier
            property_offset: NVMe property register offset (e.g., NVMeProperty.CC)
            value: Value to write to the property register

        Reference: Working nvme-cli Property Set sequence
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build Property Set command
        prop_cmd = pack_fabric_property_set_command(command_id, property_offset, value)

        # Use same PDU format as other commands: hlen=72, pdo=72
        header_len = 72
        pdo = 72
        total_len = 72  # No additional data for Property Set command

        header = pack_pdu_header(PDUType.CMD, 0, header_len, pdo, total_len)

        # Send command (no additional data needed for Property Set)
        full_pdu = header + prop_cmd

        self._logger.debug(
            f"Sending Property Set: property=0x{property_offset:02x}, value=0x{value:08x}, "
            f"hlen={header_len}, pdo={pdo}, plen={total_len}")
        self._logger.debug(f"Command start: {prop_cmd[:16].hex()}")
        self._logger.debug(f"Full command: {prop_cmd.hex()}")

        self._socket.sendall(full_pdu)

    def _send_command_pdu(self, opcode: int, command_id: int, nsid: int,
                          data: bytes | None = None) -> None:
        """
        Send NVMe command as PDU.

        Args:
            opcode: Command opcode
            command_id: Command identifier
            nsid: Namespace identifier
            data: Optional command data
        """
        # Build NVMe command
        nvme_cmd = pack_nvme_command(opcode, 0, command_id, nsid)

        # Send command PDU with correct PDU data offset
        header_len = 8
        data_len = len(nvme_cmd)
        total_len = header_len + data_len

        # For command PDUs, pdo should point to start of data after header
        header = pack_pdu_header(PDUType.CMD, 0, header_len, header_len, total_len)
        self._socket.sendall(header + nvme_cmd)

        # Send data PDU if present
        if data:
            self._send_pdu(PDUType.DATA, data)

    def _send_pdu(self, pdu_type: int, data: bytes) -> None:
        """
        Send PDU with header and data.

        Args:
            pdu_type: PDU type identifier
            data: PDU payload data
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Build PDU header
        header_len = 8
        data_len = len(data)
        total_len = header_len + data_len

        # PDU data offset should be 0 for PDUs without separate data section
        pdo = header_len if data else 0
        header = pack_pdu_header(pdu_type, 0, header_len, pdo, total_len)

        # Send header and data
        self._socket.sendall(header + data)

    def _receive_pdu(self) -> tuple[PDUHeader, bytes]:
        """
        Receive PDU header and data.

        Returns:
            Tuple of (PDU header, PDU data)
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        # Receive basic PDU header (8 bytes for all PDU types)
        # Reference: NVMe-oF TCP Transport Specification Section 3.3.1 "PDU Header Format"
        header_data = self._receive_exact(NVMEOF_TCP_PDU_BASIC_HEADER_LEN)
        self._logger.debug(f"Raw PDU header bytes: {header_data.hex()}")

        # Check for zero header (connection issue)
        if header_data == b'\x00' * NVMEOF_TCP_PDU_BASIC_HEADER_LEN:
            raise NVMeoFConnectionError("Received zero PDU header - connection may be closed")

        header = unpack_pdu_header(header_data)
        self._logger.debug(f"Parsed PDU header: type={header.pdu_type}, hlen={header.hlen}, plen={header.plen}")

        # Handle special case for ICREQ/ICRESP where hlen=plen=128
        # Reference: NVMe-oF TCP Transport Specification Section 4.2 "Initialize Connection PDUs"
        if header.hlen == header.plen and header.hlen > NVMEOF_TCP_PDU_BASIC_HEADER_LEN:
            # The "data" is actually part of the extended header for ICREQ/ICRESP
            remaining_header = self._receive_exact(header.hlen - NVMEOF_TCP_PDU_BASIC_HEADER_LEN)
            data = remaining_header
        elif header.pdu_type == PDUType.C2H_DATA:
            # C2H_DATA PDUs have extended header followed by actual data payload
            # Reference: NVMe-oF TCP Transport Specification Section 3.3.6 "C2H_DATA PDU"
            # Extended header contains command completion information, data contains the actual payload
            extended_header_len = header.hlen - NVMEOF_TCP_PDU_BASIC_HEADER_LEN
            data_len = header.plen - header.hlen

            # Skip the extended header (contains completion info we handle separately)
            if extended_header_len > 0:
                extended_header = self._receive_exact(extended_header_len)
                self._logger.debug(f"C2H_DATA extended header: {extended_header.hex()}")

            # Return only the actual data payload (e.g., Identify Controller/Namespace data)
            data = self._receive_exact(data_len) if data_len > 0 else b''
        else:
            # Normal PDU with separate data section (CMD, RSP, etc.)
            # Reference: NVMe-oF TCP Transport Specification Section 3.3 "PDU Format"
            data_len = header.plen - header.hlen
            data = self._receive_exact(data_len) if data_len > 0 else b''

        return header, data

    def _receive_response(self, command_id: int, timeout: float | None) -> dict[str, Any]:
        """
        Receive and parse command response.

        Args:
            command_id: Expected command identifier
            timeout: Response timeout

        Returns:
            Parsed response dictionary
        """
        if timeout:
            original_timeout = self._socket.gettimeout()
            self._socket.settimeout(timeout)

        try:
            header, data = self._receive_pdu()

            if header.pdu_type == PDUType.RSP:
                return ResponseParser.parse_response(data, command_id)
            else:
                raise ProtocolError(f"Unexpected PDU type: {header.pdu_type}")

        except socket.timeout:
            raise NVMeoFTimeoutError(f"Response timeout after {timeout} seconds")
        finally:
            if timeout:
                self._socket.settimeout(original_timeout)

    def _get_namespace_logical_block_size(self, nsid: int) -> int:
        """
        Get logical block size for a namespace with caching for performance.

        Args:
            nsid: Namespace identifier

        Returns:
            Logical block size in bytes

        Reference: NVM Express Base Specification Section 5.15.2.1
        """
        # Check cache first
        if nsid in self._namespace_info_cache:
            cached_info = self._namespace_info_cache[nsid]
            logical_block_size = cached_info.get('logical_block_size', 0)
            if logical_block_size > 0:
                return logical_block_size

        # Cache miss or invalid cached value - fetch namespace info
        try:
            self._logger.debug(f"Fetching namespace {nsid} info for logical block size")
            ns_info = self.identify_namespace(nsid)
            logical_block_size = ns_info.get('logical_block_size', NVME_SECTOR_SIZE)

            if logical_block_size == 0 or logical_block_size > 65536:
                self._logger.warning(
                    "Invalid logical block size %d for namespace %d, using default %d",
                    logical_block_size, nsid, NVME_SECTOR_SIZE)
                logical_block_size = NVME_SECTOR_SIZE

            # Cache the result
            self._namespace_info_cache[nsid] = ns_info
            self._logger.debug(f"Cached namespace {nsid} info: logical_block_size={logical_block_size}")

            return logical_block_size

        except Exception as e:
            self._logger.warning(
                "Failed to get namespace %d info for logical block size: %s, using default %d",
                nsid, e, NVME_SECTOR_SIZE)
            return NVME_SECTOR_SIZE

    def _send_nvme_io_command_pdu(self, command_id: int, nvme_command: bytes) -> None:
        """
        Send NVMe I/O command PDU without data payload.

        Used for commands like Read and Flush that don't send data with the command.

        Args:
            command_id: Command identifier
            nvme_command: 64-byte packed NVMe command

        Reference: NVMe-oF TCP Transport Specification Section 3.3.2 "Command PDU"
        """
        if len(nvme_command) != NVME_COMMAND_SIZE:
            raise ValueError(f"Invalid NVMe command size: {len(nvme_command)}, expected {NVME_COMMAND_SIZE}")

        self._logger.debug(f"Sending I/O command PDU (command_id={command_id})")

        # Create Command PDU header
        # Reference: NVMe-oF TCP Transport Specification Table 7 "Command PDU"
        pdu_header = pack_pdu_header(
            pdu_type=PDUType.CMD,
            flags=0,
            hlen=NVMEOF_TCP_CMD_HEADER_LEN,  # 72 bytes for command header
            pdo=0,                           # 0 bytes data offset (like nvme CLI)
            plen=NVMEOF_TCP_CMD_HEADER_LEN   # Total length = header only
        )

        # Send PDU header + NVMe command (total 72 bytes) on I/O connection
        pdu_data = pdu_header + nvme_command
        self._io_socket.send(pdu_data)

        self._logger.debug(f"I/O command PDU sent ({len(pdu_data)} bytes)")

    def _send_nvme_write_pdu(self, command_id: int, nsid: int, lba: int, data: bytes, logical_block_size: int) -> None:
        """
        Send NVMe Write command with data payload using H2C_DATA PDU.

        Args:
            command_id: Command identifier
            nsid: Namespace identifier
            lba: Starting logical block address
            data: Data to write
            logical_block_size: Size of each logical block in bytes

        Reference: NVMe-oF TCP Transport Specification Section 3.3.4 "H2C_DATA PDU"
        """
        if not data:
            raise ValueError("Write data cannot be empty")

        if len(data) % logical_block_size != 0:
            raise ValueError(f"Data size ({len(data)}) must be multiple of logical block size ({logical_block_size})")

        block_count = len(data) // logical_block_size

        self._logger.debug(f"Sending Write command PDU with {len(data)} bytes data (command_id={command_id})")

        # Pack the NVMe Write command with correct logical block size
        nvme_command = pack_nvme_write_command(command_id, nsid, lba, block_count, logical_block_size)

        # Calculate total PDU length: command header + data
        total_pdu_length = NVMEOF_TCP_CMD_HEADER_LEN + len(data)

        # Create Command PDU header with data (single PDU approach)
        # Reference: NVMe-oF TCP Transport Specification Table 7 "Command PDU"
        pdu_header = pack_pdu_header(
            pdu_type=PDUType.CMD,
            flags=0,
            hlen=NVMEOF_TCP_CMD_HEADER_LEN,  # 72 bytes for command header
            pdo=NVMEOF_TCP_CMD_PDO,          # 72 bytes data offset
            plen=total_pdu_length            # Total length = header + data
        )

        # Send PDU: header + NVMe command + data on I/O socket
        pdu_data = pdu_header + nvme_command + data
        self._io_socket.sendall(pdu_data)

        self._logger.debug(f"Write command PDU sent ({len(pdu_data)} bytes total)")

    def _send_nvme_reservation_command_pdu(self, nvme_command: bytes, data: bytes) -> None:
        """
        Send NVMe Reservation command with data payload on I/O socket.

        Args:
            nvme_command: 64-byte packed NVMe reservation command
            data: 16-byte reservation data payload

        Reference: NVMe-oF TCP Transport Specification Section 3.3.2 "Command PDU"
        """
        if len(nvme_command) != NVME_COMMAND_SIZE:
            raise ValueError(f"NVMe command must be {NVME_COMMAND_SIZE} bytes, got {len(nvme_command)}")

        if len(data) != 16:
            raise ValueError(f"Reservation data must be 16 bytes, got {len(data)}")

        self._logger.debug(f"Sending Reservation command PDU with {len(data)} bytes data")

        # Calculate total PDU length: command header + data
        total_pdu_length = NVMEOF_TCP_CMD_HEADER_LEN + len(data)

        # Create Command PDU header with data
        pdu_header = pack_pdu_header(
            pdu_type=PDUType.CMD,
            flags=0,
            hlen=NVMEOF_TCP_CMD_HEADER_LEN,  # 72 bytes for command header
            pdo=NVMEOF_TCP_CMD_PDO,          # 72 bytes data offset
            plen=total_pdu_length            # Total length = header + data
        )

        # Send PDU: header + NVMe command + data on I/O socket
        pdu_data = pdu_header + nvme_command + data
        self._io_socket.sendall(pdu_data)

        self._logger.debug(f"Reservation command PDU sent ({len(pdu_data)} bytes total)")

    def _send_nvme_reservation_pdu(self, nvme_command: bytes, data: bytes) -> None:
        """
        Send NVMe Reservation command with data payload.

        Args:
            nvme_command: 64-byte packed NVMe reservation command
            data: Reservation data payload (8 or 16 bytes depending on command)

        Reference: NVMe-oF TCP Transport Specification Section 3.3.2 "Command PDU"
        """
        if len(nvme_command) != NVME_COMMAND_SIZE:
            raise ValueError(f"NVMe command must be {NVME_COMMAND_SIZE} bytes, got {len(nvme_command)}")

        if len(data) not in [8, 16]:
            raise ValueError(f"Reservation data must be 8 or 16 bytes, got {len(data)}")

        # Calculate total PDU length: command header + data
        total_pdu_length = NVMEOF_TCP_CMD_HEADER_LEN + len(data)

        # Create Command PDU header with data
        pdu_header = pack_pdu_header(
            pdu_type=PDUType.CMD,
            flags=0,
            hlen=NVMEOF_TCP_CMD_HEADER_LEN,  # 72 bytes for command header
            pdo=NVMEOF_TCP_CMD_PDO,          # 72 bytes data offset
            plen=total_pdu_length            # Total length = header + data
        )

        # Send PDU: header + NVMe command + data
        pdu_data = pdu_header + nvme_command + data
        self._io_socket.send(pdu_data)

        self._logger.debug(f"Reservation command PDU sent ({len(pdu_data)} bytes total)")

    def _send_nvme_compare_pdu(
            self, command_id: int, nsid: int, lba: int, data: bytes,
            logical_block_size: int) -> None:
        """
        Send NVMe Compare command with data payload using H2C_DATA PDU.

        Args:
            command_id: Command identifier
            nsid: Namespace identifier
            lba: Starting logical block address
            data: Data to compare against
            logical_block_size: Size of each logical block in bytes

        Reference: NVMe-oF TCP Transport Specification Section 3.3.4 "H2C_DATA PDU"
        """
        if not data:
            raise ValueError("Compare data cannot be empty")

        if len(data) % logical_block_size != 0:
            raise ValueError(f"Data size ({len(data)}) must be multiple of logical block size ({logical_block_size})")

        block_count = len(data) // logical_block_size

        self._logger.debug(f"Sending Compare command PDU with {len(data)} bytes data (command_id={command_id})")

        # Pack the NVMe Compare command with correct logical block size
        # NVMe uses 0-based count
        nvme_command = pack_nvme_compare_command(
            command_id, nsid, lba, block_count - 1, logical_block_size)

        # Calculate total PDU length: command header + data
        total_pdu_length = NVMEOF_TCP_CMD_HEADER_LEN + len(data)

        # Create PDU header for H2C_DATA (command + data)
        pdu_header = pack_pdu_header(
            pdu_type=PDUType.CMD,
            flags=0,
            hlen=NVMEOF_TCP_CMD_HEADER_LEN,  # 72 bytes for command header
            pdo=NVMEOF_TCP_CMD_HEADER_LEN,   # Data starts after command header
            plen=total_pdu_length            # Total length = header + data
        )

        # Send PDU: header + NVMe command + data
        pdu_data = pdu_header + nvme_command + data
        self._socket.send(pdu_data)

        self._logger.debug(f"Compare command PDU sent ({len(pdu_data)} bytes total)")

    def _receive_exact(self, length: int) -> bytes:
        """
        Receive exact number of bytes from socket.

        Args:
            length: Number of bytes to receive

        Returns:
            Received data
        """
        if not self._socket:
            raise NVMeoFConnectionError("Socket not available")

        data = b''
        while len(data) < length:
            chunk = self._socket.recv(length - len(data))
            self._logger.debug(
                "Received chunk: %s (length=%d)",
                chunk.hex() if chunk else 'None', len(chunk) if chunk else 0)
            if not chunk:
                raise NVMeoFConnectionError("Connection closed by target")
            data += chunk

        return data

    def _send_termination_pdu(self) -> None:
        """
        Send Host to Controller Terminate Connection Request (H2CTermReq) PDU.

        Used for graceful disconnection from the controller.

        Reference: NVMe-oF TCP Transport Specification Rev 1.2, Section 3.6.2.4, Figure 28
        "Host to Controller Terminate Connection Request PDU (H2CTermReq)"

        PDU Structure (24 bytes total):
          Bytes 00-07: Common Header (CH)
            - PDU Type: 02h (H2C_TERM)
            - Flags: 0 (Reserved)
            - HLEN: 24 (0x18) - Fixed length per spec
            - PDO: 0 (Reserved)
            - PLEN: 24 (0x18) - No additional data for graceful disconnect
          Bytes 08-09: Fatal Error Status (FES) - 0x0000 for graceful disconnect
          Bytes 10-13: Fatal Error Information (FEI) - 0x00000000 (not applicable)
          Bytes 14-23: Reserved - must be zero
        """
        try:
            if not self._socket:
                return

            # Build 24-byte H2CTermReq PDU
            # Common header: 8 bytes
            pdu_data = bytearray(24)
            pdu_data[0] = PDUType.H2C_TERM  # PDU Type: 02h
            pdu_data[1] = 0                  # Flags: Reserved
            pdu_data[2] = 24                 # HLEN: Fixed length of 24 bytes (18h)
            pdu_data[3] = 0                  # PDO: Reserved
            pdu_data[4] = 24                 # PLEN: 24 bytes (low byte)
            pdu_data[5] = 0                  # PLEN: (mid byte)
            pdu_data[6] = 0                  # PLEN: (high byte)
            pdu_data[7] = 0                  # Reserved

            # Protocol Specific Header (PSH): bytes 8-23
            # Bytes 08-09: Fatal Error Status (FES) = 0x0000 (no error, graceful disconnect)
            pdu_data[8] = 0
            pdu_data[9] = 0

            # Bytes 10-13: Fatal Error Information (FEI) = 0x00000000 (not applicable)
            pdu_data[10] = 0
            pdu_data[11] = 0
            pdu_data[12] = 0
            pdu_data[13] = 0

            # Bytes 14-23: Reserved (already zero from bytearray initialization)

            self._socket.sendall(bytes(pdu_data))
            self._logger.debug("Sent H2CTermReq PDU for graceful disconnection")
        except Exception as e:
            self._logger.debug(f"Failed to send termination PDU: {e}")
            pass  # Best effort termination

    def _get_next_admin_command_id(self) -> int:
        """
        Get next command identifier for admin queue.

        Each NVMe queue maintains its own command ID sequence.
        Reference: NVM Express Base Specification Rev 2.1, Section 4.1
        """
        cmd_id = self._admin_command_id_counter
        self._admin_command_id_counter = (self._admin_command_id_counter + 1) & NVME_COMMAND_ID_MASK
        return cmd_id

    def _get_next_io_command_id(self) -> int:
        """
        Get next command identifier for I/O queue.

        Each NVMe queue maintains its own command ID sequence.
        Reference: NVM Express Base Specification Rev 2.1, Section 4.1
        """
        cmd_id = self._io_command_id_counter
        self._io_command_id_counter = (self._io_command_id_counter + 1) & NVME_COMMAND_ID_MASK
        return cmd_id

    def _get_next_command_id(self) -> int:
        """
        Get next command identifier for admin queue (backward compatibility).

        Note: New code should use _get_next_admin_command_id() or _get_next_io_command_id()
        to be explicit about which queue the command is for.
        """
        return self._get_next_admin_command_id()

    def _cleanup_socket(self) -> None:
        """Clean up admin socket resources."""
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def _cleanup_io_socket(self) -> None:
        """Clean up I/O socket resources."""
        if self._io_socket:
            try:
                self._io_socket.close()
            except Exception:
                pass
            self._io_socket = None

    def enable_async_events(self, event_mask: int | None = None) -> None:
        """
        Enable asynchronous event notifications via Set Features command.

        This method configures the controller to report asynchronous events
        by setting the Asynchronous Event Configuration feature.

        Args:
            event_mask: Event notification mask. If None (default), enables all events
                       supported by the controller (based on OAES field).
                       Bits correspond to specific event types as defined in
                       NVM Express Base Specification 2.3, Figure 409

        Raises:
            RuntimeError: If not connected to target
            CommandError: If Set Features command fails

        Reference: NVM Express Base Specification 2.3, Section 5.27 "Set Features command"
                   Figure 401: Set Features – Command Dword 10
                   Figure 409: Asynchronous Event Configuration – Command Dword 11
        """
        if not self._connected:
            raise RuntimeError("Not connected to target")

        # If no mask provided, build one from controller's OAES field
        if event_mask is None:
            # Get raw OAES value (need to read it from the low-level response)
            controller_dict = self.identify_controller()
            oaes = controller_dict.get('oaes', 0)
            # Mask out bits 7:0 which are for SMART/Health, keep only Notice/Immediate event bits
            event_mask = oaes & 0xFFFFFF00
            self._logger.debug(f"Built event mask from OAES: {event_mask:#010x}")

        self._logger.debug(f"Enabling async events with mask {event_mask:#010x}")

        # Send Set Features command for Asynchronous Event Configuration
        cmd_id = self._get_next_admin_command_id()
        cmd = pack_set_features_command(
            cmd_id,
            FeatureIdentifier.ASYNCHRONOUS_EVENT_CONFIG,
            event_mask
        )

        # Send command via admin queue
        self._send_admin_command_pdu(cmd)

        # Receive response (will raise exception if command fails)
        self._receive_response(cmd_id, self.timeout)

        self._async_events_enabled = True
        self._logger.info("Asynchronous events enabled")

    def request_async_events(self, count: int = 1) -> None:
        """
        Submit Asynchronous Event Request commands to the controller.

        This method sends one or more Asynchronous Event Request commands that
        will be completed by the controller when events occur. The number of
        outstanding requests is limited by the AERL field from Identify Controller.

        Args:
            count: Number of async event request commands to submit

        Raises:
            RuntimeError: If not connected or async events not enabled
            ValueError: If count exceeds controller's AERL limit
            CommandError: If command submission fails

        Reference: NVM Express Base Specification 2.3, Section 5.2.2
                   "Asynchronous Event Request command"
        """
        if not self._connected:
            raise RuntimeError("Not connected to target")

        if not self._async_events_enabled:
            raise RuntimeError("Async events not enabled - call enable_async_events() first")

        # Get AERL if not already cached
        if self._aerl is None:
            controller_info = self.get_controller_info()
            self._aerl = controller_info.aerl

        # Check if we would exceed AERL limit
        max_outstanding = self._aerl + 1  # AERL is 0-based
        if len(self._outstanding_async_requests) + count > max_outstanding:
            raise ValueError(
                f"Cannot submit {count} requests: would exceed AERL limit of {max_outstanding} "
                f"(currently {len(self._outstanding_async_requests)} outstanding)"
            )

        # Submit the requested number of async event request commands
        for _ in range(count):
            cmd_id = self._get_next_admin_command_id()
            cmd = pack_async_event_request_command(cmd_id)

            # Send command via admin queue
            self._send_admin_command_pdu(cmd)

            # Track this command ID as outstanding
            self._outstanding_async_requests.append(cmd_id)
            self._logger.debug(f"Submitted async event request with command ID {cmd_id}")

        self._logger.info(f"Submitted {count} async event requests "
                          f"(total outstanding: {len(self._outstanding_async_requests)})")

    def poll_async_events(self, timeout: float | None = 0.1) -> list[AsyncEvent]:
        """
        Poll for completed asynchronous event notifications.

        This method checks the admin queue socket for any completed Asynchronous
        Event Request commands and returns a list of events that have occurred.
        It does not block - if no events are available, returns an empty list.

        Args:
            timeout: Socket timeout for checking (default: 0.1 seconds)
                    Set to 0 for non-blocking check

        Returns:
            List of AsyncEvent objects for any events that occurred

        Raises:
            RuntimeError: If not connected or no outstanding requests

        Reference: NVM Express Base Specification 2.3, Figure 150-151
                   "Asynchronous Event Request – Completion Queue Entry"
        """
        if not self._connected:
            raise RuntimeError("Not connected to target")

        if not self._outstanding_async_requests:
            return []

        events = []

        # Temporarily set socket timeout for non-blocking poll
        original_timeout = self._socket.gettimeout()
        self._socket.settimeout(timeout)

        try:
            # Try to receive responses for outstanding async event requests
            while self._outstanding_async_requests:
                try:
                    # Receive PDU from admin queue
                    header, data = self._receive_pdu()

                    # Check PDU type - async events come as RSP PDUs
                    if header.pdu_type != PDUType.RSP:
                        self._logger.warning(f"Unexpected PDU type for async event: {header.pdu_type}")
                        continue

                    # Parse response - completion queue entry is at least 16 bytes
                    if len(data) < 16:
                        self._logger.warning(f"Short response received: {len(data)} bytes")
                        continue

                    # Extract command ID from completion queue entry (bytes 12-13)
                    cmd_id = struct.unpack('<H', data[12:14])[0]

                    # Check if this is one of our async event requests
                    if cmd_id not in self._outstanding_async_requests:
                        # This might be a response to another command - skip for now
                        self._logger.debug(f"Received response for command {cmd_id} "
                                           "(not an async event request)")
                        continue

                    # Remove from outstanding list
                    self._outstanding_async_requests.remove(cmd_id)

                    # Parse the completion queue entry
                    # Format: DW0(4) + DW1(4) + SQ_HEAD(2) + SQ_ID(2) + CID(2) + STATUS(2)
                    dw0, dw1 = struct.unpack('<LL', data[0:8])

                    # Parse async event using parser
                    event = AsyncEventParser.parse_async_event_to_object(dw0, dw1)
                    events.append(event)

                    self._logger.info(f"Async event received: {event.description}")

                except socket.timeout:
                    # No more data available
                    break
                except Exception as e:
                    self._logger.error(f"Error polling async events: {e}")
                    break

        finally:
            # Restore original socket timeout
            self._socket.settimeout(original_timeout)

        return events

    def __enter__(self):
        """Context manager entry - connects using subsystem_nqn from constructor."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
