"""
NVMe Status Code Definitions

Comprehensive status code definitions with human-readable descriptions and
specification references for NVMe and NVMe-oF operations.

References:
- NVM Express Base Specification Rev 2.1, Section 1.6 "Status Codes"
- NVM Express Command Set Specification Rev 1.0c, Section 3.1 "Status Codes"
- NVM Express over Fabrics Specification Rev 1.1a, Section 2.1 "Connect Response"
"""

from enum import IntEnum
from typing import Dict, Tuple


class NVMeStatusType(IntEnum):
    """
    NVMe Status Code Type (SCT) field values.

    Reference: NVM Express Base Specification Rev 2.1, Figure 94 "Status Field"
    """
    GENERIC = 0x0               # Generic Command Status
    COMMAND_SPECIFIC = 0x1      # Command Specific Status
    MEDIA_DATA_INTEGRITY = 0x2  # Media and Data Integrity Errors
    PATH_RELATED = 0x3          # Path Related Status
    VENDOR_SPECIFIC = 0x7       # Vendor Specific


class NVMeGenericStatus(IntEnum):
    """
    Generic Command Status Codes (SCT=0).

    Reference: NVM Express Base Specification Rev 2.1, Figure 95 "Generic Command Status Values"
    """
    SUCCESS = 0x00                        # Successful Completion
    INVALID_OPCODE = 0x01                 # Invalid Command Opcode
    INVALID_FIELD = 0x02                  # Invalid Field in Command
    COMMAND_ID_CONFLICT = 0x03            # Command ID Conflict
    DATA_TRANSFER_ERROR = 0x04            # Data Transfer Error
    COMMAND_ABORTED_POWER_LOSS = 0x05     # Commands Aborted due to Power Loss Notification
    INTERNAL_ERROR = 0x06                 # Internal Error
    COMMAND_ABORT_REQUESTED = 0x07        # Command Abort Requested
    COMMAND_ABORTED_SQ_DELETION = 0x08    # Command Aborted due to SQ Deletion
    COMMAND_ABORTED_FAILED_FUSED = 0x09   # Command Aborted due to Failed Fused Command
    COMMAND_ABORTED_MISSING_FUSED = 0x0A  # Command Aborted due to Missing Fused Command
    INVALID_NAMESPACE = 0x0B              # Invalid Namespace or Format
    COMMAND_SEQUENCE_ERROR = 0x0C         # Command Sequence Error
    INVALID_SGL_LAST_SEGMENT = 0x0D       # Invalid SGL Last Segment Descriptor
    INVALID_SGL_COUNT = 0x0E              # Invalid Number of SGL Descriptors
    INVALID_SGL_DATA_LENGTH = 0x0F        # Invalid SGL Data Length
    INVALID_SGL_METADATA_LENGTH = 0x10    # Invalid SGL Metadata Length
    INVALID_SGL_DESCRIPTOR_TYPE = 0x11    # Invalid SGL Descriptor Type
    INVALID_CMB_USE = 0x12                # Invalid use of Controller Memory Buffer
    PRP_OFFSET_INVALID = 0x13             # PRP Offset Invalid
    ATOMIC_WRITE_UNIT_EXCEEDED = 0x14     # Atomic Write Unit Exceeded
    OPERATION_DENIED = 0x15               # Operation Denied
    SGL_OFFSET_INVALID = 0x16             # SGL Offset Invalid
    HOST_ID_INCONSISTENT_FORMAT = 0x18    # Host Identifier Inconsistent Format
    KEEP_ALIVE_TIMEOUT_EXPIRED = 0x19     # Keep Alive Timer Expired
    KEEP_ALIVE_TIMEOUT_INVALID = 0x1A     # Keep Alive Timeout Invalid
    COMMAND_ABORTED_PREEMPT = 0x1B        # Command Aborted due to Preempt and Abort
    SANITIZE_FAILED = 0x1C                # Sanitize Failed
    SANITIZE_IN_PROGRESS = 0x1D           # Sanitize In Progress
    SGL_DATA_BLOCK_GRANULARITY = 0x1E     # SGL Data Block Granularity Invalid
    COMMAND_NOT_SUPPORTED_CMB = 0x1F      # Command Not Supported for Queue in CMB
    NAMESPACE_WRITE_PROTECTED = 0x20      # Namespace is Write Protected
    COMMAND_INTERRUPTED = 0x21            # Command Interrupted
    TRANSIENT_TRANSPORT_ERROR = 0x22      # Transient Transport Error


class NVMeCommandSpecificStatus(IntEnum):
    """
    Command Specific Status Codes (SCT=1).

    Reference: NVM Express Base Specification Rev 2.1, Figure 96 "Command Specific Status Values"
    """
    COMPLETION_QUEUE_INVALID = 0x00             # Completion Queue Invalid
    INVALID_QUEUE_IDENTIFIER = 0x01             # Invalid Queue Identifier
    INVALID_QUEUE_SIZE = 0x02                   # Invalid Queue Size
    ABORT_COMMAND_LIMIT_EXCEEDED = 0x03         # Abort Command Limit Exceeded
    ASYNC_EVENT_REQUEST_LIMIT = 0x05            # Asynchronous Event Request Limit Exceeded
    INVALID_FIRMWARE_SLOT = 0x06                # Invalid Firmware Slot
    INVALID_FIRMWARE_IMAGE = 0x07               # Invalid Firmware Image
    INVALID_INTERRUPT_VECTOR = 0x08             # Invalid Interrupt Vector
    INVALID_LOG_PAGE = 0x09                     # Invalid Log Page
    INVALID_FORMAT = 0x0A                       # Invalid Format
    FIRMWARE_ACTIVATION_REQUIRES_RESET = 0x0B   # Firmware Activation Requires Reset
    INVALID_QUEUE_DELETION = 0x0C               # Invalid Queue Deletion
    FEATURE_ID_NOT_SAVEABLE = 0x0D              # Feature Identifier Not Saveable
    FEATURE_NOT_CHANGEABLE = 0x0E               # Feature Not Changeable
    FEATURE_NOT_NAMESPACE_SPECIFIC = 0x0F       # Feature Not Namespace Specific
    FIRMWARE_ACTIVATION_PROHIBITED = 0x10       # Firmware Activation Prohibited
    OVERLAPPING_RANGE = 0x11                    # Overlapping Range
    NAMESPACE_INSUFFICIENT_CAPACITY = 0x12      # Namespace Insufficient Capacity
    NAMESPACE_ID_UNAVAILABLE = 0x13             # Namespace Identifier Unavailable
    NAMESPACE_ALREADY_ATTACHED = 0x15           # Namespace Already Attached
    NAMESPACE_PRIVATE = 0x16                    # Namespace Is Private
    NAMESPACE_NOT_ATTACHED = 0x17               # Namespace Not Attached
    THIN_PROVISIONING_NOT_SUPPORTED = 0x18      # Thin Provisioning Not Supported
    CONTROLLER_LIST_INVALID = 0x19              # Controller List Invalid
    DEVICE_SELF_TEST_IN_PROGRESS = 0x1D         # Device Self-test In Progress
    BOOT_PARTITION_WRITE_PROHIBITED = 0x1E      # Boot Partition Write Prohibited
    INVALID_CONTROLLER_IDENTIFIER = 0x1F        # Invalid Controller Identifier
    INVALID_SECONDARY_CONTROLLER_STATE = 0x20   # Invalid Secondary Controller State
    INVALID_NUMBER_CONTROLLER_RESOURCES = 0x21  # Invalid Number of Controller Resources
    INVALID_RESOURCE_IDENTIFIER = 0x22          # Invalid Resource Identifier
    SANITIZE_PROHIBITED_WPMRE = 0x23            # Sanitize Prohibited While Persistent Memory Region is Enabled
    ANA_GROUP_ID_INVALID = 0x24                 # ANA Group Identifier Invalid
    ANA_ATTACH_FAILED = 0x25                    # ANA Attach Failed


class NVMeFabricStatus(IntEnum):
    """
    NVMe-oF Fabric Specific Status Codes.

    Reference: NVM Express over Fabrics Specification Rev 1.1a, Figure 18 "Fabric Command Status Values"
    """
    INCOMPATIBLE_FORMAT = 0x80         # Incompatible Format
    CONTROLLER_BUSY = 0x81             # Controller Busy
    INVALID_PARAM = 0x82               # Invalid Parameter
    RESTART_DISCOVERY = 0x83           # Restart Discovery
    INVALID_HOST = 0x84                # Invalid Host


# Status code descriptions with spec references
NVME_STATUS_DESCRIPTIONS: Dict[Tuple[int, int], Tuple[str, str]] = {
    # Generic Command Status (SCT=0)
    (0, 0x00): ("Successful Completion", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x01): ("Invalid Command Opcode", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x02): ("Invalid Field in Command", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x03): ("Command ID Conflict", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x04): ("Data Transfer Error", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x05): ("Commands Aborted due to Power Loss Notification", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x06): ("Internal Error", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x07): ("Command Abort Requested", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x08): ("Command Aborted due to SQ Deletion", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x09): ("Command Aborted due to Failed Fused Command", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x0A): ("Command Aborted due to Missing Fused Command", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x0B): ("Invalid Namespace or Format", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x0C): ("Command Sequence Error", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x0D): ("Invalid SGL Last Segment Descriptor", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x0E): ("Invalid Number of SGL Descriptors", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x0F): ("Invalid SGL Data Length", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x10): ("Invalid SGL Metadata Length", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x11): ("Invalid SGL Descriptor Type", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x12): ("Invalid use of Controller Memory Buffer", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x13): ("PRP Offset Invalid", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x14): ("Atomic Write Unit Exceeded", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x15): ("Operation Denied", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x16): ("SGL Offset Invalid", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x18): ("Host Identifier Inconsistent Format", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x19): ("Keep Alive Timer Expired", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x1A): ("Keep Alive Timeout Invalid", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x1B): ("Command Aborted due to Preempt and Abort", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x1C): ("Sanitize Failed", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x1D): ("Sanitize In Progress", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x1E): ("SGL Data Block Granularity Invalid", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x1F): ("Command Not Supported for Queue in CMB", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x20): ("Namespace is Write Protected", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x21): ("Command Interrupted", "NVM Express Base Specification Rev 2.1, Figure 95"),
    (0, 0x22): ("Transient Transport Error", "NVM Express Base Specification Rev 2.1, Figure 95"),

    # Command Specific Status (SCT=1)
    (1, 0x00): ("Completion Queue Invalid", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x01): ("Invalid Queue Identifier", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x02): ("Invalid Queue Size", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x03): ("Abort Command Limit Exceeded", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x05): ("Asynchronous Event Request Limit Exceeded", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x06): ("Invalid Firmware Slot", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x07): ("Invalid Firmware Image", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x08): ("Invalid Interrupt Vector", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x09): ("Invalid Log Page", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x0A): ("Invalid Format", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x0B): ("Firmware Activation Requires Reset", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x0C): ("Invalid Queue Deletion", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x0D): ("Feature Identifier Not Saveable", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x0E): ("Feature Not Changeable", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x0F): ("Feature Not Namespace Specific", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x10): ("Firmware Activation Prohibited", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x11): ("Overlapping Range", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x12): ("Namespace Insufficient Capacity", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x13): ("Namespace Identifier Unavailable", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x15): ("Namespace Already Attached", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x16): ("Namespace Is Private", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x17): ("Namespace Not Attached", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x18): ("Thin Provisioning Not Supported", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x19): ("Controller List Invalid", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x1D): ("Device Self-test In Progress", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x1E): ("Boot Partition Write Prohibited", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x1F): ("Invalid Controller Identifier", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x20): ("Invalid Secondary Controller State", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x21): ("Invalid Number of Controller Resources", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x22): ("Invalid Resource Identifier", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x23): ("Sanitize Prohibited While Persistent Memory Region is Enabled",
                "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x24): ("ANA Group Identifier Invalid", "NVM Express Base Specification Rev 2.1, Figure 96"),
    (1, 0x25): ("ANA Attach Failed", "NVM Express Base Specification Rev 2.1, Figure 96"),

    # NVMe-oF Fabric Status (SCT=0, but fabric-specific codes)
    (0, 0x80): ("Incompatible Format", "NVM Express over Fabrics Specification Rev 1.1a, Figure 18"),
    (0, 0x81): ("Controller Busy", "NVM Express over Fabrics Specification Rev 1.1a, Figure 18"),
    (0, 0x82): ("Invalid Parameter", "NVM Express over Fabrics Specification Rev 1.1a, Figure 18"),
    (0, 0x83): ("Restart Discovery", "NVM Express over Fabrics Specification Rev 1.1a, Figure 18"),
    (0, 0x84): ("Invalid Host", "NVM Express over Fabrics Specification Rev 1.1a, Figure 18"),
}


def decode_status_code(status_word: int) -> Tuple[str, str, str]:
    """
    Decode NVMe status word into human-readable information.

    Args:
        status_word: Complete 16-bit status word from NVMe completion

    Returns:
        Tuple of (description, spec_reference, formatted_status)

    Reference: NVM Express Base Specification Rev 2.1, Figure 94 "Status Field"
    Status field format:
    - Bits 15:9: Reserved
    - Bit 8: Don't Retry (DNR)
    - Bit 7: More (M)
    - Bits 6:4: Status Code Type (SCT)
    - Bits 3:1: Status Code (SC)
    - Bit 0: Phase (P)
    """
    # Extract fields from status word
    status_code = (status_word >> 1) & 0x7F  # Bits 7:1 = Status Code
    sct = (status_word >> 9) & 0x7           # Bits 11:9 = Status Code Type
    dnr = (status_word >> 15) & 0x1          # Bit 15 = Don't Retry
    more = (status_word >> 14) & 0x1         # Bit 14 = More

    # Look up description
    key = (sct, status_code)
    if key in NVME_STATUS_DESCRIPTIONS:
        description, spec_ref = NVME_STATUS_DESCRIPTIONS[key]
    else:
        description = f"Unknown Status (SCT={sct}, SC={status_code:02x})"
        spec_ref = "Status code not documented"

    # Format complete status information
    formatted_status = f"0x{status_code:02x} ({description})"
    if dnr:
        formatted_status += " [DNR]"
    if more:
        formatted_status += " [More]"

    return description, spec_ref, formatted_status


def format_status_error(status_code: int, command_id: int = None) -> str:
    """
    Format a complete error message for an NVMe status code.

    Args:
        status_code: 8-bit status code (extracted from status word bits 7:1)
        command_id: Optional command ID for context

    Returns:
        Formatted error message with description and hex value
    """
    # The status_code passed in is already the extracted SC field (bits 7:1 of status word)
    # For most cases, assume SCT=0 (Generic Command Status)
    sct = 0
    sc = status_code

    # Look up description
    key = (sct, sc)
    if key in NVME_STATUS_DESCRIPTIONS:
        description, spec_ref = NVME_STATUS_DESCRIPTIONS[key]
        formatted_status = f"0x{sc:02x} ({description})"
    else:
        description = f"Unknown Status (SCT={sct}, SC=0x{sc:02x})"
        formatted_status = f"0x{sc:02x} ({description})"

    if command_id is not None:
        return f"Command {command_id} failed with status {formatted_status}"
    else:
        return f"Command failed with status {formatted_status}"
