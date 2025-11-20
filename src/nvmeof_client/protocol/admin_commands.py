"""
NVMe Admin Commands

Command packing functions for NVMe admin commands.
"""

import struct
from .constants import (
    NVME_CMD_FLAGS_SGL,
    NVME_COMMAND_SIZE,
)
from .types import NVMeOpcode


def pack_identify_command(command_id: int, cns: int, nsid: int = 0) -> bytes:
    """
    Pack Identify Command according to NVMe specification.

    Args:
        command_id: Command identifier
        cns: Controller or Namespace Structure selector
        nsid: Namespace identifier (0 for controller)

    Returns:
        64-byte Identify command with proper SGL descriptor

    Reference: NVM Express Base Specification Section 5.15 "Identify command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x06, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.IDENTIFY, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer (4096 bytes for Identify data)
    # SGL descriptor format for NVMe-oF TCP (8 bytes total):
    # Bytes 32-35: Length (4096 bytes = 0x1000)
    # Bytes 36-38: Reserved (3 bytes)
    # Byte 39: Type(upper 4 bits) + Subtype(lower 4 bits) = 0x5A
    struct.pack_into('<L', cmd, 32, 4096)      # Length: 4096 bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)  # Reserved
    struct.pack_into('<B', cmd, 39, 0x5A)      # Type=5, Subtype=A

    # DW10: CNS (Controller or Namespace Structure)
    struct.pack_into('<L', cmd, 40, cns)

    return bytes(cmd)


def pack_get_log_page_command(command_id: int, log_page_id: int, data_length: int, nsid: int = 0) -> bytes:
    """
    Pack Get Log Page Command according to NVMe specification.

    Args:
        command_id: Command identifier
        log_page_id: Log Page Identifier
        data_length: Number of bytes to retrieve
        nsid: Namespace identifier (0 for controller logs)

    Returns:
        64-byte Get Log Page command

    Reference: NVM Express Base Specification Section 5.14 "Get Log Page command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x02, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.GET_LOG_PAGE, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer
    # SGL descriptor format for NVMe-oF TCP (8 bytes total):
    # Bytes 32-35: Length (data_length bytes)
    # Bytes 36-38: Reserved (3 bytes)
    # Byte 39: Type(upper 4 bits) + Subtype(lower 4 bits) = 0x5A
    struct.pack_into('<L', cmd, 32, data_length)           # Length: data_length bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)   # Reserved
    struct.pack_into('<B', cmd, 39, 0x5A)                 # Type=5, Subtype=A

    # DW10: Log Page Identifier (LID) and length
    # Bits 7:0: LID, Bits 31:16: NUMDL (Number of Dwords Lower)
    numdl = (data_length // 4) - 1  # Convert bytes to dwords, 0-based
    dw10 = log_page_id | ((numdl & 0xFFFF) << 16)
    struct.pack_into('<L', cmd, 40, dw10)

    # DW11: NUMDU (Number of Dwords Upper) and other fields
    numdu = (numdl >> 16) & 0xFFFF
    struct.pack_into('<L', cmd, 44, numdu)

    return bytes(cmd)


def pack_set_features_command(command_id: int, feature_id: int, value: int, nsid: int = 0,
                              save: bool = False) -> bytes:
    """
    Pack Set Features Command according to NVMe specification.

    Args:
        command_id: Command identifier
        feature_id: Feature identifier (FID)
        value: Feature-specific value
        nsid: Namespace identifier (0 for controller features)
        save: If True, controller saves the feature setting across power cycles and resets (SV bit)

    Returns:
        64-byte Set Features command

    Reference: NVM Express Base Specification 2.3, Section 5.27 "Set Features command"
               Figure 401: Set Features – Command Dword 10
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.SET_FEATURES, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW8-9: SGL Entry 1 (zero for non-data commands)
    # For non-data commands, SGL descriptor should be zero
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10: Feature Identifier and Save bit
    # Reference: Figure 401 - Bits 7:0 = FID, Bits 30:8 = Reserved, Bit 31 = SV
    dw10 = (feature_id & 0xFF) | ((1 if save else 0) << 31)
    struct.pack_into('<L', cmd, 40, dw10)

    # DW11: Feature-specific value
    struct.pack_into('<L', cmd, 44, value)

    return bytes(cmd)


def pack_delete_io_completion_queue_command(command_id: int, queue_id: int) -> bytes:
    """
    Pack Delete I/O Completion Queue Command.

    Args:
        command_id: Command identifier
        queue_id: Queue identifier (1-based for I/O queues)

    Returns:
        64-byte Delete I/O Completion Queue command

    Reference: NVM Express Base Specification Section 5.5
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.DELETE_IO_CQ, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: Reserved (no namespace for admin commands)

    # DW8-9: SGL Entry 1 (zero for admin commands without data)
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10: Queue Identifier
    # Bits 15:0: QID, Bits 31:16: Reserved
    struct.pack_into('<L', cmd, 40, queue_id)

    return bytes(cmd)


def pack_delete_io_submission_queue_command(command_id: int, queue_id: int) -> bytes:
    """
    Pack Delete I/O Submission Queue Command.

    Args:
        command_id: Command identifier
        queue_id: Queue identifier (1-based for I/O queues)

    Returns:
        64-byte Delete I/O Submission Queue command

    Reference: NVM Express Base Specification Section 5.6
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.DELETE_IO_SQ, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: Reserved (no namespace for admin commands)

    # DW8-9: SGL Entry 1 (zero for admin commands without data)
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10: Queue Identifier
    # Bits 15:0: QID, Bits 31:16: Reserved
    struct.pack_into('<L', cmd, 40, queue_id)

    return bytes(cmd)


def pack_keep_alive_command(command_id: int) -> bytes:
    """
    Pack Keep Alive Command for NVMe-oF connection maintenance.

    Args:
        command_id: Command identifier

    Returns:
        64-byte Keep Alive command

    Reference: NVM Express Base Specification Section 5.25 "Keep Alive command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.KEEP_ALIVE, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: Reserved (no namespace for Keep Alive command)

    # DW8-9: SGL Entry 1 (zero for non-data commands)
    # Keep Alive is a non-data command, so SGL descriptor should be zero
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10-15: Reserved for Keep Alive command
    # No additional fields needed for Keep Alive

    return bytes(cmd)


def pack_create_io_completion_queue_command(command_id: int, queue_id: int, queue_size: int,
                                            physically_contiguous: bool = True) -> bytes:
    """
    Pack Create I/O Completion Queue Command.

    Args:
        command_id: Command identifier
        queue_id: Queue identifier (1-based for I/O queues)
        queue_size: Queue size in entries (0-based, so 127 = 128 entries)
        physically_contiguous: Whether queue is physically contiguous

    Returns:
        64-byte Create I/O Completion Queue command

    Reference: NVM Express Base Specification Section 5.3
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0 (CDW0): Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.CREATE_IO_CQ, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: Reserved (no namespace for admin commands)

    # DW8-9: SGL Entry 1 (zero for admin commands without data)
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10: Queue Identifier and Queue Size
    # Bits 15:0: QID, Bits 31:16: QSIZE (0-based)
    dw10 = queue_id | (queue_size << 16)
    struct.pack_into('<L', cmd, 40, dw10)

    # DW11: Queue attributes
    # Bit 0: PC (Physically Contiguous), Bit 1: IEN (Interrupts Enabled)
    # For NVMe-oF, queues are typically not physically contiguous
    pc_bit = 1 if physically_contiguous else 0
    ien_bit = 1  # Enable interrupts
    dw11 = pc_bit | (ien_bit << 1)
    struct.pack_into('<L', cmd, 44, dw11)

    return bytes(cmd)


def pack_get_features_command(command_id: int, feature_id: int, nsid: int = 0) -> bytes:
    """
    Pack Get Features Command according to NVMe specification.

    Args:
        command_id: Command identifier
        feature_id: Feature identifier (FID)
        nsid: Namespace identifier (0 for controller features)

    Returns:
        64-byte Get Features command

    Reference: NVM Express Base Specification Section 5.17 "Get Features command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.GET_FEATURES, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW8-9: SGL Entry 1 (zero for non-data commands)
    # For non-data commands, SGL descriptor should be zero
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10: Feature ID (FID)
    struct.pack_into('<L', cmd, 40, feature_id)

    return bytes(cmd)


def pack_create_io_submission_queue_command(command_id: int, queue_id: int, cq_id: int,
                                            queue_size: int, physically_contiguous: bool = True) -> bytes:
    """
    Pack Create I/O Submission Queue Command.

    Args:
        command_id: Command identifier
        queue_id: Queue identifier (1-based for I/O queues)
        cq_id: Associated completion queue identifier
        queue_size: Queue size in entries (0-based, so 127 = 128 entries)
        physically_contiguous: Whether queue is physically contiguous

    Returns:
        64-byte Create I/O Submission Queue command

    Reference: NVM Express Base Specification Section 5.4
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0 (CDW0): Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.CREATE_IO_SQ, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: Reserved (no namespace for admin commands)

    # DW8-9: SGL Entry 1 (zero for admin commands without data)
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10: Queue Identifier and Queue Size
    # Bits 15:0: QID, Bits 31:16: QSIZE (0-based)
    dw10 = queue_id | (queue_size << 16)
    struct.pack_into('<L', cmd, 40, dw10)

    # DW11: Queue attributes and completion queue ID
    # Bits 15:0: CQID, Bit 16: PC (Physically Contiguous), Bits 17:1: QPRIO (Queue Priority)
    pc_bit = 1 if physically_contiguous else 0
    qprio = 0  # Medium priority
    dw11 = cq_id | (pc_bit << 16) | (qprio << 17)
    struct.pack_into('<L', cmd, 44, dw11)

    return bytes(cmd)


def pack_async_event_request_command(command_id: int) -> bytes:
    """
    Pack Asynchronous Event Request Command according to NVMe specification.

    The Asynchronous Event Request command enables the reporting of asynchronous events
    from the controller. All command specific fields are reserved. The controller completes
    this command when there is an asynchronous event to report.

    Args:
        command_id: Command identifier

    Returns:
        64-byte Asynchronous Event Request command

    Reference: NVM Express Base Specification 2.3, Section 5.2.2 "Asynchronous Event Request command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: Command Dword 0 (use SGL mode for NVMe-oF TCP)
    # Bits 31:16: Command ID, Bits 15:14: PSDT (01b for SGL), Bits 13:8: Reserved, Bits 7:0: Opcode
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.ASYNC_EVENT_REQUEST, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: Reserved (no namespace for Asynchronous Event Request)

    # DW8-9: SGL Entry 1 (zero for non-data commands)
    # Asynchronous Event Request is a non-data command
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10-15: All reserved for Asynchronous Event Request command
    # Per spec: "All command specific fields are reserved"

    return bytes(cmd)
