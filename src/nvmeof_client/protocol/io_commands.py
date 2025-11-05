"""
NVMe I/O Commands

Command packing functions for NVMe I/O commands (Read, Write, etc.).
"""

import struct
# from .constants import *
from .constants import NVME_COMMAND_SIZE, NVME_CMD_FLAGS_SGL, NVME_SECTOR_SIZE

from .types import NVMeOpcode
from ..models import ReservationType, ReservationAction


def pack_nvme_read_command(command_id: int, nsid: int, start_lba: int, block_count: int,
                           logical_block_size: int = NVME_SECTOR_SIZE) -> bytes:
    """
    Pack NVMe Read Command for I/O operations.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        start_lba: Starting Logical Block Address
        block_count: Number of logical blocks to read (1-based, from API)
        logical_block_size: Logical block size in bytes

    Returns:
        64-byte NVMe Read command with SGL descriptor

    Reference: NVM Command Set Specification Section 4.2 "Read command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x02, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.READ, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer
    # Use same working SGL format as admin commands (0x5A format)
    data_length = block_count * logical_block_size  # block_count is 1-based from API

    # SGL descriptor format matching working admin commands:
    # Bytes 32-35: Length (4 bytes, little endian)
    # Bytes 36-38: Reserved (3 bytes)
    # Byte 39: Type(upper 4 bits) + Subtype(lower 4 bits) = 0x5A
    struct.pack_into('<L', cmd, 32, data_length)          # Length: data_length bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)   # Reserved
    struct.pack_into('<B', cmd, 39, 0x5A)                 # Type=5, Subtype=A

    # DW10-11: Starting LBA (64-bit)
    struct.pack_into('<Q', cmd, 40, start_lba)

    # DW12: Number of Logical Blocks (NLB) - 0-based value
    # Convert from 1-based API to 0-based NVMe field
    # Bits 15:0: NLB, Bits 31:16: Control fields
    nlb = block_count - 1  # Convert 1-based block_count to 0-based NLB
    struct.pack_into('<L', cmd, 48, nlb)

    return bytes(cmd)


def pack_nvme_write_command(command_id: int, nsid: int, start_lba: int, block_count: int,
                            logical_block_size: int = NVME_SECTOR_SIZE) -> bytes:
    """
    Pack NVMe Write Command for I/O operations.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        start_lba: Starting Logical Block Address
        block_count: Number of logical blocks to write (1-based, from API)
        logical_block_size: Logical block size in bytes

    Returns:
        64-byte NVMe Write command with SGL descriptor

    Reference: NVM Command Set Specification Section 4.4 "Write command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x01, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.WRITE, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer
    # Use Data Block with Offset format for WRITE operations (kernel uses 0x01)
    data_length = block_count * logical_block_size  # block_count is 1-based from API

    # SGL descriptor format for WRITE (different from read - kernel uses 0x01):
    # Bytes 32-35: Length (4 bytes, little endian)
    # Bytes 36-38: Reserved (3 bytes)
    # Byte 39: Type(upper 4 bits) + Subtype(lower 4 bits) = 0x01 (Data Block with Offset)
    struct.pack_into('<L', cmd, 32, data_length)          # Length: data_length bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)   # Reserved
    struct.pack_into('<B', cmd, 39, 0x01)                 # Type=0, Subtype=1 (Data Block with Offset)

    # DW10-11: Starting LBA (64-bit)
    struct.pack_into('<Q', cmd, 40, start_lba)

    # DW12: Number of Logical Blocks (NLB) - 0-based value
    # Convert from 1-based API to 0-based NVMe field
    # Bits 15:0: NLB, Bits 31:16: Control fields
    nlb = block_count - 1  # Convert 1-based block_count to 0-based NLB
    struct.pack_into('<L', cmd, 48, nlb)

    return bytes(cmd)


def pack_nvme_flush_command(command_id: int, nsid: int) -> bytes:
    """
    Pack NVMe Flush Command for forcing data to non-volatile media.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier

    Returns:
        64-byte NVMe Flush command

    Reference: NVM Command Set Specification Section 4.1 "Flush command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x00, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.FLUSH, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 (zero for commands without data transfer)
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # No additional fields needed for Flush command

    return bytes(cmd)


def pack_nvme_write_zeroes_command(command_id: int, nsid: int, start_lba: int, block_count: int) -> bytes:
    """
    Pack NVMe Write Zeroes Command.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        start_lba: Starting Logical Block Address
        block_count: Number of logical blocks to zero (0-based)

    Returns:
        64-byte NVMe Write Zeroes command

    Reference: NVM Command Set Specification Section 4.5 "Write Zeroes command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x08, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.WRITE_ZEROES, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 (zero for commands without data transfer)
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10-11: Starting LBA (64-bit)
    struct.pack_into('<Q', cmd, 40, start_lba)

    # DW12: Number of Logical Blocks (NLB) - 0-based value
    struct.pack_into('<L', cmd, 48, block_count)

    return bytes(cmd)


def pack_nvme_compare_command(command_id: int, nsid: int, start_lba: int, block_count: int,
                              logical_block_size: int = NVME_SECTOR_SIZE) -> bytes:
    """
    Pack NVMe Compare Command.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        start_lba: Starting Logical Block Address
        block_count: Number of logical blocks to compare (0-based)
        logical_block_size: Logical block size in bytes

    Returns:
        64-byte NVMe Compare command with SGL descriptor

    Reference: NVM Command Set Specification Section 4.3 "Compare command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x05, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.COMPARE, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer
    # Use same working SGL format as admin commands (0x5A format)
    data_length = (block_count + 1) * logical_block_size  # block_count is 0-based

    # SGL descriptor format matching working admin commands:
    # Bytes 32-35: Length (4 bytes, little endian)
    # Bytes 36-38: Reserved (3 bytes)
    # Byte 39: Type(upper 4 bits) + Subtype(lower 4 bits) = 0x5A
    struct.pack_into('<L', cmd, 32, data_length)           # Length: data_length bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)   # Reserved
    struct.pack_into('<B', cmd, 39, 0x5A)                 # Type=5, Subtype=A

    # DW10-11: Starting LBA (64-bit)
    struct.pack_into('<Q', cmd, 40, start_lba)

    # DW12: Number of Logical Blocks (NLB) - 0-based value
    struct.pack_into('<L', cmd, 48, block_count)

    return bytes(cmd)


def pack_nvme_write_uncorrectable_command(command_id: int, nsid: int, start_lba: int, block_count: int) -> bytes:
    """
    Pack NVMe Write Uncorrectable Command.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        start_lba: Starting Logical Block Address
        block_count: Number of logical blocks to mark as uncorrectable (0-based)

    Returns:
        64-byte NVMe Write Uncorrectable command

    Reference: NVM Command Set Specification Section 4.6 "Write Uncorrectable command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x04, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.WRITE_UNCORRECTABLE, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 (zero for commands without data transfer)
    struct.pack_into('<Q', cmd, 32, 0x0000000000000000)

    # DW10-11: Starting LBA (64-bit)
    struct.pack_into('<Q', cmd, 40, start_lba)

    # DW12: Number of Logical Blocks (NLB) - 0-based value
    struct.pack_into('<L', cmd, 48, block_count)

    return bytes(cmd)


def pack_nvme_reservation_register_command(command_id: int, nsid: int, reservation_action: int,
                                           iekey: bool = False, cptpl: int = 0) -> bytes:
    """
    Pack NVMe Reservation Register Command.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        reservation_action: Action to perform (0=Register, 1=Unregister, 2=Replace)
        iekey: Ignore Existing Key (IEKEY) bit
        cptpl: Change Persist Through Power Loss (CPTPL) field (0-2)

    Note:
        The reservation keys are sent separately in the 16-byte data payload,
        not as part of the command structure.

    Returns:
        64-byte NVMe Reservation Register command

    Reference: NVM Express Base Specification Rev 2.1, Figure 573 "Reservation Register command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x0D, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.RESERVATION_REGISTER, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer (16 bytes for reservation data)
    # SGL descriptor format for NVMe-oF TCP (8 bytes total):
    # Reference: NVM Express over Fabrics 1.1a, Section 4.2 "SGL Support"
    # Data-out operation (host to controller): use Data Block with Offset (0x01)
    struct.pack_into('<L', cmd, 32, 16)                  # Length: 16 bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)  # Reserved
    struct.pack_into('<B', cmd, 39, 0x01)                # Type=0, Subtype=1 (Data Block with Offset)

    # DW10: Build the complete DW10 field per Figure 573
    # Bits 30-31: CPTPL (Change Persist Through Power Loss)
    # Bits 5-29: Reserved (0)
    # Bit 4: DISNSRS (Disable Namespace Reporting Status) - not used, set to 0
    # Bit 3: IEKEY (Ignore Existing Key)
    # Bits 0-2: RREGA (Reservation Register Action)
    dw10 = (reservation_action & 0x7)  # Bits 0-2: RREGA
    if iekey:
        dw10 |= (1 << 3)  # Bit 3: IEKEY
    dw10 |= ((cptpl & 0x3) << 30)  # Bits 30-31: CPTPL

    struct.pack_into('<L', cmd, 40, dw10)

    return bytes(cmd)


def pack_nvme_reservation_report_command(command_id: int, nsid: int, data_length: int = 4096, eds: int = 1) -> bytes:
    """
    Pack NVMe Reservation Report Command.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        data_length: Number of bytes to retrieve (must be multiple of 16)
        eds: Extended Data Structure bit (0=standard format, 1=extended format)

    Returns:
        64-byte NVMe Reservation Report command

    Reference: NVM Command Set Specification 1.0c, Section 6.4 "Reservation Report command"
    Data Structure: Figure 295 "Reservation Report command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x0E, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.RESERVATION_REPORT, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer (controller to host)
    # SGL descriptor format for NVMe-oF TCP (8 bytes total):
    # Reference: NVM Express over Fabrics 1.1a, Section 4.2 "SGL Support"
    struct.pack_into('<L', cmd, 32, data_length)         # Length in bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)  # Reserved
    struct.pack_into('<B', cmd, 39, 0x5A)                # Type=5, Subtype=A

    # DW10: Number of Dwords (NUMD) - 0-based value
    # Reference: NVM Command Set Specification 1.0c, Figure 295
    numd = (data_length // 4) - 1
    struct.pack_into('<L', cmd, 40, numd)

    # DW11: Extended Data Structure (EDS) field
    # Reference: NVM Express Base Specification 2.1, Figure 580
    # EDS=1 requests extended data structure with 128-bit host identifiers
    # EDS=0 requests standard data structure with 64-bit host identifiers
    struct.pack_into('<L', cmd, 44, eds & 0x1)

    return bytes(cmd)


def pack_nvme_reservation_acquire_command(command_id: int, nsid: int, reservation_action: ReservationAction,
                                          reservation_type: ReservationType) -> bytes:
    """
    Pack NVMe Reservation Acquire Command.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        reservation_action: Action (0=Acquire, 1=Preempt, 2=Preempt and Abort)
        reservation_type: Reservation type (1-6: Write Exclusive, Exclusive Access, etc.)

    Note:
        The reservation keys are sent separately in the 16-byte data payload,
        not as part of the command structure.

    Returns:
        64-byte NVMe Reservation Acquire command

    Reference: NVM Express Base Specification Rev 2.1, Figure 569 "Reservation Acquire command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x11, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.RESERVATION_ACQUIRE, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer (16 bytes for reservation data)
    # SGL descriptor format for NVMe-oF TCP (8 bytes total):
    # Reference: NVM Express over Fabrics 1.1a, Section 4.2 "SGL Support"
    # Data-out operation (host to controller): use Data Block with Offset (0x01)
    struct.pack_into('<L', cmd, 32, 16)                  # Length: 16 bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)  # Reserved
    struct.pack_into('<B', cmd, 39, 0x01)                # Type=0, Subtype=1 (Data Block with Offset)

    # DW10: Reservation Action (RACQA) and Reservation Type (RTYPE)
    # Bits 2:0: Action, Bits 7:3: Reserved, Bits 15:8: Reservation Type
    # Reference: NVM Command Set Specification 1.0c, Figure 290
    dw10 = (reservation_action.value & 0x7) | ((reservation_type.value & 0xFF) << 8)
    struct.pack_into('<L', cmd, 40, dw10)

    return bytes(cmd)


def pack_nvme_reservation_release_command(command_id: int, nsid: int, reservation_action: ReservationAction,
                                          reservation_type: ReservationType) -> bytes:
    """
    Pack NVMe Reservation Release Command.

    Args:
        command_id: Command identifier
        nsid: Namespace identifier
        reservation_action: Release action (RELEASE or CLEAR)
        reservation_type: Reservation type (must match acquired type)

    Note:
        The reservation key is sent separately in the 16-byte data payload,
        not as part of the command structure.

    Returns:
        64-byte NVMe Reservation Release command

    Reference: NVM Express Base Specification Rev 2.1, Figure 576 "Reservation Release command"
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x15, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.RESERVATION_RELEASE, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: namespace ID
    struct.pack_into('<L', cmd, 4, nsid)

    # DW6-9: SGL Entry 1 for data transfer (8 bytes for reservation data)
    # SGL descriptor format for NVMe-oF TCP (8 bytes total):
    # Reference: NVM Express over Fabrics 1.1a, Section 4.2 "SGL Support"
    # Data-out operation (host to controller): use Data Block with Offset (0x01)
    struct.pack_into('<L', cmd, 32, 8)                   # Length: 8 bytes
    struct.pack_into('<BBB', cmd, 36, 0x00, 0x00, 0x00)  # Reserved
    struct.pack_into('<B', cmd, 39, 0x01)                # Type=0, Subtype=1 (Data Block with Offset)

    # DW10: Reservation Action (RRELA) and Reservation Type (RTYPE)
    # Bits 2:0: Action, Bits 7:3: Reserved, Bits 15:8: Reservation Type
    # Reference: NVM Command Set Specification 1.0c, Figure 293
    dw10 = (reservation_action.value & 0x7) | ((reservation_type.value & 0xFF) << 8)
    struct.pack_into('<L', cmd, 40, dw10)

    return bytes(cmd)
