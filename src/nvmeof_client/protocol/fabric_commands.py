"""
NVMe-oF Fabric Commands

Command packing functions for NVMe-oF specific fabric commands.
"""

import struct
from .constants import NVME_COMMAND_SIZE, NVME_CONNECT_DATA_SIZE, NVME_CMD_FLAGS_SGL
from .types import NVMeOpcode, FabricCommandType


def pack_fabric_connect_command(command_id: int, queue_id: int = 0, queue_size: int = 31) -> bytes:
    """
    Pack Fabric Connect Command based on NVMe-oF specification.

    Args:
        command_id: Command identifier
        queue_id: Queue ID (0 for admin, 1+ for I/O)
        queue_size: Queue size (entries - 1, so 127 = 128 entries)

    Returns:
        64-byte Fabric Connect command

    Reference: NVMe over Fabrics Specification Rev 1.1, Section 3.3
    "Connect Command" and Figure 18
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode, flags, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.FABRICS, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: fctype + reserved fields
    struct.pack_into('<BBBB', cmd, 4, FabricCommandType.CONNECT, 0, 0, 0)

    # DW6-9: SGL1 (Scatter Gather List Entry 1) for connect data
    # SGL descriptor for connect data transfer
    struct.pack_into('<BBBBBBBB', cmd, 32, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)

    # DW10-11: Connect-specific fields
    # DW10: RECFMT (bits 15:0) + QID (bits 31:16)
    # DW11: SQSIZE (bits 15:0) + other fields
    dw10 = 0 | (queue_id << 16)  # RECFMT=0, QID in upper 16 bits
    dw11 = queue_size  # SQSIZE in lower 16 bits
    struct.pack_into('<L', cmd, 40, dw10)
    struct.pack_into('<L', cmd, 44, dw11)

    return bytes(cmd)


def pack_fabric_property_get_command(command_id: int, property_offset: int, property_size: int = 4) -> bytes:
    """
    Pack Fabric Property Get Command according to NVMe-oF specification.

    Args:
        command_id: Command identifier
        property_offset: NVMe property register offset (e.g., NVMeProperty.CAP)
        property_size: Size of property in bytes (4 or 8)

    Returns:
        64-byte Fabric Property Get command with proper field encoding

    Reference: NVMe over Fabrics Specification Rev 1.1, Section 3.6
    "Property Get Command" and Figure 25
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x7F, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.FABRICS, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: fctype = Property Get (0x04), reserved bytes
    struct.pack_into('<BBBB', cmd, 4, FabricCommandType.PROPERTY_GET, 0, 0, 0)

    # DW6-9: SGL1 - Property Get has no data transfer, so zero SGL descriptor
    struct.pack_into('<BBBBBBBB', cmd, 32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)

    # DW10 (byte 40): Attributes (ATTRIB) field
    # Bits 7:3: Reserved (0)
    # Bits 2:0: Property size encoding
    #   000b = 4 bytes
    #   001b = 8 bytes
    #   010b-111b = Reserved
    attrib = 0x00 if property_size == 4 else 0x01
    struct.pack_into('<B', cmd, 40, attrib)

    # DW11 (bytes 44-47): Property offset (OFST)
    struct.pack_into('<L', cmd, 44, property_offset)

    return bytes(cmd)


def pack_fabric_property_set_command(command_id: int, property_offset: int, value: int) -> bytes:
    """
    Pack Fabric Property Set Command based on NVMe-oF specification.

    Args:
        command_id: Command identifier
        property_offset: NVMe property register offset
        value: Value to write to property register

    Returns:
        64-byte Fabric Property Set command

    Reference: NVMe over Fabrics Specification Rev 1.1, Section 3.3
    "Property Set Command" and Figure 19
    """
    cmd = bytearray(NVME_COMMAND_SIZE)

    # DW0: opcode=0x7f, flags=SGL mode, command_id
    struct.pack_into('<BBH', cmd, 0, NVMeOpcode.FABRICS, NVME_CMD_FLAGS_SGL, command_id)

    # DW1: fctype = Property Set (0x00)
    struct.pack_into('<BBBB', cmd, 4, FabricCommandType.PROPERTY_SET, 0, 0, 0)

    # DW6-9: SGL1 - Property Set has no data, so zero SGL descriptor
    struct.pack_into('<BBBBBBBB', cmd, 32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)

    # DW11 (offset 44): Property offset (0x14 for CC register)
    struct.pack_into('<L', cmd, 44, property_offset)

    # DW12 (offset 48): Property value
    struct.pack_into('<L', cmd, 48, value & 0xFFFFFFFF)

    return bytes(cmd)


def pack_fabric_connect_data(host_nqn: str, subsys_nqn: str,
                             host_id: bytes = None, controller_id: int = 0xFFFF) -> bytes:
    """
    Pack Fabric Connect Data structure.

    Args:
        host_nqn: Host NVMe Qualified Name (up to 256 chars)
        subsys_nqn: Subsystem NVMe Qualified Name (up to 256 chars)
        host_id: 16-byte host identifier (UUID)
        controller_id: Controller ID (0xFFFF for admin, 0x0001 for I/O)

    Returns:
        1024-byte connect data structure

    Reference: NVMe over Fabrics Specification Rev 1.1, Section 3.3
    "Connect Data" and Figure 20
    """
    # Validate input parameters
    if len(host_nqn) > 256:
        raise ValueError(f"Host NQN too long: {len(host_nqn)} (max 256)")
    if len(subsys_nqn) > 256:
        raise ValueError(f"Subsystem NQN too long: {len(subsys_nqn)} (max 256)")

    # Generate default host ID if not provided
    if host_id is None:
        import uuid
        host_id = uuid.uuid4().bytes

    # Create 1024-byte connect data structure
    connect_data = bytearray(NVME_CONNECT_DATA_SIZE)

    # Host ID (16 bytes at offset 0)
    connect_data[0:16] = host_id

    # Controller ID (2 bytes at offset 16) - 0xFFFF for admin, 0x0001 for I/O
    struct.pack_into('<H', connect_data, 16, controller_id)

    # Reserved fields (18-255) are already zero

    # Subsystem NQN (256 bytes at offset 256) - bytes 256:511
    subsys_nqn_bytes = subsys_nqn.encode('utf-8')
    connect_data[256:256 + len(subsys_nqn_bytes)] = subsys_nqn_bytes

    # Host NQN (256 bytes at offset 512) - bytes 512:767
    host_nqn_bytes = host_nqn.encode('utf-8')
    connect_data[512:512 + len(host_nqn_bytes)] = host_nqn_bytes

    # Reserved field (768-1023) already zero

    return bytes(connect_data)
