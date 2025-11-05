"""
NVMe-oF Protocol Utilities

Utility functions for parsing and formatting NVMe-oF data structures.
"""

import struct
from typing import Dict, Any


def pack_nvme_command(opcode: int, flags: int, command_id: int, nsid: int = 0) -> bytes:
    """
    Pack basic NVMe command structure.

    Args:
        opcode: NVMe command opcode
        flags: Command flags
        command_id: Unique command identifier
        nsid: Namespace identifier

    Returns:
        Packed command as 64-byte structure

    Reference: NVMe Base Specification Section 4.1
    """
    # Basic command structure - would be extended for specific commands
    cmd = struct.pack('<BBHI', opcode, flags, command_id, nsid)
    # Pad to 64 bytes (full command size)
    cmd += b'\x00' * (64 - len(cmd))
    return cmd


def parse_controller_capabilities(cap_data: bytes) -> Dict[str, Any]:
    """
    Parse Controller Capabilities Register (CAP) data.

    Args:
        cap_data: 8-byte CAP register data

    Returns:
        Dictionary with parsed capability fields

    Reference: NVM Express Base Specification Section 3.1.1
    """
    if len(cap_data) < 8:
        raise ValueError("CAP data must be at least 8 bytes")

    cap_value = struct.unpack('<Q', cap_data)[0]

    # Parse bit fields according to NVMe specification
    mqes = cap_value & 0xFFFF  # Maximum Queue Entries Supported
    cqr = bool((cap_value >> 16) & 0x1)  # Contiguous Queues Required
    ams = (cap_value >> 17) & 0x3  # Arbitration Mechanism Supported
    to = (cap_value >> 24) & 0xFF  # Timeout
    dstrd = (cap_value >> 32) & 0xF  # Doorbell Stride
    nssrs = bool((cap_value >> 36) & 0x1)  # NVM Subsystem Reset Supported
    css = (cap_value >> 37) & 0xFF  # Command Sets Supported
    bps = bool((cap_value >> 45) & 0x1)  # Boot Partition Support
    mpsmin = (cap_value >> 48) & 0xF  # Memory Page Size Minimum
    mpsmax = (cap_value >> 52) & 0xF  # Memory Page Size Maximum

    return {
        'max_queue_entries_supported': mqes + 1,  # MQES is 0-based
        'contiguous_queues_required': cqr,
        'arbitration_mechanism_supported': ams,
        'timeout': to * 500,  # Convert to milliseconds
        'doorbell_stride': 4 << dstrd,  # Stride in bytes
        'nvm_subsystem_reset_supported': nssrs,
        'command_sets_supported': css,
        'boot_partition_support': bps,
        'memory_page_size_minimum': 4096 << mpsmin,  # Size in bytes
        'memory_page_size_maximum': 4096 << mpsmax   # Size in bytes
    }


def parse_discovery_log_page(data: bytes) -> Dict[str, Any]:
    """
    Parse Discovery Log Page into discovery log data with generation counter.

    Args:
        data: Discovery log page data

    Returns:
        Dictionary containing:
        - 'generation': Generation counter (for cache validation)
        - 'num_records': Number of records
        - 'entries': List of discovery entry dictionaries

    Reference: NVMe-oF Base Specification Section 5.4
    """
    if len(data) < 16:
        raise ValueError("Discovery log data too short")

    # Parse header (16 bytes)
    generation_counter = struct.unpack('<Q', data[0:8])[0]
    num_records = struct.unpack('<Q', data[8:16])[0]

    entries = []
    entry_size = 1024  # Each discovery entry is 1024 bytes

    for i in range(num_records):
        entry_offset = 1024 + (i * entry_size)  # Entries start at offset 1024

        if entry_offset + entry_size > len(data):
            break  # Not enough data for complete entry

        entry_data = data[entry_offset:entry_offset + entry_size]
        entry = _parse_single_discovery_entry(entry_data)
        entries.append(entry)

    return {
        'generation': generation_counter,
        'num_records': num_records,
        'entries': entries
    }


def _parse_single_discovery_entry(data: bytes) -> Dict[str, Any]:
    """
    Parse a single discovery log entry.

    Reference: NVMe-oF Base Specification Rev 1.1c, Section 5.4.1.2
    Discovery Log Page Entry format (1024 bytes)
    """
    # TRTYPE (Transport Type) - byte 0
    transport_type = data[0]

    # ADRFAM (Address Family) - byte 1
    address_family = data[1]

    # SUBTYPE (Subsystem Type) - byte 2
    subsystem_type = data[2]

    # PORTID (Port ID) - bytes 4-5
    port_id = struct.unpack('<H', data[4:6])[0]

    # CNTLID (Controller ID) - bytes 6-7
    controller_id = struct.unpack('<H', data[6:8])[0]

    # TRSVCID (Transport Service ID) - bytes 32-63 (32 bytes, null-terminated)
    # Per NVMe-oF spec: Contains port number for TCP transport
    transport_service_id = data[32:64].rstrip(b'\x00').decode('utf-8', errors='replace')

    # SUBNQN (Subsystem NQN) - bytes 256-511 (256 bytes, null-terminated)
    # Per NVMe-oF spec: Contains the NVMe Qualified Name of the subsystem
    subsystem_nqn = data[256:512].rstrip(b'\x00').decode('utf-8', errors='replace')

    # TRADDR (Transport Address) - bytes 512-767 (256 bytes, null-terminated)
    # Per NVMe-oF spec: Contains IP address for TCP transport
    transport_address = data[512:768].rstrip(b'\x00').decode('utf-8', errors='replace')

    return {
        'transport_type': transport_type,
        'address_family': address_family,
        'subsystem_type': subsystem_type,
        'port_id': port_id,
        'controller_id': controller_id,
        'transport_address': transport_address,
        'transport_service_id': transport_service_id,
        'subsystem_nqn': subsystem_nqn
    }


def format_discovery_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format discovery entry with human-readable field names.

    Args:
        entry: Raw discovery entry dictionary

    Returns:
        Formatted discovery entry dictionary
    """
    # Transport type names
    transport_names = {
        1: 'RDMA',
        2: 'Fibre Channel',
        3: 'TCP',
        254: 'Loop'
    }

    # Address family names
    family_names = {
        1: 'IPv4',
        2: 'IPv6',
        3: 'InfiniBand',
        4: 'Fibre Channel',
        254: 'Loop'
    }

    # Subsystem type names
    subtype_names = {
        1: 'Discovery',
        2: 'NVMe',
        3: 'Current Discovery'
    }

    return {
        'transport_type': transport_names.get(entry['transport_type'], f"Unknown({entry['transport_type']})"),
        'address_family': family_names.get(entry['address_family'], f"Unknown({entry['address_family']})"),
        'subsystem_type': subtype_names.get(entry['subsystem_type'], f"Unknown({entry['subsystem_type']})"),
        'port_id': entry['port_id'],
        'controller_id': entry['controller_id'],
        'transport_address': entry['transport_address'],
        'transport_service_id': entry['transport_service_id'],
        'subsystem_nqn': entry['subsystem_nqn'],
        'raw_transport_type': entry['transport_type'],
        'raw_address_family': entry['address_family'],
        'raw_subsystem_type': entry['subsystem_type']
    }
