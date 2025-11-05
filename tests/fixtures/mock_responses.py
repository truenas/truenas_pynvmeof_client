"""
Mock NVMe-oF PDU responses for testing

Contains pre-crafted PDU responses that can be used in unit tests
to simulate target responses without requiring a live target.
"""

import struct
from nvmeof_client.protocol import PDUType


def create_icresp_pdu():
    """Create a mock ICRESP (Initialize Connection Response) PDU."""
    # ICRESP header: type=1, flags=0, hlen=128, pdo=0, plen=128
    header = struct.pack('<BBBBI', PDUType.ICRESP, 0, 128, 0, 128)
    # ICRESP data: 120 bytes of zeros (typical real response)
    data = b'\x00' * 120
    return header + data


def create_command_response_pdu(command_id: int, status: int = 0):
    """Create a mock command response PDU."""
    # Response header: type=5 (RSP), flags=0, hlen=8, pdo=8, plen=24
    header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
    # Response data: command_id, status, and other fields
    response_data = struct.pack('<HHIII', command_id, status, 0, 0, 0)
    return header + response_data


def create_data_pdu(data: bytes):
    """Create a mock C2H_DATA PDU containing the given data."""
    data_len = len(data)
    # Data header: type=7 (C2H_DATA), flags=0, hlen=8, pdo=8, plen=8+data_len
    header = struct.pack('<BBBBI', PDUType.C2H_DATA, 0, 8, 8, 8 + data_len)
    return header + data


def create_property_get_response(property_value: int):
    """Create a mock Property Get response PDU."""
    # Property Get response is a standard command response with the value in specific fields
    header = struct.pack('<BBBBI', PDUType.RSP, 0, 8, 8, 24)
    # Pack the 64-bit property value in the response
    response_data = struct.pack('<HHQQ', 1, 0, property_value, 0)  # cmd_id=1, status=0, value
    return header + response_data


def create_reservation_report_data(generation: int = 1, reservation_type: int = 1,
                                   reservation_holder: int = 0, registered_controllers=None,
                                   extended_format: bool = True):
    """Create mock reservation report data."""
    if registered_controllers is None:
        registered_controllers = []

    # Create a proper 24-byte header according to NVMe spec Figure 582
    header = bytearray(24)

    # Bytes 0-3: Generation counter (32-bit LE)
    struct.pack_into('<L', header, 0, generation)

    # Byte 4: Reservation Type (RTYPE)
    header[4] = reservation_type

    # Bytes 5-6: Number of registered controllers (16-bit LE)
    struct.pack_into('<H', header, 5, len(registered_controllers))

    # Bytes 7-8: Reserved
    # Byte 9: Persist Through Power Loss State (PTPLS)
    header[9] = 0
    # Bytes 10-23: Reserved

    # If there's a reservation holder, ensure they're in the registered controllers list
    if reservation_holder > 0:
        # Create an entry for the reservation holder if not already present
        holder_found = any(controller_id == reservation_holder for controller_id, _ in registered_controllers)
        if not holder_found:
            registered_controllers = [(reservation_holder, 0x123456789ABCDEF0)] + list(registered_controllers)

    # Add registered controller entries based on format
    controller_entries = b''
    entry_size = 64 if extended_format else 24

    for i, (controller_id, key) in enumerate(registered_controllers):
        entry = bytearray(entry_size)

        # Bytes 0-1: Controller ID
        struct.pack_into('<H', entry, 0, controller_id)

        # Byte 2: Reservation Status (set bit 0 if this controller holds reservation)
        rcsts = 1 if controller_id == reservation_holder else 0
        struct.pack_into('<B', entry, 2, rcsts)

        if extended_format:
            # Extended format (Figure 584): 64 bytes per entry
            # Bytes 8-15: Reservation Key (64-bit)
            struct.pack_into('<Q', entry, 8, key)
            # Bytes 16-31: Host Identifier (128-bit)
            # For testing, use key as lower 64 bits and key+1 as upper 64 bits
            host_id_low = key
            host_id_high = key + 1
            struct.pack_into('<Q', entry, 16, host_id_low)   # Lower 64 bits
            struct.pack_into('<Q', entry, 24, host_id_high)  # Upper 64 bits
        else:
            # Standard format (Figure 583): 24 bytes per entry
            # Bytes 8-15: Host Identifier (64-bit)
            struct.pack_into('<Q', entry, 8, key)  # Use key as host ID
            # Bytes 16-23: Reservation Key (64-bit)
            struct.pack_into('<Q', entry, 16, key)

        controller_entries += bytes(entry)

    # For extended format, add 40 reserved bytes between header and registrant data
    if extended_format:
        reserved_bytes = b'\x00' * 40
        return bytes(header) + reserved_bytes + controller_entries
    else:
        return bytes(header) + controller_entries


def create_identify_controller_data():
    """Create mock Identify Controller response data."""
    # Minimal controller identify data (4096 bytes)
    data = bytearray(4096)

    # Vendor ID (bytes 0-1)
    struct.pack_into('<H', data, 0, 0x1234)
    # Subsystem Vendor ID (bytes 2-3)
    struct.pack_into('<H', data, 2, 0x5678)
    # Serial Number (bytes 4-23)
    serial = b'TEST_SERIAL_123456\x00\x00'
    data[4:24] = serial
    # Model Number (bytes 24-63)
    model = b'Test NVMe Controller\x00' + b'\x00' * 19
    data[24:64] = model
    # Firmware Revision (bytes 64-71)
    firmware = b'1.0.0\x00\x00\x00'
    data[64:72] = firmware

    return bytes(data)


def create_identify_namespace_data(nsid: int = 1, size: int = 1024):
    """Create mock Identify Namespace response data."""
    # Minimal namespace identify data (4096 bytes)
    data = bytearray(4096)

    # Namespace Size (bytes 0-7) - in logical blocks
    struct.pack_into('<Q', data, 0, size)
    # Namespace Capacity (bytes 8-15)
    struct.pack_into('<Q', data, 8, size)
    # Namespace Utilization (bytes 16-23)
    struct.pack_into('<Q', data, 16, size // 2)

    # LBA Format 0 (bytes 128-131): LBA Data Size = 512 bytes (2^9)
    struct.pack_into('<L', data, 128, 9)  # LBADS = 9 (512 bytes)

    return bytes(data)
