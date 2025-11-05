"""
NVMe-oF TCP PDU Functions

PDU header packing and unpacking functions for NVMe-oF TCP transport.
"""

import struct
from .constants import NVME_TCP_PFV_1_0, NVMEOF_TCP_ICREQ_TOTAL_LEN, NVMEOF_TCP_ICREQ_HEADER_LEN
from .types import PDUType, PDUHeader


def pack_pdu_header(pdu_type: PDUType, flags: int, hlen: int, pdo: int, plen: int) -> bytes:
    """
    Pack NVMe-oF TCP PDU Header according to specification.

    Args:
        pdu_type: PDU type (from PDUType enum)
        flags: PDU flags
        hlen: Header length
        pdo: PDU Data Offset
        plen: PDU length

    Returns:
        8-byte PDU header

    Reference: NVMe-oF TCP Transport Specification Section 3.3.1
    """
    return struct.pack('<BBBBBBBB',
                       pdu_type,           # PDU Type
                       flags,              # Flags
                       hlen,               # Header Length
                       pdo,                # PDU Data Offset
                       plen & 0xFF,        # PDU Length (bytes 0-2)
                       (plen >> 8) & 0xFF,
                       (plen >> 16) & 0xFF,
                       0)                  # Reserved


def unpack_pdu_header(data: bytes) -> PDUHeader:
    """
    Unpack NVMe-oF TCP PDU Header from bytes.

    Args:
        data: 8-byte PDU header data

    Returns:
        PDUHeader named tuple with parsed fields

    Raises:
        ValueError: If data is not exactly 8 bytes

    Reference: NVMe-oF TCP Transport Specification Section 3.3.1
    """
    if len(data) != 8:
        raise ValueError(f"PDU header must be exactly 8 bytes, got {len(data)}")

    pdu_type, flags, hlen, pdo, plen0, plen1, plen2, reserved = struct.unpack('<BBBBBBBB', data)

    # Reconstruct 24-bit PDU length
    plen = plen0 | (plen1 << 8) | (plen2 << 16)

    return PDUHeader(
        pdu_type=pdu_type,
        flags=flags,
        hlen=hlen,
        pdo=pdo,
        plen=plen
    )


def pack_icreq_pdu(pfv: int = NVME_TCP_PFV_1_0, hpda: int = 0, digest: int = 0, maxdata: int = 0x400000) -> bytes:
    """
    Pack Initialize Connection Request (ICREQ) PDU.

    Args:
        pfv: Protocol Format Version (default: 1.0)
        hpda: Host PDU Data Alignment
        digest: Digest types supported
        maxdata: Maximum data transfer size

    Returns:
        128-byte ICREQ PDU

    Reference: NVMe-oF TCP Transport Specification Section 3.4.1
    """
    # ICREQ has extended header structure
    icreq_data = bytearray(NVMEOF_TCP_ICREQ_TOTAL_LEN)

    # Basic PDU header (8 bytes)
    icreq_data[0] = PDUType.ICREQ
    icreq_data[1] = 0  # Flags
    icreq_data[2] = NVMEOF_TCP_ICREQ_HEADER_LEN  # Header length
    icreq_data[3] = 0  # PDU Data Offset
    icreq_data[4] = NVMEOF_TCP_ICREQ_TOTAL_LEN  # PDU length (low byte)
    icreq_data[5] = 0  # PDU length (mid byte)
    icreq_data[6] = 0  # PDU length (high byte)
    icreq_data[7] = 0  # Reserved

    # ICREQ extended header fields (bytes 8-127)
    struct.pack_into('<H', icreq_data, 8, pfv)       # Protocol Format Version
    struct.pack_into('<B', icreq_data, 10, hpda)     # Host PDU Data Alignment
    struct.pack_into('<B', icreq_data, 11, digest)   # Digest types
    struct.pack_into('<L', icreq_data, 12, maxdata)  # Maximum data transfer size

    # Remaining bytes (16-127) are reserved and remain zero

    return bytes(icreq_data)


def unpack_icresp_pdu(data: bytes) -> dict:
    """
    Unpack Initialize Connection Response (ICRESP) PDU.

    Args:
        data: ICRESP PDU data (minimum 128 bytes)

    Returns:
        Dictionary with ICRESP fields

    Reference: NVMe-oF TCP Transport Specification Section 3.4.2
    """
    if len(data) < 16:
        raise ValueError(f"ICRESP data too short: {len(data)} (minimum 16)")

    # Parse basic PDU header first
    header = unpack_pdu_header(data[:8])

    if header.pdu_type != PDUType.ICRESP:
        raise ValueError(f"Expected ICRESP PDU type {PDUType.ICRESP}, got {header.pdu_type}")

    # Parse ICRESP extended header fields
    pfv = struct.unpack('<H', data[8:10])[0] if len(data) >= 10 else 0
    cpda = data[10] if len(data) >= 11 else 0
    digest = data[11] if len(data) >= 12 else 0
    maxdata = struct.unpack('<L', data[12:16])[0] if len(data) >= 16 else 0

    return {
        'header': header,
        'protocol_format_version': pfv,
        'controller_pdu_data_alignment': cpda,
        'digest_types': digest,
        'max_data_transfer_size': maxdata
    }
