"""
NVMe-oF protocol-level parsing.

This module handles parsing of NVMe-oF protocol data units (PDUs)
and basic protocol structures.
"""

from typing import Dict, Any
from .base import BaseParser


class ProtocolParser(BaseParser):
    """Parser for NVMe-oF protocol structures."""

    @classmethod
    def parse_pdu_header(cls, header_data: bytes) -> Dict[str, Any]:
        """
        Parse basic NVMe-oF TCP PDU header.

        Args:
            header_data: 8-byte PDU header data

        Returns:
            Dictionary with parsed PDU header fields

        Reference: NVMe-oF TCP Transport Specification Section 3.3
        """
        cls.validate_data_length(header_data, 8, "PDU header")

        # Parse basic PDU header (8 bytes)
        # Byte 0: PDU Type
        # Byte 1: Flags
        # Bytes 2-3: Header Length (HLEN)
        # Bytes 4-7: PDU Length (PLEN)
        pdu_type, flags, hlen, plen = cls.safe_unpack('<BBHI', header_data, 0)

        return {
            'pdu_type': pdu_type,
            'flags': flags,
            'hlen': hlen,
            'plen': plen
        }

    @classmethod
    def parse_connect_response(cls, data: bytes) -> Dict[str, Any]:
        """
        Parse NVMe-oF Connect command response data.

        Args:
            data: Connect response data

        Returns:
            Dictionary with parsed connect response fields

        Reference: NVMe-oF Base Specification Section 3.1.4
        """
        if len(data) < 16:
            return {}

        # Parse the response completion data
        # This typically contains controller-specific information
        dw0, dw1, dw2, dw3 = cls.safe_unpack('<LLLL', data, 0)

        return {
            'controller_id': dw0 & 0xFFFF,  # Controller ID typically in lower 16 bits
            'dw0': dw0,
            'dw1': dw1,
            'dw2': dw2,
            'dw3': dw3
        }
