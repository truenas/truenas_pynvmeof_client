"""
Base parser class with common utilities for NVMe-oF data parsing.
"""

import struct
import logging

logger = logging.getLogger(__name__)


class BaseParser:
    """Base class for all NVMe-oF data parsers."""

    @staticmethod
    def safe_unpack(format_string: str, data: bytes, offset: int = 0) -> tuple:
        """
        Safely unpack binary data with error handling.

        Args:
            format_string: struct format string
            data: binary data to unpack
            offset: offset into data buffer

        Returns:
            Unpacked tuple

        Raises:
            ValueError: If unpacking fails
        """
        try:
            size = struct.calcsize(format_string)
            if len(data) < offset + size:
                raise ValueError(f"Insufficient data: need {offset + size} bytes, got {len(data)}")
            return struct.unpack(format_string, data[offset:offset + size])
        except struct.error as e:
            raise ValueError(f"Failed to unpack data: {e}")

    @staticmethod
    def extract_string(data: bytes, offset: int, length: int, encoding: str = 'ascii') -> str:
        """
        Extract and clean a string from binary data.

        Args:
            data: binary data
            offset: offset into data
            length: maximum string length
            encoding: string encoding

        Returns:
            Cleaned string
        """
        try:
            raw_bytes = data[offset:offset + length]
            # Remove null bytes and trailing whitespace
            return raw_bytes.rstrip(b'\x00 ').decode(encoding, errors='replace')
        except (UnicodeDecodeError, IndexError) as e:
            logger.warning(f"Failed to extract string at offset {offset}: {e}")
            return ""

    @staticmethod
    def bytes_to_hex_string(data: bytes) -> str:
        """Convert bytes to hex string representation."""
        return data.hex() if data else ""

    @staticmethod
    def validate_data_length(data: bytes, expected_min_length: int, name: str = "data") -> None:
        """
        Validate that data meets minimum length requirements.

        Args:
            data: binary data to validate
            expected_min_length: minimum expected length
            name: descriptive name for error messages

        Raises:
            ValueError: If data is too short
        """
        if len(data) < expected_min_length:
            raise ValueError(f"{name} too short: got {len(data)} bytes, need at least {expected_min_length}")
