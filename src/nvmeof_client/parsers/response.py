"""
NVMe response and completion parsing.

This module handles parsing of basic NVMe response structures and
completion queue entries.
"""

from typing import Dict, Any
from .base import BaseParser


class ResponseParser(BaseParser):
    """Parser for NVMe response and completion data structures."""

    @classmethod
    def parse_response(cls, data: bytes, expected_command_id: int) -> Dict[str, Any]:
        """
        Parse NVMe response data.

        Args:
            data: Response payload
            expected_command_id: Expected command ID

        Returns:
            Parsed response dictionary

        Reference: NVMe Base Specification Section 4.1.3
        """
        if len(data) < 16:  # Minimum completion queue entry size
            raise ValueError(f"NVMe completion queue entry too short: got {len(data)} bytes, need at least 16")

        # Parse basic completion queue entry (16 bytes)
        # Format: DW0(4) + DW1(4) + SQ_HEAD(2) + SQ_ID(2) + CID(2) + STATUS(2)
        dw0, dw1, sq_head, sq_id, command_id, status = cls.safe_unpack('<LLHHHH', data, 0)

        if command_id != expected_command_id:
            raise ValueError(
                f"Command ID mismatch: expected {expected_command_id}, got {command_id}")

        # Extract status code from bits 10:1 of the status field
        # Reference: NVMe Base Specification Section 4.1.3, Figure 92
        status_code = (status >> 1) & 0x3FF
        if status_code != 0:
            # Format enhanced error message with status description
            try:
                from ..protocol.status_codes import format_status_error
                error_message = format_status_error(status_code, command_id)
            except ImportError:
                # Fallback to basic formatting
                error_message = f"Command failed with status {status_code:02x}"
                if command_id is not None:
                    error_message = f"Command {command_id} " + error_message

            from ..exceptions import CommandError
            raise CommandError(error_message, status_code, command_id)

        return {
            'command_id': command_id,
            'status': status_code,
            'dw0': dw0,
            'dw1': dw1,
            'result': dw0,  # For Property Get operations, result is in dw0
            'data': data[16:] if len(data) > 16 else b''
        }
