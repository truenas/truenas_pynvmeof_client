"""
NVMe Controller capabilities and status parsing.

This module handles parsing of NVMe controller capability and status
register data structures.
"""

from typing import Dict, Any
from .base import BaseParser


class CapabilityParser(BaseParser):
    """Parser for NVMe controller capabilities and status."""

    @classmethod
    def parse_controller_capabilities(cls, cap_data: bytes) -> Dict[str, Any]:
        """
        Parse Controller Capabilities Register (CAP) data.

        Args:
            cap_data: 8-byte CAP register data

        Returns:
            Dictionary with parsed capability fields

        Reference: NVM Express Base Specification Section 3.1.1
        """
        cls.validate_data_length(cap_data, 8, "CAP register data")

        cap_value = cls.safe_unpack('<Q', cap_data, 0)[0]

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

    @classmethod
    def parse_controller_status(cls, csts_data: bytes) -> Dict[str, Any]:
        """
        Parse Controller Status Register (CSTS) data.

        Args:
            csts_data: 4-byte CSTS register data

        Returns:
            Dictionary with parsed status fields

        Reference: NVM Express Base Specification Section 3.1.6
        """
        cls.validate_data_length(csts_data, 4, "CSTS register data")

        csts_value = cls.safe_unpack('<L', csts_data, 0)[0]

        # Extract bit fields
        rdy = bool(csts_value & 0x1)  # Ready
        cfs = bool((csts_value >> 1) & 0x1)  # Controller Fatal Status
        shst = (csts_value >> 2) & 0x3  # Shutdown Status
        nssro = bool((csts_value >> 4) & 0x1)  # NVM Subsystem Reset Occurred
        pp = bool((csts_value >> 5) & 0x1)  # Processing Paused

        return {
            'ready': rdy,
            'controller_fatal_status': cfs,
            'shutdown_status': shst,
            'nvm_subsystem_reset_occurred': nssro,
            'processing_paused': pp
        }
