"""
NVMe Discovery log page parsing.

This module handles parsing of NVMe-oF Discovery Log Page data structures
as defined in the NVMe-oF Base Specification.
"""

from typing import Dict, Any
from .base import BaseParser


class DiscoveryDataParser(BaseParser):
    """Parser for NVMe-oF Discovery Log Page data structures."""

    @classmethod
    def parse_discovery_log_page(cls, data: bytes) -> Dict[str, Any]:
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
        cls.validate_data_length(data, 16, "Discovery log page header")

        # Parse header (16 bytes)
        generation_counter = cls.safe_unpack('<Q', data, 0)[0]
        num_records = cls.safe_unpack('<Q', data, 8)[0]

        entries = []
        entry_size = 1024  # Each discovery entry is 1024 bytes

        for i in range(num_records):
            entry_offset = 1024 + (i * entry_size)  # Entries start at offset 1024

            if entry_offset + entry_size > len(data):
                break  # Not enough data for complete entry

            entry_data = data[entry_offset:entry_offset + entry_size]
            entry = cls._parse_single_discovery_entry(entry_data)
            entries.append(entry)

        return {
            'generation': generation_counter,
            'num_records': num_records,
            'entries': entries
        }

    @classmethod
    def _parse_single_discovery_entry(cls, data: bytes) -> Dict[str, Any]:
        """
        Parse a single discovery log entry.

        Args:
            data: 1024-byte discovery log entry data

        Returns:
            Dictionary containing parsed discovery entry

        Reference: NVMe-oF Base Specification Rev 1.1c, Section 5.4.1.2
        Discovery Log Page Entry format (1024 bytes)
        """
        cls.validate_data_length(data, 1024, "Discovery log entry")

        # TRTYPE (Transport Type) - byte 0
        transport_type = data[0]

        # ADRFAM (Address Family) - byte 1
        address_family = data[1]

        # SUBTYPE (Subsystem Type) - byte 2
        subsystem_type = data[2]

        # PORTID (Port ID) - bytes 4-5
        port_id = cls.safe_unpack('<H', data, 4)[0]

        # CNTLID (Controller ID) - bytes 6-7
        controller_id = cls.safe_unpack('<H', data, 6)[0]

        # TRSVCID (Transport Service ID) - bytes 32-63 (32 bytes, null-terminated)
        transport_service_id = cls.extract_string(data, 32, 32)

        # SUBNQN (Subsystem NQN) - bytes 256-511 (256 bytes, null-terminated)
        subsystem_nqn = cls.extract_string(data, 256, 256)

        # TRADDR (Transport Address) - bytes 512-767 (256 bytes, null-terminated)
        transport_address = cls.extract_string(data, 512, 256)

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

    @staticmethod
    def format_discovery_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format discovery entry with human-readable field names.

        Args:
            entry: Raw discovery entry dictionary

        Returns:
            Formatted discovery entry dictionary with both human-readable
            and raw values
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
            2: 'NVMe'
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
