"""
NVMe Reservation Report data parsing.

This module handles parsing of NVMe Reservation Report data structures
as defined in the NVMe Base Specification.
"""

from typing import Any
from .base import BaseParser


class ReservationDataParser(BaseParser):
    """Parser for NVMe Reservation Report data structures."""

    @classmethod
    def parse_reservation_report(cls, data: bytes, extended_format: bool = True) -> dict[str, Any]:
        """
        Parse NVMe Reservation Report data structure.

        Args:
            data: Raw reservation report data
            extended_format: True if EDS=1 (extended format), False if EDS=0 (standard format)

        Returns:
            Dictionary containing parsed reservation information

        References:
        - Figure 581: Reservation Status Data Structure (standard format, EDS=0)
        - Figure 582: Reservation Status Extended Data Structure (extended format, EDS=1)
        - Figure 583: Registered Controller Data Structure (24 bytes, 64-bit host ID)
        - Figure 584: Registered Controller Extended Data Structure (64 bytes, 128-bit host ID)
        """
        cls.validate_data_length(data, 24, "Reservation report data")

        # Parse common header (same for both formats)
        header = cls._parse_header(data[:24])

        # Parse registrant data structures based on format
        if extended_format:
            # Extended format: 40 reserved bytes (24-63), then registrants start at byte 64
            registrants = cls._parse_extended_registrants(data[64:], header['num_registered_controllers'])
            entry_size = 64  # Extended format: 64 bytes per entry (128-bit host ID)
        else:
            # Standard format: registrants start immediately after header at byte 24
            registrants = cls._parse_standard_registrants(data[24:], header['num_registered_controllers'])
            entry_size = 24  # Standard format: 24 bytes per entry (64-bit host ID)

        return {
            'generation': header['generation'],
            'num_registered_controllers': header['num_registered_controllers'],
            'persist_through_power_loss': header['persist_through_power_loss'],
            'reservation_type': header['reservation_type'],
            'registrants': registrants,
            'extended_format': extended_format,
            'entry_size': entry_size
        }

    @classmethod
    def _parse_header(cls, data: bytes) -> dict[str, Any]:
        """
        Parse reservation status header (24 bytes, same for both formats).

        Reference: NVM Express Base Specification Rev 2.1, Section 7.8, Figure 582
        """
        cls.validate_data_length(data, 24, "Reservation status header")

        # Bytes 0-3: Generation counter (GEN) (32-bit LE)
        generation = cls.safe_unpack('<L', data, 0)[0]

        # Byte 4: Reservation Type (RTYPE)
        reservation_type = data[4]

        # Bytes 5-6: Number of Registrants (REGSTRNT) (16-bit LE)
        num_registered_controllers = cls.safe_unpack('<H', data, 5)[0]

        # Byte 9: Persist Through Power Loss State (PTPLS)
        persist_through_power_loss = bool(data[9] & 0x1)

        return {
            'generation': generation,
            'reservation_type': reservation_type,
            'num_registered_controllers': num_registered_controllers,
            'persist_through_power_loss': persist_through_power_loss
        }

    @classmethod
    def _parse_standard_registrants(cls, data: bytes, num_registrants: int = None) -> list[dict[str, Any]]:
        """
        Parse standard format registrant data structures (24 bytes each).

        Figure 583: Registered Controller Data Structure
        - Bytes 0-1: Controller ID (CNTLID)
        - Byte 2: Reservation Status (RCSTS)
        - Bytes 3-7: Reserved
        - Bytes 8-15: Host Identifier (HOSTID) - 64-bit
        - Bytes 16-23: Reservation Key (RKEY) - 64-bit

        Note: Only include registrants with valid controller IDs.
        """
        registrants = []
        entry_size = 24

        # If num_registrants is specified, only parse that many entries
        max_entries = num_registrants if num_registrants is not None else len(data) // entry_size

        for i in range(max_entries):
            offset = i * entry_size
            if offset + entry_size > len(data):
                break

            entry = data[offset:offset + entry_size]

            # Bytes 0-1: Controller ID
            controller_id = cls.safe_unpack('<H', entry, 0)[0]

            # Only process entries with valid controller IDs
            if controller_id == 0:
                continue

            # Bytes 16-23: Reservation Key (64-bit)
            reservation_key = cls.safe_unpack('<Q', entry, 16)[0]

            # Byte 2: Reservation Status
            rcsts = entry[2]
            holds_reservation = bool(rcsts & 0x1)

            # Bytes 8-15: Host Identifier (64-bit)
            host_identifier = cls.safe_unpack('<Q', entry, 8)[0]

            registrants.append({
                'controller_id': controller_id,
                'holds_reservation': holds_reservation,
                'reservation_key': reservation_key,
                'host_identifier': host_identifier,
                'host_identifier_size': 64
            })

        return registrants

    @classmethod
    def _parse_extended_registrants(cls, data: bytes, num_registrants: int = None) -> list[dict[str, Any]]:
        """
        Parse extended format registrant data structures (64 bytes each).

        Figure 584: Registered Controller Extended Data Structure
        - Bytes 0-1: Controller ID (CNTLID)
        - Byte 2: Reservation Status (RCSTS)
        - Bytes 3-7: Reserved
        - Bytes 8-15: Reservation Key (RKEY) - 64-bit
        - Bytes 16-31: Host Identifier (HOSTID) - 128-bit
        - Bytes 32-63: Reserved

        Note: Only include registrants with valid controller IDs.
        """
        registrants = []
        entry_size = 64

        # If num_registrants is specified, only parse that many entries
        max_entries = num_registrants if num_registrants is not None else len(data) // entry_size

        for i in range(max_entries):
            offset = i * entry_size
            if offset + entry_size > len(data):
                break

            entry = data[offset:offset + entry_size]

            # Bytes 0-1: Controller ID
            controller_id = cls.safe_unpack('<H', entry, 0)[0]

            # Only process entries with valid controller IDs
            if controller_id == 0:
                continue

            # Bytes 8-15: Reservation Key (64-bit)
            reservation_key = cls.safe_unpack('<Q', entry, 8)[0]

            # Byte 2: Reservation Status
            rcsts = entry[2]
            holds_reservation = bool(rcsts & 0x1)

            # Bytes 16-31: Host Identifier (128-bit)
            host_identifier = cls.safe_unpack('<QQ', entry, 16)
            # Convert to single 128-bit integer: high_64 << 64 | low_64
            host_identifier = (host_identifier[1] << 64) | host_identifier[0]

            registrants.append({
                'controller_id': controller_id,
                'holds_reservation': holds_reservation,
                'reservation_key': reservation_key,
                'host_identifier': host_identifier,
                'host_identifier_size': 128
            })

        return registrants
