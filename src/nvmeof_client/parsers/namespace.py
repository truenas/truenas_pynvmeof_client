"""
NVMe Namespace identification data parsing.

This module handles parsing of NVMe Identify Namespace data structures
as defined in the NVMe Base Specification.
"""

import logging
from typing import Any
from .base import BaseParser

logger = logging.getLogger(__name__)

# LBA Format Constants
NVME_LBAF_COUNT = 16
NVME_LBAF_ENTRY_SIZE = 4
NVME_LBAF_ARRAY_OFFSET = 128
NVME_LBAF_MS_MASK = 0xFFFF
NVME_LBAF_LBADS_MASK = 0xFF
NVME_LBAF_LBADS_SHIFT = 16
NVME_LBAF_RP_MASK = 0x3
NVME_LBAF_RP_SHIFT = 24

# Format and Data Size Constants
NVME_FLBAS_FORMAT_INDEX_MASK = 0xF
NVME_LBADS_MIN_VALUE = 9   # 512 bytes minimum
NVME_LBADS_MAX_VALUE = 16  # 64KB maximum

# Data Structure Offsets
NVME_NAMESPACE_VS_OFFSET = 3584
NVME_IDENTIFY_DATA_SIZE = 4096


class NamespaceDataParser(BaseParser):
    """Parser for NVMe Identify Namespace data structures."""

    @classmethod
    def parse(cls, data: bytes, nsid: int = None) -> dict[str, Any]:
        """
        Parse NVMe Identify Namespace data structure.

        Args:
            data: 4096-byte identify namespace data structure
            nsid: namespace identifier (optional, for logging)

        Returns:
            Dictionary containing parsed namespace information

        Reference: NVM Express Base Specification Rev 2.1, Figure 273
        "Identify Namespace Data Structure"
        """
        cls.validate_data_length(data, 4096, "Identify Namespace data")

        # Parse the Identify Namespace data structure (4096 bytes)
        # All integer fields are little-endian
        parsed = {}

        # Bytes 0-7: Namespace Size (NSZE) - 64-bit
        parsed['nsze'] = cls.safe_unpack('<Q', data, 0)[0]

        # Bytes 8-15: Namespace Capacity (NCAP) - 64-bit
        parsed['ncap'] = cls.safe_unpack('<Q', data, 8)[0]

        # Bytes 16-23: Namespace Utilization (NUSE) - 64-bit
        parsed['nuse'] = cls.safe_unpack('<Q', data, 16)[0]

        # Byte 24: Namespace Features (NSFEAT)
        parsed['nsfeat'] = data[24]

        # Byte 25: Number of LBA Formats (NLBAF)
        parsed['nlbaf'] = data[25]

        # Byte 26: Formatted LBA Size (FLBAS)
        parsed['flbas'] = data[26]

        # Byte 27: Metadata Capabilities (MC)
        parsed['mc'] = data[27]

        # Byte 28: End-to-end Data Protection Capabilities (DPC)
        parsed['dpc'] = data[28]

        # Byte 29: End-to-end Data Protection Type Settings (DPS)
        parsed['dps'] = data[29]

        # Byte 30: Namespace Multi-path I/O and Namespace Sharing Capabilities (NMIC)
        parsed['nmic'] = data[30]

        # Byte 31: Reservation Capabilities (RESCAP)
        parsed['rescap'] = data[31]

        # Byte 32: Format Progress Indicator (FPI)
        parsed['fpi'] = data[32]

        # Byte 33: Deallocate Logical Block Features (DLFEAT)
        parsed['dlfeat'] = data[33]

        # Bytes 34-35: Namespace Atomic Write Unit Normal (NAWUN)
        parsed['nawun'] = cls.safe_unpack('<H', data, 34)[0]

        # Bytes 36-37: Namespace Atomic Write Unit Power Fail (NAWUPF)
        parsed['nawupf'] = cls.safe_unpack('<H', data, 36)[0]

        # Bytes 38-39: Namespace Atomic Compare & Write Unit (NACWU)
        parsed['nacwu'] = cls.safe_unpack('<H', data, 38)[0]

        # Bytes 40-41: Namespace Atomic Boundary Size Normal (NABSN)
        parsed['nabsn'] = cls.safe_unpack('<H', data, 40)[0]

        # Bytes 42-43: Namespace Atomic Boundary Offset (NABO)
        parsed['nabo'] = cls.safe_unpack('<H', data, 42)[0]

        # Bytes 44-45: Namespace Atomic Boundary Size Power Fail (NABSPF)
        parsed['nabspf'] = cls.safe_unpack('<H', data, 44)[0]

        # Bytes 46-47: Namespace Optimal IO Boundary (NOIOB)
        parsed['noiob'] = cls.safe_unpack('<H', data, 46)[0]

        # Bytes 48-63: NVM Capacity (NVMCAP) - 128-bit little-endian
        parsed['nvmcap'] = cls.safe_unpack('<QQ', data, 48)

        # Bytes 64-65: Namespace Preferred Write Granularity (NPWG)
        parsed['npwg'] = cls.safe_unpack('<H', data, 64)[0]

        # Bytes 66-67: Namespace Preferred Write Alignment (NPWA)
        parsed['npwa'] = cls.safe_unpack('<H', data, 66)[0]

        # Bytes 68-69: Namespace Preferred Deallocate Granularity (NPDG)
        parsed['npdg'] = cls.safe_unpack('<H', data, 68)[0]

        # Bytes 70-71: Namespace Preferred Deallocate Alignment (NPDA)
        parsed['npda'] = cls.safe_unpack('<H', data, 70)[0]

        # Bytes 72-73: Namespace Optimal Write Size (NOWS)
        parsed['nows'] = cls.safe_unpack('<H', data, 72)[0]

        # Bytes 74-77: Maximum Single Source Range Length (MSSRL)
        parsed['mssrl'] = cls.safe_unpack('<L', data, 74)[0]

        # Bytes 78-81: Maximum Copy Length (MCL)
        parsed['mcl'] = cls.safe_unpack('<L', data, 78)[0]

        # Byte 82: Maximum Source Range Count (MSRC)
        parsed['msrc'] = data[82]

        # Byte 91: Number of Unique LBA Formats (NULBAF)
        parsed['nulbaf'] = data[91]

        # Bytes 92-95: ANA Group Identifier (ANAGRPID)
        parsed['anagrpid'] = cls.safe_unpack('<L', data, 92)[0]

        # Byte 99: Namespace Attributes (NSATTR)
        parsed['nsattr'] = data[99]

        # Bytes 100-101: NVM Set Identifier (NVMSETID)
        parsed['nvmsetid'] = cls.safe_unpack('<H', data, 100)[0]

        # Bytes 102-103: Endurance Group Identifier (ENDGID)
        parsed['endgid'] = cls.safe_unpack('<H', data, 102)[0]

        # Bytes 104-119: Namespace Globally Unique Identifier (NGUID) - 128-bit
        parsed['nguid'] = cls.bytes_to_hex_string(data[104:120])

        # Bytes 120-127: IEEE Extended Unique Identifier (EUI64) - 64-bit
        parsed['eui64'] = cls.bytes_to_hex_string(data[120:128])

        # Parse LBA Format Support (LBAF0-LBAF15)
        parsed['lbaf'] = cls._parse_lba_formats(data)

        # Calculate logical block size from FLBAS
        parsed['logical_block_size'] = cls._calculate_logical_block_size(
            parsed['flbas'], parsed['lbaf'])

        # Bytes 3584-4095: Vendor Specific (VS)
        parsed['vs'] = data[NVME_NAMESPACE_VS_OFFSET:NVME_IDENTIFY_DATA_SIZE]

        return parsed

    @classmethod
    def _parse_lba_formats(cls, data: bytes) -> list[dict[str, Any]]:
        """
        Parse LBA Format Support entries.

        Args:
            data: Full namespace identification data

        Returns:
            List of LBA format dictionaries
        """
        lbaf_entries = []

        # Bytes 128-191: LBA Format Support (LBAF0-LBAF15) - 16 entries, 4 bytes each
        for i in range(NVME_LBAF_COUNT):
            offset = NVME_LBAF_ARRAY_OFFSET + (i * NVME_LBAF_ENTRY_SIZE)
            if offset + NVME_LBAF_ENTRY_SIZE <= len(data):
                lbaf_value = cls.safe_unpack('<L', data, offset)[0]

                # Parse LBA Format fields according to specification
                ms = lbaf_value & NVME_LBAF_MS_MASK                                   # Metadata Size (bits 15:0)
                lbads = (lbaf_value >> NVME_LBAF_LBADS_SHIFT) & NVME_LBAF_LBADS_MASK  # LBA Data Size (bits 23:16)
                # Relative Performance (bits 25:24)
                rp = (lbaf_value >> NVME_LBAF_RP_SHIFT) & NVME_LBAF_RP_MASK

                lbaf_entries.append({
                    'ms': ms,        # Metadata Size
                    'lbads': lbads,  # LBA Data Size (2^lbads bytes)
                    'rp': rp,        # Relative Performance
                    'raw': lbaf_value
                })

        return lbaf_entries

    @classmethod
    def _calculate_logical_block_size(cls, flbas: int, lbaf_entries: list[dict[str, Any]]) -> int:
        """
        Calculate logical block size from FLBAS and LBA format entries.

        Args:
            flbas: Formatted LBA Size field
            lbaf_entries: List of LBA format entries

        Returns:
            Logical block size in bytes
        """
        # Calculate logical block size from FLBAS
        current_lba_format = flbas & NVME_FLBAS_FORMAT_INDEX_MASK  # Lower 4 bits indicate current format

        # Find the first valid LBA format with reasonable lbads value
        logical_block_size = 0
        if current_lba_format < len(lbaf_entries):
            lbaf = lbaf_entries[current_lba_format]
            lbads = lbaf['lbads']
            # Sanity check: LBADS should be between 9 (512 bytes) and 16 (64KB)
            if NVME_LBADS_MIN_VALUE <= lbads <= NVME_LBADS_MAX_VALUE:
                logical_block_size = 2 ** lbads

        # If current format is invalid, search for a valid format (fallback logic)
        if logical_block_size == 0:
            for i, lbaf in enumerate(lbaf_entries):
                lbads = lbaf['lbads']
                if NVME_LBADS_MIN_VALUE <= lbads <= NVME_LBADS_MAX_VALUE and lbaf['raw'] != 0:
                    logical_block_size = 2 ** lbads
                    logger.debug(f"Using LBAF{i} instead of FLBAS format {current_lba_format}")
                    break

        return logical_block_size
