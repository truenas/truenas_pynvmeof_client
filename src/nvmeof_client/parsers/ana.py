"""
NVMe ANA (Asymmetric Namespace Access) Log Page parsing.

This module handles parsing of the ANA log page data structures
as defined in the NVMe Base Specification.
"""

from .base import BaseParser
from ..models import (
    ANAGroupDescriptor,
    ANALogPage,
    ANAState,
)


class ANALogPageParser(BaseParser):
    """Parser for NVMe ANA Log Page data structures."""

    @classmethod
    def parse_ana_log_page(cls, data: bytes) -> ANALogPage:
        """
        Parse NVMe ANA Log Page data structure.

        Args:
            data: Raw ANA log page data

        Returns:
            ANALogPage dataclass with parsed information

        Reference:
            NVM Express Base Specification 2.3, Figure 227 "Asymmetric Namespace Access Log Page"
        """
        cls.validate_data_length(data, 16, "ANA log page")

        # Parse header (16 bytes)
        change_count, num_descriptors = cls._parse_header(data[:16])

        # Parse ANA Group Descriptors starting at byte 16
        groups = cls._parse_ana_group_descriptors(data[16:], num_descriptors)

        return ANALogPage(
            change_count=change_count,
            num_ana_group_descriptors=num_descriptors,
            groups=groups
        )

    @classmethod
    def _parse_header(cls, data: bytes) -> tuple:
        """
        Parse ANA log page header (16 bytes).

        Structure:
        - Bytes 0-7: Change Count (CHGC) - 64-bit LE
        - Bytes 8-9: Number of ANA Group Descriptors (NAGD) - 16-bit LE
        - Bytes 10-15: Reserved

        Reference:
            NVM Express Base Specification 2.3, Figure 227

        Returns:
            Tuple of (change_count, num_ana_group_descriptors)
        """
        cls.validate_data_length(data, 16, "ANA log page header")

        # Bytes 0-7: Change Count (CHGC) - 64-bit LE
        change_count = cls.safe_unpack('<Q', data, 0)[0]

        # Bytes 8-9: Number of ANA Group Descriptors (NAGD) - 16-bit LE
        num_descriptors = cls.safe_unpack('<H', data, 8)[0]

        return change_count, num_descriptors

    @classmethod
    def _parse_ana_group_descriptors(cls, data: bytes, num_descriptors: int) -> list[ANAGroupDescriptor]:
        """
        Parse list of ANA Group Descriptors.

        Each descriptor has variable length:
        - 32 bytes of header/fixed fields
        - 4 bytes per namespace ID

        Reference:
            NVM Express Base Specification 2.3, Figure 228 "ANA Group Descriptor format"

        Args:
            data: Raw descriptor data
            num_descriptors: Number of descriptors to parse

        Returns:
            List of ANAGroupDescriptor dataclasses
        """
        groups = []
        offset = 0

        for i in range(num_descriptors):
            if offset >= len(data):
                break

            # Need at least 32 bytes for descriptor header
            if offset + 32 > len(data):
                raise ValueError(
                    f"Insufficient data for ANA Group Descriptor {i}: "
                    f"need at least {offset + 32} bytes, got {len(data)}"
                )

            group, descriptor_size = cls._parse_single_ana_group_descriptor(data[offset:])
            groups.append(group)
            offset += descriptor_size

        return groups

    @classmethod
    def _parse_single_ana_group_descriptor(cls, data: bytes) -> tuple:
        """
        Parse a single ANA Group Descriptor.

        Reference:
            NVM Express Base Specification 2.3, Figure 228

        Structure:
        - Bytes 0-3: ANA Group ID (AGID) - 32-bit LE
        - Bytes 4-7: Number of NSID Values (NNV) - 32-bit LE
        - Bytes 8-15: Change Count (CHGC) - 64-bit LE
        - Byte 16 bits 0-3: ANA State (ANAS)
        - Bytes 17-31: Reserved
        - Bytes 32+: Namespace ID list (4 bytes each)

        Args:
            data: Raw descriptor data

        Returns:
            Tuple of (ANAGroupDescriptor, descriptor_size_in_bytes)
        """
        cls.validate_data_length(data, 32, "ANA Group Descriptor header")

        # Bytes 0-3: ANA Group ID (32-bit LE)
        ana_group_id = cls.safe_unpack('<L', data, 0)[0]

        # Bytes 4-7: Number of NSID Values (32-bit LE)
        num_namespaces = cls.safe_unpack('<L', data, 4)[0]

        # Bytes 8-15: Change Count (64-bit LE)
        change_count = cls.safe_unpack('<Q', data, 8)[0]

        # Byte 16 bits 0-3: ANA State
        ana_state_value = data[16] & 0x0F
        try:
            ana_state = ANAState(ana_state_value)
        except ValueError:
            # If we get an unknown state value, default to CHANGE
            ana_state = ANAState.CHANGE

        # Parse namespace ID list starting at byte 32
        namespace_ids = cls._parse_namespace_id_list(data[32:], num_namespaces)

        # Calculate total descriptor size: 32 byte header + 4 bytes per NSID
        descriptor_size = 32 + (4 * num_namespaces)

        descriptor = ANAGroupDescriptor(
            ana_group_id=ana_group_id,
            num_namespaces=num_namespaces,
            change_count=change_count,
            ana_state=ana_state,
            namespace_ids=namespace_ids
        )

        return descriptor, descriptor_size

    @classmethod
    def _parse_namespace_id_list(cls, data: bytes, num_namespaces: int) -> list[int]:
        """
        Parse list of namespace IDs from ANA Group Descriptor.

        Each NSID is 4 bytes (32-bit LE).

        Args:
            data: Raw NSID list data
            num_namespaces: Number of NSIDs to parse

        Returns:
            List of namespace IDs
        """
        namespace_ids = []
        required_size = num_namespaces * 4

        if len(data) < required_size:
            raise ValueError(
                f"Insufficient data for namespace ID list: "
                f"need {required_size} bytes for {num_namespaces} NSIDs, got {len(data)}"
            )

        for i in range(num_namespaces):
            offset = i * 4
            nsid = cls.safe_unpack('<L', data, offset)[0]
            namespace_ids.append(nsid)

        return namespace_ids
