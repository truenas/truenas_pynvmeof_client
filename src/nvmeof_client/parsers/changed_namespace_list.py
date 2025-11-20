"""
Parser for NVMe Changed Attached Namespace List Log Page.

Reference: NVM Express Base Specification 2.3
           Section 5.2.12.1.5 "Changed Attached Namespace List (Log Page Identifier 04h)"
           Figure 139 "Namespace List Format"
"""

import struct
from .base import BaseParser


class ChangedNamespaceListParser(BaseParser):
    """Parser for Changed Attached Namespace List log page."""

    @classmethod
    def parse_changed_namespace_list(cls, data: bytes) -> list[int]:
        """
        Parse Changed Attached Namespace List log page.

        This log page describes changes to attached namespaces for the controller since
        the last time this log page was read. Namespaces are included if they:
        a) have changed information in their Identify Namespace data structures
        b) were previously unattached and have been attached to the controller
        c) were previously attached and have been detached from the controller
        d) were deleted

        The log page contains a Namespace List with up to 1,024 entries (4096 bytes).
        Each entry is a 32-bit namespace ID in little-endian format.

        Special case: If more than 1,024 namespaces have changed, the first entry
        is set to FFFFFFFFh and the remainder of the list is zero filled.

        Args:
            data: Raw log page data (up to 4096 bytes)

        Returns:
            List of namespace IDs that have changed (in ascending order), or
            [0xFFFFFFFF] if more than 1,024 namespaces changed

        Reference:
            NVM Express Base Specification 2.3
            Section 5.2.12.1.5 "Changed Attached Namespace List (Log Page Identifier 04h)"
            Figure 139 "Namespace List Format"
        """
        if not data:
            return []

        # Validate data length (should be multiple of 4 bytes, up to 4096)
        if len(data) % 4 != 0:
            # Pad to multiple of 4 if needed
            padding = 4 - (len(data) % 4)
            data = data + b'\x00' * padding

        changed_namespaces = []

        # Parse up to 1,024 entries (4096 bytes / 4 bytes per NSID)
        max_entries = min(len(data) // 4, 1024)

        for i in range(max_entries):
            offset = i * 4
            nsid = struct.unpack('<L', data[offset:offset + 4])[0]

            # Zero NSID indicates end of list (unused entry)
            if nsid == 0:
                break

            # Check for overflow indicator (first entry = FFFFFFFFh)
            if i == 0 and nsid == 0xFFFFFFFF:
                # More than 1,024 namespaces changed
                return [0xFFFFFFFF]

            # Add to list (entries are in ascending order per spec)
            changed_namespaces.append(nsid)

        return changed_namespaces

    @classmethod
    def format_changed_namespace_list(cls, nsids: list[int]) -> str:
        """
        Format changed namespace list for human-readable display.

        Args:
            nsids: List of namespace IDs

        Returns:
            Formatted string describing the changed namespaces
        """
        if not nsids:
            return "No namespace changes detected"

        # Check for overflow indicator
        if nsids == [0xFFFFFFFF]:
            return "More than 1,024 namespaces changed (overflow)"

        if len(nsids) == 1:
            return f"1 namespace changed: NSID {nsids[0]}"

        # For multiple namespaces, show list
        nsid_str = ", ".join(str(nsid) for nsid in nsids[:10])
        if len(nsids) > 10:
            nsid_str += f", ... ({len(nsids) - 10} more)"

        return f"{len(nsids)} namespaces changed: {nsid_str}"
