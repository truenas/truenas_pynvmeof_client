"""
NVMe Asynchronous Event parsing.

This module handles parsing of NVMe Asynchronous Event Request completions.
"""

from typing import Any
from .base import BaseParser
from ..models import (
    AsyncEvent,
    AsyncEventType,
    AsyncEventInfoNotice,
    AsyncEventInfoImmediate
)


class AsyncEventParser(BaseParser):
    """Parser for NVMe Asynchronous Event completions."""

    @classmethod
    def parse_async_event_completion(cls, dw0: int, dw1: int) -> dict[str, Any]:
        """
        Parse Asynchronous Event Request completion queue entry.

        Args:
            dw0: Completion queue entry Dword 0
            dw1: Completion queue entry Dword 1 (Event Specific Parameter)

        Returns:
            Dictionary with parsed async event information

        Reference: NVM Express Base Specification 2.3, Section 5.2.2
                   Figure 150: Asynchronous Event Request – Completion Queue Entry Dword 0
                   Figure 151: Asynchronous Event Request – Completion Queue Entry Dword 1
        """
        # Parse Dword 0 fields
        # Bits 2:0 = Asynchronous Event Type (AET)
        # Bits 7:3 = Reserved
        # Bits 15:8 = Asynchronous Event Information (AEI)
        # Bits 23:16 = Log Page Identifier (LID)
        # Bits 31:24 = Reserved
        event_type_raw = dw0 & 0x7  # Bits 2:0
        event_info = (dw0 >> 8) & 0xFF  # Bits 15:8
        log_page_id = (dw0 >> 16) & 0xFF  # Bits 23:16

        # Convert to enum
        try:
            event_type = AsyncEventType(event_type_raw)
        except ValueError:
            event_type = event_type_raw  # Keep raw value if unknown

        # Event Specific Parameter from Dword 1
        event_specific_param = dw1 if dw1 != 0 else None

        # Generate human-readable description
        description = cls._describe_event(event_type, event_info, log_page_id)

        return {
            'event_type': event_type,
            'event_info': event_info,
            'log_page_id': log_page_id,
            'event_specific_param': event_specific_param,
            'description': description,
            'raw_dword0': dw0
        }

    @classmethod
    def parse_async_event_to_object(cls, dw0: int, dw1: int) -> AsyncEvent:
        """
        Parse Asynchronous Event Request completion to AsyncEvent object.

        Args:
            dw0: Completion queue entry Dword 0
            dw1: Completion queue entry Dword 1

        Returns:
            AsyncEvent dataclass instance
        """
        parsed = cls.parse_async_event_completion(dw0, dw1)
        return AsyncEvent(**parsed)

    @staticmethod
    def _describe_event(event_type: AsyncEventType, event_info: int, log_page_id: int) -> str:
        """
        Generate human-readable description of async event.

        Args:
            event_type: Event type enum value
            event_info: Event information code
            log_page_id: Associated log page identifier

        Returns:
            Human-readable description string

        Reference: Figures 152-157 in NVM Express Base Specification 2.3
        """
        if event_type == AsyncEventType.ERROR_STATUS:
            return f"Error Status Event (info={event_info:#x}, log_page={log_page_id:#x})"

        elif event_type == AsyncEventType.SMART_HEALTH_STATUS:
            smart_events = {
                0x00: "NVM Subsystem Reliability",
                0x01: "Temperature Threshold",
                0x02: "Spare Capacity Below Threshold"
            }
            desc = smart_events.get(event_info, f"Unknown SMART Event {event_info:#x}")
            return f"SMART/Health Status: {desc}"

        elif event_type == AsyncEventType.NOTICE:
            try:
                notice = AsyncEventInfoNotice(event_info)
                return f"Notice: {notice.name}"
            except ValueError:
                return f"Notice Event (info={event_info:#x}, log_page={log_page_id:#x})"

        elif event_type == AsyncEventType.IMMEDIATE:
            try:
                immediate = AsyncEventInfoImmediate(event_info)
                return f"Immediate Event: {immediate.name}"
            except ValueError:
                return f"Immediate Event (info={event_info:#x})"

        elif event_type == AsyncEventType.ONE_SHOT:
            return f"One-Shot Event (info={event_info:#x})"

        elif event_type == AsyncEventType.IO_COMMAND_SPECIFIC:
            return f"I/O Command Specific Event (info={event_info:#x})"

        elif event_type == AsyncEventType.VENDOR_SPECIFIC:
            return f"Vendor Specific Event (info={event_info:#x}, log_page={log_page_id:#x})"

        else:
            return f"Unknown Event Type {event_type} (info={event_info:#x}, log_page={log_page_id:#x})"
