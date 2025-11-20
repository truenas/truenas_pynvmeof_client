"""
NVMe-oF TCP Client Library

A Python library for implementing an NVMe over Fabrics (NVMe-oF) client using TCP transport.
This library provides synchronous communication with NVMe-oF targets following the
NVMe-oF TCP Transport Specification.

Author: Generated with Claude Code
Version: 1.0.0
"""

from .client import NVMeoFClient
from .exceptions import (
    CommandError,
    ConnectionError,
    NVMeoFConnectionError,
    NVMeoFError,
    NVMeoFTimeoutError,
    ProtocolError,
    TimeoutError,
)
from .models import (
    ANAGroupDescriptor,
    ANALogPage,
    ANAState,
    AsyncEvent,
    AsyncEventType,
    ConnectionInfo,
    ControllerCapabilities,
    ControllerInfo,
    ControllerStatus,
    DiscoveryEntry,
    NamespaceInfo,
    QueueInfo,
    ReservationAction,
    ReservationInfo,
    ReservationStatus,
    ReservationType,
)

__version__ = "1.0.0"
__all__ = [
    "NVMeoFClient",
    "NVMeoFError",
    "NVMeoFConnectionError",
    "NVMeoFTimeoutError",
    "ConnectionError",
    "TimeoutError",
    "CommandError",
    "ProtocolError",
    "ControllerInfo",
    "NamespaceInfo",
    "DiscoveryEntry",
    "ConnectionInfo",
    "QueueInfo",
    "ControllerCapabilities",
    "ControllerStatus",
    "ANAState",
    "ANAGroupDescriptor",
    "ANALogPage",
    "AsyncEvent",
    "AsyncEventType",
    "ReservationType",
    "ReservationAction",
    "ReservationStatus",
    "ReservationInfo"
]
