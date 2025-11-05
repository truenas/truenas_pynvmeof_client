"""
NVMe-oF Client Exception Classes

Custom exception classes for NVMe-oF TCP client operations.
Provides specific error types for different failure scenarios.

References:
- NVMe-oF TCP Transport Specification Section 4.x (Error Handling)
- NVMe Base Specification Section 1.6 (Status Codes)
"""


class NVMeoFError(Exception):
    """Base exception class for all NVMe-oF client errors."""
    pass


class NVMeoFConnectionError(NVMeoFError):
    """
    Raised when TCP connection establishment or maintenance fails.

    This includes failures during:
    - Initial TCP connection
    - Connection parameter negotiation
    - Connection loss during operation
    """
    pass


class NVMeoFTimeoutError(NVMeoFError):
    """
    Raised when operations exceed specified timeout values.

    Covers timeouts for:
    - Connection establishment
    - Command execution
    - Response reception
    """
    pass


# Aliases for backward compatibility and cleaner imports
ConnectionError = NVMeoFConnectionError
TimeoutError = NVMeoFTimeoutError


class CommandError(NVMeoFError):
    """
    Raised when NVMe command execution fails.

    Includes failures due to:
    - Invalid command parameters
    - Target-side command processing errors
    - Command-specific error conditions

    Attributes:
        status_code: Raw 8-bit NVMe status code
        command_id: Command identifier that failed
        status_description: Human-readable status description
        spec_reference: NVMe specification reference for the status code

    Reference: NVM Express Base Specification Rev 2.1, Section 1.6 "Status Codes"
    """
    def __init__(self, message, status_code=None, command_id=None):
        super().__init__(message)
        self.status_code = status_code
        self.command_id = command_id
        self.status_description = None
        self.spec_reference = None

        # Decode status code if provided
        if status_code is not None:
            try:
                from .protocol.status_codes import decode_status_code
                description, spec_ref, _ = decode_status_code(status_code << 1)
                self.status_description = description
                self.spec_reference = spec_ref
            except ImportError:
                # Fallback if status_codes module not available
                pass


class ProtocolError(NVMeoFError):
    """
    Raised when NVMe-oF protocol violations occur.

    Covers errors such as:
    - Invalid PDU format
    - Protocol state violations
    - Unsupported protocol features
    """
    pass
