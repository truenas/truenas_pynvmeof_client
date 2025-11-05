"""
Unit tests for exception classes

Tests custom exception hierarchy and error information.
"""

import unittest

from nvmeof_client.exceptions import (
    NVMeoFError,
    ConnectionError,
    TimeoutError,
    CommandError,
    ProtocolError
)


class TestExceptionHierarchy(unittest.TestCase):
    """Test exception class hierarchy."""

    def test_base_exception(self):
        """Test base NVMeoFError exception."""
        exc = NVMeoFError("Base error")
        self.assertIsInstance(exc, Exception)
        self.assertEqual(str(exc), "Base error")

    def test_connection_error_inheritance(self):
        """Test ConnectionError inheritance."""
        exc = ConnectionError("Connection failed")
        self.assertIsInstance(exc, NVMeoFError)
        self.assertIsInstance(exc, Exception)
        self.assertEqual(str(exc), "Connection failed")

    def test_timeout_error_inheritance(self):
        """Test TimeoutError inheritance."""
        exc = TimeoutError("Operation timed out")
        self.assertIsInstance(exc, NVMeoFError)
        self.assertIsInstance(exc, Exception)
        self.assertEqual(str(exc), "Operation timed out")

    def test_protocol_error_inheritance(self):
        """Test ProtocolError inheritance."""
        exc = ProtocolError("Protocol violation")
        self.assertIsInstance(exc, NVMeoFError)
        self.assertIsInstance(exc, Exception)
        self.assertEqual(str(exc), "Protocol violation")


class TestCommandError(unittest.TestCase):
    """Test CommandError with additional attributes."""

    def test_command_error_basic(self):
        """Test basic CommandError."""
        exc = CommandError("Command failed")
        self.assertIsInstance(exc, NVMeoFError)
        self.assertEqual(str(exc), "Command failed")
        self.assertIsNone(exc.status_code)
        self.assertIsNone(exc.command_id)

    def test_command_error_with_status(self):
        """Test CommandError with status code."""
        exc = CommandError("Command failed", status_code=0x02)
        self.assertEqual(str(exc), "Command failed")
        self.assertEqual(exc.status_code, 0x02)
        self.assertIsNone(exc.command_id)

    def test_command_error_with_command_id(self):
        """Test CommandError with command ID."""
        exc = CommandError("Command failed", command_id=123)
        self.assertEqual(str(exc), "Command failed")
        self.assertIsNone(exc.status_code)
        self.assertEqual(exc.command_id, 123)

    def test_command_error_complete(self):
        """Test CommandError with all attributes."""
        exc = CommandError("Command failed", status_code=0x05, command_id=456)
        self.assertEqual(str(exc), "Command failed")
        self.assertEqual(exc.status_code, 0x05)
        self.assertEqual(exc.command_id, 456)


class TestExceptionRaising(unittest.TestCase):
    """Test exception raising and catching."""

    def test_raise_and_catch_base(self):
        """Test raising and catching base exception."""
        with self.assertRaises(NVMeoFError) as cm:
            raise NVMeoFError("Test error")

        self.assertEqual(str(cm.exception), "Test error")

    def test_catch_specific_as_base(self):
        """Test catching specific exception as base."""
        with self.assertRaises(NVMeoFError):
            raise ConnectionError("Connection error")

    def test_catch_command_error_attributes(self):
        """Test catching CommandError and accessing attributes."""
        try:
            raise CommandError("Test command error", status_code=0x10, command_id=789)
        except CommandError as e:
            self.assertEqual(e.status_code, 0x10)
            self.assertEqual(e.command_id, 789)
        except Exception:
            self.fail("Should have caught CommandError")

    def test_exception_chaining(self):
        """Test exception chaining."""
        try:
            try:
                raise ValueError("Original error")
            except ValueError as e:
                raise ConnectionError("Connection failed") from e
        except ConnectionError as e:
            self.assertIsInstance(e.__cause__, ValueError)
            self.assertEqual(str(e.__cause__), "Original error")


if __name__ == '__main__':
    unittest.main()
