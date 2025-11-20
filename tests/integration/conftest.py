"""
Pytest configuration for integration tests

Provides fixtures and configuration for tests that require a live NVMe-oF target.
"""

import os
import pytest
from nvmeof_client.client import NVMeoFClient
from ..fixtures.test_helpers import (
    check_target_availability,
    get_test_target_config,
    should_skip_integration_tests,
)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--manual",
        action="store_true",
        default=False,
        help="Run manual tests that require human intervention"
    )


def pytest_configure(config):
    """Configure pytest for integration tests."""
    config.addinivalue_line(
        "markers", "integration: mark test as requiring a live NVMe-oF target"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "manual: mark test as requiring manual intervention (skipped by default)"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle integration test markers."""
    if should_skip_integration_tests():
        skip_integration = pytest.mark.skip(reason="Integration tests disabled (NVMEOF_SKIP_INTEGRATION set)")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    # Skip manual tests by default unless --manual flag is provided
    if not config.getoption("--manual", default=False):
        skip_manual = pytest.mark.skip(reason="Manual test (use --manual to run)")
        for item in items:
            if "manual" in item.keywords:
                item.add_marker(skip_manual)


@pytest.fixture(scope="session")
def target_config():
    """Get target configuration for integration tests."""
    return get_test_target_config()


@pytest.fixture(scope="session")
def target_available(target_config):
    """Check if target is available before running tests."""
    if should_skip_integration_tests():
        pytest.skip("Integration tests disabled")

    if not check_target_availability(target_config['host'], target_config['port']):
        pytest.skip(f"NVMe-oF target not available at {target_config['host']}:{target_config['port']}")

    return True


@pytest.fixture
def client(target_config, target_available):
    """Create a connected NVMe-oF client for testing."""
    client = NVMeoFClient(
        target_config['host'],
        port=target_config['port'],
        timeout=target_config['timeout']
    )

    try:
        client.connect()
        yield client
    finally:
        if client.is_connected:
            client.disconnect()


@pytest.fixture
def discovery_client(target_config, target_available):
    """Create a client connected to discovery subsystem."""
    client = NVMeoFClient(
        target_config['host'],
        "nqn.2014-08.org.nvmexpress.discovery",
        port=target_config['port'],
        timeout=target_config['timeout']
    )

    try:
        # Connect to discovery subsystem
        client.connect()
        yield client
    finally:
        if client.is_connected:
            client.disconnect()


@pytest.fixture
def nvme_client(target_config, target_available):
    """Create a client connected to NVMe subsystem for I/O operations."""
    client = NVMeoFClient(
        target_config['host'],
        target_config['nqn'],
        port=target_config['port'],
        timeout=target_config['timeout']
    )

    try:
        # Connect to the configured NVMe subsystem
        client.connect()
        yield client
    finally:
        if client.is_connected:
            client.disconnect()


@pytest.fixture
def test_namespace_id():
    """Get namespace ID for testing."""
    return int(os.getenv('NVMEOF_TEST_NSID', '1'))


@pytest.fixture
def test_reservation_key():
    """Get test reservation key."""
    return int(os.getenv('NVMEOF_TEST_KEY', '0x123456789ABCDEF0'), 0)
