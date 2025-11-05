# NVMe-oF Client Test Suite

This directory contains comprehensive tests for the NVMe-oF TCP client implementation.

## Test Structure

```
tests/
├── unit/                    # Unit tests (no live target required)
│   ├── test_protocol.py     # Protocol command generation and parsing
│   ├── test_models.py       # Data models and validation
│   ├── test_exceptions.py   # Exception hierarchy
│   ├── test_client.py       # Client methods with mocked I/O
│   └── test_reservations.py # Reservation-specific unit tests
├── integration/             # Integration tests (live target required)
│   ├── conftest.py          # Pytest fixtures and configuration
│   ├── test_basic_operations.py    # Basic connectivity and admin ops
│   └── test_reservation_flows.py   # Complete reservation workflows
└── fixtures/                # Test data and utilities
    ├── mock_responses.py    # Pre-crafted PDU responses
    └── test_helpers.py      # Common test utilities
```

## Running Tests

### Quick Start

**Recommended: Run from project root directory**
```bash
# Run unit tests only (fast, no target required)
python nvmeof_client/run_tests.py unit

# Run integration tests (requires live target)  
python nvmeof_client/run_tests.py integration

# Run all tests
python nvmeof_client/run_tests.py all
```

**Alternative: Run from within package directory**
```bash
cd nvmeof_client
python run_tests.py unit
```

### Unit Tests (No Target Required)

Unit tests can be run without any NVMe-oF target infrastructure:

```bash
# Using the test runner
python nvmeof_client/run_tests.py unit

# Using unittest directly
python -m unittest discover -s nvmeof_client/tests/unit -v

# Run specific test file
python -m unittest nvmeof_client.tests.unit.test_reservations -v

# Run specific test method
python -m unittest nvmeof_client.tests.unit.test_models.TestEnums.test_reservation_type_enum -v
```

**Unit Test Categories:**
- **Protocol Command Generation** - Binary command structure validation
- **Data Model Testing** - Model validation and serialization  
- **Parameter Validation** - Input sanitization and bounds checking
- **Mocked Method Testing** - Client methods with simulated responses
- **Exception Handling** - Error propagation and status codes

### Integration Tests (Target Required)

Integration tests require a live NVMe-oF target. Use pytest for better fixtures and configuration:

```bash
# Install pytest if not available
pip install pytest

# Run integration tests
python nvmeof_client/run_tests.py integration

# Using pytest directly
python -m pytest nvmeof_client/tests/integration -v -m integration

# Run specific integration test
python -m pytest nvmeof_client/tests/integration/test_reservation_flows.py::TestBasicReservationWorkflow::test_reservation_register_unregister -v
```

**Integration Test Categories:**
- **Basic Connectivity** - Connection establishment and management
- **Discovery Operations** - Discovery subsystem interactions
- **Administrative Operations** - Controller and namespace identification  
- **I/O Operations** - Read, write, flush with real data
- **Reservation Workflows** - Complete reservation lifecycles
- **Error Recovery** - Network failures and timeout handling

## Configuration

### Environment Variables

Integration tests can be configured via environment variables:

```bash
# Target configuration
export NVMEOF_TARGET_HOST=192.168.56.115
export NVMEOF_TARGET_PORT=4420
export NVMEOF_TARGET_NQN=nqn.2011-06.com.truenas:uuid:68bf9433-63ef-49f5-a921-4c0f8190fd94:foo1
export NVMEOF_TARGET_TRANSPORT=tcp
export NVMEOF_TARGET_TIMEOUT=10.0

# Test configuration  
export NVMEOF_TEST_NSID=1
export NVMEOF_TEST_KEY=0x123456789ABCDEF0

# Skip integration tests
export NVMEOF_SKIP_INTEGRATION=1
```

### Test Runner Options

```bash
# Specify target for integration tests
python nvmeof_client/run_tests.py integration \
  --target-host 192.168.56.115 \
  --target-port 4420 \
  --target-nqn "nqn.2011-06.com.truenas:uuid:68bf9433-63ef-49f5-a921-4c0f8190fd94:foo1" \
  --target-transport tcp \
  --target-timeout 15.0

# Skip integration tests even if requested
python nvmeof_client/run_tests.py all --skip-integration

# Quick test against specific target
python nvmeof_client/run_tests.py integration --target-host 192.168.56.115
```

## Target Requirements

### For Integration Tests

Integration tests require an NVMe-oF target that supports:

- **Basic Operations**: Connection, property get/set, identify commands
- **I/O Operations**: Read, write, flush commands  
- **Reservation Support**: Full NVMe reservation command set
- **Multiple Namespaces**: At least one accessible namespace

### Recommended Targets

- **Linux Kernel Target** (nvmet)
- **SPDK NVMe-oF Target**
- **Hardware NVMe-oF targets** with reservation support

### Target Setup Example (SPDK)

```bash
# Start SPDK target
sudo ./app/spdk_tgt/spdk_tgt &

# Create namespace
sudo ./scripts/rpc.py bdev_malloc_create -b malloc0 64 512
sudo ./scripts/rpc.py nvmf_create_transport -t TCP -u 16384 -m 8 -c 8192
sudo ./scripts/rpc.py nvmf_create_subsystem nqn.2019-05.io.spdk:target -a -s TEST_SERIAL
sudo ./scripts/rpc.py nvmf_subsystem_add_ns nqn.2019-05.io.spdk:target -n malloc0
sudo ./scripts/rpc.py nvmf_subsystem_add_listener nqn.2019-05.io.spdk:target -t tcp -a 0.0.0.0 -s 4420
```

## Writing New Tests

### Unit Tests

Unit tests should:
- Not require external dependencies
- Use mocking for I/O operations
- Test individual components in isolation
- Have fast execution times (<1s per test)

Example unit test:
```python
def test_reservation_command_generation(self):
    \"\"\"Test reservation command binary generation.\"\"\"
    cmd = pack_nvme_reservation_register_command(
        command_id=123, nsid=1, reservation_action=0, 
        reservation_key=0x123456789ABCDEF0
    )
    
    # Verify command structure
    assert_command_structure(self, cmd, NVMeOpcode.RESERVATION_REGISTER, expected_nsid=1)
```

### Integration Tests

Integration tests should:
- Use pytest fixtures for setup/teardown
- Include proper error handling and cleanup
- Test real protocol interactions
- Mark as `@pytest.mark.integration`

Example integration test:
```python
@pytest.mark.integration
def test_reservation_workflow(self, nvme_client, test_namespace_id, test_reservation_key):
    \"\"\"Test complete reservation workflow.\"\"\"
    try:
        # Register -> Acquire -> Report -> Release -> Unregister
        result = nvme_client.reservation_register(
            test_namespace_id, ReservationAction.REGISTER, test_reservation_key)
        assert result.success
        
        # ... continue workflow
        
    finally:
        # Always cleanup
        try:
            nvme_client.reservation_register(
                test_namespace_id, ReservationAction.UNREGISTER, test_reservation_key)
        except:
            pass  # Best effort cleanup
```

## Continuous Integration

Unit tests should run on every commit:
```yaml
# Example GitHub Actions
- name: Run Unit Tests
  run: python nvmeof_client/run_tests.py unit
```

Integration tests can run on specific triggers with target setup:
```yaml
# Example with containerized target
- name: Start SPDK Target
  run: docker run -d --name spdk-target spdk/nvmf-target
  
- name: Run Integration Tests  
  run: python nvmeof_client/run_tests.py integration --target-host localhost
```

## Test Data and Fixtures

- **Mock Responses** (`fixtures/mock_responses.py`) - Pre-crafted PDU data
- **Test Helpers** (`fixtures/test_helpers.py`) - Common utilities and patterns
- **Sample Data** - Realistic test data for various scenarios

## Debugging Tests

### Enable Verbose Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Run Single Test with Debug

```bash
python -m pytest nvmeof_client/tests/integration/test_reservation_flows.py::TestBasicReservationWorkflow::test_reservation_register_unregister -v -s
```

### Capture Network Traffic

For protocol debugging:
```bash
sudo tcpdump -i any port 4420 -w test_capture.pcap
```