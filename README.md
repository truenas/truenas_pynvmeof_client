# NVMe-oF Client

A pure Python implementation of an NVMe over Fabrics (NVMe-oF) TCP client library.

## Features

- **Pure Python**: No external dependencies, uses only Python standard library
- **NVMe-oF TCP Transport**: Full implementation of NVMe-oF TCP specification
- **Synchronous API**: Simple, straightforward synchronous operations
- **ANA Support**: Full Asymmetric Namespace Access (ANA) multipath support
- **Discovery**: NVMe Discovery service support
- **Async Events**: Asynchronous Event Notification (AEN) handling
- **Reservations**: NVMe Persistent Reservations support
- **Well-Tested**: Comprehensive unit and integration test suite

## Requirements

- Python 3.8 or higher
- Linux operating system
- Network access to NVMe-oF TCP targets

## Installation

### From Git Repository

Install directly from the repository:

```bash
pip install git+https://github.com/truenas/truenas_pynvmeof_client.git
```

Or add to your `requirements.txt`:

```
nvmeof-client @ git+https://github.com/truenas/truenas_pynvmeof_client.git
```

For a specific version/tag:

```bash
pip install git+https://github.com/truenas/truenas_pynvmeof_client.git@v1.0.0
```

### From Local Clone

For development:

```bash
git clone https://github.com/truenas/truenas_pynvmeof_client.git
cd nvmeof_client
pip install -e .
```

### With Optional Dependencies

Install with development tools:

```bash
pip install "nvmeof-client[dev] @ git+https://github.com/truenas/truenas_pynvmeof_client.git"
```

Install with testing tools only:

```bash
pip install "nvmeof-client[test] @ git+https://github.com/truenas/truenas_pynvmeof_client.git"
```

## Quick Start

### Basic Connection

```python
from nvmeof_client import NVMeoFClient

# Connect to NVMe-oF target
client = NVMeoFClient("192.168.1.100", "nqn.2024-01.com.example:nvme:subsystem1")
client.connect()

# Get controller info
controller_info = client.get_controller_info()
print(f"Model: {controller_info.model_number}")
print(f"Serial: {controller_info.serial_number}")

# Disconnect
client.disconnect()
```

### Using Context Manager

```python
with NVMeoFClient("192.168.1.100", "nqn.2024-01.com.example:subsystem1") as client:
    # List namespaces
    namespaces = client.list_namespaces()
    print(f"Available namespaces: {namespaces}")

    # Read from namespace
    data = client.read_data(nsid=1, lba=0, block_count=1)
    print(f"Read {len(data)} bytes")
```

### Discovery

```python
# Connect to discovery service (subsystem_nqn can be omitted or set to discovery NQN)
client = NVMeoFClient("192.168.1.100", "nqn.2014-08.org.nvmexpress.discovery", port=8009)
client.connect()

# Get discovery log
entries = client.get_discovery_log()
for entry in entries:
    print(f"Subsystem: {entry.subsystem_nqn}")
    print(f"  Address: {entry.traddr}:{entry.trsvcid}")
    print(f"  Type: {entry.subsystem_type}")
```

### ANA Multipath

```python
# Connect to controller A
client = NVMeoFClient("192.168.1.100", "nqn.2024-01.com.example:subsystem1")
client.connect()

# Get ANA state
ana_log = client.get_ana_log_page()
for group in ana_log.groups:
    print(f"ANA Group {group.ana_group_id}: {group.ana_state}")
    print(f"  Namespaces: {group.namespace_ids}")

# Monitor for ANA changes via Async Events
def handle_aen(event):
    if event.event_type == "ANA_CHANGE":
        print("ANA state changed!")
        # Re-read ANA log page
        ana_log = client.get_ana_log_page()

client.enable_async_events(callback=handle_aen)
```

### I/O Operations

```python
# Write data (must be block-aligned, typically 512 bytes)
data = b"Hello, NVMe!".ljust(512, b'\x00')  # Pad to 512 bytes
client.write_data(nsid=1, lba=0, data=data)

# Read data back
read_data = client.read_data(nsid=1, lba=0, block_count=1)
print(read_data[:12])  # b"Hello, NVMe!"

# Flush
client.flush_namespace(nsid=1)
```

## API Reference

### Main Client Class

#### `NVMeoFClient`

The main client class for NVMe-oF operations.

**Constructor:**

```python
NVMeoFClient(host, subsystem_nqn=None, port=4420, timeout=None, host_nqn=None)
```
- `host` - Target hostname or IP address
- `subsystem_nqn` - Subsystem NQN to connect to (if None, connects to discovery service)
- `port` - Target port (default: 4420)
- `timeout` - Default timeout for operations in seconds
- `host_nqn` - Host NQN (auto-generated if None)

**Methods:**

Connection Management:
- `connect(subsystem_nqn=None)` - Connect to target using subsystem_nqn from constructor (can override with parameter)
- `disconnect()` - Disconnect from target

High-Level Information APIs (return structured objects):
- `get_controller_info()` - Get controller information (returns ControllerInfo object)
- `get_namespace_info(nsid)` - Get namespace information (returns NamespaceInfo object)
- `list_namespaces()` - List all active namespace IDs

Low-Level Identify Commands (return raw dicts):
- `identify_controller()` - Get controller information (returns dict)
- `identify_namespace(nsid)` - Get namespace information (returns dict)

I/O Operations:
- `read_data(nsid, lba, block_count)` - Read data from namespace
- `write_data(nsid, lba, data)` - Write data to namespace
- `write_zeroes(nsid, lba, block_count)` - Write zeroes to logical blocks
- `flush_namespace(nsid)` - Flush data to persistent media

Log Pages and Features:
- `get_log_page(log_id, nsid=0xFFFFFFFF)` - Get log page
- `get_ana_log_page()` - Get ANA log page
- `get_discovery_log()` - Get discovery log
- `enable_async_events(callback)` - Enable async event notifications
- `get_features(feature_id, nsid=0)` - Get feature
- `set_features(feature_id, value, nsid=0)` - Set feature

### Models

All data structures are available in `nvmeof_client.models`:

- `ControllerInfo` - Controller identification data
- `NamespaceInfo` - Namespace identification data
- `ANALogPage`, `ANAGroupDescriptor`, `ANAState` - ANA structures
- `DiscoveryEntry` - Discovery log entry
- `AsyncEvent`, `AsyncEventType` - Async event structures

### Exceptions

All exceptions inherit from `NVMeoFError`:

- `NVMeoFConnectionError` - Connection-related errors
- `NVMeoFTimeoutError` - Timeout errors
- `CommandError` - NVMe command failures
- `ProtocolError` - Protocol violations

## Testing

### Run Unit Tests

```bash
# Install with test dependencies
pip install -e ".[test]"

# Run unit tests only
pytest tests/unit/

# Run with coverage
pytest --cov=nvmeof_client tests/unit/
```

### Run Integration Tests

Integration tests require a running NVMe-oF target.

```bash
# Set target configuration
export NVME_TARGET_IP=192.168.1.100
export NVME_TARGET_PORT=4420
export NVME_SUBSYSTEM_NQN=nqn.2024-01.com.example:subsystem1

# Run integration tests
pytest tests/integration/

# Skip manual tests
pytest tests/integration/ -m "not manual"
```

### Run All Tests in Parallel

```bash
pytest -n auto tests/
```

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/truenas/truenas_pynvmeof_client.git
cd nvmeof_client

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Code Quality

```bash
# Format code
black nvmeof_client/ tests/

# Lint code
ruff check nvmeof_client/ tests/

# Type check
mypy nvmeof_client/
```

### Run Tests

```bash
# All tests
pytest

# Specific test file
pytest tests/unit/test_client.py

# Specific test
pytest tests/unit/test_client.py::TestNVMeoFClient::test_connect

# With verbose output
pytest -v

# With logs
pytest -s --log-cli-level=DEBUG
```

## Architecture

```
nvmeof_client/
├── __init__.py           # Public API exports
├── client.py             # Main NVMeoFClient class
├── models.py             # Data structures
├── exceptions.py         # Exception classes
├── parsers/              # Protocol parsers
│   ├── __init__.py
│   ├── base.py
│   ├── response.py
│   ├── controller.py
│   ├── namespace.py
│   ├── discovery.py
│   ├── ana.py
│   └── async_event.py
└── protocol/             # Protocol structures
    ├── __init__.py
    ├── pdu.py
    ├── commands.py
    ├── io_commands.py
    └── utils.py
```

## Compatibility

### Target Implementations

Tested with:
- Linux Kernel NVMe target (`nvmet`)
- SPDK NVMe-oF target

### Transport Protocols

- ✅ TCP - Fully supported
- ⚠️  RDMA - Planned (not yet implemented)
- ⚠️  FC - Planned (not yet implemented)

### NVMe Features

- ✅ Basic I/O (Read, Write, Flush)
- ✅ Admin commands (Identify, Get/Set Features)
- ✅ Discovery service
- ✅ Asymmetric Namespace Access (ANA)
- ✅ Asynchronous Event Notifications (AEN)
- ✅ Persistent Reservations
- ✅ Keep-Alive
- ⚠️  In-band Authentication - Planned

## Troubleshooting

### Connection Refused

```python
NVMeoFConnectionError: [Errno 111] Connection refused
```

**Solution:** Ensure NVMe-oF target is running and accessible:

```bash
# Check target is listening
sudo netstat -tlnp | grep 4420

# Test network connectivity
ping 192.168.1.100
telnet 192.168.1.100 4420
```

### Command Timeout

```python
NVMeoFTimeoutError: Command timed out after 30 seconds
```

**Solution:** Increase timeout or check target responsiveness:

```python
client = NVMeoFClient("192.168.1.100", "nqn.2024-01.com.example:subsystem1", timeout=60)  # Increase to 60 seconds
```

### Invalid Subsystem NQN

```python
CommandError: Connect command failed: Invalid subsystem
```

**Solution:** Verify subsystem NQN is correct:

```bash
# List available subsystems (via discovery)
nvme discover -t tcp -a 192.168.1.100 -s 8009
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run the test suite
5. Submit a pull request

## License

This project is licensed under the GNU Lesser General Public License v3.0 or later (LGPL-3.0-or-later).

See the [LICENSE](LICENSE) file for the full license text.

### What This Means

- ✅ You can use this library in commercial applications
- ✅ You can modify this library
- ✅ If you modify the library itself, you must share those modifications under LGPL
- ✅ Your application code that uses this library can remain proprietary
- ℹ️  This is a permissive library license - your application doesn't need to be open source

## Support

- Issues: https://github.com/truenas/truenas_pynvmeof_client/issues
- Documentation: https://github.com/truenas/truenas_pynvmeof_client/blob/main/README.md

## References

- [NVMe Base Specification](https://nvmexpress.org/specifications/)
- [NVMe over Fabrics Specification](https://nvmexpress.org/specifications/)
- [NVMe/TCP Transport Specification](https://nvmexpress.org/specifications/)
