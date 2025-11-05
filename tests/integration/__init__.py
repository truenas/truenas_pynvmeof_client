"""
Integration Tests

Tests that require a live NVMe-oF target to validate end-to-end functionality.
These tests verify real protocol communication and target interactions.

Requirements:
- Running NVMe-oF target (e.g., Linux kernel target, SPDK)
- Network connectivity to target
- Proper target configuration

Environment Variables:
- NVMEOF_TARGET_HOST: Target hostname/IP (default: localhost)
- NVMEOF_TARGET_PORT: Target port (default: 4420)
- NVMEOF_TARGET_NQN: Target subsystem NQN
- NVMEOF_TARGET_TRANSPORT: Transport type (default: tcp)
- NVMEOF_TARGET_TIMEOUT: Connection timeout (default: 10.0)
- NVMEOF_SKIP_INTEGRATION: Skip integration tests if set

Example for TrueNAS target:
- Host: 192.168.56.115
- Port: 4420
- NQN: nqn.2011-06.com.truenas:uuid:68bf9433-63ef-49f5-a921-4c0f8190fd94:foo1
"""
