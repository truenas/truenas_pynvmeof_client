# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-11-04

### Added
- Initial release of NVMe-oF TCP client library
- Pure Python implementation using only standard library
- Full NVMe-oF TCP transport protocol support
- Synchronous API for all operations
- Admin command support (Identify, Get/Set Features, etc.)
- I/O command support (Read, Write, Flush)
- Discovery service support
- Asymmetric Namespace Access (ANA) support
- Asynchronous Event Notification (AEN) handling
- Persistent Reservations support
- Keep-Alive support
- Comprehensive test suite (unit and integration tests)
- Support for both Linux kernel and SPDK NVMe targets

### Notes
- Tested with Python 3.8+
- Requires Linux operating system
- TCP transport only (RDMA planned for future release)
