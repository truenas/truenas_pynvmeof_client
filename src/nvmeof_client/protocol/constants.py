"""
NVMe-oF Protocol Constants

All protocol constants and magic numbers used in NVMe-oF TCP implementation.
"""

# Protocol Constants
# Reference: NVMe-oF TCP Transport Specification Section 3.2
NVMEOF_TCP_PORT = 4420  # Standard NVMe-oF TCP port
NVME_IOSQES = 6  # I/O Submission Queue Entry Size (64 bytes = 2^6)
NVME_IOCQES = 4  # I/O Completion Queue Entry Size (16 bytes = 2^4)

# NVMe-oF TCP Protocol Constants
NVME_TCP_PFV_1_0 = 0x0000  # Protocol Format Version 1.0
NVME_TCP_HDR_DIGEST_ENABLE = 0x01  # Header digest enable flag
NVME_TCP_DATA_DIGEST_ENABLE = 0x02  # Data digest enable flag
NVME_TCP_ADMIN_CCSZ = 8192  # Admin Command Capsule Size
NVME_TCP_DIGEST_LENGTH = 4  # Digest length in bytes

# NVMe Command Flags
# Reference: NVM Express Base Specification Rev 2.1, Section 4.1
NVME_CMD_FLAGS_SGL = 0x40  # PSDT=01b (SGL for Data Transfer)
NVME_CMD_FLAGS_PRP = 0x00  # PSDT=00b (PRP for Data Transfer)

# Controller Configuration Register Bit Masks
# Reference: NVM Express Base Specification Rev 2.1, Section 3.1.4
CC_EN_MASK = 0x1      # Enable bit mask
CC_CSS_MASK = 0x7     # Command Set Selection mask (3 bits)
CC_MPS_MASK = 0xF     # Memory Page Size mask (4 bits)
CC_AMS_MASK = 0x7     # Arbitration Mechanism mask (3 bits)
CC_IOSQES_MASK = 0xF  # I/O Submission Queue Entry Size mask (4 bits)
CC_IOCQES_MASK = 0xF  # I/O Completion Queue Entry Size mask (4 bits)

# Status Code Masks
NVME_STATUS_CODE_MASK = 0xFF    # Status code is lower 8 bits
NVME_COMMAND_ID_MASK = 0xFFFF   # Command ID is 16 bits

# NVMe Data Structure Sizes
NVME_COMMAND_SIZE = 64          # NVMe command size in bytes
NVME_IDENTIFY_DATA_SIZE = 4096  # Identify Controller data structure size
NVME_DISCOVERY_LOG_SIZE = 3072  # Discovery log initial request size
NVME_CONNECT_DATA_SIZE = 1024   # Fabric Connect data structure size

# Discovery Log Page Constants
# Reference: NVMe-oF Base Specification Section 5.4 "Discovery Log Page"
NVME_DISCOVERY_LOG_HEADER_SIZE = 16         # Discovery log page header size
NVME_DISCOVERY_LOG_ENTRY_SIZE = 1024        # Discovery log entry size
NVME_DISCOVERY_LOG_ENTRIES_OFFSET = 1024    # Offset where entries start

# Discovery Log Entry Field Constants
# Reference: NVMe-oF Base Specification Table 51 "Discovery Log Entry Format"
NVME_TRTYPE_TCP = 3                         # TCP transport type
NVME_ADRFAM_IPV4 = 1                        # IPv4 address family
NVME_SUBTYPE_DISCOVERY = 3                  # Current discovery subsystem
NVME_SUBTYPE_NVME = 2                       # NVMe subsystem

# NVMe I/O Command Constants
# Reference: NVM Command Set Specification Section 4 "I/O Commands"
NVME_SECTOR_SIZE = 512                      # Standard sector size in bytes (2^9)
NVME_MAX_IO_SIZE = 65536                    # Maximum I/O size in blocks per command
NVME_TRANSPORT_SGL_TYPE = 0x04              # Transport specific SGL descriptor type (NVMe-oF)
NVME_TRANSPORT_SGL_SUBTYPE = 0x00           # Transport SGL Data Block sub type
NVME_SGL_SEGMENT = 0x02                     # SGL Segment descriptor type
NVME_SGL_LAST_SEGMENT = 0x03                # SGL Last Segment descriptor type

# NVMe Identify Namespace Data Structure Constants
# Reference: NVM Express Base Specification Section 5.15 "Identify command"
NVME_FLBAS_FORMAT_INDEX_MASK = 0xF      # FLBAS format index mask (lower 4 bits)
NVME_LBADS_MIN_VALUE = 9                # Minimum LBADS value (512 bytes = 2^9)
NVME_LBADS_MAX_VALUE = 16               # Maximum LBADS value (64KB = 2^16)
NVME_NAMESPACE_VS_OFFSET = 3584         # Vendor Specific field offset in Identify Namespace

# LBA Format Data Structure Constants
# Reference: NVM Express Base Specification Figure 273 "LBA Format Data Structure"
NVME_LBAF_COUNT = 16                    # Number of LBA format entries (LBAF0-LBAF15)
NVME_LBAF_ARRAY_OFFSET = 128            # LBA format array offset in Identify Namespace
NVME_LBAF_ENTRY_SIZE = 4                # LBA format entry size in bytes
NVME_LBAF_MS_MASK = 0xFFFF              # Metadata Size mask (bits 15:0)
NVME_LBAF_LBADS_MASK = 0xFF             # LBA Data Size mask (bits 23:16)
NVME_LBAF_LBADS_SHIFT = 16              # LBA Data Size bit shift
NVME_LBAF_RP_MASK = 0x3                 # Relative Performance mask (bits 25:24)
NVME_LBAF_RP_SHIFT = 24                 # Relative Performance bit shift

# PDU Header Sizes for NVMe-oF TCP
# Reference: NVMe-oF TCP Transport Specification Section 3.3 "PDU Format"
NVMEOF_TCP_PDU_BASIC_HEADER_LEN = 8    # Basic PDU header length (all PDU types)

# ICREQ/ICRESP PDU Sizes
# Reference: NVMe-oF TCP Transport Specification Rev 1.2, Section 3.6.2.2-3, Figures 26-27
# "Header Length (HLEN): Fixed length of 128 bytes (80h)."
# "PDU Length (PLEN): Fixed length of 128 bytes (80h)."
NVMEOF_TCP_ICREQ_HEADER_LEN = 128      # ICREQ header length (extended header)
NVMEOF_TCP_ICREQ_TOTAL_LEN = 128       # ICREQ total PDU length
NVMEOF_TCP_CMD_HEADER_LEN = 72         # Command PDU header length
NVMEOF_TCP_CMD_PDO = 72                # Command PDU data offset

# NVMe Command Offsets
NVME_CMD_NSID_OFFSET = 4               # Namespace ID offset in command
NVME_CMD_SGL1_OFFSET = 32              # SGL Entry 1 offset in command
NVME_CMD_SGL1_TYPE_OFFSET = 32         # SGL Type field offset
NVME_CMD_LBA_OFFSET = 40               # LBA offset in I/O commands
NVME_CMD_BLOCK_COUNT_OFFSET = 48       # Block count offset in I/O commands

# Default Values
NVME_DEFAULT_MAX_ENTRIES = 128         # Default queue size
NVME_IOSQES_64_BYTES = 6              # 64-byte submission queue entries (2^6)
NVME_IOCQES_16_BYTES = 4              # 16-byte completion queue entries (2^4)
