"""
NVMe Controller identification data parsing.

This module handles parsing of NVMe Identify Controller data structures
as defined in the NVMe Base Specification.
"""

from typing import Any
from .base import BaseParser


class ControllerDataParser(BaseParser):
    """Parser for NVMe Identify Controller data structures."""

    @classmethod
    def parse(cls, data: bytes) -> dict[str, Any]:
        """
        Parse NVMe Identify Controller data structure.

        Args:
            data: 4096-byte identify controller data structure

        Returns:
            Dictionary containing parsed controller information

        Reference: NVM Express Base Specification Rev 2.1, Figure 275
        "Identify Controller Data Structure"
        """
        cls.validate_data_length(data, 4096, "Identify Controller data")

        # Parse the Identify Controller data structure (4096 bytes)
        # All integer fields are little-endian
        parsed = {}

        # Bytes 0-1: PCI Vendor ID (VID)
        parsed['vid'] = cls.safe_unpack('<H', data, 0)[0]

        # Bytes 2-3: PCI Subsystem Vendor ID (SSVID)
        parsed['ssvid'] = cls.safe_unpack('<H', data, 2)[0]

        # Bytes 4-23: Serial Number (SN) - ASCII string, space padded
        parsed['sn'] = cls.extract_string(data, 4, 20).strip()

        # Bytes 24-63: Model Number (MN) - ASCII string, space padded
        parsed['mn'] = cls.extract_string(data, 24, 40).strip()

        # Bytes 64-71: Firmware Revision (FR) - ASCII string, space padded
        parsed['fr'] = cls.extract_string(data, 64, 8).strip()

        # Byte 72: Recommended Arbitration Burst (RAB)
        parsed['rab'] = data[72]

        # Bytes 73-75: IEEE OUI Identifier (IEEE)
        parsed['ieee'] = cls.safe_unpack('<I', data, 73)[0] & 0xFFFFFF  # Only 24 bits

        # Byte 76: Controller Multi-Path I/O and Namespace Sharing Capabilities (CMIC)
        parsed['cmic'] = data[76]

        # Byte 77: Maximum Data Transfer Size (MDTS)
        parsed['mdts'] = data[77]

        # Bytes 78-79: Controller ID (CNTLID)
        parsed['cntlid'] = cls.safe_unpack('<H', data, 78)[0]

        # Bytes 80-83: Version (VER)
        parsed['ver'] = cls.safe_unpack('<L', data, 80)[0]

        # Bytes 84-87: RTD3 Resume Latency (RTD3R)
        parsed['rtd3r'] = cls.safe_unpack('<L', data, 84)[0]

        # Bytes 88-91: RTD3 Entry Latency (RTD3E)
        parsed['rtd3e'] = cls.safe_unpack('<L', data, 88)[0]

        # Bytes 92-95: Optional Asynchronous Events Supported (OAES)
        parsed['oaes'] = cls.safe_unpack('<L', data, 92)[0]

        # Bytes 96-99: Controller Attributes (CTRATT)
        parsed['ctratt'] = cls.safe_unpack('<L', data, 96)[0]

        # Bytes 100-101: Read Recovery Levels Supported (RRLS)
        parsed['rrls'] = cls.safe_unpack('<H', data, 100)[0]

        # Byte 106: Controller Type (CNTRLTYPE)
        parsed['cntrltype'] = data[106]

        # Bytes 112-127: FRU GUID (FGUID)
        parsed['fguid'] = cls.bytes_to_hex_string(data[112:128])

        # Bytes 128-129: Command Retry Delay Time 1 (CRDT1)
        parsed['crdt1'] = cls.safe_unpack('<H', data, 128)[0]

        # Bytes 130-131: Command Retry Delay Time 2 (CRDT2)
        parsed['crdt2'] = cls.safe_unpack('<H', data, 130)[0]

        # Bytes 132-133: Command Retry Delay Time 3 (CRDT3)
        parsed['crdt3'] = cls.safe_unpack('<H', data, 132)[0]

        # Bytes 256-257: Optional Admin Command Support (OACS)
        parsed['oacs'] = cls.safe_unpack('<H', data, 256)[0]

        # Byte 258: Abort Command Limit (ACL)
        parsed['acl'] = data[258]

        # Byte 259: Asynchronous Event Request Limit (AERL)
        parsed['aerl'] = data[259]

        # Byte 260: Firmware Updates (FRMW)
        parsed['frmw'] = data[260]

        # Byte 261: Log Page Attributes (LPA)
        parsed['lpa'] = data[261]

        # Byte 262: Error Log Page Entries (ELPE)
        parsed['elpe'] = data[262]

        # Byte 263: Number of Power States Support (NPSS)
        parsed['npss'] = data[263]

        # Byte 264: Admin Vendor Specific Command Configuration (AVSCC)
        parsed['avscc'] = data[264]

        # Byte 265: Autonomous Power State Transition Attributes (APSTA)
        parsed['apsta'] = data[265]

        # Bytes 266-267: Warning Composite Temperature Threshold (WCTEMP)
        parsed['wctemp'] = cls.safe_unpack('<H', data, 266)[0]

        # Bytes 268-269: Critical Composite Temperature Threshold (CCTEMP)
        parsed['cctemp'] = cls.safe_unpack('<H', data, 268)[0]

        # Bytes 270-271: Maximum Time for Firmware Activation (MTFA)
        parsed['mtfa'] = cls.safe_unpack('<H', data, 270)[0]

        # Bytes 272-275: Host Memory Buffer Preferred Size (HMPRE)
        parsed['hmpre'] = cls.safe_unpack('<L', data, 272)[0]

        # Bytes 276-279: Host Memory Buffer Minimum Size (HMMIN)
        parsed['hmmin'] = cls.safe_unpack('<L', data, 276)[0]

        # Bytes 280-295: Total NVM Capacity (TNVMCAP) - 128-bit little-endian
        parsed['tnvmcap'] = cls.safe_unpack('<QQ', data, 280)

        # Bytes 296-311: Unallocated NVM Capacity (UNVMCAP) - 128-bit little-endian
        parsed['unvmcap'] = cls.safe_unpack('<QQ', data, 296)

        # Bytes 312-315: Replay Protected Memory Block Support (RPMBS)
        parsed['rpmbs'] = cls.safe_unpack('<L', data, 312)[0]

        # Bytes 316-317: Extended Device Self-test Time (EDSTT)
        parsed['edstt'] = cls.safe_unpack('<H', data, 316)[0]

        # Byte 318: Device Self-test Options (DSTO)
        parsed['dsto'] = data[318]

        # Byte 319: Firmware Update Granularity (FWUG)
        parsed['fwug'] = data[319]

        # Bytes 320-321: Keep Alive Support (KAS)
        parsed['kas'] = cls.safe_unpack('<H', data, 320)[0]

        # Bytes 322-323: Host Controlled Thermal Management Attributes (HCTMA)
        parsed['hctma'] = cls.safe_unpack('<H', data, 322)[0]

        # Bytes 324-325: Minimum Thermal Management Temperature (MNTMT)
        parsed['mntmt'] = cls.safe_unpack('<H', data, 324)[0]

        # Bytes 326-327: Maximum Thermal Management Temperature (MXTMT)
        parsed['mxtmt'] = cls.safe_unpack('<H', data, 326)[0]

        # Bytes 328-331: Sanitize Capabilities (SANICAP)
        parsed['sanicap'] = cls.safe_unpack('<L', data, 328)[0]

        # Bytes 332-335: Host Memory Buffer Minimum Descriptor Entry Size (HMMINDS)
        parsed['hmminds'] = cls.safe_unpack('<L', data, 332)[0]

        # Bytes 336-337: Host Memory Buffer Maximum Descriptors (HMMAXD)
        parsed['hmmaxd'] = cls.safe_unpack('<H', data, 336)[0]

        # Bytes 338-339: NVM Set Identifier Maximum (NSETIDMAX)
        parsed['nsetidmax'] = cls.safe_unpack('<H', data, 338)[0]

        # Bytes 340-341: Endurance Group Identifier Maximum (ENDGIDMAX)
        parsed['endgidmax'] = cls.safe_unpack('<H', data, 340)[0]

        # Byte 342: ANA Transition Time (ANATT)
        parsed['anatt'] = data[342]

        # Byte 343: Asymmetric Namespace Access Capabilities (ANACAP)
        parsed['anacap'] = data[343]

        # Bytes 344-347: ANA Group Identifier Maximum (ANAGRPMAX)
        parsed['anagrpmax'] = cls.safe_unpack('<L', data, 344)[0]

        # Bytes 348-351: Number of ANA Group Identifiers (NANAGRPID)
        parsed['nanagrpid'] = cls.safe_unpack('<L', data, 348)[0]

        # Bytes 352-355: Persistent Event Log Size (PELS)
        parsed['pels'] = cls.safe_unpack('<L', data, 352)[0]

        # Byte 512: Submission Queue Entry Size (SQES)
        parsed['sqes'] = data[512]

        # Byte 513: Completion Queue Entry Size (CQES)
        parsed['cqes'] = data[513]

        # Bytes 514-515: Maximum Outstanding Commands (MAXCMD)
        parsed['maxcmd'] = cls.safe_unpack('<H', data, 514)[0]

        # Bytes 516-519: Number of Namespaces (NN)
        parsed['nn'] = cls.safe_unpack('<L', data, 516)[0]

        # Bytes 520-521: Optional NVM Command Support (ONCS)
        parsed['oncs'] = cls.safe_unpack('<H', data, 520)[0]

        # Bytes 522-523: Fused Operation Support (FUSES)
        parsed['fuses'] = cls.safe_unpack('<H', data, 522)[0]

        # Byte 524: Format NVM Attributes (FNA)
        parsed['fna'] = data[524]

        # Byte 525: Volatile Write Cache (VWC)
        parsed['vwc'] = data[525]

        # Bytes 526-527: Atomic Write Unit Normal (AWUN)
        parsed['awun'] = cls.safe_unpack('<H', data, 526)[0]

        # Bytes 528-529: Atomic Write Unit Power Fail (AWUPF)
        parsed['awupf'] = cls.safe_unpack('<H', data, 528)[0]

        # Byte 530: NVM Vendor Specific Command Configuration (NVSCC)
        parsed['nvscc'] = data[530]

        # Byte 531: Namespace Write Protection Capabilities (NWPC)
        parsed['nwpc'] = data[531]

        # Bytes 532-533: Atomic Compare & Write Unit (ACWU)
        parsed['acwu'] = cls.safe_unpack('<H', data, 532)[0]

        # Bytes 536-539: SGL Support (SGLS)
        parsed['sgls'] = cls.safe_unpack('<L', data, 536)[0]

        # Bytes 540-543: Maximum Number of Allowed Namespaces (MNAN)
        parsed['mnan'] = cls.safe_unpack('<L', data, 540)[0]

        # Bytes 768-1023: NVM Subsystem NVMe Qualified Name (SUBNQN) - NQN string
        parsed['subnqn'] = cls.extract_string(data, 768, 256).strip()

        # Bytes 1792-1795: I/O Queue Command Capsule Supported Size (IOCCSZ)
        parsed['ioccsz'] = cls.safe_unpack('<L', data, 1792)[0]

        # Bytes 1796-1799: I/O Queue Response Capsule Supported Size (IORCSZ)
        parsed['iorcsz'] = cls.safe_unpack('<L', data, 1796)[0]

        # Bytes 1800-1801: In Capsule Data Offset (ICDOFF)
        parsed['icdoff'] = cls.safe_unpack('<H', data, 1800)[0]

        # Byte 1802: Controller Attributes (CTRATTR)
        parsed['ctrattr'] = data[1802]

        # Byte 1803: Maximum SGL Data Block Descriptors (MSDBD)
        parsed['msdbd'] = data[1803]

        return parsed
