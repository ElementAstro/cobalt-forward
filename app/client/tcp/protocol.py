import struct
from typing import Optional, Union, Dict, Tuple, Any
import zlib
from loguru import logger
import time
import json
from enum import IntEnum


class PacketType(IntEnum):
    """Packet type enumeration"""
    DATA = 0       # Regular data
    HEARTBEAT = 1  # Heartbeat packet
    CONTROL = 2    # Control command
    ERROR = 3      # Error information
    ACK = 4        # Acknowledgment packet


class PacketProtocol:
    """Packet protocol handler class

    Packet format:
    +--------+--------+--------+----------+----------------+
    | Magic  | Type   | Compr. | Data Len | Data           |
    | 2 bytes| 1 byte | 1 byte | 4 bytes  | Variable       |
    +--------+--------+--------+----------+----------------+

    Magic: Fixed as 0xCB55 (CoBalt 55)
    Type: Packet type, see PacketType
    Compression flag: 0=uncompressed, 1=zlib compressed
    Data length: Length of data, excludes header
    Data: Actual data, JSON or binary
    """

    MAGIC = 0xCB55  # Magic number
    HEADER_SIZE = 8  # Header size
    HEADER_FORMAT = '>HBBl'  # Header format for struct.pack/unpack

    def __init__(self, compression_threshold: int = 1024, compression_level: int = 6):
        """Initialize protocol handler

        Args:
            compression_threshold: Threshold for enabling compression
            compression_level: Compression level (0-9)
        """
        self.compression_threshold = compression_threshold
        self.compression_level = compression_level
        logger.debug(
            f"Initialized PacketProtocol with compression_threshold={compression_threshold}, compression_level={compression_level}")

    @staticmethod
    def calculate_checksum(data: bytes) -> int:
        """Calculate checksum for data verification

        Args:
            data: Data to calculate checksum for

        Returns:
            Checksum value
        """
        # Simple CRC32 checksum
        return zlib.crc32(data) & 0xFFFFFFFF

    def pack(self, data: Union[bytes, Dict[str, Any]], packet_type: PacketType = PacketType.DATA) -> bytes:
        """Pack data into binary format

        Args:
            data: Data to send, can be bytes or dictionary
            packet_type: Packet type

        Returns:
            Packed binary data
        """
        # Convert dictionary to JSON
        if isinstance(data, dict):
            data = json.dumps(data).encode('utf-8')

        # Determine if compression should be used
        compressed = 0
        original_len = len(data)  # Store original length for logging
        if original_len >= self.compression_threshold:
            compressed_data = zlib.compress(data, self.compression_level)
            # Only use compressed data if it's actually smaller
            if len(compressed_data) < original_len:
                data = compressed_data
                compressed = 1
                logger.debug(
                    f"Compressed data from {original_len} to {len(data)} bytes")

        # Construct packet header
        header = struct.pack(self.HEADER_FORMAT, self.MAGIC,
                             packet_type, compressed, len(data))

        # Return complete packet
        return header + data

    def unpack(self, data: bytes) -> Tuple[PacketType, bytes, int]:
        """Unpack binary data

        Args:
            data: Received binary data

        Returns:
            Tuple (packet_type, data_body, header_size)

        Raises:
            ValueError: If data format is invalid
        """
        if len(data) < self.HEADER_SIZE:
            raise ValueError(
                f"Data too short: {len(data)} bytes, expected at least {self.HEADER_SIZE}")

        # Parse header
        magic, ptype, compressed, length = struct.unpack(
            self.HEADER_FORMAT, data[:self.HEADER_SIZE])

        # Validate magic number
        if magic != self.MAGIC:
            raise ValueError(
                f"Invalid magic number: 0x{magic:04X}, expected 0x{self.MAGIC:04X}")

        # Check data length
        if len(data) < self.HEADER_SIZE + length:
            raise ValueError(
                f"Incomplete data: got {len(data)}, expected {self.HEADER_SIZE + length}")

        # Extract data body
        body = data[self.HEADER_SIZE:self.HEADER_SIZE + length]

        # Decompress if needed
        if compressed:
            try:
                body = zlib.decompress(body)
                logger.debug(
                    f"Decompressed data from {length} to {len(body)} bytes")
            except zlib.error as e:
                logger.error(f"Failed to decompress data: {e}")
                raise ValueError(f"Decompression failed: {e}")

        return PacketType(ptype), body, self.HEADER_SIZE + length

    def try_parse_json(self, data: bytes) -> Union[Dict[str, Any], bytes]:
        """Try to parse data as JSON, return original data if failed"""
        try:
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return data

    def create_packet(self, packet_type: PacketType, data: Union[bytes, Dict[str, Any]]) -> bytes:
        """Create a packet with the specified type and data

        Args:
            packet_type: Type of packet to create
            data: Data to include in packet

        Returns:
            Complete packet as bytes
        """
        return self.pack(data, packet_type)

    def create_heartbeat(self) -> bytes:
        """Create heartbeat packet"""
        timestamp = time.time()
        return self.pack({'timestamp': timestamp}, PacketType.HEARTBEAT)

    def create_ack(self, message_id: Optional[str] = None) -> bytes:
        """Create acknowledgment packet"""
        data: Dict[str, Any] = {'timestamp': time.time()}
        if message_id:
            data['message_id'] = message_id
        return self.pack(data, PacketType.ACK)

    def create_error(self, error_code: int, error_message: str) -> bytes:
        """Create error packet"""
        return self.pack({
            'error_code': error_code,
            'error_message': error_message,
            'timestamp': time.time()
        }, PacketType.ERROR)

    def create_control_packet(self, command: str, params: Optional[Dict[str, Any]] = None) -> bytes:
        """Create control packet

        Args:
            command: Control command
            params: Optional command parameters

        Returns:
            Control packet as bytes
        """
        data: Dict[str, Any] = {
            'command': command,
            'timestamp': time.time()
        }
        if params:
            data['params'] = params

        return self.pack(data, PacketType.CONTROL)
