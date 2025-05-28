from enum import IntEnum
import zlib
import json
import struct
import time
from typing import Tuple, Union, Dict, Any, Optional
from loguru import logger


class PacketType(IntEnum):
    """Packet type enumeration"""
    DATA = 0       # Regular data
    HEARTBEAT = 1  # Heartbeat packet
    CONTROL = 2    # Control command
    ERROR = 3      # Error message
    ACK = 4        # Acknowledgment packet
    RELIABLE = 5   # Reliable transmission packet
    FRAGMENT = 6   # Fragment packet


class PacketProtocol:
    """Packet protocol handler class

    Packet format:
    +--------+--------+--------+---------+------+----------+----------------+
    | Magic  | Type   | Compress| Sequence| Flags| Data Len | Data           |
    | 2 bytes| 1 byte | 1 byte  | 4 bytes | 1 byte| 4 bytes  | Variable length|
    +--------+--------+--------+---------+------+----------+----------------+

    Magic: Fixed value 0xCB55 (CoBalt 55)
    Type: Packet type, see PacketType
    Compress: Compression flag: 0=uncompressed, 1=zlib compressed
    Sequence: Packet sequence number, used for reliable transmission and ordering
    Flags: Bit flags field, used for various controls
    Data Len: Data length, excluding the header
    Data: Actual data, JSON or binary
    """

    MAGIC = 0xCB55  # Magic number
    HEADER_SIZE = 13  # Header size
    HEADER_FORMAT = '>HBBLBl'  # Header format for struct.pack/unpack

    def __init__(self, compression_threshold: int = 1024, compression_level: int = 6):
        """Initialize the packet protocol handler

        Args:
            compression_threshold: Compression threshold (bytes), data larger than this will be compressed
            compression_level: Compression level (0-9)
        """
        self.compression_threshold = compression_threshold
        self.compression_level = compression_level
        self.next_sequence = 0

    def get_next_sequence(self) -> int:
        """Get the next sequence number"""
        seq = self.next_sequence
        # Wrap around 32-bit integer
        self.next_sequence = (self.next_sequence + 1) & 0xFFFFFFFF
        return seq

    @staticmethod
    def calculate_checksum(data: bytes) -> int:
        """Calculate data checksum

        Args:
            data: Data to calculate checksum for

        Returns:
            Checksum value
        """
        checksum = 0
        for i in range(0, len(data), 2):
            if i + 1 < len(data):
                checksum += (data[i] << 8) + data[i + 1]
            else:
                checksum += data[i] << 8

        while checksum > 0xFFFF:
            checksum = (checksum & 0xFFFF) + (checksum >> 16)

        return checksum ^ 0xFFFF  # Invert

    def pack(self, data: Union[bytes, Dict[str, Any]], packet_type: PacketType = PacketType.DATA,
             sequence: Optional[int] = None, flags: int = 0) -> bytes:
        """Pack data into a packet

        Args:
            data: Data to pack (bytes or dictionary)
            packet_type: Packet type
            sequence: Sequence number (auto-generated if None)
            flags: Flags field

        Returns:
            Packed packet data
        """
        # If data is a dictionary, convert to JSON
        if isinstance(data, dict):
            data_bytes = json.dumps(data).encode('utf-8')
        else:
            data_bytes = data

        compressed = 0
        final_data = data_bytes

        # If compression is enabled and data size exceeds threshold, compress data
        if (self.compression_threshold > 0 and
                len(data_bytes) > self.compression_threshold):
            try:
                compressed_data_bytes = zlib.compress(
                    data_bytes, self.compression_level)

                # Only use compressed data if it's smaller
                if len(compressed_data_bytes) < len(data_bytes):
                    final_data = compressed_data_bytes
                    compressed = 1
                    logger.debug(
                        f"Compressed data from {len(data_bytes)} to {len(final_data)} bytes")
            except Exception as e:
                logger.error(f"Failed to compress data: {e}")

        # If sequence number is not provided, generate a new one
        if sequence is None:
            sequence = self.get_next_sequence()

        # Create header
        header = struct.pack(
            self.HEADER_FORMAT,
            self.MAGIC,          # Magic number
            packet_type.value,   # Packet type
            compressed,          # Compression flag
            sequence,            # Sequence number
            flags,               # Flags field
            len(final_data)      # Data length
        )

        # Return complete packet
        return header + final_data

    def unpack(self, data: bytes) -> Tuple[PacketType, bytes, int, int, int]:
        """Unpack a packet

        Args:
            data: Packet data to unpack

        Returns:
            Tuple (packet type, data, sequence number, flags, header + data length)

        Raises:
            ValueError: If the packet format is invalid
        """
        if len(data) < self.HEADER_SIZE:
            raise ValueError(f"Packet too small: {len(data)} bytes")

        # Parse header
        try:
            magic, ptype, compressed, sequence, flags, length = struct.unpack(
                self.HEADER_FORMAT,
                data[:self.HEADER_SIZE]
            )
        except struct.error as e:
            raise ValueError(f"Failed to parse packet header: {e}")

        # Validate magic number
        if magic != self.MAGIC:
            raise ValueError(f"Invalid magic number: 0x{magic:04X}")

        # Validate data length
        if self.HEADER_SIZE + length > len(data):
            raise ValueError(
                f"Incomplete packet: requires {self.HEADER_SIZE + length} bytes, but only {len(data)} bytes available")

        # Extract body
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

        return PacketType(ptype), body, sequence, flags, self.HEADER_SIZE + length

    def try_parse_json(self, data: bytes) -> Union[Dict[str, Any], bytes]:
        """Try to parse data as JSON, return original data if it fails

        Args:
            data: Data to parse

        Returns:
            Parsed JSON object or original data
        """
        try:
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return data

    def create_packet(self, packet_type: PacketType, data: Union[bytes, Dict[str, Any]],
                      sequence: Optional[int] = None, flags: int = 0) -> bytes:
        """Create a packet of the specified type

        Args:
            packet_type: Packet type
            data: Data (bytes or dictionary)
            sequence: Sequence number (auto-generated if None)
            flags: Flags field

        Returns:
            Packed packet data
        """
        return self.pack(data, packet_type, sequence, flags)

    def create_heartbeat(self) -> bytes:
        """Create a heartbeat packet

        Returns:
            Heartbeat packet data
        """
        timestamp = int(time.time())
        return self.create_packet(
            PacketType.HEARTBEAT,
            {'timestamp': timestamp, 'type': 'heartbeat'}
        )

    def create_ack(self, sequence: int, message_id: Optional[str] = None) -> bytes:
        """Create an acknowledgment packet

        Args:
            sequence: Sequence number of the packet to acknowledge
            message_id: Optional message ID

        Returns:
            Acknowledgment packet data
        """
        ack_data: Dict[str, Any] = {'ack_sequence': sequence}
        if message_id:
            ack_data['message_id'] = message_id

        return self.create_packet(
            PacketType.ACK,
            ack_data,
            sequence
        )

    def create_error(self, error_code: int, error_message: str) -> bytes:
        """Create an error packet

        Args:
            error_code: Error code
            error_message: Error message

        Returns:
            Error packet data
        """
        return self.create_packet(
            PacketType.ERROR,
            {
                'code': error_code,
                'message': error_message,
                'timestamp': int(time.time())
            }
        )

    def create_control_packet(self, command: str, params: Optional[Dict[str, Any]] = None) -> bytes:
        """Create a control packet

        Args:
            command: Control command
            params: Command parameters

        Returns:
            Control packet data
        """
        control_data: Dict[str, Any] = {
            'command': command,
            'timestamp': int(time.time())
        }

        if params:
            control_data['params'] = params

        return self.create_packet(PacketType.CONTROL, control_data)

    def create_reliable_packet(self, data: Union[bytes, Dict[str, Any]], message_id: str) -> bytes:
        """Create a reliable transmission packet

        Args:
            data: Data to transmit
            message_id: Message ID for deduplication and acknowledgment

        Returns:
            Reliable transmission packet data
        """
        if isinstance(data, dict):
            # Ensure we're working with a copy if 'data' might be reused elsewhere
            payload_data = data.copy()
            payload_data['_message_id'] = message_id
            payload_data['_timestamp'] = int(time.time())
            return self.create_packet(PacketType.RELIABLE, payload_data)
        else:
            # For binary data, add message ID to the header flags
            # This approach of embedding JSON in binary might be complex; consider alternatives
            # For now, sticking to the original logic but ensuring types are correct.
            header_info = json.dumps(
                {'message_id': message_id}).encode('utf-8')
            combined = header_info + b'\0' + data  # Use NULL byte as separator
            return self.create_packet(PacketType.RELIABLE, combined)

    def create_fragment_packet(self, data: bytes, fragment_id: int,
                               total_fragments: int, sequence: int) -> bytes:
        """Create a fragment packet for large data transmission

        Args:
            data: Fragment data
            fragment_id: Fragment ID (0-based)
            total_fragments: Total number of fragments
            sequence: Sequence number of the original packet

        Returns:
            Fragment packet data
        """
        header_info_dict: Dict[str, Any] = {
            'fragment_id': fragment_id,
            'total_fragments': total_fragments,
            'original_seq': sequence,
            'timestamp': int(time.time())
        }
        header_info_bytes = json.dumps(header_info_dict).encode('utf-8')

        combined = header_info_bytes + b'\0' + data  # Use NULL byte as separator

        # Use fragment ID as the high 8 bits of the sequence number to ensure fragment order
        fragment_sequence = ((fragment_id << 24) | (sequence & 0x00FFFFFF))

        # Fragment flag: set the 0th bit to indicate fragment packet
        # If it's the last fragment, set the 1st bit
        flags = 0x01
        if fragment_id == total_fragments - 1:
            flags |= 0x02

        return self.create_packet(
            PacketType.FRAGMENT,
            combined,
            fragment_sequence,
            flags
        )
