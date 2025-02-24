import asyncio
import struct
from typing import Optional, Union
import zlib
from loguru import logger
import numpy as np
from concurrent.futures import ThreadPoolExecutor


class PacketProtocol:
    """TCP packet protocol implementation"""
    HEADER_FORMAT = "!III"  # magic + length + checksum
    MAGIC = 0xDEADBEEF
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    _compression_pool = ThreadPoolExecutor(max_workers=4)
    _memory_pool = []
    POOL_SIZE = 1000

    @classmethod
    def _get_buffer(cls, size: int) -> bytearray:
        """从内存池获取缓冲区"""
        if cls._memory_pool:
            buf = cls._memory_pool.pop()
            if len(buf) >= size:
                return buf
        return bytearray(size)

    @classmethod
    def _return_buffer(cls, buf: bytearray):
        """返回缓冲区到内存池"""
        if len(cls._memory_pool) < cls.POOL_SIZE:
            cls._memory_pool.append(buf)

    @staticmethod
    async def pack(data: Union[bytes, bytearray, memoryview],
                   compress: bool = False,
                   compression_level: int = 6) -> bytes:
        """优化的数据打包方法"""
        buf = PacketProtocol._get_buffer(len(data) + PacketProtocol.HEADER_SIZE)
        try:
            if compress and len(data) > 512:  # 只压缩大于512字节的数据
                loop = asyncio.get_event_loop()
                data = await loop.run_in_executor(
                    PacketProtocol._compression_pool,
                    lambda: zlib.compress(data, level=compression_level)
                )
            
            checksum = zlib.crc32(data)
            struct.pack_into(PacketProtocol.HEADER_FORMAT, buf, 0,
                           PacketProtocol.MAGIC, len(data), checksum)
            buf[PacketProtocol.HEADER_SIZE:PacketProtocol.HEADER_SIZE+len(data)] = data
            
            return bytes(buf[:PacketProtocol.HEADER_SIZE+len(data)])
        finally:
            PacketProtocol._return_buffer(buf)

    @staticmethod
    def unpack(packet: bytes) -> Optional[bytes]:
        """Unpack data from packet format"""
        try:
            if len(packet) < PacketProtocol.HEADER_SIZE:
                return None

            magic, length, checksum = struct.unpack(
                PacketProtocol.HEADER_FORMAT,
                packet[:PacketProtocol.HEADER_SIZE]
            )

            if magic != PacketProtocol.MAGIC:
                logger.error("Invalid packet magic number")
                return None

            data = packet[PacketProtocol.HEADER_SIZE:]
            if len(data) != length:
                logger.error("Packet length mismatch")
                return None

            if zlib.crc32(data) != checksum:
                logger.error("Packet checksum mismatch")
                return None

            try:
                # 尝试解压，如果失败则返回原始数据
                return zlib.decompress(data)
            except zlib.error:
                return data

        except Exception as e:
            logger.error(f"Error unpacking packet: {e}")
            return None
