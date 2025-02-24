import mmap
from typing import Optional, List, Union
import asyncio
from loguru import logger


class PacketBuffer:
    """Enhanced buffer with zero-copy support"""

    def __init__(self, max_size: int = 16 * 1024 * 1024):
        self._buffer = memoryview(bytearray(max_size))
        self._read_pos = 0
        self._write_pos = 0
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def append(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """优化的零拷贝追加实现"""
        async with self._lock:
            available = len(self._buffer) - self._write_pos
            if len(data) > available:
                # 压缩缓冲区
                if self._read_pos > 0:
                    remain = self._write_pos - self._read_pos
                    self._buffer[0:remain] = self._buffer[self._read_pos:self._write_pos]
                    self._write_pos = remain
                    self._read_pos = 0

            self._buffer[self._write_pos:self._write_pos + len(data)] = data
            self._write_pos += len(data)
            self._event.set()

    async def read(self, size: int, timeout: float = None) -> Optional[bytes]:
        """优化的零拷贝读取实现"""
        async with self._lock:
            available = self._write_pos - self._read_pos
            if available < size:
                if timeout is not None:
                    try:
                        await asyncio.wait_for(self._event.wait(), timeout)
                    except asyncio.TimeoutError:
                        return None
                else:
                    await self._event.wait()

            result = bytes(self._buffer[self._read_pos:self._read_pos + size])
            self._read_pos += size

            if self._read_pos == self._write_pos:
                self._event.clear()
                self._read_pos = self._write_pos = 0

            return result
