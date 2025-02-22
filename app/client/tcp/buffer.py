import mmap
from typing import Optional, List, Union
import asyncio
from loguru import logger


class PacketBuffer:
    """Enhanced buffer with zero-copy support"""

    def __init__(self, max_size: int = 16 * 1024 * 1024):
        self._buffers: List[memoryview] = []
        self._max_size = max_size
        self._current_size = 0
        self._event = asyncio.Event()

    def append(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """使用零拷贝方式追加数据"""
        if isinstance(data, (bytes, bytearray)):
            data = memoryview(data)

        if self._current_size + len(data) > self._max_size:
            logger.warning("Buffer overflow, clearing buffer")
            self.clear()

        self._buffers.append(data)
        self._current_size += len(data)
        self._event.set()

    def clear(self) -> None:
        """Clear buffer"""
        self._buffers.clear()
        self._current_size = 0
        self._event.clear()

    async def read(self, size: int, timeout: float = None) -> Optional[bytes]:
        """Zero-copy read implementation"""
        try:
            if timeout:
                await asyncio.wait_for(self._event.wait(), timeout)
            else:
                await self._event.wait()

            if self._current_size < size:
                return None

            result = bytearray(size)
            view = memoryview(result)
            copied = 0
            while copied < size and self._buffers:
                first_buffer = self._buffers[0]
                remaining = size - copied
                to_copy = min(len(first_buffer), remaining)

                view[copied:copied + to_copy] = first_buffer[:to_copy]
                copied += to_copy

                if to_copy < len(first_buffer):
                    self._buffers[0] = first_buffer[to_copy:]
                else:
                    self._buffers.pop(0)

            self._current_size -= copied
            if not self._current_size:
                self._event.clear()

            return result

        except asyncio.TimeoutError:
            return None
