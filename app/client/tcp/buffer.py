import mmap
from typing import Optional, List, Union, Deque
import asyncio
from loguru import logger
from collections import deque


class PacketBuffer:
    """Enhanced buffer with zero-copy support for efficient network I/O"""

    def __init__(self, max_size: int = 16 * 1024 * 1024):
        """Initialize packet buffer
        
        Args:
            max_size: Maximum buffer size in bytes (default 16 MB)
        """
        self._buffer = memoryview(bytearray(max_size))
        self._read_pos = 0
        self._write_pos = 0
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._small_buffer_cache = deque(maxlen=32)  # Small buffer cache to avoid frequent allocations
        self._read_waiters = 0
        self._stats = {
            'compactions': 0,
            'appends': 0,
            'reads': 0,
            'cache_hits': 0,
            'cache_misses': 0,
        }

    def _get_available_space(self) -> int:
        """Get available space in buffer"""
        return len(self._buffer) - self._write_pos

    def _get_used_space(self) -> int:
        """Get used space in buffer"""
        return self._write_pos - self._read_pos

    async def append(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """Append data to buffer with optimized zero-copy implementation
        
        Args:
            data: Data to append to buffer
            
        Raises:
            BufferError: If buffer overflow occurs
        """
        self._stats['appends'] += 1
        data_len = len(data)

        # Fast path - if data is small and buffer has enough space
        if data_len < 1024 and self._get_available_space() >= data_len:
            self._buffer[self._write_pos:self._write_pos + data_len] = data
            self._write_pos += data_len
            if self._read_waiters > 0:
                self._event.set()
            return

        async with self._lock:
            available = self._get_available_space()
            if data_len > available:
                # Need to compact buffer
                if self._read_pos > 0:
                    self._stats['compactions'] += 1
                    remain = self._write_pos - self._read_pos
                    # Use memoryview for efficient copying
                    self._buffer[0:remain] = self._buffer[self._read_pos:self._write_pos]
                    self._write_pos = remain
                    self._read_pos = 0
                    available = len(self._buffer) - self._write_pos

                # If still not enough space after compaction, raise error
                if data_len > available:
                    raise BufferError(
                        f"Buffer overflow: needed {data_len}, available {available}")

            # Direct copy using memoryview, avoiding intermediate buffers
            self._buffer[self._write_pos:self._write_pos + data_len] = data
            self._write_pos += data_len

            if self._read_waiters > 0:
                self._event.set()

    async def read(self, size: int, timeout: float = None) -> Optional[bytes]:
        """Read data from buffer with optimized zero-copy implementation
        
        Args:
            size: Number of bytes to read
            timeout: Optional timeout in seconds
            
        Returns:
            Read data or None if timeout occurred
        """
        self._stats['reads'] += 1

        # Fast path - if enough data is already available
        if self._read_pos + size <= self._write_pos:
            result = bytes(self._buffer[self._read_pos:self._read_pos + size])
            self._read_pos += size

            # Reset pointers if buffer is empty
            if self._read_pos == self._write_pos:
                self._read_pos = self._write_pos = 0

            return result

        # Need to wait for more data
        try:
            self._read_waiters += 1

            # Use lock to protect read operation
            async with self._lock:
                available = self._write_pos - self._read_pos

                # Wait for data
                if available < size:
                    self._event.clear()  # Ensure event is cleared

                    if timeout is not None:
                        try:
                            await asyncio.wait_for(self._event.wait(), timeout)
                        except asyncio.TimeoutError:
                            return None
                    else:
                        await self._event.wait()

                # Read available data
                available = min(self._write_pos - self._read_pos, size)

                # Use buffer pool for small buffers to reduce memory allocations
                if available <= 4096 and self._small_buffer_cache:
                    self._stats['cache_hits'] += 1
                    buf = self._small_buffer_cache.popleft()
                    buf[0:available] = self._buffer[self._read_pos:self._read_pos + available]
                    result = bytes(buf[0:available])
                else:
                    self._stats['cache_misses'] += 1
                    result = bytes(
                        self._buffer[self._read_pos:self._read_pos + available])

                    # Cache small buffers for future use
                    if available <= 4096 and len(self._small_buffer_cache) < self._small_buffer_cache.maxlen:
                        if not self._small_buffer_cache or len(self._small_buffer_cache[-1]) < available:
                            self._small_buffer_cache.append(
                                bytearray(available))

                self._read_pos += available

                # Reset buffer if empty
                if self._read_pos == self._write_pos:
                    self._read_pos = self._write_pos = 0

                return result
        finally:
            self._read_waiters -= 1

    def get_stats(self) -> dict:
        """Get buffer statistics
        
        Returns:
            Dictionary with buffer statistics
        """
        return {
            **self._stats,
            'buffer_size': len(self._buffer),
            'used_space': self._get_used_space(),
            'available_space': self._get_available_space(),
            'read_pos': self._read_pos,
            'write_pos': self._write_pos,
            'small_buffer_cache_size': len(self._small_buffer_cache),
        }

    def clear(self) -> None:
        """Clear buffer contents"""
        self._read_pos = 0
        self._write_pos = 0
        self._event.clear()
        
    async def peek(self, size: int) -> Optional[bytes]:
        """Peek at data without removing it from buffer
        
        Args:
            size: Number of bytes to peek
            
        Returns:
            Peeked data or None if not enough data
        """
        if self._read_pos + size <= self._write_pos:
            return bytes(self._buffer[self._read_pos:self._read_pos + size])
        return None
