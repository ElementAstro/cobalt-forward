import asyncio
from typing import Optional, Union, Deque
from collections import deque


class PacketBuffer:
    """Enhanced buffer supporting zero-copy network I/O operations"""

    def __init__(self, max_size: int = 16 * 1024 * 1024):
        """Initialize the packet buffer

        Args:
            max_size: Maximum buffer size (bytes, default 16 MB)
        """
        self._buffer = memoryview(bytearray(max_size))
        self._read_pos: int = 0
        self._write_pos: int = 0
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()
        # Small buffer cache to avoid frequent allocations
        self._small_buffer_cache: Deque[bytearray] = deque(maxlen=32)
        self._read_waiters = 0
        self._stats: dict[str, int] = {
            'compactions': 0,    # Number of compaction operations
            'appends': 0,        # Number of append operations
            'reads': 0,          # Number of read operations
            'cache_hits': 0,     # Number of cache hits
            'cache_misses': 0,   # Number of cache misses
            'overflows': 0,      # Number of buffer overflows
        }

    def _get_available_space(self) -> int:
        """Get the available space in the buffer"""
        return len(self._buffer) - self._write_pos

    def _get_used_space(self) -> int:
        """Get the used space in the buffer"""
        return self._write_pos - self._read_pos

    async def append(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """Append data to the buffer

        Args:
            data: Data to append

        Raises:
            BufferError: If the buffer is insufficient
        """
        data_len = len(data)
        self._stats['appends'] += 1

        async with self._lock:
            available = self._get_available_space()

            # If space is insufficient, try to compact the buffer
            if data_len > available:
                # If the read position is not at the beginning, compact the buffer by moving data
                if self._read_pos > 0:
                    self._stats['compactions'] += 1
                    remain = self._write_pos - self._read_pos
                    # Use memoryview for efficient copying
                    self._buffer[0:remain] = self._buffer[self._read_pos:self._write_pos]
                    self._write_pos = remain
                    self._read_pos = 0
                    available = len(self._buffer) - self._write_pos

                # If still insufficient after compaction, raise an error
                if data_len > available:
                    self._stats['overflows'] += 1
                    raise BufferError(
                        f"Buffer overflow: Need {data_len}, available {available}"
                    )

            # Copy directly, avoiding intermediate buffers
            self._buffer[self._write_pos:self._write_pos + data_len] = data
            self._write_pos += data_len

            if self._read_waiters > 0:
                self._event.set()

    async def read(self, size: int, timeout: Optional[float] = None) -> Optional[bytes]:
        """Read data from the buffer using optimized zero-copy implementation

        Args:
            size: Number of bytes to read
            timeout: Optional timeout (seconds)

        Returns:
            Read data, or None if timed out
        """
        self._stats['reads'] += 1

        # Fast path - if enough data is already available
        if self._read_pos + size <= self._write_pos:
            result = bytes(self._buffer[self._read_pos:self._read_pos + size])
            self._read_pos += size

            # If the buffer is empty, reset pointers
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
                    self._event.clear()  # Ensure the event is cleared

                    if timeout is not None:
                        try:
                            await asyncio.wait_for(self._event.wait(), timeout)
                        except asyncio.TimeoutError:
                            return None
                    else:
                        await self._event.wait()

                # Read available data
                available = min(self._write_pos - self._read_pos, size)

                # Use buffer pool for small buffers to reduce memory allocation
                if available <= 4096 and self._small_buffer_cache:
                    self._stats['cache_hits'] += 1
                    buf = self._small_buffer_cache.popleft()
                    buf[0:available] = self._buffer[self._read_pos:self._read_pos + available]
                    result = bytes(buf[0:available])
                else:
                    self._stats['cache_misses'] += 1
                    result = bytes(
                        self._buffer[self._read_pos:self._read_pos + available]
                    )

                    # Cache small buffers for future use
                    if available <= 4096 and \
                       self._small_buffer_cache.maxlen is not None and \
                       len(self._small_buffer_cache) < self._small_buffer_cache.maxlen:
                        # Check if the last cached buffer is smaller than the current one,
                        # or if the cache is empty, to potentially cache a larger "small" buffer.
                        if not self._small_buffer_cache or len(self._small_buffer_cache[-1]) < available:
                            self._small_buffer_cache.append(
                                bytearray(available))

                self._read_pos += available

                # If the buffer is empty, reset pointers
                if self._read_pos == self._write_pos:
                    self._read_pos = self._write_pos = 0

                return result
        finally:
            self._read_waiters -= 1

    def get_stats(self) -> dict[str, int]:
        """Get buffer statistics

        Returns:
            Dictionary containing buffer statistics
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
        """Clear the buffer content"""
        self._read_pos = 0
        self._write_pos = 0
        self._event.clear()

    async def peek(self, size: int) -> Optional[bytes]:
        """Peek data without removing it from the buffer

        Args:
            size: Number of bytes to peek

        Returns:
            Peeked data, or None if insufficient data
        """
        if self._read_pos + size <= self._write_pos:
            return bytes(self._buffer[self._read_pos:self._read_pos + size])
        return None
