import mmap
from typing import Optional, List, Union, Deque
import asyncio
from loguru import logger
from collections import deque


class PacketBuffer:
    """Enhanced buffer with zero-copy support"""

    def __init__(self, max_size: int = 16 * 1024 * 1024):
        self._buffer = memoryview(bytearray(max_size))
        self._read_pos = 0
        self._write_pos = 0
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._small_buffer_cache = deque(maxlen=32)  # 小缓冲区缓存，避免频繁分配
        self._read_waiters = 0
        self._stats = {
            'compactions': 0,
            'appends': 0,
            'reads': 0,
            'cache_hits': 0,
            'cache_misses': 0,
        }

    def _get_available_space(self) -> int:
        """获取可用空间"""
        return len(self._buffer) - self._write_pos

    def _get_used_space(self) -> int:
        """获取已使用空间"""
        return self._write_pos - self._read_pos

    async def append(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """优化的零拷贝追加实现"""
        self._stats['appends'] += 1
        data_len = len(data)

        # 快速路径 - 如果数据很小且缓冲区有足够空间
        if data_len < 1024 and self._get_available_space() >= data_len:
            self._buffer[self._write_pos:self._write_pos + data_len] = data
            self._write_pos += data_len
            if self._read_waiters > 0:
                self._event.set()
            return

        async with self._lock:
            available = self._get_available_space()
            if data_len > available:
                # 需要压缩缓冲区
                if self._read_pos > 0:
                    self._stats['compactions'] += 1
                    remain = self._write_pos - self._read_pos
                    # 使用内存视图进行高效复制
                    self._buffer[0:remain] = self._buffer[self._read_pos:self._write_pos]
                    self._write_pos = remain
                    self._read_pos = 0
                    available = len(self._buffer) - self._write_pos

                # 如果压缩后仍然空间不足，则报错
                if data_len > available:
                    raise BufferError(
                        f"Buffer overflow: needed {data_len}, available {available}")

            # 直接使用内存视图拷贝，避免中间缓冲区
            self._buffer[self._write_pos:self._write_pos + data_len] = data
            self._write_pos += data_len

            if self._read_waiters > 0:
                self._event.set()

    async def read(self, size: int, timeout: float = None) -> Optional[bytes]:
        """优化的零拷贝读取实现"""
        self._stats['reads'] += 1

        # 快速路径 - 已有足够数据
        if self._read_pos + size <= self._write_pos:
            result = bytes(self._buffer[self._read_pos:self._read_pos + size])
            self._read_pos += size

            # 如果缓冲区已读完，重置指针
            if self._read_pos == self._write_pos:
                self._read_pos = self._write_pos = 0

            return result

        # 需要等待更多数据
        try:
            self._read_waiters += 1

            # 使用锁保护读取操作
            async with self._lock:
                available = self._write_pos - self._read_pos

                # 等待数据
                if available < size:
                    self._event.clear()  # 确保事件被清除

                    if timeout is not None:
                        try:
                            await asyncio.wait_for(self._event.wait(), timeout)
                        except asyncio.TimeoutError:
                            return None
                    else:
                        await self._event.wait()

                # 读取可用数据
                available = min(self._write_pos - self._read_pos, size)

                # 使用缓存池来获取小缓冲区，减少内存分配
                if available <= 4096 and self._small_buffer_cache:
                    self._stats['cache_hits'] += 1
                    buf = self._small_buffer_cache.popleft()
                    buf[0:available] = self._buffer[self._read_pos:self._read_pos + available]
                    result = bytes(buf[0:available])
                else:
                    self._stats['cache_misses'] += 1
                    result = bytes(
                        self._buffer[self._read_pos:self._read_pos + available])

                    # 缓存小缓冲区以供将来使用
                    if available <= 4096 and len(self._small_buffer_cache) < self._small_buffer_cache.maxlen:
                        if not self._small_buffer_cache or len(self._small_buffer_cache[-1]) < available:
                            self._small_buffer_cache.append(
                                bytearray(available))

                self._read_pos += available

                # 重置缓冲区，如果已读完
                if self._read_pos == self._write_pos:
                    self._read_pos = self._write_pos = 0

                return result
        finally:
            self._read_waiters -= 1

    def get_stats(self) -> dict:
        """获取缓冲区统计信息"""
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
        """清空缓冲区"""
        self._read_pos = 0
        self._write_pos = 0
        self._event.clear()
