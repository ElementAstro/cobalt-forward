import asyncio
from typing import Optional, Callable, Any, List, Dict, TYPE_CHECKING
from ..base import BaseClient, BaseConfig
from dataclasses import dataclass

if TYPE_CHECKING:
    from .client import TCPClient
from loguru import logger
import random

from .config import ClientConfig
from .state import ClientState, ConnectionStats
from .protocol import PacketProtocol
from .buffer import PacketBuffer
from ...utils.error_handler import CircuitBreaker
from ...utils.performance import RateLimiter, async_performance_monitor


class ConnectionPool:
    """优化的连接池实现"""
    
    def __init__(self, size: int, config: ClientConfig):
        self.size = size
        self.config = config
        self.connections = asyncio.Queue(maxsize=size)
        self.semaphore = asyncio.Semaphore(size)
        self._health_check_task = None

    async def initialize(self):
        """优化的连接池初始化"""
        for _ in range(self.size):
            client = TCPClient(self.config)
            await client.connect()
            await self.connections.put(client)
        self._health_check_task = asyncio.create_task(self._health_check())

    async def get_connection(self) -> 'TCPClient':
        """优化的连接获取"""
        async with self.semaphore:
            client = await self.connections.get()
            if not client.connected:
                await client.connect()
            return client

    async def release_connection(self, client: 'TCPClient'):
        """归还连接到连接池"""
        await self.connections.put(client)


@dataclass
class TCPConfig(BaseConfig):
    max_packet_size: int = 65536
    compression_enabled: bool = False
    compression_level: int = -1
    auto_reconnect: bool = True
    heartbeat_interval: float = 30.0
    advanced_settings: dict = None

class TCPClient(BaseClient):
    """Enhanced TCP Client implementation with connection pooling"""

    def __init__(self, config: TCPConfig):
        super().__init__(config)
        self._reader = None
        self._writer = None
        self._buffer = PacketBuffer(config.max_packet_size)
        self._protocol = PacketProtocol()
        self._rate_limiter = RateLimiter(rate_limit=1000)
        self._circuit_breaker = CircuitBreaker()
        self._heartbeat_task = None
        self._reconnect_task = None
        self._write_queue = asyncio.Queue()
        self._write_task = None

        # Event handlers
        self._callbacks: dict[str, list[Callable]] = {
            'connected': [],
            'disconnected': [],
            'message': [],
            'error': [],
            'heartbeat': []
        }

        logger.info(f"TCP client initialized for {config.host}:{config.port}")

    async def connect(self) -> bool:
        """实现TCP连接"""
        try:
            reader, writer = await asyncio.open_connection(
                self.config.host,
                self.config.port
            )
            self._reader = reader
            self._writer = writer
            self.connected = True
            return True
        except Exception as e:
            logger.error(f"TCP connection failed: {str(e)}")
            return False

    async def disconnect(self) -> None:
        """实现TCP断开连接"""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
        self.connected = False

    async def _write_loop(self):
        """优化的批量写入循环"""
        batch = []
        while True:
            try:
                data = await self._write_queue.get()
                batch.append(data)
                
                # 尝试快速收集更多数据
                while len(batch) < self._config.write_batch_size:
                    try:
                        data = await asyncio.wait_for(
                            self._write_queue.get(),
                            timeout=0.001
                        )
                        batch.append(data)
                    except asyncio.TimeoutError:
                        break
                
                if batch:
                    combined_data = b''.join(batch)
                    await self._protected_send(combined_data)
                    batch.clear()
                    
            except Exception as e:
                logger.error(f"Write loop error: {e}")
                await asyncio.sleep(1)

    async def send(self, data: Any) -> bool:
        """实现TCP数据发送"""
        return await self.execute_with_retry(self._protected_send, data)

    async def _protected_send(self, data: bytes):
        """Protected send implementation"""
        if not await self._rate_limiter.acquire():
            raise Exception("Rate limit exceeded")

        await self._circuit_breaker.execute(
            lambda: self._raw_send(data)
        )

    async def _raw_send(self, data: bytes):
        """Raw send implementation"""
        if not self._writer or self._state != ClientState.CONNECTED:
            raise ConnectionError("Not connected")

        try:
            packet = await self._protocol.pack(
                data,
                compress=self._config.compression_enabled,
                compression_level=self._config.compression_level
            )
            self._writer.write(packet)
            await self._writer.drain()
            self._stats.bytes_sent += len(packet)
            self._stats.packets_sent += 1
        except Exception as e:
            self._stats.errors += 1
            raise

    async def receive(self) -> Optional[Any]:
        """实现TCP数据接收"""
        if not self._reader:
            return None
        return await self.execute_with_retry(self._reader.read, self.config.buffer_size)

    # ...existing code...
    # 保留之前实现的其他方法，如 disconnect(), receive(), 等
