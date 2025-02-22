import asyncio
from typing import Optional, Callable, Any, List, Dict, TYPE_CHECKING

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
    """TCP Connection Pool"""

    def __init__(self, size: int, config: ClientConfig):
        self.size = size
        self.config = config
        self.connections: List[TCPClient] = []
        self.active_connections: Dict[TCPClient, bool] = {}

    async def initialize(self):
        """Initialize connection pool"""
        for _ in range(self.size):
            client = TCPClient(self.config)
            await client.connect()
            self.connections.append(client)
            self.active_connections[client] = True

    async def get_connection(self) -> TCPClient:
        """Get available connection using round-robin"""
        active = [c for c, status in self.active_connections.items() if status]
        if not active:
            raise ConnectionError("No active connections available")
        return random.choice(active)


class TCPClient:
    """Enhanced TCP Client implementation with connection pooling"""

    def __init__(self, config: ClientConfig):
        self._config = config
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._state = ClientState.DISCONNECTED
        self._stats = ConnectionStats()
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

    async def connect(self) -> None:
        """建立连接并配置socket"""
        try:
            self._state = ClientState.CONNECTING

            # 创建连接
            reader, writer = await asyncio.open_connection(
                self._config.host,
                self._config.port
            )

            # 配置socket
            socket = writer.get_extra_info('socket')
            for option, value in self._config.advanced_settings.items():
                if hasattr(socket, f"set{option}"):
                    getattr(socket, f"set{option}")(value)

            self._reader = reader
            self._writer = writer
            self._state = ClientState.CONNECTED
            self._stats.connection_time = asyncio.get_event_loop().time()

            # 启动服务
            if self._config.auto_reconnect:
                await self.enable_auto_reconnect()
            await self.start_heartbeat(self._config.heartbeat_interval)

            self._trigger_callback('connected')
            logger.success(
                f"Connected to {self._config.host}:{self._config.port}")

            # 启动写入循环
            self._write_task = asyncio.create_task(self._write_loop())

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self._state = ClientState.ERROR
            self._trigger_callback('error', e)
            raise

    async def _write_loop(self):
        """Batch write loop for better performance"""
        while True:
            try:
                batch = []
                batch_size = 0

                # 收集批量数据
                while batch_size < self._config.buffer_size:
                    try:
                        data = await asyncio.wait_for(
                            self._write_queue.get(),
                            timeout=0.001
                        )
                        batch.append(data)
                        batch_size += len(data)
                    except asyncio.TimeoutError:
                        break

                if batch:
                    # 合并发送
                    combined_data = b''.join(batch)
                    await self._protected_send(combined_data)

            except Exception as e:
                logger.error(f"Write loop error: {e}")
                await asyncio.sleep(1)

    async def send(self, data: bytes) -> None:
        """Queue data for sending"""
        await self._write_queue.put(data)

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

    # ...existing code...
    # 保留之前实现的其他方法，如 disconnect(), receive(), 等
