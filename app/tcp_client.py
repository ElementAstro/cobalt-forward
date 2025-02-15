import asyncio
from dataclasses import dataclass
from typing import Optional, Callable, Any
from enum import Enum, auto
from loguru import logger

from app.utils.error_handler import CircuitBreaker
from app.utils.performance import RateLimiter, async_performance_monitor


@dataclass
class ClientConfig:
    """Configuration for TCP client"""
    host: str
    port: int
    timeout: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0


class ClientState(Enum):
    """TCP client states"""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    ERROR = auto()


class TCPClient:
    """Modern TCP Client implementation with async support"""

    def __init__(self, config: ClientConfig):
        self._config = config
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._state = ClientState.DISCONNECTED
        self._callbacks: dict[str, list[Callable]] = {
            'connected': [],
            'disconnected': [],
            'message': [],
            'error': []
        }
        self._buffer = bytearray()
        self._packet_queue = asyncio.Queue()
        self._heartbeat_task = None
        self._reconnect_task = None
        self._compression_enabled = False
        self._buffer_size = 64 * 1024  # 64KB buffer
        self._stats = {
            'bytes_sent': 0,
            'bytes_received': 0,
            'packets_sent': 0,
            'packets_received': 0,
            'errors': 0,
            'reconnects': 0
        }
        self._rate_limiter = RateLimiter(rate_limit=1000)  # 限制每秒1000个请求
        self._circuit_breaker = CircuitBreaker()
        logger.info(f"Initializing TCP client for {config.host}:{config.port}")
        logger.debug(f"Client configuration: {config}")

    @property
    def state(self) -> ClientState:
        return self._state

    async def connect(self) -> None:
        """Establish connection with retry mechanism"""
        logger.info(
            f"Attempting to connect to {self._config.host}:{self._config.port}")

        for attempt in range(self._config.retry_attempts):
            try:
                logger.debug(
                    f"Connection attempt {attempt + 1}/{self._config.retry_attempts}")
                self._state = ClientState.CONNECTING

                logger.trace("Creating connection...")
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        self._config.host, self._config.port),
                    timeout=self._config.timeout
                )

                self._state = ClientState.CONNECTED
                self._trigger_callback('connected')
                logger.success(
                    f"Successfully connected to {self._config.host}:{self._config.port}")
                return

            except asyncio.TimeoutError as e:
                logger.warning(
                    f"Connection attempt {attempt + 1} timed out after {self._config.timeout}s")
                await self._handle_connection_failure(attempt, e)
            except ConnectionRefusedError as e:
                logger.warning(
                    f"Connection attempt {attempt + 1} refused by server")
                await self._handle_connection_failure(attempt, e)
            except Exception as e:
                logger.error(
                    f"Connection attempt {attempt + 1} failed with unexpected error: {str(e)}")
                await self._handle_connection_failure(attempt, e)

    async def _handle_connection_failure(self, attempt: int, error: Exception) -> None:
        """Handle connection failure with proper logging"""
        if attempt < self._config.retry_attempts - 1:
            delay = self._config.retry_delay
            logger.info(f"Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
        else:
            self._state = ClientState.ERROR
            self._trigger_callback('error', error)
            logger.error(
                f"Failed to connect after {self._config.retry_attempts} attempts")
            raise ConnectionError(
                f"Failed to connect after {self._config.retry_attempts} attempts")

    async def disconnect(self) -> None:
        """Close the connection gracefully"""
        if self._writer:
            try:
                logger.info("Initiating graceful disconnect")
                self._writer.close()
                await self._writer.wait_closed()
                logger.success("Connection closed successfully")
            except Exception as e:
                logger.error(f"Error during disconnect: {str(e)}")
            finally:
                self._state = ClientState.DISCONNECTED
                self._trigger_callback('disconnected')
                self._reader = None
                self._writer = None
                logger.debug("Client resources cleaned up")

    @async_performance_monitor()
    async def send(self, data: bytes) -> None:
        """Send data with rate limiting and circuit breaker"""
        if not await self._rate_limiter.acquire():
            raise Exception("Rate limit exceeded")

        async def _protected_send():
            if not self._writer or self._state != ClientState.CONNECTED:
                raise ConnectionError("Not connected to server")

            try:
                self._writer.write(data)
                await self._writer.drain()
                self._stats['bytes_sent'] += len(data)
                self._stats['packets_sent'] += 1
            except Exception as e:
                self._stats['errors'] += 1
                raise

        await self._circuit_breaker.execute(_protected_send)

    async def receive(self, buffer_size: int = 1024) -> bytes:
        """Receive data from the server"""
        if not self._reader or self._state != ClientState.CONNECTED:
            logger.error("Attempted to receive data while not connected")
            raise ConnectionError("Not connected to server")

        try:
            logger.debug(
                f"Waiting to receive data (buffer size: {buffer_size})")
            data = await self._reader.read(buffer_size)
            if data:
                logger.debug(f"Received {len(data)} bytes")
                logger.trace(f"Data: {data}")
                self._trigger_callback('message', data)
            else:
                logger.warning(
                    "Received empty data, connection might be closed")
            return data
        except Exception as e:
            self._state = ClientState.ERROR
            self._trigger_callback('error', e)
            logger.exception("Error receiving data")
            raise

    def on(self, event: str, callback: Callable) -> None:
        """Register event callbacks"""
        if event in self._callbacks:
            logger.debug(f"Registering callback for event: {event}")
            self._callbacks[event].append(callback)
        else:
            logger.warning(
                f"Attempted to register callback for unknown event: {event}")

    def _trigger_callback(self, event: str, *args: Any) -> None:
        """Trigger registered callbacks for an event"""
        logger.trace(f"Triggering callbacks for event: {event}")
        for callback in self._callbacks.get(event, []):
            try:
                callback(*args)
                logger.trace(f"Successfully executed callback for {event}")
            except Exception as e:
                logger.error(f"Callback error for event {event}: {str(e)}")

    async def enable_compression(self, enabled: bool = True):
        """启用或禁用数据压缩"""
        self._compression_enabled = enabled
        logger.info(f"Data compression {'enabled' if enabled else 'disabled'}")

    async def start_heartbeat(self, interval: float = 30.0):
        """启动心跳检测"""
        async def heartbeat_loop():
            while self._state == ClientState.CONNECTED:
                try:
                    await self.send(b'ping')
                    await asyncio.sleep(interval)
                except Exception as e:
                    logger.error(f"Heartbeat error: {e}")
                    await self._handle_connection_failure(0, e)
                    break

        self._heartbeat_task = asyncio.create_task(heartbeat_loop())
        logger.info(f"Heartbeat started with interval {interval}s")

    async def send_with_retry(self, data: bytes, max_retries: int = 3) -> bool:
        """发送数据并自动重试"""
        for attempt in range(max_retries):
            try:
                await self.send(data)
                return True
            except Exception as e:
                logger.warning(f"Send attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                continue
        return False

    @async_performance_monitor()
    async def receive_packet(self) -> Optional[bytes]:
        """Enhanced packet receiving with performance monitoring"""
        try:
            packet = await super().receive_packet()
            if packet:
                self._stats['packets_received'] += 1
            return packet
        except Exception as e:
            self._stats['errors'] += 1
            raise

    def get_stats(self) -> dict:
        """获取连接统计信息"""
        return self._stats.copy()

    async def send_file(self, file_path: str, chunk_size: int = 8192) -> None:
        """发送文件"""
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    await self.send(chunk)
                    self._stats['bytes_sent'] += len(chunk)
        except Exception as e:
            logger.error(f"Error sending file {file_path}: {e}")
            raise

    async def enable_auto_reconnect(self, enabled: bool = True):
        """启用自动重连"""
        if enabled and not self._reconnect_task:
            async def reconnect_loop():
                while True:
                    if self._state == ClientState.DISCONNECTED:
                        try:
                            await self.connect()
                            self._stats['reconnects'] += 1
                        except Exception as e:
                            logger.error(f"Reconnection failed: {e}")
                    await asyncio.sleep(5)

            self._reconnect_task = asyncio.create_task(reconnect_loop())
            logger.info("Auto reconnect enabled")
        elif not enabled and self._reconnect_task:
            self._reconnect_task.cancel()
            self._reconnect_task = None
            logger.info("Auto reconnect disabled")
