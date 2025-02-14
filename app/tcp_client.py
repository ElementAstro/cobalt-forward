import asyncio
from dataclasses import dataclass
from typing import Optional, Callable, Any
import logging
from enum import Enum, auto


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
        self._logger = logging.getLogger(__name__)

    @property
    def state(self) -> ClientState:
        return self._state

    async def connect(self) -> None:
        """Establish connection with retry mechanism"""
        for attempt in range(self._config.retry_attempts):
            try:
                self._state = ClientState.CONNECTING
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        self._config.host, self._config.port),
                    timeout=self._config.timeout
                )
                self._state = ClientState.CONNECTED
                self._trigger_callback('connected')
                self._logger.info(
                    f"Connected to {self._config.host}:{self._config.port}")
                return
            except Exception as e:
                self._logger.error(
                    f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self._config.retry_attempts - 1:
                    await asyncio.sleep(self._config.retry_delay)
                else:
                    self._state = ClientState.ERROR
                    self._trigger_callback('error', e)
                    raise ConnectionError(
                        f"Failed to connect after {self._config.retry_attempts} attempts")

    async def disconnect(self) -> None:
        """Close the connection gracefully"""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            finally:
                self._state = ClientState.DISCONNECTED
                self._trigger_callback('disconnected')
                self._reader = None
                self._writer = None

    async def send(self, data: bytes) -> None:
        """Send data to the server"""
        if not self._writer or self._state != ClientState.CONNECTED:
            raise ConnectionError("Not connected to server")

        try:
            self._writer.write(data)
            await self._writer.drain()
        except Exception as e:
            self._state = ClientState.ERROR
            self._trigger_callback('error', e)
            raise

    async def receive(self, buffer_size: int = 1024) -> bytes:
        """Receive data from the server"""
        if not self._reader or self._state != ClientState.CONNECTED:
            raise ConnectionError("Not connected to server")

        try:
            data = await self._reader.read(buffer_size)
            if data:
                self._trigger_callback('message', data)
            return data
        except Exception as e:
            self._state = ClientState.ERROR
            self._trigger_callback('error', e)
            raise

    def on(self, event: str, callback: Callable) -> None:
        """Register event callbacks"""
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    def _trigger_callback(self, event: str, *args: Any) -> None:
        """Trigger registered callbacks for an event"""
        for callback in self._callbacks.get(event, []):
            try:
                callback(*args)
            except Exception as e:
                self._logger.error(
                    f"Callback error for event {event}: {str(e)}")
