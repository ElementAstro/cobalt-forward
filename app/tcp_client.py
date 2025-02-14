import asyncio
from dataclasses import dataclass
from typing import Optional, Callable, Any
from enum import Enum, auto
from loguru import logger


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

    async def send(self, data: bytes) -> None:
        """Send data to the server"""
        if not self._writer or self._state != ClientState.CONNECTED:
            logger.error("Attempted to send data while not connected")
            raise ConnectionError("Not connected to server")

        try:
            logger.debug(f"Sending {len(data)} bytes")
            logger.trace(f"Data: {data}")
            self._writer.write(data)
            await self._writer.drain()
            logger.debug("Data sent successfully")
        except Exception as e:
            self._state = ClientState.ERROR
            self._trigger_callback('error', e)
            logger.exception("Error sending data")
            raise

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
