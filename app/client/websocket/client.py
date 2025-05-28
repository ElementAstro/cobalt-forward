from ..base import BaseClient, BaseConfig
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, AsyncGenerator, List, Type, Set, Dict, Awaitable
import asyncio
import ssl
from contextlib import asynccontextmanager, suppress
from loguru import logger
import time
# websockets.typing.Data is Union[str, bytes]
from websockets.typing import Data as WebSocketData
from websockets.exceptions import ConnectionClosed

from .events import EventManager
from .connection import ConnectionManager
from .config import WebSocketConfig as WSConfig # Alias for clarity
from .utils import parse_message, serialize_message
from .plugin import PluginManager, WebSocketPlugin


@dataclass
class WebSocketConfig(BaseConfig):
    """WebSocket Client Configuration"""
    uri: str
    ssl_context: Optional[ssl.SSLContext] = None
    headers: Optional[Dict[str, str]] = None
    receive_timeout: Optional[float] = None # Timeout for individual receive operations
    max_reconnect_attempts: int = -1 # For ConnectionManager's auto-reconnect
    reconnect_interval: float = 5.0  # For ConnectionManager's auto-reconnect
    compression_enabled: bool = False
    compression_level: int = -1 # e.g. for permessage-deflate window bits (if > 0)
    ping_interval: float = 20.0 # For ConnectionManager's keep-alive
    ping_timeout: float = 10.0  # For ConnectionManager's keep-alive
    subprotocols: List[str] = field(default_factory=list)
    max_message_size: int = 2**20  # 1MB, for websockets.connect max_size

    def to_connection_config(self) -> WSConfig:
        """Convert to connection configuration for ConnectionManager."""
        # WSConfig is expected to map to websockets.connect parameters
        return WSConfig(
            uri=self.uri,
            ssl_context=self.ssl_context,
            headers=self.headers,
            subprotocols=self.subprotocols,
            max_size=self.max_message_size,
            compression="deflate" if self.compression_enabled else None,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
            auto_reconnect=True, # This tells ConnectionManager to try
            max_reconnect_attempts=self.max_reconnect_attempts,
            reconnect_interval=self.reconnect_interval,
            # WSConfig likely doesn't take receive_timeout, it's for client logic
            extra_options={
                "compression_options": {"client_max_window_bits": self.compression_level}
            } if self.compression_enabled and self.compression_level > 0 else {}
        )


class WebSocketClient(BaseClient):
    """Enhanced WebSocket Client Implementation"""

    def __init__(self, config: WebSocketConfig):
        super().__init__(config)
        # Ensure self.config is specifically typed as WebSocketConfig for methods in this class
        self.config: WebSocketConfig = config
        self.event_manager = EventManager()
        connection_cfg = self.config.to_connection_config()
        self.connection_manager = ConnectionManager(connection_cfg, self.event_manager)
        self._message_queue: asyncio.Queue[Any] = asyncio.Queue()
        self._stream_tasks: Set[asyncio.Task[Any]] = set()
        self._last_activity = time.time()

        self.plugin_manager = PluginManager()
        
        self.event_manager.add_callback('connect', self._on_connect)
        self.event_manager.add_callback('disconnect', self._on_disconnect)
        self.event_manager.add_callback('error', self._on_error)

    def add_callback(self, event: str, callback: Callable[..., Any]) -> None:
        """Overrides BaseClient.add_callback"""
        self.event_manager.add_callback(event, callback)

    def remove_callback(self, event: str, callback: Callable[..., Any]) -> None:
        """Overrides BaseClient.remove_callback"""
        self.event_manager.remove_callback(event, callback)

    async def connect(self) -> bool:
        """Overrides BaseClient.connect"""
        try:
            self.add_callback('message', self._queue_message)
            success = await self.connection_manager.connect()
            self.connected = success
            if success:
                self._last_activity = time.time()
            return success
        except Exception as e:
            logger.error(f"WebSocket connection failed: {type(e).__name__}: {str(e)}")
            self.connected = False
            return False

    def register_plugin(self, plugin_class: Type[WebSocketPlugin]) -> bool:
        return self.plugin_manager.register(plugin_class)
    
    def unregister_plugin(self, plugin_name: str) -> bool:
        return self.plugin_manager.unregister(plugin_name)
    
    async def _on_connect(self) -> None:
        await self.plugin_manager.initialize(self)
        await self.plugin_manager.on_connect()
    
    async def _on_disconnect(self) -> None:
        await self.plugin_manager.on_disconnect()
    
    async def _on_error(self, error: Exception) -> None:
        await self.plugin_manager.on_error(error)
    
    async def send_message(self, message: Any) -> bool:
        """Actual send logic, used by execute_with_retry."""
        if not self.connection_manager.connected or not self.connection_manager.connection:
            logger.warning("Cannot send: Not connected or connection object missing.")
            return False
        try:
            processed_message = await self.plugin_manager.pre_send_chain(message)
            if processed_message is None:
                logger.debug("Message sending cancelled by plugin")
                return False
            serialized_message = serialize_message(processed_message)
            await self.connection_manager.connection.send(serialized_message)
            self._last_activity = time.time()
            logger.debug(f"Sent message: {str(serialized_message)[:100]}...")
            return True
        except ConnectionClosed:
            logger.warning("Connection closed while sending")
            await self._handle_connection_error() 
            return False
        except Exception as e:
            logger.error(f"Failed to send message: {type(e).__name__}: {str(e)}")
            await self.event_manager.trigger('error', e)
            return False
    
    async def receive_message(self) -> Optional[Any]:
        """Actual receive logic, used by execute_with_retry."""
        if not self.connection_manager.connected or not self.connection_manager.connection:
            logger.warning("Cannot receive: Not connected or connection object missing.")
            return None
        try:
            message_data: WebSocketData
            if self.config.receive_timeout is not None:
                message_data = await asyncio.wait_for(
                    self.connection_manager.connection.recv(),
                    timeout=self.config.receive_timeout
                )
            else:
                message_data = await self.connection_manager.connection.recv()
            
            self._last_activity = time.time()
            parsed_message = parse_message(message_data) # type: ignore[arg-type]
            
            processed_message = await self.plugin_manager.post_receive_chain(parsed_message)
            if processed_message is None:
                logger.debug("Message processing cancelled by plugin")
                return None
            await self.event_manager.trigger('message', processed_message)
            return processed_message
        except asyncio.TimeoutError:
            logger.debug("Receive timeout")
            return None 
        except ConnectionClosed:
            logger.warning("Connection closed while receiving")
            await self._handle_connection_error()
            return None
        except Exception as e:
            logger.error(f"Failed to receive message: {type(e).__name__}: {str(e)}")
            await self.event_manager.trigger('error', e)
            return None

    async def _handle_connection_error(self) -> None:
        if self.connected: 
            self.connected = False

    async def _queue_message(self, message: Any) -> None:
        await self._message_queue.put(message)

    async def disconnect(self) -> None:
        """Overrides BaseClient.disconnect"""
        task: asyncio.Task[Any]
        for task in list(self._stream_tasks):
            if not task.done():
                task.cancel()
        if self._stream_tasks:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait(
                    [t for t in self._stream_tasks if isinstance(t, asyncio.Task)], 
                    timeout=1.0
                )
        self.remove_callback('message', self._queue_message)
        await self.plugin_manager.shutdown()
        await self.connection_manager.close()
        self.connected = False
        while not self._message_queue.empty():
            try:
                self._message_queue.get_nowait()
                self._message_queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def send(self, data: Any) -> bool:
        """Overrides BaseClient.send"""
        result = await self.execute_with_retry(self.send_message, data)
        return bool(result) 

    async def receive(self) -> Optional[Any]:
        """Overrides BaseClient.receive"""
        try:
            return await asyncio.wait_for(self._message_queue.get(), timeout=0.01)
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            return await self.execute_with_retry(self.receive_message)

    async def execute_with_retry(self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """Execute function with automatic retry, specific to WebSocketClient."""
        for attempt in range(self.config.retry_attempts + 1): 
            if not self.connection_manager.connected and attempt > 0:
                logger.info(f"Attempting to reconnect before retry attempt {attempt + 1}")
                await self.connect()

            if self.connection_manager.connected:
                try:
                    result: Any = await func(*args, **kwargs)
                    if func == self.send_message:
                        if result is True: 
                            return True
                    elif func == self.receive_message:
                        return result 
                    elif result is not None: # Generic success for other funcs
                        return result
                except ConnectionClosed: 
                    logger.warning(f"ConnectionClosed during operation (attempt {attempt + 1}), handling...")
                    await self._handle_connection_error()
                except Exception as e: 
                    logger.error(f"Unhandled error in operation (attempt {attempt + 1}): {e}")
                    if func == self.receive_message: return None
                    return False # Default to False for other func errors

            if attempt < self.config.retry_attempts:
                actual_retry_interval = self.config.retry_interval * (2 ** attempt) # Exponential backoff
                logger.info(f"Operation failed (attempt {attempt + 1}/{self.config.retry_attempts + 1}). Retrying in {actual_retry_interval:.2f}s...")
                await asyncio.sleep(actual_retry_interval)
        
        logger.error(f"Operation failed after {self.config.retry_attempts + 1} attempts.")
        if func == self.receive_message:
            return None
        return False


    @asynccontextmanager
    async def session(self) -> AsyncGenerator["WebSocketClient", None]:
        try:
            await self.connect()
            yield self
        finally:
            await self.disconnect()

    async def receive_stream(self) -> AsyncGenerator[Any, None]:
        current_task: Optional[asyncio.Task[Any]] = asyncio.current_task()
        if current_task:
            self._stream_tasks.add(current_task)
        try:
            while self.connection_manager.connected:
                message = await self.receive() 
                if message is not None: 
                    yield message
                else: 
                    await asyncio.sleep(0.01)
        finally:
            if current_task:
                self._stream_tasks.discard(current_task)

    async def run_forever(self) -> None:
        while True:
            if not self.connection_manager.connected:
                logger.info("Not connected. Attempting to connect in run_forever loop...")
                success = await self.connect()
                if not success:
                    logger.warning(f"Connection attempt failed. Retrying in {self.config.reconnect_interval}s.")
                    await asyncio.sleep(self.config.reconnect_interval)
                    continue
                logger.info("Successfully connected in run_forever loop.")

            try:
                async for _ in self.receive_stream(): 
                    await asyncio.sleep(0) 
            except asyncio.CancelledError:
                logger.info("run_forever task cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in run_forever's receive_stream: {type(e).__name__}: {str(e)}")
                await self.event_manager.trigger('error', e)
                if not self.connection_manager.connected:
                    logger.info("Connection lost during run_forever stream. Loop will attempt reconnect.")
                await asyncio.sleep(1.0)
