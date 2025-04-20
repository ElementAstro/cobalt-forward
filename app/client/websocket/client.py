from ..base import BaseClient, BaseConfig
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, AsyncGenerator, Dict, List, Union, Type
import asyncio
from contextlib import asynccontextmanager, suppress
from loguru import logger
import time
from websockets.exceptions import ConnectionClosed, WebSocketException

from .events import EventManager
from .connection import ConnectionManager
from .config import WebSocketConfig as WSConfig
from .utils import parse_message, serialize_message
from .plugin import PluginManager, WebSocketPlugin


@dataclass
class WebSocketConfig(BaseConfig):
    """WebSocket Client Configuration"""
    receive_timeout: Optional[float] = None
    max_reconnect_attempts: int = -1
    reconnect_interval: float = 5.0
    compression_enabled: bool = False
    compression_level: int = -1
    ping_interval: float = 20.0
    ping_timeout: float = 10.0
    subprotocols: List[str] = field(default_factory=list)
    max_message_size: int = 2**20  # 1MB

    def to_connection_config(self) -> WSConfig:
        """Convert to connection configuration"""
        return WSConfig(
            uri=self.url,
            ssl_context=self.ssl_context,
            headers=self.headers,
            subprotocols=self.subprotocols,
            max_size=self.max_message_size,
            compression="deflate" if self.compression_enabled else None,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
            auto_reconnect=True,
            max_reconnect_attempts=self.max_reconnect_attempts,
            reconnect_interval=self.reconnect_interval,
            receive_timeout=self.receive_timeout,
            extra_options={
                "compression_options": {"client_max_window_bits": self.compression_level}
            } if self.compression_enabled and self.compression_level > 0 else {}
        )


class WebSocketClient(BaseClient):
    """Enhanced WebSocket Client Implementation
    
    Provides robust WebSocket communication with automatic reconnection,
    event handling, message streaming, and session management.
    """

    def __init__(self, config: WebSocketConfig):
        super().__init__(config)
        self.event_manager = EventManager()
        self.connection_manager = ConnectionManager(
            config.to_connection_config(), self.event_manager)
        self._message_queue = asyncio.Queue()
        self._stream_tasks = set()
        self._last_activity = time.time()
        self._reconnect_backoff = 1.0
        # Initialize plugin manager
        self.plugin_manager = PluginManager()
        
        # Connect event handlers
        self.event_manager.add_callback('connect', self._on_connect)
        self.event_manager.add_callback('disconnect', self._on_disconnect)
        self.event_manager.add_callback('error', self._on_error)

    def add_callback(self, event: str, callback: Callable) -> bool:
        """Register event callback
        
        Args:
            event: Event name to listen for
            callback: Function to execute when event occurs
            
        Returns:
            bool: True if successfully registered, False otherwise
        """
        return self.event_manager.add_callback(event, callback)

    def remove_callback(self, event: str, callback: Callable) -> bool:
        """Remove event callback
        
        Args:
            event: Event name
            callback: Function to remove
            
        Returns:
            bool: True if successfully removed, False otherwise
        """
        return self.event_manager.remove_callback(event, callback)

    async def connect(self) -> bool:
        """Establish WebSocket connection
        
        Returns:
            bool: True if successfully connected, False otherwise
        """
        try:
            # Register message handling callback
            self.add_callback('message', self._queue_message)

            success = await self.connection_manager.connect()
            self.connected = success

            if success:
                self._last_activity = time.time()
                self._reconnect_backoff = 1.0  # Reset backoff time

            return success
        except Exception as e:
            logger.error(
                f"WebSocket connection failed: {type(e).__name__}: {str(e)}")
            return False

    def register_plugin(self, plugin_class: Type[WebSocketPlugin]) -> bool:
        """Register a WebSocket plugin
        
        Args:
            plugin_class: Plugin class to register
            
        Returns:
            bool: True if plugin was successfully registered
        """
        return self.plugin_manager.register(plugin_class)
    
    def unregister_plugin(self, plugin_name: str) -> bool:
        """Unregister a WebSocket plugin
        
        Args:
            plugin_name: Name of the plugin to unregister
            
        Returns:
            bool: True if plugin was successfully unregistered
        """
        return self.plugin_manager.unregister(plugin_name)
    
    async def _on_connect(self) -> None:
        """Internal connect event handler"""
        await self.plugin_manager.initialize(self)
        await self.plugin_manager.on_connect()
    
    async def _on_disconnect(self) -> None:
        """Internal disconnect event handler"""
        await self.plugin_manager.on_disconnect()
    
    async def _on_error(self, error: Exception) -> None:
        """Internal error event handler
        
        Args:
            error: Exception that occurred
        """
        await self.plugin_manager.on_error(error)
    
    async def send_message(self, message: Any) -> bool:
        """Send message through WebSocket
        
        Args:
            message: Message to send (will be serialized)
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not self.connection_manager.connected:
            logger.error("No active connection for sending")
            return False

        try:
            # Process message through plugin chain
            processed_message = await self.plugin_manager.pre_send_chain(message)
            if processed_message is None:
                logger.debug("Message sending cancelled by plugin")
                return False
                
            serialized_message = serialize_message(processed_message)
            await self.connection_manager.connection.send(serialized_message)
            self._last_activity = time.time()
            logger.debug(f"Sent message: {serialized_message[:100]}...")
            return True

        except ConnectionClosed:
            logger.warning("Connection closed while sending")
            await self._handle_connection_error()
            return False
        except Exception as e:
            logger.error(
                f"Failed to send message: {type(e).__name__}: {str(e)}")
            await self.event_manager.trigger('error', e)
            return False
    
    async def receive_message(self) -> Optional[Any]:
        """Receive message from WebSocket
        
        Returns:
            Any: Parsed message or None if error occurred
        """
        if not self.connection_manager.connected:
            logger.error("No active connection for receiving")
            return None

        try:
            if self.config.receive_timeout:
                message = await asyncio.wait_for(
                    self.connection_manager.connection.recv(),
                    timeout=self.config.receive_timeout
                )
            else:
                message = await self.connection_manager.connection.recv()

            self._last_activity = time.time()
            parsed_message = parse_message(message)
            
            # Process message through plugin chain
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
            logger.error(
                f"Failed to receive message: {type(e).__name__}: {str(e)}")
            await self.event_manager.trigger('error', e)
            return None

    async def _handle_connection_error(self):
        """Handle connection errors
        
        Marks the connection as disconnected. Connection manager
        will handle reconnection logic.
        """
        if not self.connection_manager.connected:
            return

        # Mark as disconnected, trigger reconnection
        self.connected = False
        # Connection manager handles reconnection logic

    async def _queue_message(self, message):
        """Add message to the internal queue
        
        Args:
            message: Message to queue
        """
        await self._message_queue.put(message)

    async def disconnect(self) -> None:
        """Disconnect from WebSocket server
        
        Cleans up all resources and closes the connection gracefully.
        """
        # Clean up all streaming tasks
        for task in list(self._stream_tasks):
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self._stream_tasks:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait([t for t in self._stream_tasks], timeout=1.0)

        # Remove message callback
        self.remove_callback('message', self._queue_message)

        # Shutdown plugins
        await self.plugin_manager.shutdown()

        # Close connection
        await self.connection_manager.close()
        self.connected = False

        # Clear message queue
        while not self._message_queue.empty():
            try:
                self._message_queue.get_nowait()
                self._message_queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def send(self, data: Any) -> bool:
        """Implement WebSocket message sending
        
        Overrides BaseClient.send with automatic retry
        
        Args:
            data: Data to send
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        return await self.execute_with_retry(self.send_message, data)

    async def receive(self) -> Optional[Any]:
        """Implement WebSocket message receiving
        
        Overrides BaseClient.receive with automatic retry and queue support
        
        Returns:
            Any: Received message or None if error occurred
        """
        # Try to get from queue first, if empty then receive directly
        try:
            return await asyncio.wait_for(self._message_queue.get(), timeout=0.01)
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            return await self.execute_with_retry(self.receive_message)

    async def execute_with_retry(self, func, *args, **kwargs):
        """Execute function with automatic retry
        
        Args:
            func: Function to execute
            *args: Arguments to pass to function
            **kwargs: Keyword arguments to pass to function
            
        Returns:
            Any: Function result or None/False if all retries failed
        """
        max_retries = 2  # Maximum retry attempts
        for i in range(max_retries + 1):
            if not self.connection_manager.connected and i > 0:
                # Reconnect if not connected
                await self.connect()

            if self.connection_manager.connected:
                result = await func(*args, **kwargs)
                if result is not None or func == self.receive_message:
                    return result

            if i < max_retries:
                await asyncio.sleep(0.5 * (i + 1))  # Progressive backoff

        return None if func == self.receive_message else False

    @asynccontextmanager
    async def session(self):
        """Client session context manager
        
        Creates a context where the connection is guaranteed to be established
        and properly closed when exiting the context.
        
        Yields:
            WebSocketClient: Self reference
        """
        try:
            await self.connect()
            yield self
        finally:
            await self.disconnect()

    async def receive_stream(self) -> AsyncGenerator[Any, None]:
        """Message stream receiver
        
        Creates an asynchronous generator for continuous message receiving.
        
        Yields:
            Any: Received messages
        """
        task = asyncio.current_task()
        if task:
            self._stream_tasks.add(task)

        try:
            while self.connection_manager.connected:
                message = await self.receive()
                if message is not None:
                    yield message
                else:
                    # Small pause to prevent CPU hogging if no messages
                    await asyncio.sleep(0.01)
        finally:
            if task:
                self._stream_tasks.discard(task)

    async def run_forever(self):
        """Enhanced perpetual operation mode
        
        Continuously runs the client, handling reconnections automatically.
        """
        while True:
            if not self.connection_manager.connected:
                success = await self.connect()
                if not success:
                    await asyncio.sleep(self.config.reconnect_interval)
                    continue

            try:
                # Use custom message handling
                async for message in self.receive_stream():
                    # Message is already processed in receive_stream
                    await asyncio.sleep(0)  # Yield control

            except asyncio.CancelledError:
                logger.info("Run forever cancelled")
                break
            except Exception as e:
                logger.error(
                    f"Error in message stream: {type(e).__name__}: {str(e)}")
                await self.event_manager.trigger('error', e)
                # Brief pause before continuing
                await asyncio.sleep(1.0)
