import asyncio
from contextlib import asynccontextmanager, suppress
# Added Coroutine and Tuple
from typing import Any, Dict, Set, Optional, Coroutine
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.client import WebSocketClientProtocol  # For typing self.connection
from loguru import logger
from .config import WebSocketConfig
from .events import EventManager


class ConnectionManager:
    """WebSocket Connection Manager

    Manages WebSocket connection lifecycle, including connection establishment,
    monitoring, heartbeat, automatic reconnection and graceful shutdown.
    """

    def __init__(self, config: WebSocketConfig, event_manager: EventManager):
        self.config = config
        self.event_manager = event_manager
        self.connection: Optional[WebSocketClientProtocol] = None
        self.connected = False
        self._ping_task: Optional[asyncio.Task[Optional[Any]]] = None
        self._reconnect_attempts = 0
        self._tasks: Set[asyncio.Task[Optional[Any]]] = set()
        self._lock = asyncio.Lock()
        self._closing = False
        self._closed_event = asyncio.Event()
        self._closed_event.set()  # Initially marked as closed

    @asynccontextmanager
    async def connection_context(self):
        """Connection context manager

        Creates a context where a connection is guaranteed to be established
        and properly closed when exiting the context.

        Yields:
            ConnectionManager: Self reference
        """
        try:
            await self.connect()
            yield self
        finally:
            await self.close()

    async def connect(self) -> bool:
        """Establish WebSocket connection

        Connects to WebSocket server using the provided configuration.
        Implements connection locking to prevent concurrent connection attempts.

        Returns:
            bool: True if connected successfully, False otherwise
        """
        if self._closing:
            return False

        # Return immediately if already connected
        if self.connected and self.connection and not self.connection.closed:
            return True

        # Fast lock check to avoid lock contention
        if self._lock.locked():
            return False

        async with self._lock:
            # Double-check to avoid duplicate connections
            if self.connected and self.connection and not self.connection.closed:
                return True

            # Wait for complete closure if closing
            if self._closing:
                await self._closed_event.wait()

            self._closed_event.clear()
            await self.event_manager.trigger('before_connect')

            try:
                # uri must be the first positional argument to websockets.connect
                uri = self.config.uri

                connect_kwargs: Dict[str, Any] = {
                    # 'uri': self.config.uri, # URI is now a positional argument
                    'ssl': self.config.ssl_context,
                    'extra_headers': self.config.headers,
                    'subprotocols': self.config.subprotocols,
                    'max_size': self.config.max_size,
                    'compression': self.config.compression,
                    'close_timeout': self.config.close_timeout,
                    # Add connection timeout
                    'open_timeout': 30.0  # Ensure this is a float or None
                }

                # Filter out None parameters
                # Explicitly type k and v for clarity if needed, though inferred types should work
                connect_kwargs = {k: v for k,
                                  v in connect_kwargs.items() if v is not None}

                if self.config.extra_options:
                    connect_kwargs.update(self.config.extra_options)

                self.connection = await websockets.connect(uri, **connect_kwargs)
                self.connected = True
                self._reconnect_attempts = 0

                # Start heartbeat task
                self._ping_task = self._create_task(self._keep_alive())
                self._create_task(self._monitor_connection())

                await self.event_manager.trigger('connect')
                return True

            except Exception as e:
                self._closed_event.set()  # Mark as closed on connection failure
                logger.error(
                    f"Connection failed: {type(e).__name__}: {str(e)}")
                await self.event_manager.trigger('error', e)
                return False

    def _create_task(self, coro: Coroutine[Any, Any, Optional[Any]]) -> asyncio.Task[Optional[Any]]:
        """Create and track a task

        Args:
            coro: Coroutine to execute as a task

        Returns:
            asyncio.Task: Created task
        """
        task: asyncio.Task[Optional[Any]] = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _keep_alive(self):
        """Keep connection alive with ping/pong mechanism

        Periodically sends ping frames and monitors pong responses.
        Triggers reconnection if ping times out.
        """
        with suppress(asyncio.CancelledError):
            while self.connected and self.connection and not self.connection.closed:
                try:
                    if self.connection and not self.connection.closed:
                        pong_waiter = await self.connection.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.config.ping_timeout)
                        logger.trace("Ping-pong successful")
                    await asyncio.sleep(self.config.ping_interval)
                except asyncio.TimeoutError:
                    logger.warning("Ping timeout, reconnecting...")
                    await self._handle_disconnection(reconnect=True)
                    break
                except (ConnectionClosed, WebSocketException):
                    await self._handle_disconnection()
                    break
                except Exception as e:
                    logger.error(
                        f"Keep-alive error: {type(e).__name__}: {str(e)}")
                    break

    async def _monitor_connection(self):
        """Monitor connection state

        Detects unexpected connection closures and handles disconnection events.
        """
        with suppress(asyncio.CancelledError):
            try:
                while self.connected and self.connection and not self.connection.closed:
                    try:
                        await self.connection.wait_closed()
                        break
                    except (ConnectionClosed, WebSocketException):
                        break
                    except Exception as e:
                        logger.error(
                            f"Connection monitor error: {type(e).__name__}: {str(e)}")
                        break

                # Only process disconnection at loop end
                if self.connected:
                    await self._handle_disconnection()

            except Exception as e:
                logger.error(
                    f"Connection monitor fatal error: {type(e).__name__}: {str(e)}")
                if self.connected:
                    await self._handle_disconnection()

    async def _handle_disconnection(self, reconnect: bool = False):
        """Handle connection closure

        Args:
            reconnect: Whether to attempt reconnection
        """
        if not self.connected:
            return

        async with self._lock:
            if not self.connected:  # Double-check
                return

            self.connected = False
            await self.event_manager.trigger('disconnect')

            # Cancel all tasks
            task: asyncio.Task[Optional[Any]]
            for task in list(self._tasks):
                if not task.done() and task != asyncio.current_task():
                    task.cancel()

            # Close connection
            if self.connection and not self.connection.closed:
                with suppress(Exception):
                    await self.connection.close()

            await self.event_manager.trigger('after_disconnect')
            self._closed_event.set()  # Mark as fully closed

            # Auto-reconnect
            if reconnect and self.config.auto_reconnect and not self._closing:
                if self.config.max_reconnect_attempts == -1 or self._reconnect_attempts < self.config.max_reconnect_attempts:
                    self._reconnect_attempts += 1
                    self._create_task(self._reconnect())

    async def _reconnect(self):
        """Reconnect after connection loss

        Implements exponential backoff for reconnection attempts.
        """
        await asyncio.sleep(self.config.reconnect_interval)
        logger.info(
            f"Attempting to reconnect (attempt {self._reconnect_attempts})...")
        await self.event_manager.trigger('reconnect', self._reconnect_attempts)
        await self.connect()

    async def close(self):
        """Close the WebSocket connection gracefully

        Ensures all pending tasks are cancelled and connection is properly closed.
        """
        if not self.connected and self._closed_event.is_set():
            return

        self._closing = True

        async with self._lock:
            if not self.connected:
                self._closing = False
                return

            try:
                self.connected = False

                # Cancel all tasks
                task: asyncio.Task[Optional[Any]]
                for task in list(self._tasks):
                    if not task.done():
                        task.cancel()

                # Wait for task cancellation to complete
                if self._tasks:
                    with suppress(asyncio.TimeoutError):
                        # Ensure 't' is properly typed if list comprehension is complex,
                        # but here it should infer from self._tasks
                        await asyncio.wait([t for t in self._tasks], timeout=2.0)

                # Close WebSocket connection
                if self.connection and not self.connection.closed:
                    await self.connection.close(code=1000, reason="Client closed connection")

                await self.event_manager.trigger('disconnect')
                await self.event_manager.trigger('after_disconnect')

            except Exception as e:
                logger.error(
                    f"Failed to close connection: {type(e).__name__}: {str(e)}")
                await self.event_manager.trigger('error', e)
            finally:
                self._closed_event.set()
                self._closing = False
