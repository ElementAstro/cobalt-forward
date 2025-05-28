import asyncio
from typing import Callable, TYPE_CHECKING, Union, Dict, Any, List, Optional, Tuple, Coroutine
import zlib
from ..base import BaseClient, BaseConfig
from dataclasses import dataclass, field
import time
# import functools # Unused import
import random
from contextlib import asynccontextmanager
import socket
import struct

if TYPE_CHECKING:
    # Removed forward reference to TCPClient itself as it's defined in this file.
    # If TCPClient were in another file and imported here, this would be needed.
    pass  # Keep TYPE_CHECKING block if other forward refs are needed later

from loguru import logger

from .config import ClientConfig  # TCPSocketOptions is used via ClientConfig
from .state import ClientState, ConnectionStats
from .protocol import PacketProtocol, PacketType
from .buffer import PacketBuffer
from ...utils.error_handler import CircuitBreaker
from ...utils.performance import RateLimiter, async_performance_monitor


class ConnectionPool:
    """Optimized connection pool implementation"""

    def __init__(self, size: int, config: ClientConfig):
        """Initialize the connection pool

        Args:
            size: Maximum number of connections in the pool
            config: Client configuration
        """
        self.size = size
        self.config = config
        self.connections: asyncio.Queue['TCPClient'] = asyncio.Queue(
            maxsize=size)
        # Track active connections
        self.active_connections: Dict[int, 'TCPClient'] = {}
        self.semaphore = asyncio.Semaphore(size)
        self._health_check_task: Optional[asyncio.Task[None]] = None
        self._stats: Dict[str, int] = {
            'created': 0,
            'borrowed': 0,
            'returned': 0,
            'errors': 0,
            'health_checks': 0,
            'reconnects': 0
        }
        self._closed = False
        self._init_lock = asyncio.Lock()
        self._initialized = False
        logger.info(
            f"Initialized connection pool with size={size} for {config.host}:{config.port}")

    async def initialize(self):
        """Optimized connection pool initialization with lazy loading"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        async with self._init_lock:
            if self._initialized:
                return

            initial_conns = max(1, self.size // 2)
            for _ in range(initial_conns):
                try:
                    client = await self._create_connection()
                    await self.connections.put(client)
                except Exception as e:
                    logger.error(f"Failed to initialize connection: {e}")

            self._health_check_task = asyncio.create_task(self._health_check())
            self._initialized = True
            logger.info(
                f"Connection pool initialized with {initial_conns} initial connections")

    async def _create_connection(self) -> 'TCPClient':
        """Create a new connection

        Returns:
            New TCPClient instance
        """
        # Ensure TCPClient is fully defined before this point or use string literal 'TCPClient'
        client = TCPClient(self.config)
        await client.connect()
        self._stats['created'] += 1
        return client

    async def get_connection(self) -> 'TCPClient':
        """Get a connection from the pool

        Returns:
            TCPClient instance

        Raises:
            RuntimeError: If the pool is closed
            Exception: If failed to get a connection
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        if not self._initialized:
            await self.initialize()

        async with self.semaphore:
            client: 'TCPClient'
            try:
                try:
                    client = await asyncio.wait_for(self.connections.get(), timeout=0.1)
                    if not client.connected:
                        logger.debug("Reconnecting stale connection from pool")
                        await client.connect()
                        if not client.connected:
                            logger.warning(
                                "Failed to reconnect stale connection, creating new one")
                            client = await self._create_connection()
                except asyncio.TimeoutError:
                    logger.debug(
                        "Connection pool empty, creating new connection")
                    client = await self._create_connection()

                self.active_connections[id(client)] = client
                self._stats['borrowed'] += 1
                return client

            except Exception as e:
                self._stats['errors'] += 1
                logger.error(f"Failed to get connection: {e}")
                raise

    async def release_connection(self, client: 'TCPClient'):
        """Return a connection to the pool

        Args:
            client: TCPClient instance to return
        """
        if self._closed:
            await client.disconnect()
            return

        conn_id = id(client)
        if conn_id in self.active_connections:
            del self.active_connections[conn_id]

        if client.connected:
            await self.connections.put(client)
            self._stats['returned'] += 1
        else:
            try:
                logger.debug("Replacing broken connection in pool")
                new_client = await self._create_connection()
                await self.connections.put(new_client)
            except Exception as e:
                logger.error(f"Failed to replace broken connection: {e}")

    @asynccontextmanager
    async def connection(self) -> asyncio.AbstractAsyncContextManager['TCPClient']:
        """Connection context manager that automatically returns connections

        Yields:
            TCPClient instance
        """
        client = await self.get_connection()
        try:
            yield client
        finally:
            await self.release_connection(client)

    async def _health_check(self):
        """Periodic health check for pool connections"""
        while not self._closed:
            try:
                await asyncio.sleep(60)
                self._stats['health_checks'] += 1

                active_conns_copy = list(self.active_connections.items())
                for client_id, client_instance in active_conns_copy:
                    if client_id not in self.active_connections:  # Check if still active
                        continue
                    if not client_instance.connected or client_instance._stats.get_idle_time() > 300:
                        logger.warning(
                            f"Detected stale active connection {client_id}, will be replaced when returned to pool")

                checked = 0
                # Create a temporary list of connections to check from the queue
                connections_to_check: List['TCPClient'] = []
                for _ in range(min(2, self.connections.qsize())):
                    if self.connections.empty():
                        break
                    connections_to_check.append(await self.connections.get())

                re_added_connections: List['TCPClient'] = []
                for client_instance in connections_to_check:
                    checked += 1
                    if not client_instance.connected:
                        logger.info(
                            f"Replacing broken connection in pool (ID: {id(client_instance)})")
                        try:
                            new_client_instance = await self._create_connection()
                            re_added_connections.append(new_client_instance)
                            self._stats['reconnects'] += 1
                        except Exception as e:
                            logger.error(
                                f"Failed to replace connection during health check: {e}")
                            # If creation fails, don't add the old broken one back either
                    else:
                        re_added_connections.append(
                            client_instance)  # Connection is OK

                for conn_instance in re_added_connections:  # Add checked/new connections back
                    await self.connections.put(conn_instance)

                logger.debug(
                    f"Health check completed: checked {checked} connections, pool stats: {self.get_stats()}")

            except Exception as e:
                logger.error(f"Error during connection pool health check: {e}")

    async def close(self):
        """Close the connection pool and all its connections"""
        if self._closed:
            return
        self._closed = True

        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                logger.debug("Health check task cancelled.")

        while not self.connections.empty():
            client = await self.connections.get()
            await client.disconnect()

        active_conns_values = list(self.active_connections.values())
        for client in active_conns_values:  # Close any remaining active connections
            await client.disconnect()
        self.active_connections.clear()

        logger.info(f"Connection pool closed. Stats: {self._stats}")

    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics

        Returns:
            Dictionary with pool statistics
        """
        return {
            **self._stats,
            'pool_size': self.size,
            'available': self.connections.qsize(),
            'active': len(self.active_connections),
            'initialized': self._initialized,
            'closed': self._closed,
        }


@dataclass
# This was defined in the problem, assuming it's what's intended.
class TCPConfig(BaseConfig):
    """TCP client configuration"""
    max_packet_size: int = 65536
    compression_enabled: bool = False
    compression_level: int = -1  # zlib.Z_DEFAULT_COMPRESSION
    auto_reconnect: bool = True
    heartbeat_interval: float = 30.0
    # Use field for mutable default, and Optional for None default
    advanced_settings: Optional[Dict[str, Any]] = field(default_factory=dict)


class TCPClient(BaseClient):
    """Enhanced TCP Client implementation with connection pooling"""

    def __init__(self, config: ClientConfig):
        """Initialize TCP client

        Args:
            config: Client configuration
        """
        super().__init__(config)
        self.config: ClientConfig = config  # Explicitly type self.config
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._buffer = PacketBuffer(config.read_buffer_size)
        self._protocol = PacketProtocol(
            compression_threshold=4096 if config.compression_enabled else 0,
            compression_level=config.compression_level
        )
        self._rate_limiter = RateLimiter(rate_limit=1000)
        self._circuit_breaker = CircuitBreaker()

        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._reconnect_task: Optional[asyncio.Task[None]] = None
        self._write_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._write_task: Optional[asyncio.Task[None]] = None
        self._last_activity = time.time()
        self._write_lock = asyncio.Lock()

        # Queue for processed messages for the receive() method
        self._processed_message_queue: asyncio.Queue[Tuple[PacketType, Any]] = asyncio.Queue(
        )
        self._read_task: Optional[asyncio.Task[None]] = None

        self._state = ClientState.DISCONNECTED
        self._stats = ConnectionStats()
        self._nagle_disabled = True

        self._callbacks: Dict[str, List[Callable[..., Coroutine[Any, Any, None]]]] = {
            'connected': [],
            'disconnected': [],
            'message': [],
            'error': [],
            'heartbeat': []
        }
        logger.info(f"TCP client initialized for {config.host}:{config.port}")

    @property
    def connected(self) -> bool:
        return self._state == ClientState.CONNECTED

    async def connect(self) -> bool:
        if self.connected:
            return True
        if self._state == ClientState.CONNECTING:
            # Wait up to connection_timeout
            for _ in range(int(self.config.connection_timeout / 0.1) + 1):
                await asyncio.sleep(0.1)
                if self.connected:
                    return True
                if self._state != ClientState.CONNECTING:
                    break  # State changed, stop waiting

        self._state = ClientState.CONNECTING
        connect_exception: Optional[Exception] = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.config.host, self.config.port),
                timeout=self.config.connection_timeout
            )
            sock = writer.get_extra_info('socket')
            if sock:
                self._apply_socket_options(sock)

            self._reader = reader
            self._writer = writer
            self._state = ClientState.CONNECTED
            self._stats.connection_time = time.time()
            self._last_activity = time.time()
            self._start_background_tasks()

            for callback in self._callbacks.get('connected', []):
                try:
                    await callback(self)  # Assuming callbacks are async
                except Exception as cb_e:
                    logger.error(f"Error in connected callback: {cb_e}")
            return True
        except Exception as e:
            connect_exception = e
            self._state = ClientState.ERROR
            self._stats.errors += 1
            logger.error(f"TCP connection failed: {str(e)}")

            for callback in self._callbacks.get('error', []):
                try:
                    await callback(self, e)  # Assuming callbacks are async
                except Exception as cb_e:
                    logger.error(f"Error in error callback: {cb_e}")

            if self.config.auto_reconnect:
                self._schedule_reconnect()
            return False

    def _apply_socket_options(self, sock: socket.socket):
        if not hasattr(self.config, 'socket_options') or self.config.socket_options is None:
            return

        socket_opts = self.config.socket_options

        if socket_opts.nodelay:  # Nagle is often part of TCPSocketOptions
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._nagle_disabled = bool(socket_opts.nodelay)

        if socket_opts.recvbuf_size > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                            socket_opts.recvbuf_size)
        if socket_opts.sendbuf_size > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                            socket_opts.sendbuf_size)

        if socket_opts.keepalive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, 'TCP_KEEPIDLE') and hasattr(socket_opts, 'keepalive_idle'):
                sock.setsockopt(socket.IPPROTO_TCP,
                                socket.TCP_KEEPIDLE, socket_opts.keepalive_idle)
            if hasattr(socket, 'TCP_KEEPINTVL') and hasattr(socket_opts, 'keepalive_interval'):
                sock.setsockopt(
                    socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, socket_opts.keepalive_interval)
            if hasattr(socket, 'TCP_KEEPCNT') and hasattr(socket_opts, 'keepalive_count'):
                sock.setsockopt(socket.IPPROTO_TCP,
                                socket.TCP_KEEPCNT, socket_opts.keepalive_count)

        if hasattr(socket_opts, 'linger') and socket_opts.linger is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack('ii', 1, socket_opts.linger))

        if hasattr(socket_opts, 'reuseaddr') and socket_opts.reuseaddr:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        logger.debug(
            f"Applied socket options: nodelay={self._nagle_disabled}, keepalive={socket_opts.keepalive}")

    def _start_background_tasks(self):
        if not self._write_task or self._write_task.done():
            self._write_task = asyncio.create_task(self._write_loop())
        if not self._read_task or self._read_task.done():
            self._read_task = asyncio.create_task(self._read_loop())
        if self.config.heartbeat_interval > 0 and (not self._heartbeat_task or self._heartbeat_task.done()):
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def disconnect(self) -> None:
        if self._state == ClientState.DISCONNECTED:
            return

        current_state = self._state
        # Set state early to prevent race conditions
        self._state = ClientState.DISCONNECTED

        tasks_to_cancel: List[Optional[asyncio.Task[None]]] = [
            self._write_task, self._read_task, self._heartbeat_task, self._reconnect_task
        ]
        active_tasks: List[asyncio.Task[None]] = []

        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
                active_tasks.append(task)

        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                logger.debug(f"Error closing writer: {e}")

        self._reader = None
        self._writer = None

        if current_state != ClientState.DISCONNECTED:  # Avoid double callbacks if already disconnected
            for callback in self._callbacks.get('disconnected', []):
                try:
                    await callback(self)  # Assuming callbacks are async
                except Exception as e:
                    logger.error(f"Error in disconnected callback: {e}")
        logger.info(
            f"Client {self.config.host}:{self.config.port} disconnected.")

    @async_performance_monitor
    async def _write_loop(self):
        batch: List[bytes] = []
        batch_size = 0
        max_batch_size = getattr(self.config, 'write_batch_size', 8192)
        max_batch_delay = 0.001

        while self.connected:  # Use self.connected for loop condition
            data_item: Optional[bytes] = None
            try:
                data_item = await asyncio.wait_for(self._write_queue.get(), timeout=max_batch_delay)
                batch.append(data_item)
                batch_size += len(data_item)
                self._write_queue.task_done()

                # Try to gather more items non-blockingly or with very short timeout
                while batch_size < max_batch_size:
                    try:
                        data_item = self._write_queue.get_nowait()
                        batch.append(data_item)
                        batch_size += len(data_item)
                        self._write_queue.task_done()
                    except asyncio.QueueEmpty:
                        break

            except asyncio.TimeoutError:  # Timeout waiting for the first item or more items
                pass  # This means queue was empty or no new items for max_batch_delay
            except asyncio.CancelledError:
                logger.debug("Write loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Write loop error: {e}")
                self._stats.errors += 1
                await self.disconnect()  # Critical error in writing, disconnect
                break

            if batch:
                try:
                    if self._writer:
                        # Combine small batches
                        if batch_size < 65536 and len(batch) > 1:
                            combined_data = b''.join(batch)
                            await self._protected_send(combined_data)
                        else:  # Send items individually or as a single large item
                            for chunk in batch:
                                await self._protected_send(chunk)
                    else:  # Should not happen if self.connected is true
                        logger.error(
                            "Writer is None in _write_loop while connected.")
                        await self.disconnect()
                        break
                except Exception as e:  # Error during sending
                    logger.error(f"Error sending batch in _write_loop: {e}")
                    await self.disconnect()  # Disconnect on send error
                    break  # Exit loop
                finally:
                    batch.clear()
                    batch_size = 0

            if not self.connected:  # Check connection status again before sleeping/looping
                break
        logger.debug("Write loop exited")

    async def _read_loop(self):
        while self.connected and self._reader:
            try:
                header_data = await asyncio.wait_for(
                    self._reader.readexactly(PacketProtocol.HEADER_SIZE),
                    timeout=self.config.timeout +
                    # More generous timeout
                    (self.config.heartbeat_interval if self.config.heartbeat_interval > 0 else 30.0)
                )
                if not header_data:
                    break  # Connection closed

                magic, ptype_val, compressed, length = struct.unpack(
                    PacketProtocol.HEADER_FORMAT, header_data
                )
                if magic != PacketProtocol.MAGIC:
                    logger.error(f"Invalid magic number: 0x{magic:04X}")
                    continue

                packet_body = await asyncio.wait_for(
                    self._reader.readexactly(length), timeout=self.config.timeout
                )
                if not packet_body:
                    break  # Connection closed

                if compressed:
                    try:
                        # Run decompress in a thread to avoid blocking event loop for large data
                        packet_body = await asyncio.to_thread(zlib.decompress, packet_body)
                    except Exception as e:
                        logger.error(f"Failed to decompress data: {e}")
                        continue

                packet_type = PacketType(ptype_val)
                self._stats.bytes_received += PacketProtocol.HEADER_SIZE + length
                self._stats.messages_received += 1
                self._last_activity = time.time()
                await self._process_packet(packet_type, packet_body)

            except asyncio.IncompleteReadError:
                logger.warning("Connection closed by peer (incomplete read).")
                await self.disconnect()
                break
            except asyncio.TimeoutError:
                logger.warning("Timeout reading from server.")
                await self.disconnect()
                break
            except asyncio.CancelledError:
                logger.debug("Read loop cancelled.")
                break
            except ConnectionResetError:
                logger.warning("Connection reset by peer.")
                await self.disconnect()
                break
            except Exception as e:
                logger.error(f"Read loop error: {e}")
                self._stats.errors += 1
                await self.disconnect()  # Disconnect on other read errors
                break

        if self.connected:  # If loop exited but still connected, means an issue
            await self.disconnect()
        logger.debug("Read loop exited")

    async def _heartbeat_loop(self):
        while self.connected:
            try:
                await asyncio.sleep(self.config.heartbeat_interval)
                if self.connected:  # Re-check connection before sending
                    await self.send_heartbeat()
            except asyncio.CancelledError:
                logger.debug("Heartbeat loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")
                self._stats.errors += 1
                # Consider if disconnect is needed here
        logger.debug("Heartbeat loop exited")

    async def send_heartbeat(self):
        if self.connected:
            try:
                heartbeat_data = self._protocol.create_heartbeat()
                # send_raw will put it on _write_queue
                await self.send_raw(heartbeat_data)
                for callback in self._callbacks.get('heartbeat', []):
                    try:
                        await callback(self)  # Assuming callbacks are async
                    except Exception as e:
                        logger.error(f"Error in heartbeat callback: {e}")
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")
                self._stats.errors += 1

    async def send(self, data: Union[bytes, Dict[str, Any]], packet_type: PacketType = PacketType.DATA) -> bool:
        if not self.connected:
            logger.warning("Attempted to send data while not connected.")
            return False  # Changed to return bool
        try:
            packet = self._protocol.create_packet(packet_type, data)
            await self._write_queue.put(packet)
            self._stats.messages_sent += 1
            self._stats.bytes_sent += len(packet)
            return True  # Changed to return bool
        except Exception as e:
            logger.error(f"Error queueing data for send: {e}")
            return False

    async def send_raw(self, data: bytes) -> bool:
        if not self.connected:
            logger.warning("Attempted to send raw data while not connected.")
            return False
        try:
            await self._write_queue.put(data)
            # Raw sends might not always be counted as "messages" in the same way
            # self._stats.messages_sent += 1
            self._stats.bytes_sent += len(data)
            return True
        except Exception as e:
            logger.error(f"Error queueing raw data for send: {e}")
            return False

    async def _protected_send(self, data: bytes):
        if not self._writer:
            logger.error("Protected send called with no writer.")
            await self.disconnect()
            raise ConnectionError("Writer is not available.")
        try:
            async with self._write_lock:  # Ensure _write_lock is defined
                self._writer.write(data)
                await self._writer.drain()
                self._last_activity = time.time()
        except ConnectionResetError as e:
            logger.error(f"Connection reset during send: {e}")
            await self.disconnect()
            raise
        except Exception as e:
            logger.error(f"Failed to send data: {e}")
            self._stats.errors += 1
            await self.disconnect()
            raise

    async def _process_packet(self, packet_type: PacketType, data: bytes):
        # Default to raw bytes if not JSON or other specific parsing
        parsed_content: Any = data
        if packet_type == PacketType.HEARTBEAT:
            logger.debug(
                "Received heartbeat response (if applicable) or server heartbeat.")
            # Potentially update latency stats if heartbeat is a ping/pong
            # Also queue heartbeats
            await self._processed_message_queue.put((packet_type, parsed_content))
            return

        # Attempt to parse JSON for relevant types, or handle based on protocol
        # This is a generic handler; specific protocols might differ.
        try:
            # Example: if DATA or CONTROL packets are expected to be JSON
            if packet_type in [PacketType.DATA, PacketType.CONTROL, PacketType.ERROR, PacketType.ACK]:
                parsed_content = self._protocol.try_parse_json(data)
        except Exception as e:
            logger.error(
                f"Error parsing packet content for type {packet_type.name}: {e}")
            # Decide if to queue raw data or an error indicator

        await self._processed_message_queue.put((packet_type, parsed_content))

        for callback in self._callbacks.get('message', []):
            try:
                # Callbacks now receive the parsed content
                # Assuming callbacks are async
                await callback(self, packet_type, parsed_content)
            except Exception as e:
                logger.error(f"Error in message callback: {e}")

    async def receive(self, timeout: Optional[float] = None) -> Tuple[PacketType, Any]:
        """Receive a processed message from the client.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            A tuple containing the PacketType and the processed message data.

        Raises:
            RuntimeError: If not connected and no messages are in the queue.
            asyncio.TimeoutError: If the timeout is reached.
        """
        if not self.connected and self._processed_message_queue.empty():
            raise RuntimeError("Not connected and no messages in queue.")
        try:
            return await asyncio.wait_for(self._processed_message_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            # Re-raise asyncio.TimeoutError as it's standard
            raise
        except Exception as e:  # Catch other potential QueueErrors, though less likely with wait_for
            logger.error(f"Error receiving message from queue: {e}")
            raise RuntimeError(f"Failed to receive message: {e}")

    def on(self, event: str, callback: Callable[..., Coroutine[Any, Any, None]]):
        if event in self._callbacks:
            self._callbacks[event].append(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    def off(self, event: str, callback: Callable[..., Coroutine[Any, Any, None]]):
        if event in self._callbacks:
            try:
                self._callbacks[event].remove(callback)
            except ValueError:
                logger.warning(
                    f"Callback not found for event '{event}' during off().")
        else:
            raise ValueError(f"Unknown event: {event}")

    def _schedule_reconnect(self):
        if not self.config.auto_reconnect:
            return
        if not self._reconnect_task or self._reconnect_task.done():
            logger.info("Scheduling reconnection task.")
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
        else:
            logger.debug("Reconnection task already scheduled or running.")

    async def _reconnect_loop(self):
        logger.info("Reconnect loop started.")
        retry = 0
        # Check state before starting loop, in case it connected quickly by other means
        while self._state != ClientState.CONNECTED and self.config.auto_reconnect:
            retry_policy = getattr(self.config, 'retry_policy', None)
            delay: float
            if retry_policy:
                if retry >= retry_policy.max_retries:
                    logger.error(
                        f"Reconnect failed after {retry} attempts (max retries reached), giving up.")
                    # Or DISCONNECTED if giving up means no more attempts
                    self._state = ClientState.ERROR
                    break
                delay = retry_policy.get_retry_delay(retry)
            else:
                if retry >= 10:  # Default max retries if no policy
                    logger.error(
                        f"Reconnect failed after {retry} attempts (default max), giving up.")
                    self._state = ClientState.ERROR
                    break
                delay = min(30.0, (2 ** retry) * 0.1 + random.uniform(0, 0.1))

            retry += 1
            logger.info(
                f"Reconnect attempt {retry} scheduled in {delay:.2f} seconds for {self.config.host}:{self.config.port}")

            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                logger.info("Reconnect loop sleep cancelled.")
                break  # Exit if cancelled during sleep

            if self._state == ClientState.CONNECTED:  # Check if connected during sleep
                logger.info(
                    "Reconnected successfully by another task while waiting.")
                break

            logger.info(
                f"Attempting to reconnect (try {retry}) to {self.config.host}:{self.config.port}")
            # Set state to RECONNECTING before attempting to connect
            self._state = ClientState.RECONNECTING
            success = await self.connect()  # connect() will set state to CONNECTED or ERROR

            if success:
                logger.info(
                    f"Reconnect successful after {retry} attempt(s) to {self.config.host}:{self.config.port}")
                self._stats.reconnects += 1
                # self.connect() already sets state to CONNECTED
                break
            else:  # connect() failed, it would have set state to ERROR
                logger.warning(
                    f"Reconnect attempt {retry} failed for {self.config.host}:{self.config.port}. Current state: {self._state.name}")
                # connect() handles scheduling next reconnect if auto_reconnect is on and state is ERROR
                # but this loop is the primary driver for retries.
                # If connect sets state to ERROR, this loop will continue if max_retries not hit.
                if self._state != ClientState.ERROR and self._state != ClientState.RECONNECTING:
                    # If state changed to something unexpected (e.g. DISCONNECTED by external call)
                    logger.info(
                        f"Reconnect loop exiting due to state change to {self._state.name}")
                    break

        if self._state != ClientState.CONNECTED and self.config.auto_reconnect:
            logger.warning(
                f"Reconnect loop finished for {self.config.host}:{self.config.port}, but client is not connected. Final state: {self._state.name}")
        elif not self.config.auto_reconnect:
            logger.info(
                f"Auto-reconnect disabled for {self.config.host}:{self.config.port}. Reconnect loop will not run.")
        else:
            logger.info(
                f"Reconnect loop exited for {self.config.host}:{self.config.port}. Final state: {self._state.name}")
        self._reconnect_task = None  # Clear task when loop finishes or is cancelled

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self._state.name,
            "connected": self.connected,
            "host": self.config.host,
            "port": self.config.port,
            "last_activity": self._last_activity,
            "idle_time": time.time() - self._last_activity if self.connected else -1,
            "connection_stats": self._stats.to_dict()
        }
