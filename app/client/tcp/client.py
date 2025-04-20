import asyncio
from typing import Optional, Callable, Any, List, Dict, TYPE_CHECKING, Union
import zlib
from ..base import BaseClient, BaseConfig
from dataclasses import dataclass
import time
import functools
import random
from contextlib import asynccontextmanager
import socket
import struct

if TYPE_CHECKING:
    from .client import TCPClient
from loguru import logger

from .config import ClientConfig, TCPSocketOptions
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
        self.connections = asyncio.Queue(maxsize=size)
        self.active_connections = {}  # Track active connections
        self.semaphore = asyncio.Semaphore(size)
        self._health_check_task = None
        self._stats = {
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
        logger.info(f"Initialized connection pool with size={size} for {config.host}:{config.port}")

    async def initialize(self):
        """Optimized connection pool initialization with lazy loading"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        async with self._init_lock:
            if self._initialized:
                return

            # Initialize half of the connections, load others on demand
            initial_conns = max(1, self.size // 2)
            for _ in range(initial_conns):
                try:
                    client = await self._create_connection()
                    await self.connections.put(client)
                except Exception as e:
                    logger.error(f"Failed to initialize connection: {e}")

            self._health_check_task = asyncio.create_task(self._health_check())
            self._initialized = True
            logger.info(f"Connection pool initialized with {initial_conns} initial connections")

    async def _create_connection(self) -> 'TCPClient':
        """Create a new connection
        
        Returns:
            New TCPClient instance
        """
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
            try:
                # Try to get a connection from the pool
                try:
                    client = await asyncio.wait_for(self.connections.get(), timeout=0.1)

                    # Check if the connection is valid, reconnect if necessary
                    if not client.connected:
                        logger.debug("Reconnecting stale connection from pool")
                        await client.connect()
                        if not client.connected:
                            logger.warning("Failed to reconnect stale connection, creating new one")
                            client = await self._create_connection()
                except asyncio.TimeoutError:
                    # Create a new connection
                    logger.debug("Connection pool empty, creating new connection")
                    client = await self._create_connection()

                # Mark as active connection
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
            # If connection is broken, create a new one for the pool
            try:
                logger.debug("Replacing broken connection in pool")
                new_client = await self._create_connection()
                await self.connections.put(new_client)
            except Exception as e:
                logger.error(f"Failed to replace broken connection: {e}")

    @asynccontextmanager
    async def connection(self):
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
                await asyncio.sleep(60)  # Check every minute
                self._stats['health_checks'] += 1

                # Check all active connections
                for client_id, client in list(self.active_connections.items()):
                    if not client.connected or client._stats.get_idle_time() > 300:  # 5 minutes idle
                        logger.warning(
                            f"Detected stale connection, will be replaced when returned to pool")

                # Try to check connections in the pool
                checked = 0
                for _ in range(min(2, self.connections.qsize())):  # Check at most 2 connections
                    if self.connections.empty():
                        break

                    client = await self.connections.get()
                    if not client.connected:
                        logger.info("Replacing broken connection in pool")
                        try:
                            new_client = await self._create_connection()
                            await self.connections.put(new_client)
                            self._stats['reconnects'] += 1
                        except Exception as e:
                            logger.error(
                                f"Failed to replace connection during health check: {e}")
                    else:
                        # Connection is OK, return to pool
                        await self.connections.put(client)

                    checked += 1

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

        # Close all connections
        while not self.connections.empty():
            client = await self.connections.get()
            await client.disconnect()

        # Log final stats
        logger.info(f"Connection pool closed. Stats: {self._stats}")

    def get_stats(self):
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
class TCPConfig(BaseConfig):
    """TCP client configuration"""
    max_packet_size: int = 65536
    compression_enabled: bool = False
    compression_level: int = -1
    auto_reconnect: bool = True
    heartbeat_interval: float = 30.0
    advanced_settings: dict = None


class TCPClient(BaseClient):
    """Enhanced TCP Client implementation with connection pooling"""

    def __init__(self, config: ClientConfig):
        """Initialize TCP client
        
        Args:
            config: Client configuration
        """
        super().__init__(config)
        self._reader = None
        self._writer = None
        self._buffer = PacketBuffer(config.read_buffer_size)
        self._protocol = PacketProtocol(
            compression_threshold=4096 if config.compression_enabled else 0, 
            compression_level=config.compression_level
        )
        self._rate_limiter = RateLimiter(rate_limit=1000)
        self._circuit_breaker = CircuitBreaker()
        self._heartbeat_task = None
        self._reconnect_task = None
        self._write_queue = asyncio.Queue()
        self._write_task = None
        self._last_activity = time.time()
        self._write_lock = asyncio.Lock()
        self._read_queue = asyncio.Queue()
        self._read_task = None
        self._state = ClientState.DISCONNECTED
        self._stats = ConnectionStats()
        self._nagle_disabled = True  # Disable Nagle's algorithm by default

        # Event handlers
        self._callbacks: dict[str, list[Callable]] = {
            'connected': [],
            'disconnected': [],
            'message': [],
            'error': [],
            'heartbeat': []
        }

        logger.info(f"TCP client initialized for {config.host}:{config.port}")

    @property
    def connected(self) -> bool:
        """Check if client is connected
        
        Returns:
            True if connected
        """
        return self._state == ClientState.CONNECTED

    async def connect(self) -> bool:
        """Establish TCP connection
        
        Returns:
            True if connection was successful
        """
        if self._state == ClientState.CONNECTED:
            return True

        if self._state == ClientState.CONNECTING:
            # Wait for existing connection attempt to complete
            for _ in range(10):
                await asyncio.sleep(0.1)
                if self._state == ClientState.CONNECTED:
                    return True
                elif self._state != ClientState.CONNECTING:
                    break

        self._state = ClientState.CONNECTING

        try:
            # Set connection timeout
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.host,
                    self.config.port
                ),
                timeout=self.config.connection_timeout
            )

            # Apply TCP socket options
            sock = writer.get_extra_info('socket')
            if sock:
                # Apply socket options
                self._apply_socket_options(sock)

            self._reader = reader
            self._writer = writer
            self._state = ClientState.CONNECTED
            self._stats.connection_time = time.time()
            self._last_activity = time.time()

            # Start background tasks
            self._start_background_tasks()

            # Call connected callbacks
            for callback in self._callbacks.get('connected', []):
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(self)
                    else:
                        callback(self)
                except Exception as e:
                    logger.error(f"Error in connected callback: {e}")

            return True

        except Exception as e:
            self._state = ClientState.ERROR
            self._stats.errors += 1
            logger.error(f"TCP connection failed: {str(e)}")

            # Call error callbacks
            for callback in self._callbacks.get('error', []):
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(self, e)
                    else:
                        callback(self, e)
                except Exception as e:
                    logger.error(f"Error in error callback: {e}")

            # Schedule reconnect if auto-reconnect is enabled
            if getattr(self.config, 'auto_reconnect', True):
                self._schedule_reconnect()

            return False

    def _apply_socket_options(self, sock: socket.socket):
        """Apply TCP socket options
        
        Args:
            sock: Socket object
        """
        if not hasattr(self.config, 'socket_options'):
            return
            
        socket_opts = self.config.socket_options
        
        # Disable Nagle's algorithm
        if self._nagle_disabled:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Set buffer sizes
        if socket_opts.recvbuf_size > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, socket_opts.recvbuf_size)
        if socket_opts.sendbuf_size > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, socket_opts.sendbuf_size)
            
        # Set keepalive options
        if socket_opts.keepalive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            # Set platform-specific keepalive options if available
            if hasattr(socket, 'TCP_KEEPIDLE'):  # Linux
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, socket_opts.keepalive_idle)
            if hasattr(socket, 'TCP_KEEPINTVL'):  # Linux
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, socket_opts.keepalive_interval)
            if hasattr(socket, 'TCP_KEEPCNT'):  # Linux
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, socket_opts.keepalive_count)
            # Windows/BSD equivalent options could be added here
            
        # Set linger option
        if socket_opts.linger is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, 
                            struct.pack('ii', 1, socket_opts.linger))
                            
        # Set address reuse
        if socket_opts.reuseaddr:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
        logger.debug(f"Applied socket options: nodelay={self._nagle_disabled}, keepalive={socket_opts.keepalive}")

    def _start_background_tasks(self):
        """Start background processing tasks"""
        if not self._write_task or self._write_task.done():
            self._write_task = asyncio.create_task(self._write_loop())

        if not self._read_task or self._read_task.done():
            self._read_task = asyncio.create_task(self._read_loop())

        if self.config.heartbeat_interval > 0 and (not self._heartbeat_task or self._heartbeat_task.done()):
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def disconnect(self) -> None:
        """Disconnect from server"""
        # Cancel background tasks
        tasks = []
        if self._write_task and not self._write_task.done():
            self._write_task.cancel()
            tasks.append(self._write_task)

        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
            tasks.append(self._read_task)

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            tasks.append(self._heartbeat_task)

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            tasks.append(self._reconnect_task)

        # Wait for tasks to complete cancellation
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Close connection
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                logger.debug(f"Error closing writer: {e}")

        self._state = ClientState.DISCONNECTED
        self._reader = None
        self._writer = None

        # Call disconnected callbacks
        for callback in self._callbacks.get('disconnected', []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self)
                else:
                    callback(self)
            except Exception as e:
                logger.error(f"Error in disconnected callback: {e}")

    @async_performance_monitor
    async def _write_loop(self):
        """Optimized batch write loop"""
        batch = []
        batch_size = 0
        max_batch_size = getattr(self.config, 'write_batch_size', 8192)
        max_batch_delay = 0.001  # 1ms batch delay

        while self._state == ClientState.CONNECTED:
            try:
                # Get first data item or wait
                data = await self._write_queue.get()
                batch.append(data)
                batch_size = len(data)

                # Collect more data in non-blocking mode
                batch_start_time = time.time()
                while batch_size < max_batch_size and time.time() - batch_start_time < max_batch_delay:
                    try:
                        if self._write_queue.empty():
                            # If queue is empty, wait a short time
                            await asyncio.sleep(0.0001)  # 100 microseconds
                            if self._write_queue.empty():
                                break

                        data = self._write_queue.get_nowait()
                        batch.append(data)
                        batch_size += len(data)
                    except asyncio.QueueEmpty:
                        break

                if batch:
                    # If data size is small, combine
                    if batch_size < 65536:
                        combined_data = b''.join(batch)
                        await self._protected_send(combined_data)
                    else:
                        # Send in chunks for large data
                        for chunk in batch:
                            await self._protected_send(chunk)

                    batch.clear()
                    batch_size = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Write loop error: {e}")
                self._stats.errors += 1
                await asyncio.sleep(0.1)  # Short pause on error

        logger.debug("Write loop exited")

    async def _read_loop(self):
        """Background read loop"""
        while self._state == ClientState.CONNECTED and self._reader:
            try:
                # Read header
                header_data = await asyncio.wait_for(
                    self._reader.readexactly(PacketProtocol.HEADER_SIZE),
                    timeout=60.0
                )

                if not header_data or len(header_data) != PacketProtocol.HEADER_SIZE:
                    logger.error("Failed to read packet header")
                    break

                # Parse header
                magic, packet_type, compressed, length = struct.unpack(
                    PacketProtocol.HEADER_FORMAT,
                    header_data
                )

                # Validate magic
                if magic != PacketProtocol.MAGIC:
                    logger.error(f"Invalid magic number: 0x{magic:04X}")
                    continue

                # Read data
                data = await asyncio.wait_for(
                    self._reader.readexactly(length),
                    timeout=60.0
                )

                if not data or len(data) != length:
                    logger.error("Failed to read packet data")
                    break

                # Process data based on packet type
                packet_type_enum = PacketType(packet_type)
                
                # Decompress if needed
                if compressed:
                    try:
                        data = await asyncio.to_thread(lambda: zlib.decompress(data))
                    except Exception as e:
                        logger.error(f"Failed to decompress data: {e}")
                        continue

                # Update stats
                self._stats.bytes_received += length
                self._stats.messages_received += 1
                self._last_activity = time.time()

                # Process packet
                await self._process_packet(packet_type_enum, data)

            except asyncio.CancelledError:
                break
            except asyncio.IncompleteReadError:
                logger.error("Connection closed while reading")
                break
            except Exception as e:
                logger.error(f"Read loop error: {e}")
                self._stats.errors += 1
                await asyncio.sleep(0.1)  # Short pause on error

        # Connection lost or error occurred
        if self._state == ClientState.CONNECTED:
            logger.warning("Read loop ended while connected, initiating disconnect")
            asyncio.create_task(self.disconnect())
            
        logger.debug("Read loop exited")

    async def _heartbeat_loop(self):
        """Heartbeat send loop"""
        while self._state == ClientState.CONNECTED:
            try:
                await asyncio.sleep(self.config.heartbeat_interval)
                await self.send_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")
                self._stats.errors += 1

        logger.debug("Heartbeat loop exited")

    async def send_heartbeat(self):
        """Send heartbeat packet"""
        if self._state == ClientState.CONNECTED:
            try:
                heartbeat_data = self._protocol.create_heartbeat()
                await self.send_raw(heartbeat_data)
                
                # Call heartbeat callbacks
                for callback in self._callbacks.get('heartbeat', []):
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(self)
                        else:
                            callback(self)
                    except Exception as e:
                        logger.error(f"Error in heartbeat callback: {e}")

            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")
                self._stats.errors += 1

    async def send(self, data: Union[bytes, dict], packet_type: PacketType = PacketType.DATA):
        """Send data packet
        
        Args:
            data: Data to send (bytes or dictionary)
            packet_type: Type of packet
            
        Raises:
            RuntimeError: If client is not connected
        """
        if self._state != ClientState.CONNECTED:
            raise RuntimeError("Client is not connected")

        packet = self._protocol.create_packet(packet_type, data)
        await self._write_queue.put(packet)
        self._stats.messages_sent += 1
        self._stats.bytes_sent += len(packet)

    async def send_raw(self, data: bytes):
        """Send raw data directly
        
        Args:
            data: Raw data to send
            
        Raises:
            RuntimeError: If client is not connected
        """
        if self._state != ClientState.CONNECTED:
            raise RuntimeError("Client is not connected")

        await self._write_queue.put(data)
        self._stats.messages_sent += 1
        self._stats.bytes_sent += len(data)
        
    async def _protected_send(self, data: bytes):
        """Protected send method with error handling
        
        Args:
            data: Data to send
        """
        try:
            async with self._write_lock:
                self._writer.write(data)
                await self._writer.drain()
                self._last_activity = time.time()
        except Exception as e:
            logger.error(f"Failed to send data: {e}")
            self._stats.errors += 1
            await self.disconnect()

    async def _process_packet(self, packet_type: PacketType, data: bytes):
        """Process received packet
        
        Args:
            packet_type: Type of packet
            data: Packet data
        """
        # Handle different packet types
        if packet_type == PacketType.HEARTBEAT:
            # For heartbeats, just update activity time
            logger.debug("Received heartbeat")
            return

        # Call message callbacks for data packets
        for callback in self._callbacks.get('message', []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self, packet_type, data)
                else:
                    callback(self, packet_type, data)
            except Exception as e:
                logger.error(f"Error in message callback: {e}")

    def on(self, event: str, callback: Callable):
        """Register event callback
        
        Args:
            event: Event name
            callback: Callback function
            
        Raises:
            ValueError: If event is unknown
        """
        if event in self._callbacks:
            self._callbacks[event].append(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    def off(self, event: str, callback: Callable):
        """Unregister event callback
        
        Args:
            event: Event name
            callback: Callback function
            
        Raises:
            ValueError: If event is unknown
        """
        if event in self._callbacks:
            if callback in self._callbacks[event]:
                self._callbacks[event].remove(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    def _schedule_reconnect(self):
        """Schedule reconnection task"""
        if not self._reconnect_task or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        """Reconnection loop with exponential backoff"""
        retry = 0
        while self._state != ClientState.CONNECTED:
            try:
                # Get retry delay with exponential backoff
                if hasattr(self.config, 'retry_policy'):
                    delay = self.config.retry_policy.get_retry_delay(retry)
                else:
                    # Default backoff if no retry policy
                    delay = min(30, (2 ** retry) * 0.1 + random.uniform(0, 0.1))
                
                retry += 1
                logger.info(f"Reconnect attempt {retry} scheduled in {delay:.2f} seconds")
                await asyncio.sleep(delay)
                
                logger.info(f"Attempting to reconnect (try {retry})")
                success = await self.connect()
                
                if success:
                    logger.info(f"Reconnect successful after {retry} attempt(s)")
                    self._stats.reconnects += 1
                    break
                    
                # Check if max retries reached
                if hasattr(self.config, 'retry_policy') and retry >= self.config.retry_policy.max_retries:
                    logger.error(f"Reconnect failed after {retry} attempts, giving up")
                    break
                    
            except asyncio.CancelledError:
                logger.debug("Reconnect loop cancelled")
                break
            except Exception as e:
                logger.error(f"Reconnect loop error: {e}")
                self._stats.errors += 1

        logger.debug("Reconnect loop exited")
        
    def get_stats(self) -> dict:
        """Get client statistics
        
        Returns:
            Dictionary with client statistics
        """
        return {
            "state": self._state.name,
            "connected": self.connected,
            "host": self.config.host,
            "port": self.config.port,
            "last_activity": self._last_activity,
            "idle_time": time.time() - self._last_activity,
            "stats": self._stats.to_dict()
        }
