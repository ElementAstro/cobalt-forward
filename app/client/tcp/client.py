import asyncio
from typing import Optional, Callable, Any, List, Dict, TYPE_CHECKING
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
        self.active_connections = {}  # 追踪活动连接
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

    async def initialize(self):
        """优化的连接池初始化，懒加载"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        async with self._init_lock:
            if self._initialized:
                return

            # 初始化一半的连接，其余按需加载
            initial_conns = max(1, self.size // 2)
            for _ in range(initial_conns):
                try:
                    client = await self._create_connection()
                    await self.connections.put(client)
                except Exception as e:
                    logger.error(f"Failed to initialize connection: {e}")

            self._health_check_task = asyncio.create_task(self._health_check())
            self._initialized = True

    async def _create_connection(self) -> 'TCPClient':
        """创建新连接"""
        client = TCPClient(self.config)
        await client.connect()
        self._stats['created'] += 1
        return client

    async def get_connection(self) -> 'TCPClient':
        """优化的连接获取"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        if not self._initialized:
            await self.initialize()

        async with self.semaphore:
            try:
                # 尝试从池中获取连接
                try:
                    client = await asyncio.wait_for(self.connections.get(), timeout=0.1)

                    # 检查连接是否有效，如果无效则重新创建
                    if not client.connected:
                        await client.connect()
                        if not client.connected:
                            client = await self._create_connection()
                except asyncio.TimeoutError:
                    # 创建新连接
                    client = await self._create_connection()

                # 标记为活动连接
                self.active_connections[id(client)] = client
                self._stats['borrowed'] += 1
                return client

            except Exception as e:
                self._stats['errors'] += 1
                logger.error(f"Failed to get connection: {e}")
                raise

    async def release_connection(self, client: 'TCPClient'):
        """归还连接到连接池"""
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
            # 如果连接已断开，则创建新连接放入池中
            try:
                new_client = await self._create_connection()
                await self.connections.put(new_client)
            except Exception as e:
                logger.error(f"Failed to replace broken connection: {e}")

    @asynccontextmanager
    async def connection(self):
        """连接上下文管理器，自动归还连接"""
        client = await self.get_connection()
        try:
            yield client
        finally:
            await self.release_connection(client)

    async def _health_check(self):
        """定期健康检查"""
        while not self._closed:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次
                self._stats['health_checks'] += 1

                # 检查所有活跃连接
                for client_id, client in list(self.active_connections.items()):
                    if not client.connected or client._stats.get_idle_time() > 300:  # 5分钟无活动
                        logger.warning(
                            f"Detected stale connection, will be replaced when returned to pool")

                # 尝试检查连接池中的连接
                checked = 0
                for _ in range(min(2, self.connections.qsize())):  # 每次最多检查2个连接
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
                        # 连接正常，放回池中
                        await self.connections.put(client)

                    checked += 1

                logger.debug(
                    f"Health check completed: checked {checked} connections, pool stats: {self.get_stats()}")

            except Exception as e:
                logger.error(f"Error during connection pool health check: {e}")

    async def close(self):
        """关闭连接池"""
        if self._closed:
            return

        self._closed = True

        if self._health_check_task:
            self._health_check_task.cancel()

        # 关闭所有连接
        while not self.connections.empty():
            client = await self.connections.get()
            await client.disconnect()

        # 记录最终状态
        logger.info(f"Connection pool closed. Stats: {self._stats}")

    def get_stats(self):
        """获取连接池统计信息"""
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
        self._last_activity = time.time()
        self._write_lock = asyncio.Lock()
        self._read_queue = asyncio.Queue()
        self._read_task = None
        self._state = ClientState.DISCONNECTED
        self._stats = ConnectionStats()
        self._nagle_disabled = True  # 默认禁用Nagle算法

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
        """实现优化的TCP连接"""
        if self._state == ClientState.CONNECTED:
            return True

        if self._state == ClientState.CONNECTING:
            # 等待已有的连接完成
            for _ in range(10):
                await asyncio.sleep(0.1)
                if self._state == ClientState.CONNECTED:
                    return True
                elif self._state != ClientState.CONNECTING:
                    break

        self._state = ClientState.CONNECTING

        try:
            # 设置连接超时
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.host,
                    self.config.port
                ),
                timeout=5.0
            )

            # 应用配置的TCP选项
            if hasattr(self.config, 'advanced_settings'):
                sock = writer.get_extra_info('socket')
                if sock:
                    # 禁用Nagle算法
                    if self._nagle_disabled:
                        sock.setsockopt(socket.IPPROTO_TCP,
                                        socket.TCP_NODELAY, 1)

                    # 设置缓冲区大小
                    if hasattr(self.config, 'socket_options'):
                        socket_opts = self.config.socket_options
                        if socket_opts.recvbuf_size > 0:
                            sock.setsockopt(
                                socket.SOL_SOCKET, socket.SO_RCVBUF, socket_opts.recvbuf_size)
                        if socket_opts.sendbuf_size > 0:
                            sock.setsockopt(
                                socket.SOL_SOCKET, socket.SO_SNDBUF, socket_opts.sendbuf_size)
                        if socket_opts.keepalive:
                            sock.setsockopt(socket.SOL_SOCKET,
                                            socket.SO_KEEPALIVE, 1)

            self._reader = reader
            self._writer = writer
            self._state = ClientState.CONNECTED
            self._stats.connection_time = time.time()
            self._last_activity = time.time()

            # 启动后台任务
            self._start_background_tasks()

            # 调用连接回调
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

            # 调用错误回调
            for callback in self._callbacks.get('error', []):
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(self, e)
                    else:
                        callback(self, e)
                except Exception as e:
                    logger.error(f"Error in error callback: {e}")

            # 如果启用自动重连，则安排重连任务
            if getattr(self.config, 'auto_reconnect', True):
                self._schedule_reconnect()

            return False

    def _start_background_tasks(self):
        """启动后台任务"""
        if not self._write_task or self._write_task.done():
            self._write_task = asyncio.create_task(self._write_loop())

        if not self._read_task or self._read_task.done():
            self._read_task = asyncio.create_task(self._read_loop())

        if self.config.heartbeat_interval > 0 and (not self._heartbeat_task or self._heartbeat_task.done()):
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def disconnect(self) -> None:
        """实现优化的TCP断开连接"""
        # 取消后台任务
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

        # 等待任务取消完成
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 关闭连接
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                logger.debug(f"Error closing writer: {e}")

        self._state = ClientState.DISCONNECTED
        self._reader = None
        self._writer = None

        # 调用断开连接回调
        for callback in self._callbacks.get('disconnected', []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self)
                else:
                    callback(self)
            except Exception as e:
                logger.error(f"Error in disconnected callback: {e}")

    async def _write_loop(self):
        """优化的批量写入循环"""
        batch = []
        batch_size = 0
        max_batch_size = getattr(self.config, 'write_batch_size', 8192)
        max_batch_delay = 0.001  # 1ms 批处理延迟

        while self._state == ClientState.CONNECTED:
            try:
                # 获取第一个数据项或等待
                data = await self._write_queue.get()
                batch.append(data)
                batch_size = len(data)

                # 非阻塞方式收集更多数据
                batch_start_time = time.time()
                while batch_size < max_batch_size and time.time() - batch_start_time < max_batch_delay:
                    try:
                        if self._write_queue.empty():
                            # 如果队列为空，等待一小段时间
                            await asyncio.sleep(0.0001)  # 100微秒
                            if self._write_queue.empty():
                                break

                        data = self._write_queue.get_nowait()
                        batch.append(data)
                        batch_size += len(data)
                    except asyncio.QueueEmpty:
                        break

                if batch:
                    # 如果数据量小，可以直接组合
                    if batch_size < 65536:
                        combined_data = b''.join(batch)
                        await self._protected_send(combined_data)
                    else:
                        # 数据量大时分批发送
                        for chunk in batch:
                            await self._protected_send(chunk)

                    batch.clear()
                    batch_size = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Write loop error: {e}")
                self._stats.errors += 1
                await asyncio.sleep(0.1)  # 出错时短暂暂停

        logger.debug("Write loop exited")

    async def _read_loop(self):
        """后台读取循环"""
        while self._state == ClientState.CONNECTED and self._reader:
            try:
                # 读取头部
                header_data = await asyncio.wait_for(
                    self._reader.readexactly(PacketProtocol.HEADER_SIZE),
                    timeout=60.0
                )

                if not header_data or len(header_data) != PacketProtocol.HEADER_SIZE:
                    logger.error("Failed to read packet header")
                    break

                # 解析头部
                magic, length, checksum = struct.unpack(
                    PacketProtocol.HEADER_FORMAT,
                    header_data
                )

                # 读取数据
                data = await asyncio.wait_for(
                    self._reader.readexactly(length),
                    timeout=60.0
                )

                if not data or len(data) != length:
                    logger.error("Failed to read packet data")
                    break

                # 校验数据
                if PacketProtocol.calculate_checksum(data) != checksum:
                    logger.error("Checksum mismatch")
                    continue

                # 处理数据
                await self._process_packet(magic, data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Read loop error: {e}")
                self._stats.errors += 1
                await asyncio.sleep(0.1)  # 出错时短暂暂停

        logger.debug("Read loop exited")

    async def _heartbeat_loop(self):
        """心跳包发送循环"""
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
        """发送心跳包"""
        if self._state == ClientState.CONNECTED:
            try:
                await self.send(PacketProtocol.HEARTBEAT_MAGIC, b'')
                self._stats.heartbeats += 1

                # 调用心跳回调
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

    async def send(self, magic: int, data: bytes):
        """发送数据包"""
        if self._state != ClientState.CONNECTED:
            raise RuntimeError("Client is not connected")

        packet = self._protocol.create_packet(magic, data)
        await self._write_queue.put(packet)

    async def _protected_send(self, data: bytes):
        """受保护的发送方法，处理发送错误"""
        try:
            async with self._write_lock:
                self._writer.write(data)
                await self._writer.drain()
                self._last_activity = time.time()
        except Exception as e:
            logger.error(f"Failed to send data: {e}")
            self._stats.errors += 1
            await self.disconnect()

    async def _process_packet(self, magic: int, data: bytes):
        """处理接收到的数据包"""
        if magic == PacketProtocol.HEARTBEAT_MAGIC:
            logger.debug("Received heartbeat")
            return

        # 调用消息回调
        for callback in self._callbacks.get('message', []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self, magic, data)
                else:
                    callback(self, magic, data)
            except Exception as e:
                logger.error(f"Error in message callback: {e}")

    def on(self, event: str, callback: Callable):
        """注册事件回调"""
        if event in self._callbacks:
            self._callbacks[event].append(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    def off(self, event: str, callback: Callable):
        """取消事件回调"""
        if event in self._callbacks:
            self._callbacks[event].remove(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    def _schedule_reconnect(self):
        """安排重连任务"""
        if not self._reconnect_task or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        """重连循环"""
        while self._state != ClientState.CONNECTED:
            try:
                await asyncio.sleep(5)  # 重连间隔
                await self.connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconnect loop error: {e}")
                self._stats.errors += 1

        logger.debug("Reconnect loop exited")
