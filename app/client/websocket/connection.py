import asyncio
import weakref
from contextlib import asynccontextmanager, suppress
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from loguru import logger
from .config import WebSocketConfig
from .events import EventManager


class ConnectionManager:
    """连接管理器"""

    def __init__(self, config: WebSocketConfig, event_manager: EventManager):
        self.config = config
        self.event_manager = event_manager
        self.connection = None
        self.connected = False
        self._ping_task = None
        self._reconnect_attempts = 0
        self._tasks = set()
        self._lock = asyncio.Lock()
        self._closing = False
        self._closed_event = asyncio.Event()
        self._closed_event.set()  # 初始状态为已关闭

    @asynccontextmanager
    async def connection_context(self):
        """连接上下文管理器"""
        try:
            await self.connect()
            yield self
        finally:
            await self.close()

    async def connect(self) -> bool:
        """改进的连接实现"""
        if self._closing:
            return False

        # 如果已连接，直接返回
        if self.connected and self.connection and not self.connection.closed:
            return True

        # 使用快速锁检查避免锁竞争
        if self._lock.locked():
            return False

        async with self._lock:
            # 双重检查，避免重复连接
            if self.connected and self.connection and not self.connection.closed:
                return True

            # 如果正在关闭，等待完全关闭
            if self._closing:
                await self._closed_event.wait()

            self._closed_event.clear()
            await self.event_manager.trigger('before_connect')

            try:
                connect_kwargs = {
                    'uri': self.config.uri,
                    'ssl': self.config.ssl_context,
                    'extra_headers': self.config.headers,
                    'subprotocols': self.config.subprotocols,
                    'max_size': self.config.max_size,
                    'compression': self.config.compression,
                    'close_timeout': self.config.close_timeout,
                    # 添加连接超时
                    'open_timeout': 30.0
                }

                # 过滤None值的参数
                connect_kwargs = {k: v for k,
                                  v in connect_kwargs.items() if v is not None}

                if self.config.extra_options:
                    connect_kwargs.update(self.config.extra_options)

                self.connection = await websockets.connect(**connect_kwargs)
                self.connected = True
                self._reconnect_attempts = 0

                # 启动心跳任务
                self._ping_task = self._create_task(self._keep_alive())
                self._create_task(self._monitor_connection())

                await self.event_manager.trigger('connect')
                return True

            except Exception as e:
                self._closed_event.set()  # 连接失败，设置为已关闭
                logger.error(
                    f"Connection failed: {type(e).__name__}: {str(e)}")
                await self.event_manager.trigger('error', e)
                return False

    def _create_task(self, coro):
        """创建和跟踪任务"""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _keep_alive(self):
        """保持连接活跃"""
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
        """监控连接状态"""
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

                # 只有在循环结束时才处理断连
                if self.connected:
                    await self._handle_disconnection()

            except Exception as e:
                logger.error(
                    f"Connection monitor fatal error: {type(e).__name__}: {str(e)}")
                if self.connected:
                    await self._handle_disconnection()

    async def _handle_disconnection(self, reconnect=False):
        """处理断开连接"""
        if not self.connected:
            return

        async with self._lock:
            if not self.connected:  # 双重检查
                return

            self.connected = False
            await self.event_manager.trigger('disconnect')

            # 取消所有任务
            for task in list(self._tasks):
                if not task.done() and task != asyncio.current_task():
                    task.cancel()

            # 关闭连接
            if self.connection and not self.connection.closed:
                with suppress(Exception):
                    await self.connection.close()

            await self.event_manager.trigger('after_disconnect')
            self._closed_event.set()  # 标记完全关闭

            # 自动重连
            if reconnect and self.config.auto_reconnect and not self._closing:
                if self.config.max_reconnect_attempts == -1 or self._reconnect_attempts < self.config.max_reconnect_attempts:
                    self._reconnect_attempts += 1
                    self._create_task(self._reconnect())

    async def _reconnect(self):
        """重新连接逻辑"""
        await asyncio.sleep(self.config.reconnect_interval)
        logger.info(
            f"Attempting to reconnect (attempt {self._reconnect_attempts})...")
        await self.event_manager.trigger('reconnect', self._reconnect_attempts)
        await self.connect()

    async def close(self):
        """关闭连接"""
        if not self.connected and self._closed_event.is_set():
            return

        self._closing = True

        async with self._lock:
            if not self.connected:
                self._closing = False
                return

            try:
                self.connected = False

                # 取消所有任务
                for task in list(self._tasks):
                    if not task.done():
                        task.cancel()

                # 等待任务取消完成
                if self._tasks:
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait([t for t in self._tasks], timeout=2.0)

                # 关闭WebSocket连接
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
