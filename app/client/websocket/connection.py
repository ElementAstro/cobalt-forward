import asyncio
import weakref
from contextlib import asynccontextmanager
from fastapi import websockets
from websockets.exceptions import ConnectionClosed
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
        self._tasks = weakref.WeakSet()
        self._lock = asyncio.Lock()

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
        async with self._lock:
            if self.connected:
                return True

            await self.event_manager.trigger('before_connect')
            try:
                connect_kwargs = {
                    'uri': self.config.uri,
                    'ssl': self.config.ssl_context,
                    'extra_headers': self.config.headers,
                    'subprotocols': self.config.subprotocols,
                    'max_size': self.config.max_size,
                    'compression': self.config.compression,
                    'close_timeout': self.config.close_timeout
                }
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
                logger.error(f"Connection failed: {str(e)}")
                await self.event_manager.trigger('error', e)
                return False

    def _create_task(self, coro):
        """创建和跟踪任务"""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        return task

    async def _keep_alive(self):
        """保持连接活跃"""
        while self.connected:
            try:
                if self.connection and self.connection.open:
                    pong_waiter = await self.connection.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.config.ping_timeout)
                    logger.trace("Ping-pong successful")
                await asyncio.sleep(self.config.ping_interval)
            except asyncio.TimeoutError:
                logger.warning("Ping timeout, closing connection")
                await self.close()
                break
            except Exception as e:
                logger.error(f"Keep-alive error: {str(e)}")
                break

    async def _monitor_connection(self):
        """监控连接状态"""
        try:
            while self.connected:
                try:
                    await self.connection.wait_closed()
                except ConnectionClosed:
                    pass
                finally:
                    await self._handle_disconnection()
        except Exception as e:
            logger.error(f"Connection monitor error: {str(e)}")

    async def _handle_disconnection(self):
        """处理断开连接"""
        self.connected = False
        await self.event_manager.trigger('disconnect')
        for task in self._tasks:
            if not task.done():
                task.cancel()
        await self.event_manager.trigger('after_disconnect')

    async def close(self):
        """关闭连接"""
        if self.connected:
            try:
                if self._ping_task:
                    self._ping_task.cancel()
                await self.connection.close()
                self.connected = False
                await self.event_manager.trigger('disconnect')
                await self.event_manager.trigger('after_disconnect')
            except Exception as e:
                logger.error(f"Failed to close connection: {str(e)}")
                await self.event_manager.trigger('error', e)
