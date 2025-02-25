from ..base import BaseClient, BaseConfig
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, AsyncGenerator, Dict, List, Union
import asyncio
from contextlib import asynccontextmanager, suppress
from loguru import logger
import time
from websockets.exceptions import ConnectionClosed, WebSocketException

from .events import EventManager
from .connection import ConnectionManager
from .config import WebSocketConfig as WSConfig
from .utils import parse_message, serialize_message


@dataclass
class WebSocketConfig(BaseConfig):
    """WebSocket客户端配置"""
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
        """转换为连接配置"""
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
    """增强的WebSocket客户端实现"""

    def __init__(self, config: WebSocketConfig):
        super().__init__(config)
        self.event_manager = EventManager()
        self.connection_manager = ConnectionManager(
            config.to_connection_config(), self.event_manager)
        self._message_queue = asyncio.Queue()
        self._stream_tasks = set()
        self._last_activity = time.time()
        self._reconnect_backoff = 1.0

    def add_callback(self, event: str, callback: Callable) -> bool:
        """注册事件回调"""
        return self.event_manager.add_callback(event, callback)

    def remove_callback(self, event: str, callback: Callable) -> bool:
        """移除事件回调"""
        return self.event_manager.remove_callback(event, callback)

    async def connect(self) -> bool:
        """实现WebSocket连接"""
        try:
            # 添加消息处理回调
            self.add_callback('message', self._queue_message)

            success = await self.connection_manager.connect()
            self.connected = success

            if success:
                self._last_activity = time.time()
                self._reconnect_backoff = 1.0  # 重置退避时间

            return success
        except Exception as e:
            logger.error(
                f"WebSocket connection failed: {type(e).__name__}: {str(e)}")
            return False

    async def send_message(self, message: Any) -> bool:
        """发送消息"""
        if not self.connection_manager.connected:
            logger.error("No active connection for sending")
            return False

        try:
            serialized_message = serialize_message(message)
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
        """接收消息"""
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
            await self.event_manager.trigger('message', parsed_message)
            return parsed_message

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
        """处理连接错误"""
        if not self.connection_manager.connected:
            return

        # 标记为已断开，触发重连
        self.connected = False
        # 连接管理器会处理重连逻辑

    async def _queue_message(self, message):
        """将消息添加到队列"""
        await self._message_queue.put(message)

    async def disconnect(self) -> None:
        """实现WebSocket断开连接"""
        # 清理所有流任务
        for task in list(self._stream_tasks):
            if not task.done():
                task.cancel()

        # 等待任务完成
        if self._stream_tasks:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait([t for t in self._stream_tasks], timeout=1.0)

        # 移除消息回调
        self.remove_callback('message', self._queue_message)

        # 关闭连接
        await self.connection_manager.close()
        self.connected = False

        # 清空消息队列
        while not self._message_queue.empty():
            try:
                self._message_queue.get_nowait()
                self._message_queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def send(self, data: Any) -> bool:
        """实现WebSocket消息发送"""
        return await self.execute_with_retry(self.send_message, data)

    async def receive(self) -> Optional[Any]:
        """实现WebSocket消息接收"""
        # 尝试从队列获取，队列为空则直接接收
        try:
            return await asyncio.wait_for(self._message_queue.get(), timeout=0.01)
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            return await self.execute_with_retry(self.receive_message)

    async def execute_with_retry(self, func, *args, **kwargs):
        """带重试的执行函数"""
        max_retries = 2  # 最大重试次数
        for i in range(max_retries + 1):
            if not self.connection_manager.connected and i > 0:
                # 重新连接
                await self.connect()

            if self.connection_manager.connected:
                result = await func(*args, **kwargs)
                if result is not None or func == self.receive_message:
                    return result

            if i < max_retries:
                await asyncio.sleep(0.5 * (i + 1))  # 渐进式退避

        return None if func == self.receive_message else False

    @asynccontextmanager
    async def session(self):
        """客户端会话上下文管理器"""
        try:
            await self.connect()
            yield self
        finally:
            await self.disconnect()

    async def receive_stream(self) -> AsyncGenerator[Any, None]:
        """消息流接收器"""
        task = asyncio.current_task()
        if task:
            self._stream_tasks.add(task)

        try:
            while self.connection_manager.connected:
                message = await self.receive()
                if message is not None:
                    yield message
                else:
                    # 如果没有消息，短暂暂停以避免CPU占用
                    await asyncio.sleep(0.01)
        finally:
            if task:
                self._stream_tasks.discard(task)

    async def run_forever(self):
        """改进的永久运行模式"""
        while True:
            if not self.connection_manager.connected:
                success = await self.connect()
                if not success:
                    await asyncio.sleep(self.config.reconnect_interval)
                    continue

            try:
                # 使用自定义消息处理
                async for message in self.receive_stream():
                    # 消息已经在receive_stream中处理
                    await asyncio.sleep(0)  # 让出控制权

            except asyncio.CancelledError:
                logger.info("Run forever cancelled")
                break
            except Exception as e:
                logger.error(
                    f"Error in message stream: {type(e).__name__}: {str(e)}")
                await self.event_manager.trigger('error', e)
                # 短暂暂停后继续
                await asyncio.sleep(1.0)
