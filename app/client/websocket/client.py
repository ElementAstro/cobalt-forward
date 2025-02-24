from ..base import BaseClient, BaseConfig
from dataclasses import dataclass
from typing import Any, Callable, Optional
import asyncio
from loguru import logger
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from .events import EventManager
from .connection import ConnectionManager
from .utils import parse_message, serialize_message


@dataclass
class WebSocketConfig(BaseConfig):
    receive_timeout: Optional[float] = None
    max_reconnect_attempts: int = -1
    reconnect_interval: float = 5.0
    compression_enabled: bool = False
    compression_level: int = -1


class WebSocketClient(BaseClient):
    """增强的WebSocket客户端实现"""

    def __init__(self, config: WebSocketConfig):
        super().__init__(config)
        self.event_manager = EventManager()
        self.connection_manager = ConnectionManager(config, self.event_manager)

    def add_callback(self, event: str, callback: Callable):
        """注册事件回调"""
        return self.event_manager.add_callback(event, callback)

    def remove_callback(self, event: str, callback: Callable):
        """移除事件回调"""
        return self.event_manager.remove_callback(event, callback)

    async def connect(self) -> bool:
        """实现WebSocket连接"""
        try:
            success = await self.connection_manager.connect()
            self.connected = success
            return success
        except Exception as e:
            logger.error(f"WebSocket connection failed: {str(e)}")
            return False

    async def send_message(self, message: Any):
        """发送消息"""
        if not self.connection_manager.connected:
            logger.error("No active connection")
            return False

        try:
            serialized_message = serialize_message(message)
            await self.connection_manager.connection.send(serialized_message)
            logger.debug(f"Sent message: {serialized_message}")
            return True

        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            await self.event_manager.trigger('error', e)
            return False

    async def receive_message(self):
        """接收消息"""
        if not self.connection_manager.connected:
            logger.error("No active connection")
            return None

        try:
            if self.config.receive_timeout:
                message = await asyncio.wait_for(
                    self.connection_manager.connection.recv(),
                    timeout=self.config.receive_timeout
                )
            else:
                message = await self.connection_manager.connection.recv()

            logger.debug(f"Received message: {message}")
            parsed_message = parse_message(message)
            await self.event_manager.trigger('message', parsed_message)
            return parsed_message

        except Exception as e:
            logger.error(f"Failed to receive message: {str(e)}")
            await self.event_manager.trigger('error', e)
            return None

    async def disconnect(self) -> None:
        """实现WebSocket断开连接"""
        await self.connection_manager.close()
        self.connected = False

    async def send(self, data: Any) -> bool:
        """实现WebSocket消息发送"""
        return await self.execute_with_retry(self.send_message, data)

    async def receive(self) -> Optional[Any]:
        """实现WebSocket消息接收"""
        return await self.execute_with_retry(self.receive_message)

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
        while self.connection_manager.connected:
            message = await self.receive()
            if message is not None:
                yield message

    async def run_forever(self):
        """改进的永久运行模式"""
        backoff = 1.0
        max_backoff = 60.0

        while True:
            if not self.connection_manager.connected:
                success = await self.connect()
                if not success:
                    self.connection_manager._reconnect_attempts += 1
                    if (self.config.max_reconnect_attempts != -1 and
                            self.connection_manager._reconnect_attempts > self.config.max_reconnect_attempts):
                        logger.error("Maximum reconnect attempts exceeded")
                        break

                    await self.event_manager.trigger(
                        'reconnect',
                        self.connection_manager._reconnect_attempts
                    )

                    # 指数退避重连
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue
                backoff = 1.0  # 重置退避时间

            try:
                async for message in self.receive_stream():
                    await self.event_manager.trigger('message', message)
            except Exception as e:
                logger.error(f"Error in message stream: {str(e)}")
                await self.event_manager.trigger('error', e)
                self.connection_manager.connected = False
