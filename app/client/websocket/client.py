import asyncio
from typing import Any, Callable
from loguru import logger

from .config import WebSocketConfig
from .events import EventManager
from .connection import ConnectionManager
from .utils import parse_message, serialize_message


class WebSocketClient:
    """增强的WebSocket客户端实现"""

    def __init__(self, config: WebSocketConfig):
        self.config = config
        self.event_manager = EventManager()
        self.connection_manager = ConnectionManager(config, self.event_manager)

    def add_callback(self, event: str, callback: Callable):
        """注册事件回调"""
        return self.event_manager.add_callback(event, callback)

    def remove_callback(self, event: str, callback: Callable):
        """移除事件回调"""
        return self.event_manager.remove_callback(event, callback)

    async def connect(self):
        """建立WebSocket连接"""
        return await self.connection_manager.connect()

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

    async def close_connection(self):
        """关闭连接"""
        await self.connection_manager.close()

    async def run_forever(self):
        """持续运行客户端，自动重连"""
        while True:
            if not self.connection_manager.connected:
                success = await self.connect()
                if not success:
                    self.connection_manager._reconnect_attempts += 1
                    if (self.config.max_reconnect_attempts != -1 and
                            self.connection_manager._reconnect_attempts > self.config.max_reconnect_attempts):
                        logger.error("Exceeded maximum reconnect attempts")
                        break
                    logger.info(
                        f"Reconnect attempt {self.connection_manager._reconnect_attempts} "
                        f"in {self.config.reconnect_interval} seconds..."
                    )
                    await self.event_manager.trigger(
                        'reconnect',
                        self.connection_manager._reconnect_attempts
                    )
                    await asyncio.sleep(self.config.reconnect_interval)
                    continue

            try:
                await self.connection_manager.connection.wait_closed()
                logger.warning("Connection closed unexpectedly")
                self.connection_manager.connected = False
            except Exception as e:
                logger.error(
                    f"Error while waiting for connection to close: {e}")
                self.connection_manager.connected = False
