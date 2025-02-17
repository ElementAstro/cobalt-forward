import asyncio
import websockets
import ssl
from dataclasses import dataclass
from typing import Optional, Dict, Any, Callable
from loguru import logger


@dataclass
class WebSocketConfig:
    """WebSocket客户端配置"""
    uri: str
    ssl_context: Optional[ssl.SSLContext] = None
    headers: Optional[Dict[str, str]] = None
    subprotocols: Optional[list] = None
    extra_options: Optional[Dict[str, Any]] = None
    ping_interval: float = 20.0
    ping_timeout: float = 10.0
    close_timeout: float = 10.0
    max_size: int = 2**20  # 1MB
    compression: Optional[str] = None
    # 新增可配置项
    auto_reconnect: bool = False
    max_reconnect_attempts: int = -1  # -1 表示无限重试
    reconnect_interval: float = 5.0   # 重连等待间隔，单位秒
    receive_timeout: Optional[float] = None  # 接收消息的超时时间


class WebSocketClient:
    """增强的WebSocket客户端实现"""

    def __init__(self, config: WebSocketConfig):
        self.config = config
        self.connection = None
        self.connected = False
        self._callbacks = {
            'connect': [],
            'disconnect': [],
            'message': [],
            'error': [],
            'reconnect': []
        }
        self._ping_task = None
        self._reconnect_attempts = 0
        logger.info(f"Initializing WebSocket client for {config.uri}")

    def add_callback(self, event: str, callback: Callable):
        """注册事件回调"""
        if event in self._callbacks:
            self._callbacks[event].append(callback)
            logger.debug(f"Added callback for event: {event}")

    async def _trigger_callbacks(self, event: str, *args, **kwargs):
        """触发事件回调"""
        for callback in self._callbacks.get(event, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(*args, **kwargs)
                else:
                    callback(*args, **kwargs)
            except Exception as e:
                logger.error(f"Callback error for event {event}: {str(e)}")

    async def _keep_alive(self):
        """保持连接活跃的心跳任务"""
        while self.connected:
            try:
                if self.connection and self.connection.open:
                    pong_waiter = await self.connection.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.config.ping_timeout)
                    logger.trace("Ping-pong successful")
                await asyncio.sleep(self.config.ping_interval)
            except asyncio.TimeoutError:
                logger.warning("Ping timeout, closing connection")
                await self.close_connection()
                break
            except Exception as e:
                logger.error(f"Keep-alive error: {str(e)}")
                break

    async def connect(self):
        """建立WebSocket连接"""
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
            logger.success(f"Connected to {self.config.uri}")

            # 启动心跳任务
            self._ping_task = asyncio.create_task(self._keep_alive())

            await self._trigger_callbacks('connect')
            return True

        except Exception as e:
            logger.error(f"Failed to connect: {str(e)}")
            await self._trigger_callbacks('error', e)
            return False

    async def send_message(self, message: Any):
        """发送消息"""
        if not self.connected:
            logger.error("No active connection")
            return False

        try:
            if isinstance(message, (dict, list)):
                import json
                message = json.dumps(message)
            await self.connection.send(message)
            logger.debug(f"Sent message: {message}")
            return True

        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            await self._trigger_callbacks('error', e)
            return False

    async def receive_message(self):
        """接收消息"""
        if not self.connected:
            logger.error("No active connection")
            return None

        try:
            if self.config.receive_timeout:
                message = await asyncio.wait_for(self.connection.recv(), timeout=self.config.receive_timeout)
            else:
                message = await self.connection.recv()

            logger.debug(f"Received message: {message}")
            try:
                import json
                parsed_message = json.loads(message)
                logger.trace("Message parsed as JSON")
                await self._trigger_callbacks('message', parsed_message)
                return parsed_message
            except json.JSONDecodeError:
                logger.trace("Message is not JSON")
                await self._trigger_callbacks('message', message)
                return message

        except Exception as e:
            logger.error(f"Failed to receive message: {str(e)}")
            await self._trigger_callbacks('error', e)
            return None

    async def close_connection(self):
        """关闭连接"""
        if self.connected:
            try:
                if self._ping_task:
                    self._ping_task.cancel()
                await self.connection.close()
                self.connected = False
                logger.info("Connection closed")
                await self._trigger_callbacks('disconnect')
            except Exception as e:
                logger.error(f"Failed to close connection: {str(e)}")
                await self._trigger_callbacks('error', e)
        else:
            logger.warning("No connection to close")

    async def run_forever(self):
        """持续运行客户端，自动重连（如果配置了auto_reconnect）"""
        while True:
            if not self.connected:
                success = await self.connect()
                if not success:
                    self._reconnect_attempts += 1
                    if (self.config.max_reconnect_attempts != -1 and
                            self._reconnect_attempts > self.config.max_reconnect_attempts):
                        logger.error(
                            "Exceeded maximum reconnect attempts. Exiting run_forever loop.")
                        break
                    logger.info(
                        f"Reconnect attempt {self._reconnect_attempts} in {self.config.reconnect_interval} seconds...")
                    await self._trigger_callbacks('reconnect', self._reconnect_attempts)
                    await asyncio.sleep(self.config.reconnect_interval)
                    continue
            try:
                # 等待连接关闭
                await self.connection.wait_closed()
                logger.warning("Connection closed unexpectedly.")
                self.connected = False
            except Exception as e:
                logger.error(
                    f"Error while waiting for connection to close: {e}")
                self.connected = False

    @staticmethod
    def create_ssl_context(
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        ca_path: Optional[str] = None,
        verify_ssl: bool = True
    ) -> ssl.SSLContext:
        """创建SSL上下文"""
        ssl_context = ssl.create_default_context()
        if not verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        if cert_path and key_path:
            ssl_context.load_cert_chain(cert_path, key_path)
        if ca_path:
            ssl_context.load_verify_locations(ca_path)
        return ssl_context
