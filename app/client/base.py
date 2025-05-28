import asyncio
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Type, Awaitable
from types import TracebackType
from loguru import logger
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
import queue
from dataclasses import dataclass


@dataclass
class BaseConfig:
    """基础配置类"""
    host: str
    port: int
    timeout: float = 30.0
    retry_attempts: int = 3
    retry_interval: float = 5.0
    keep_alive: bool = True
    keep_alive_interval: int = 60
    buffer_size: int = 8192
    max_connections: int = 10
    connection_timeout: float = 10.0


class PerformanceMetrics:
    """性能指标收集"""

    def __init__(self):
        self.start_time: float = time.time()
        self.total_requests: int = 0
        self.failed_requests: int = 0
        self.total_bytes_sent: int = 0
        self.total_bytes_received: int = 0
        self.response_times: List[float] = []
        self._lock: Lock = Lock()

    def record_request(self, success: bool, bytes_sent: int = 0,
                       bytes_received: int = 0, response_time: float = 0) -> None:
        with self._lock:
            self.total_requests += 1
            if not success:
                self.failed_requests += 1
            self.total_bytes_sent += bytes_sent
            self.total_bytes_received += bytes_received
            self.response_times.append(response_time)

    def get_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        with self._lock:
            elapsed_time = time.time() - self.start_time
            avg_response_time = sum(
                self.response_times) / len(self.response_times) if self.response_times else 0.0

            return {
                'total_requests': self.total_requests,
                'requests_per_second': self.total_requests / elapsed_time if elapsed_time > 0 else 0,
                'failure_rate': self.failed_requests / max(1, self.total_requests),
                'total_bytes_sent': self.total_bytes_sent,
                'total_bytes_received': self.total_bytes_received,
                'average_response_time': avg_response_time
            }


class BaseClient(ABC):
    """基础客户端类"""

    def __init__(self, config: BaseConfig):
        self.config: BaseConfig = config
        self.connected: bool = False
        self.metrics: PerformanceMetrics = PerformanceMetrics()
        self.connection_pool: queue.Queue[Any] = queue.Queue(
            maxsize=config.max_connections)
        self._lock: Lock = Lock()
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=4)
        self._callbacks: Dict[str, List[Callable[..., Any]]] = {}
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @abstractmethod
    async def connect(self) -> bool:
        """建立连接"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        pass

    @abstractmethod
    async def send(self, data: Any) -> bool:
        """发送数据"""
        pass

    @abstractmethod
    async def receive(self) -> Optional[Any]:
        """接收数据"""
        pass

    def add_callback(self, event: str, callback: Callable[..., Any]) -> None:
        """添加事件回调"""
        with self._lock:
            if event not in self._callbacks:
                self._callbacks[event] = []
            self._callbacks[event].append(callback)

    def remove_callback(self, event: str, callback: Callable[..., Any]) -> None:
        """移除事件回调"""
        with self._lock:
            if event in self._callbacks:
                try:
                    self._callbacks[event].remove(callback)
                except ValueError:
                    logger.warning(
                        f"Callback {callback} not found for event {event}")

    async def _trigger_callbacks(self, event: str, *args: Any, **kwargs: Any) -> None:
        """触发事件回调"""
        callbacks = self._callbacks.get(event, [])
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(*args, **kwargs)
                else:
                    await self.loop.run_in_executor(
                        self._executor, callback, *args, **kwargs)
            except Exception as e:
                logger.error(f"Callback error: {str(e)}")

    def _get_connection(self) -> Any:
        """从连接池获取连接"""
        try:
            return self.connection_pool.get(timeout=self.config.connection_timeout)
        except queue.Empty:
            raise ConnectionError("No available connections")

    def _return_connection(self, conn: Any) -> None:
        """归还连接到连接池"""
        try:
            self.connection_pool.put(conn, timeout=1)
        except queue.Full:
            logger.warning("Connection pool is full")

    async def reconnect(self) -> bool:
        """重连逻辑"""
        for attempt in range(self.config.retry_attempts):
            try:
                await self.disconnect()
                if await self.connect():
                    logger.info("Reconnection successful")
                    return True

                logger.warning(
                    f"Reconnection attempt {attempt + 1}/{self.config.retry_attempts} failed"
                )
                await asyncio.sleep(self.config.retry_interval)

            except Exception as e:
                logger.error(f"Reconnection error: {str(e)}")
                await asyncio.sleep(self.config.retry_interval)

        return False

    async def execute_with_retry(self, operation: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """通用重试执行方法"""
        start_time = time.time()
        last_exception: Optional[Exception] = None
        for attempt in range(self.config.retry_attempts):
            try:
                result: Any = await operation(*args, **kwargs)
                self.metrics.record_request(
                    success=True,
                    response_time=time.time() - start_time
                )
                return result
            except Exception as e:
                logger.error(f"Operation failed: {str(e)}")
                last_exception = e
                self.metrics.record_request(
                    success=False,
                    response_time=time.time() - start_time
                )
                if attempt < self.config.retry_attempts - 1:
                    await asyncio.sleep(self.config.retry_interval)
                else:
                    raise last_exception

    def __enter__(self) -> "BaseClient":
        """上下文管理器支持"""
        # Consider if connect should be blocking here or if the user should manage the event loop
        # For simplicity, using asyncio.run, but this might not be ideal in all contexts.
        if self.loop.is_running():
            # If loop is already running, cannot use asyncio.run.
            # This part needs careful consideration based on how the client is used.
            # For now, let's assume it's called from a non-async context where a new loop can be started.
            # Or, the user is expected to call connect explicitly if in an async context.
            logger.warning(
                "Entering BaseClient in a running event loop. Consider calling connect() explicitly.")
        else:
            self.loop.run_until_complete(self.connect())
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> Optional[bool]:
        """退出上下文管理器"""
        if self.loop.is_running():
            # Similar to __enter__, handling disconnect in a running loop.
            logger.warning(
                "Exiting BaseClient in a running event loop. Consider calling disconnect() explicitly.")
        else:
            self.loop.run_until_complete(self.disconnect())
        return None  # Returning None means exceptions are not suppressed

    async def __aenter__(self) -> "BaseClient":
        """异步上下文管理器支持"""
        await self.connect()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> Optional[bool]:
        """退出异步上下文管理器"""
        await self.disconnect()
        return None  # Returning None means exceptions are not suppressed
