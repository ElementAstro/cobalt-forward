from functools import wraps
import time
import asyncio
from typing import Callable, TypeVar, cast, Any, Optional, Awaitable
from loguru import logger
import psutil
from dataclasses import dataclass
from collections import deque

T = TypeVar('T')
R = TypeVar('R')


@dataclass
class PerformanceMetrics:
    cpu_percent: float
    memory_percent: float
    connection_count: int
    message_rate: float
    average_latency: float
    operation_count: int = 0
    error_count: int = 0


def sync_performance_monitor() -> Callable[[Callable[..., R]], Callable[..., R]]:
    """同步函数性能监控装饰器"""
    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            start_time = time.perf_counter()
            try:
                result: R = func(*args, **kwargs)
                execution_time = time.perf_counter() - start_time
                logger.debug(
                    f"{func.__name__} executed in {execution_time:.4f} seconds")
                return result
            except Exception as e:
                execution_time = time.perf_counter() - start_time
                logger.error(
                    f"{func.__name__} failed after {execution_time:.4f} seconds: {str(e)}")
                raise
        return wrapper
    return decorator


def async_performance_monitor() -> Callable[[Callable[..., Awaitable[R]]], Callable[..., Awaitable[R]]]:
    """异步函数性能监控装饰器"""
    def decorator(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> R:
            start_time = time.perf_counter()
            try:
                result: R = await func(*args, **kwargs)
                execution_time = time.perf_counter() - start_time
                logger.debug(
                    f"{func.__name__} executed in {execution_time:.4f} seconds")
                return result
            except Exception as e:
                execution_time = time.perf_counter() - start_time
                logger.error(
                    f"{func.__name__} failed after {execution_time:.4f} seconds: {str(e)}")
                raise
        return cast(Callable[..., Awaitable[R]], wrapper)
    return decorator


class PerformanceMonitor:
    def __init__(self, history_size: int = 100):
        self.start_time = time.time()
        self.message_timestamps: deque[float] = deque(maxlen=history_size)
        self.latencies: deque[float] = deque(maxlen=history_size)
        self._running = False
        self._monitor_task: Optional[asyncio.Task[None]] = None
        self.operation_count = 0
        self.error_count = 0

    async def start(self) -> None:
        """启动性能监控"""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Performance monitoring started")

    async def stop(self) -> None:
        """停止性能监控"""
        self._running = False
        if self._monitor_task:
            await self._monitor_task
        logger.info("Performance monitoring stopped")

    def record_forward(self) -> None:
        """记录消息转发"""
        self.operation_count += 1
        self.record_message(0)  # 默认延迟为0

    def record_message(self, latency: float) -> None:
        """记录消息处理时间"""
        self.message_timestamps.append(time.time())
        self.latencies.append(latency)

    def record_error(self) -> None:
        """记录错误"""
        self.error_count += 1

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                metrics = self.get_current_metrics()
                self._log_metrics(metrics)
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error in performance monitoring: {e}")

    def get_current_metrics(self) -> PerformanceMetrics:
        """获取当前性能指标"""
        now = time.time()
        recent_messages = sum(
            1 for t in self.message_timestamps if now - t <= 60)
        message_rate = recent_messages / 60 if recent_messages else 0
        avg_latency = sum(self.latencies) / \
            len(self.latencies) if self.latencies else 0

        return PerformanceMetrics(
            cpu_percent=psutil.cpu_percent(),
            memory_percent=psutil.virtual_memory().percent,
            connection_count=len(psutil.net_connections()),
            message_rate=message_rate,
            average_latency=avg_latency,
            operation_count=self.operation_count,
            error_count=self.error_count
        )

    def _log_metrics(self, metrics: PerformanceMetrics) -> None:
        """记录性能指标"""
        logger.info(
            "Performance Metrics:\n"
            f"CPU Usage: {metrics.cpu_percent}%\n"
            f"Memory Usage: {metrics.memory_percent}%\n"
            f"Connections: {metrics.connection_count}\n"
            f"Message Rate: {metrics.message_rate:.2f} msg/s\n"
            f"Avg Latency: {metrics.average_latency:.2f} ms\n"
            f"Operations: {metrics.operation_count}\n"
            f"Errors: {metrics.error_count}"
        )


class RateLimiter:
    """速率限制器"""

    def __init__(self, rate_limit: int, time_window: float = 1.0):
        self.rate_limit = rate_limit
        self.time_window = time_window
        self.tokens = rate_limit
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            time_passed = now - self.last_update
            self.tokens = min(
                self.rate_limit,
                self.tokens + time_passed *
                (self.rate_limit / self.time_window)
            )
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False
