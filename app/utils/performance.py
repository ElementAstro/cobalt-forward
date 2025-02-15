from functools import wraps
import time
import asyncio
from typing import Callable, TypeVar, Any
from loguru import logger
import cProfile
import pstats
import io

T = TypeVar('T')

def async_performance_monitor():
    """异步函数性能监控装饰器"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                execution_time = time.perf_counter() - start_time
                logger.debug(f"{func.__name__} executed in {execution_time:.4f} seconds")
                return result
            except Exception as e:
                execution_time = time.perf_counter() - start_time
                logger.error(f"{func.__name__} failed after {execution_time:.4f} seconds: {str(e)}")
                raise
        return wrapper
    return decorator

class PerformanceProfiler:
    """性能分析器"""
    def __init__(self):
        self.profiler = cProfile.Profile()
        
    def start(self):
        self.profiler.enable()
        
    def stop(self) -> str:
        self.profiler.disable()
        s = io.StringIO()
        stats = pstats.Stats(self.profiler, stream=s).sort_stats('cumulative')
        stats.print_stats()
        return s.getvalue()

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
                self.tokens + time_passed * (self.rate_limit / self.time_window)
            )
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False
