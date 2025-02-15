import time
from typing import Type, Dict, Callable, Any
from functools import wraps
import traceback
from loguru import logger
import asyncio

class ExceptionHandler:
    """全局异常处理器"""
    
    def __init__(self):
        self._handlers: Dict[Type[Exception], Callable] = {}
        
    def register(self, exception_type: Type[Exception], handler: Callable):
        """注册异常处理器"""
        self._handlers[exception_type] = handler
        
    async def handle(self, exception: Exception) -> Any:
        """处理异常"""
        handler = self._handlers.get(type(exception))
        if handler:
            return await handler(exception)
        return await self._default_handler(exception)
        
    async def _default_handler(self, exception: Exception):
        """默认异常处理"""
        logger.error(f"Unhandled exception: {str(exception)}")
        logger.debug(f"Exception traceback: {traceback.format_exc()}")
        return {"error": str(exception), "type": type(exception).__name__}

def error_boundary(error_handler: ExceptionHandler):
    """异常边界装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                return await error_handler.handle(e)
        return wrapper
    return decorator

class CircuitBreaker:
    """熔断器"""
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self._lock = asyncio.Lock()
        
    async def execute(self, func: Callable, *args, **kwargs):
        """执行被保护的函数"""
        async with self._lock:
            if await self._is_open():
                raise Exception("Circuit breaker is open")
            
            try:
                result = await func(*args, **kwargs)
                await self._on_success()
                return result
            except Exception as e:
                await self._on_failure()
                raise
                
    async def _is_open(self) -> bool:
        """检查熔断器是否打开"""
        if self.failure_count >= self.failure_threshold:
            if time.time() - self.last_failure_time >= self.reset_timeout:
                self.failure_count = 0
                return False
            return True
        return False
        
    async def _on_success(self):
        """处理成功请求"""
        self.failure_count = 0
        
    async def _on_failure(self):
        """处理失败请求"""
        self.failure_count += 1
        self.last_failure_time = time.time()
