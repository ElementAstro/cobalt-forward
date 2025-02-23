import time
from typing import Type, Dict, Callable, Any, Optional, List
from functools import wraps
import traceback
from loguru import logger
import asyncio
from enum import Enum
from dataclasses import dataclass

class ErrorLevel(Enum):
    """异常级别定义"""
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"

class BaseCustomException(Exception):
    """自定义异常基类"""
    def __init__(self, message: str, error_code: str = None, level: ErrorLevel = ErrorLevel.ERROR):
        self.message = message
        self.error_code = error_code
        self.level = level
        super().__init__(self.message)

class BusinessException(BaseCustomException):
    """业务异常"""
    pass

class SystemException(BaseCustomException):
    """系统异常"""
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message, error_code, ErrorLevel.CRITICAL)

@dataclass
class ErrorStats:
    """异常统计信息"""
    total_count: int = 0
    error_counts: Dict[Type[Exception], int] = None
    last_error_time: float = 0
    
    def __post_init__(self):
        if self.error_counts is None:
            self.error_counts = {}

class RetryConfig:
    """重试配置"""
    def __init__(self, max_retries: int = 3, delay: float = 1.0, 
                 backoff_factor: float = 2.0, exceptions: tuple = (Exception,)):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff_factor = backoff_factor
        self.exceptions = exceptions

class ExceptionHandler:
    """增强的异常处理器"""
    
    def __init__(self):
        self._handlers: Dict[Type[Exception], Callable] = {}
        self._stats = ErrorStats()
        self._error_queue: asyncio.Queue = asyncio.Queue()
        self._is_processing = False
        
    def register(self, exception_type: Type[Exception], handler: Callable):
        """注册异常处理器"""
        self._handlers[exception_type] = handler

    async def start_processing(self):
        """启动异常处理"""
        self._is_processing = True
        asyncio.create_task(self._process_error_queue())
        
    async def stop_processing(self):
        """停止异常处理"""
        self._is_processing = False
        
    async def _process_error_queue(self):
        """处理异常队列"""
        while self._is_processing:
            try:
                error = await self._error_queue.get()
                await self.handle(error)
                self._error_queue.task_done()
            except Exception as e:
                logger.error(f"Error processing exception: {e}")
                
    async def handle(self, exception: Exception) -> Any:
        """处理异常"""
        self._update_stats(exception)
        
        for exc_type, handler in self._handlers.items():
            if isinstance(exception, exc_type):
                return await handler(exception)
                
        return await self._default_handler(exception)
        
    def _update_stats(self, exception: Exception):
        """更新异常统计"""
        self._stats.total_count += 1
        exc_type = type(exception)
        self._stats.error_counts[exc_type] = self._stats.error_counts.get(exc_type, 0) + 1
        self._stats.last_error_time = time.time()
        
    async def _default_handler(self, exception: Exception):
        """增强的默认异常处理"""
        error_info = {
            "error": str(exception),
            "type": type(exception).__name__,
            "timestamp": time.time(),
            "traceback": traceback.format_exc()
        }
        
        if isinstance(exception, BaseCustomException):
            error_info.update({
                "error_code": exception.error_code,
                "level": exception.level.value
            })
            
        logger.error(f"Unhandled exception: {error_info}")
        return error_info

    @property
    def stats(self) -> ErrorStats:
        """获取异常统计信息"""
        return self._stats

def error_boundary(error_handler: ExceptionHandler, retry_config: Optional[RetryConfig] = None):
    """增强的异常边界装饰器，支持重试机制"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            retry_count = 0
            
            while retry_config is None or retry_count <= retry_config.max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if retry_config and isinstance(e, retry_config.exceptions):
                        retry_count += 1
                        if retry_count <= retry_config.max_retries:
                            delay = retry_config.delay * (retry_config.backoff_factor ** (retry_count - 1))
                            logger.warning(f"Retrying {func.__name__} in {delay} seconds... (Attempt {retry_count})")
                            await asyncio.sleep(delay)
                            continue
                    return await error_handler.handle(e)
                    
            return await error_handler.handle(last_error)
            
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
