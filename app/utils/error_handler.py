import time
from typing import Type, Dict, Callable, Any, Optional, TypeVar, Tuple, cast
from functools import wraps
import traceback
from loguru import logger
import asyncio
from enum import Enum
from dataclasses import dataclass, field


class ErrorLevel(Enum):
    """Error level definitions"""
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class BaseCustomException(Exception):
    """Base class for custom exceptions"""

    def __init__(self, message: str, error_code: Optional[str] = "", level: ErrorLevel = ErrorLevel.ERROR):
        self.message = message
        self.error_code = error_code
        self.level = level
        super().__init__(self.message)


class BusinessException(BaseCustomException):
    """Business exception"""
    pass


class SystemException(BaseCustomException):
    """System exception"""

    def __init__(self, message: str, error_code: Optional[str] = ""):
        super().__init__(message, error_code, ErrorLevel.CRITICAL)


@dataclass
class ErrorStats:
    """Error statistics"""
    total_count: int = 0
    error_counts: Dict[Type[Exception], int] = field(
        default_factory=lambda: dict())
    last_error_time: float = 0


T = TypeVar('T')
R = TypeVar('R')


class RetryConfig:
    """Retry configuration"""

    def __init__(self, max_retries: int = 3, delay: float = 1.0,
                 backoff_factor: float = 2.0, exceptions: Tuple[Type[Exception], ...] = (Exception,)):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff_factor = backoff_factor
        self.exceptions = exceptions


class ExceptionHandler:
    """Enhanced exception handler"""

    def __init__(self) -> None:
        self._handlers: Dict[Type[Exception], Callable[[Exception], Any]] = {}
        self._stats = ErrorStats()
        self._error_queue: asyncio.Queue[Exception] = asyncio.Queue()
        self._is_processing = False

    def register(self, exception_type: Type[Exception], handler: Callable[[Exception], Any]) -> None:
        """Register exception handler"""
        self._handlers[exception_type] = handler

    async def start_processing(self) -> None:
        """Start exception processing"""
        self._is_processing = True
        asyncio.create_task(self._process_error_queue())

    async def stop_processing(self) -> None:
        """Stop exception processing"""
        self._is_processing = False

    async def _process_error_queue(self) -> None:
        """Process exception queue"""
        while self._is_processing:
            try:
                error = await self._error_queue.get()
                await self.handle(error)
                self._error_queue.task_done()
            except Exception:
                logger.error(
                    f"Error processing exception: {traceback.format_exc()}")

    async def handle(self, exception: Exception) -> Dict[str, Any]:
        """Handle exception"""
        self._update_stats(exception)

        for exc_type, handler in self._handlers.items():
            if isinstance(exception, exc_type):
                return await handler(exception)

        return await self._default_handler(exception)

    def _update_stats(self, exception: Exception) -> None:
        """Update exception statistics"""
        self._stats.total_count += 1
        exc_type = type(exception)
        self._stats.error_counts[exc_type] = self._stats.error_counts.get(
            exc_type, 0) + 1
        self._stats.last_error_time = time.time()

    async def _default_handler(self, exception: Exception) -> Dict[str, Any]:
        """Enhanced default exception handler"""
        error_info: Dict[str, Any] = {
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
        """Get exception statistics"""
        return self._stats


F = TypeVar('F', bound=Callable[..., Any])


def error_boundary(error_handler: ExceptionHandler, retry_config: Optional[RetryConfig] = None) -> Callable[[F], F]:
    """Enhanced error boundary decorator with retry mechanism"""
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Optional[Exception] = None
            retry_count = 0

            while retry_config is None or retry_count <= retry_config.max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if retry_config and isinstance(e, retry_config.exceptions):
                        retry_count += 1
                        if retry_count <= retry_config.max_retries:
                            delay = retry_config.delay * \
                                (retry_config.backoff_factor ** (retry_count - 1))
                            logger.warning(
                                f"Retrying {func.__name__} in {delay} seconds... (Attempt {retry_count})")
                            await asyncio.sleep(delay)
                            continue
                    return await error_handler.handle(e)

            # If we get here, we've exceeded the max retries
            if last_error is not None:
                return await error_handler.handle(last_error)
            # This should never happen, but to satisfy the type checker
            raise RuntimeError("Unexpected error in error_boundary")

        return cast(F, wrapper)
    return decorator


class CircuitBreaker:
    """Circuit breaker"""

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self._lock = asyncio.Lock()

    async def execute(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute protected function"""
        async with self._lock:
            if await self._is_open():
                raise Exception("Circuit breaker is open")

            try:
                result = await func(*args, **kwargs)
                await self._on_success()
                return result
            except Exception:
                await self._on_failure()
                raise

    async def _is_open(self) -> bool:
        """Check if circuit breaker is open"""
        if self.failure_count >= self.failure_threshold:
            if time.time() - self.last_failure_time >= self.reset_timeout:
                self.failure_count = 0
                return False
            return True
        return False

    async def _on_success(self) -> None:
        """Handle successful request"""
        self.failure_count = 0

    async def _on_failure(self) -> None:
        """Handle failed request"""
        self.failure_count += 1
        self.last_failure_time = time.time()
