import json
import zlib
import time
from typing import Any, Union, Dict, Callable, TypeVar, Awaitable
from functools import wraps, lru_cache
from loguru import logger
import asyncio

T = TypeVar('T')


@lru_cache(maxsize=1000)
def validate_topic(topic: str) -> bool:
    """Validate MQTT topic format"""
    if not topic or len(topic) == 0:
        return False
    if len(topic) > 65535:
        return False
    return all(ord(c) > 0 for c in topic)


def parse_payload(payload: Union[str, bytes]) -> Any:
    """Parse message payload"""
    if isinstance(payload, bytes):
        try:
            payload = payload.decode('utf-8')
        except UnicodeDecodeError:
            logger.warning("Failed to decode payload as UTF-8")
            return payload

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload


def compress_payload(payload: Union[str, bytes]) -> bytes:
    """Compress message payload"""
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    return zlib.compress(payload)


def decompress_payload(payload: bytes) -> Union[str, bytes]:
    """Decompress message payload"""
    try:
        return zlib.decompress(payload).decode('utf-8')
    except UnicodeDecodeError:
        return zlib.decompress(payload)


def measure_time(func):
    """Performance timing decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.debug(f"{func.__name__} took {end - start:.3f} seconds")
        return result
    return wrapper


def async_measure_time(func):
    """Async performance timing decorator"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        end = time.perf_counter()
        logger.debug(f"{func.__name__} took {end - start:.3f} seconds")
        return result
    return wrapper


def create_ssl_config(
    ca_certs: str = None,
    certfile: str = None,
    keyfile: str = None,
    cert_reqs: bool = True
) -> Dict:
    """Create SSL configuration"""
    ssl_config = {}
    if ca_certs:
        ssl_config['ca_certs'] = ca_certs
    if certfile:
        ssl_config['certfile'] = certfile
    if keyfile:
        ssl_config['keyfile'] = keyfile
    ssl_config['cert_reqs'] = cert_reqs
    return ssl_config


class CircuitBreaker:
    """Circuit breaker implementation for fault tolerance"""

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60):
        self._failures = 0
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._last_failure_time = 0
        self._is_open = False
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """Execute function with circuit breaker protection"""
        async with self._lock:
            if self._is_open:
                if time.time() - self._last_failure_time >= self._reset_timeout:
                    logger.info("Circuit breaker reset after timeout")
                    self._is_open = False
                    self._failures = 0
                else:
                    logger.warning("Circuit breaker is open, request rejected")
                    raise ConnectionError("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self._failures = 0
            return result
        except Exception as e:
            await self._handle_failure()
            raise e
            
    async def _handle_failure(self):
        """Handle circuit breaker failure"""
        async with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()
            
            if self._failures >= self._threshold:
                logger.warning(f"Circuit breaker opened after {self._failures} failures")
                self._is_open = True
                
    @property
    async def is_open(self) -> bool:
        """Check if circuit breaker is open"""
        async with self._lock:
            if self._is_open and time.time() - self._last_failure_time >= self._reset_timeout:
                self._is_open = False
                self._failures = 0
                return False
            return self._is_open
            
    @property
    async def failure_count(self) -> int:
        """Get current failure count"""
        async with self._lock:
            return self._failures
