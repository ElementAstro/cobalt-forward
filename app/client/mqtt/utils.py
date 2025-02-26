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
    """验证MQTT主题格式"""
    if not topic or len(topic) == 0:
        return False
    if len(topic) > 65535:
        return False
    return all(ord(c) > 0 for c in topic)


def parse_payload(payload: Union[str, bytes]) -> Any:
    """解析消息负载"""
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
    """压缩消息负载"""
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    return zlib.compress(payload)


def decompress_payload(payload: bytes) -> Union[str, bytes]:
    """解压消息负载"""
    try:
        return zlib.decompress(payload).decode('utf-8')
    except UnicodeDecodeError:
        return zlib.decompress(payload)


def measure_time(func):
    """性能计时装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.debug(f"{func.__name__} took {end - start:.3f} seconds")
        return result
    return wrapper


async def async_measure_time(func):
    """异步性能计时装饰器"""
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
    """创建SSL配置"""
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
    """断路器实现"""

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60):
        self._failures = 0
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._last_failure_time = 0
        self._is_open = False
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        async with self._lock:
            if self._is_open:
                if time.time() - self._last_failure_time >= self._reset_timeout:
                    self._is_open = False
                    self._failures = 0
                else:
                    raise ConnectionError("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)
            self._failures = 0
            return result
        except Exception as e:
            await self._handle_failure()
            raise e
