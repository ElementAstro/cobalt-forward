import asyncio
from typing import Dict, List, Callable, Any, AsyncGenerator
from contextlib import asynccontextmanager
from loguru import logger


class EventManager:
    """事件管理器"""

    def __init__(self):
        self._callbacks: Dict[str, List[Callable]] = {
            'connect': [],
            'disconnect': [],
            'message': [],
            'error': [],
            'reconnect': [],
            'before_connect': [],
            'after_disconnect': []
        }

    @asynccontextmanager
    async def event_context(self, event: str, callback: Callable) -> AsyncGenerator[None, None]:
        """事件回调的上下文管理器"""
        self.add_callback(event, callback)
        try:
            yield
        finally:
            self.remove_callback(event, callback)

    def add_callback(self, event: str, callback: Callable) -> bool:
        """添加事件回调"""
        if event in self._callbacks:
            self._callbacks[event].append(callback)
            logger.debug(f"Added callback for event: {event}")
            return True
        return False

    def remove_callback(self, event: str, callback: Callable) -> bool:
        """移除事件回调"""
        if event in self._callbacks and callback in self._callbacks[event]:
            self._callbacks[event].remove(callback)
            logger.debug(f"Removed callback for event: {event}")
            return True
        return False

    async def trigger(self, event: str, *args, **kwargs) -> None:
        """优化的事件触发器"""
        callbacks = self._callbacks.get(event, [])
        if not callbacks:
            logger.debug(f"No callbacks registered for event: {event}")
            return

        tasks = []
        for callback in callbacks:
            if asyncio.iscoroutinefunction(callback):
                tasks.append(asyncio.create_task(callback(*args, **kwargs)))
            else:
                tasks.append(asyncio.to_thread(callback, *args, **kwargs))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(
                        f"Callback error for event {event}: {str(result)}")
