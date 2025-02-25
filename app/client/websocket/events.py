import asyncio
from typing import Dict, List, Callable, Any, AsyncGenerator, Set
from contextlib import asynccontextmanager
from loguru import logger


class EventManager:
    """事件管理器"""

    def __init__(self):
        # 使用集合而非列表，避免重复回调
        self._callbacks: Dict[str, Set[Callable]] = {
            'connect': set(),
            'disconnect': set(),
            'message': set(),
            'error': set(),
            'reconnect': set(),
            'before_connect': set(),
            'after_disconnect': set()
        }
        self._running_tasks = set()

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
            if callback not in self._callbacks[event]:
                self._callbacks[event].add(callback)
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
        callbacks = list(self._callbacks.get(event, set()))
        if not callbacks:
            logger.trace(f"No callbacks for event: {event}")
            return

        tasks = []
        for callback in callbacks:
            if asyncio.iscoroutinefunction(callback):
                task = asyncio.create_task(
                    self._safe_callback(callback, *args, **kwargs))
                self._running_tasks.add(task)
                task.add_done_callback(self._running_tasks.discard)
                tasks.append(task)
            else:
                tasks.append(asyncio.to_thread(
                    self._safe_sync_callback, callback, args, kwargs))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_callback(self, callback, *args, **kwargs):
        """安全地执行异步回调"""
        try:
            return await callback(*args, **kwargs)
        except Exception as e:
            logger.error(f"Async callback error: {type(e).__name__}: {str(e)}")
            return None

    def _safe_sync_callback(self, callback, args, kwargs):
        """安全地执行同步回调"""
        try:
            return callback(*args, **kwargs)
        except Exception as e:
            logger.error(f"Sync callback error: {type(e).__name__}: {str(e)}")
            return None
