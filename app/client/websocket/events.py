import asyncio
from typing import Dict, List, Callable, Any
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
        """触发事件回调"""
        for callback in self._callbacks.get(event, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(*args, **kwargs)
                else:
                    callback(*args, **kwargs)
            except Exception as e:
                logger.error(f"Callback error for event {event}: {str(e)}")
