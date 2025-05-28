import asyncio
from typing import Dict, Callable, Any, AsyncGenerator, Set, List, Optional, Awaitable
from contextlib import asynccontextmanager
from loguru import logger


class EventManager:
    """Event Manager for WebSocket Communications"""

    def __init__(self):
        # Using sets instead of lists to avoid duplicate callbacks
        self._callbacks: Dict[str, Set[Callable[..., Any]]] = {
            'connect': set(),
            'disconnect': set(),
            'message': set(),
            'error': set(),
            'reconnect': set(),
            'before_connect': set(),
            'after_disconnect': set()
        }
        self._running_tasks: Set[asyncio.Task[Optional[Any]]] = set()

    @asynccontextmanager
    async def event_context(self, event: str, callback: Callable[..., Any]) -> AsyncGenerator[None, None]:
        """Context manager for event callbacks.

        Args:
            event: Event name
            callback: Callback function to be executed when event is triggered

        Yields:
            None
        """
        self.add_callback(event, callback)
        try:
            yield
        finally:
            self.remove_callback(event, callback)

    def add_callback(self, event: str, callback: Callable[..., Any]) -> bool:
        """Add event callback.

        Args:
            event: Event name
            callback: Callback function

        Returns:
            True if callback was added, False otherwise
        """
        if event in self._callbacks:
            if callback not in self._callbacks[event]:
                self._callbacks[event].add(callback)
                logger.debug(f"Added callback for event: {event}")
            return True
        return False

    def remove_callback(self, event: str, callback: Callable[..., Any]) -> bool:
        """Remove event callback.

        Args:
            event: Event name
            callback: Callback function to remove

        Returns:
            True if callback was removed, False otherwise
        """
        if event in self._callbacks and callback in self._callbacks[event]:
            self._callbacks[event].remove(callback)
            logger.debug(f"Removed callback for event: {event}")
            return True
        return False

    async def trigger(self, event: str, *args: Any, **kwargs: Any) -> None:
        """Optimized event trigger.

        Args:
            event: Event name to trigger
            *args: Arguments to pass to callbacks
            **kwargs: Keyword arguments to pass to callbacks
        """
        callbacks = list(self._callbacks.get(event, set()))
        if not callbacks:
            logger.trace(f"No callbacks for event: {event}")
            return

        tasks: List[Awaitable[Optional[Any]]] = []
        for callback_item in callbacks:  # Renamed to avoid conflict with typing.Callable
            if asyncio.iscoroutinefunction(callback_item):
                # Assuming _safe_callback returns Awaitable[Optional[Any]]
                task: asyncio.Task[Optional[Any]] = asyncio.create_task(
                    self._safe_callback(callback_item, *args, **kwargs))
                self._running_tasks.add(task)
                task.add_done_callback(self._running_tasks.discard)
                tasks.append(task)
            else:
                # asyncio.to_thread returns a Coroutine
                tasks.append(asyncio.to_thread(
                    self._safe_sync_callback, callback_item, args, kwargs))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_callback(self, callback: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Optional[Any]:
        """Safely execute async callback.

        Args:
            callback: Async function to execute
            *args: Arguments to pass to callback
            **kwargs: Keyword arguments to pass to callback

        Returns:
            Callback result or None if exception occurred
        """
        try:
            return await callback(*args, **kwargs)
        except Exception as e:
            logger.error(f"Async callback error: {type(e).__name__}: {str(e)}")
            return None

    def _safe_sync_callback(self, callback: Callable[..., Any], args: Any, kwargs: Any) -> Optional[Any]:
        """Safely execute synchronous callback.

        Args:
            callback: Synchronous function to execute
            args: Arguments to pass to callback
            kwargs: Keyword arguments to pass to callback

        Returns:
            Callback result or None if exception occurred
        """
        try:
            return callback(*args, **kwargs)
        except Exception as e:
            logger.error(f"Sync callback error: {type(e).__name__}: {str(e)}")
            return None
