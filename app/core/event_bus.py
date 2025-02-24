from typing import Any, Callable, Dict, List, Optional
import asyncio
from enum import Enum
from loguru import logger

class EventPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2

class Event:
    def __init__(self, name: str, data: Any = None, priority: EventPriority = EventPriority.NORMAL):
        self.name = name
        self.data = data
        self.priority = priority
        self.timestamp = asyncio.get_event_loop().time()

class EventBus:
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = {}
        self._priorities: Dict[str, Dict[EventPriority, List[Callable]]] = {}
        self._event_queue = asyncio.PriorityQueue()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._process_events())
        logger.info("EventBus started")

    async def stop(self):
        self._running = False
        if self._task:
            await self._task
        logger.info("EventBus stopped")

    def subscribe(self, event_name: str, callback: Callable, priority: EventPriority = EventPriority.NORMAL):
        if event_name not in self._subscribers:
            self._subscribers[event_name] = []
            self._priorities[event_name] = {p: [] for p in EventPriority}
        
        self._subscribers[event_name].append(callback)
        self._priorities[event_name][priority].append(callback)
        logger.debug(f"Subscribed to event {event_name} with priority {priority}")

    def unsubscribe(self, event_name: str, callback: Callable):
        if event_name in self._subscribers:
            self._subscribers[event_name].remove(callback)
            for priority_callbacks in self._priorities[event_name].values():
                if callback in priority_callbacks:
                    priority_callbacks.remove(callback)
            logger.debug(f"Unsubscribed from event {event_name}")

    async def publish(self, event: Event):
        await self._event_queue.put((event.priority.value, event))
        logger.trace(f"Published event {event.name}")

    async def _process_events(self):
        while self._running:
            try:
                _, event = await self._event_queue.get()
                if event.name in self._subscribers:
                    for priority in EventPriority:
                        callbacks = self._priorities[event.name][priority]
                        for callback in callbacks:
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(event)
                                else:
                                    callback(event)
                            except Exception as e:
                                logger.error(f"Error processing event {event.name}: {e}")
            except Exception as e:
                logger.error(f"Error in event processing loop: {e}")
            finally:
                self._event_queue.task_done()
