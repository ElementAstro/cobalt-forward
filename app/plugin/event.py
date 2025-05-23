import asyncio
import time
import logging
import traceback
from typing import Dict, List, Any, Callable, Set, Tuple, Optional, Union, Awaitable, cast
from dataclasses import dataclass, field, asdict
import threading

logger = logging.getLogger(__name__)

# Define a more specific type for event handlers
# Forward reference for Event using quotes
EventCallback = Callable[['Event'], Union[Awaitable[None], None]]


@dataclass
class Event:
    """Event class for plugin system event passing"""
    name: str
    data: Any = None
    source: str = "system"
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: f"{time.time():.6f}-{threading.get_ident()}") # Added thread ident for more uniqueness

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create event from dictionary"""
        return cls(**data)


class EventBus:
    """Event bus providing event dispatch and handling functionality"""

    def __init__(self, max_queue_size: int = 1000, max_history: int = 100):
        self._handlers: Dict[str, List[Tuple[str, EventCallback]]] = {}
        self._subscribers: Dict[str, Set[str]] = {}
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._event_history: List[Event] = []
        self._max_history = max_history

        self._lock = threading.RLock() # RLock for re-entrant acquisition if needed
        self._processing = False
        self._processor_task: Optional[asyncio.Task[None]] = None

        # Performance statistics
        self._stats: Dict[str, Union[int, float, Dict[str, Dict[str, Union[int, float]]]]] = {
            "events_processed": 0,
            "events_dropped": 0,
            "processing_errors": 0,
            "avg_processing_time": 0.0,
            "total_processing_time": 0.0,
            "handler_stats": {} # Dict[subscriber_id, Dict[stat_name, value]]
        }

    def subscribe(self, event_name: str, handler: EventCallback, subscriber_id: str):
        """Subscribe to an event"""
        with self._lock:
            if event_name not in self._handlers:
                self._handlers[event_name] = []

            # Record subscriber
            if event_name not in self._subscribers:
                self._subscribers[event_name] = set()
            self._subscribers[event_name].add(subscriber_id)

            # Add handler
            handler_tuple: Tuple[str, EventCallback] = (subscriber_id, handler)
            if handler_tuple not in self._handlers[event_name]:
                self._handlers[event_name].append(handler_tuple)

                # Initialize handler statistics
                # Ensure handler_stats is correctly typed for nested access
                handler_stats_dict = self._stats["handler_stats"]
                if isinstance(handler_stats_dict, dict) and subscriber_id not in handler_stats_dict:
                    handler_stats_dict[subscriber_id] = {
                        "total_calls": 0,
                        "errors": 0,
                        "avg_time": 0.0,
                        "total_time": 0.0
                    }

    def unsubscribe(self, event_name: str, subscriber_id: str):
        """Unsubscribe from an event"""
        with self._lock:
            if event_name in self._handlers:
                self._handlers[event_name] = [
                    (sid, h) for sid, h in self._handlers[event_name] if sid != subscriber_id
                ]
                if not self._handlers[event_name]: # Clean up if no handlers left
                    del self._handlers[event_name]


            # Update subscriber records
            if event_name in self._subscribers:
                self._subscribers[event_name].discard(subscriber_id)
                if not self._subscribers[event_name]:
                    del self._subscribers[event_name]

    def unsubscribe_all(self, subscriber_id: str):
        """Unsubscribe from all events"""
        with self._lock:
            for event_name in list(self._handlers.keys()): # list() to avoid modification during iteration
                self.unsubscribe(event_name, subscriber_id)

    async def emit(self, event: Union[Event, str], data: Any = None, source: str = "system") -> bool:
        """Emit an event"""
        try:
            current_event: Event
            if isinstance(event, str):
                current_event = Event(name=event, data=data, source=source)
            else:
                current_event = event # If not str, it's already an Event


            # Add to event history
            with self._lock:
                self._event_history.append(current_event)
                if len(self._event_history) > self._max_history:
                    self._event_history = self._event_history[-self._max_history:]

            # If queue is full, drop the event
            try:
                await asyncio.wait_for(
                    self._event_queue.put(current_event),
                    timeout=0.5 # Configurable?
                )
                return True
            except asyncio.TimeoutError:
                logger.warning(
                    f"Event queue is full, dropping event: {current_event.name}")
                with self._lock:
                    self._stats["events_dropped"] = cast(int, self._stats.get("events_dropped", 0)) + 1
                return False

        except Exception as e:
            logger.error(f"Failed to emit event: {e}", exc_info=True)
            return False

    async def emit_batch(self, events: List[Union[Event, Tuple[str, Any, str]]]) -> int:
        """Emit events in batch"""
        success_count = 0
        for item in events:
            if isinstance(item, tuple):
                event_name, data, source = item
                if await self.emit(event_name, data, source):
                    success_count += 1
            else:
                if await self.emit(item):
                    success_count += 1
        return success_count

    async def start_processing(self):
        """Start processing the event queue"""
        if self._processing:
            return

        self._processing = True
        self._processor_task = asyncio.create_task(self._process_events())
        logger.info("Event bus processor started")

    async def stop_processing(self):
        """Stop processing the event queue"""
        if self._processor_task and self._processing: # Check _processing as well
            self._processing = False # Signal loop to stop
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                logger.info("Event bus processor task cancelled.")
            except Exception as e:
                logger.error(f"Error during event bus processor shutdown: {e}", exc_info=True)
            finally:
                self._processor_task = None
            logger.info("Event bus processor stopped")
        elif self._processor_task: # Task exists but not processing (e.g. already stopped)
             self._processor_task = None # Ensure it's cleared

    async def _process_events(self):
        """Task for processing the event queue"""
        while self._processing:
            try:
                event: Event = await self._event_queue.get()
                start_time = time.monotonic() # Use monotonic for duration

                await self._process_single_event(event)

                processing_time = time.monotonic() - start_time
                with self._lock:
                    self._stats["events_processed"] = cast(int, self._stats.get("events_processed", 0)) + 1
                    self._stats["total_processing_time"] = cast(float, self._stats.get("total_processing_time", 0.0)) + processing_time
                    
                    events_processed_val = self._stats["events_processed"]
                    total_processing_time_val = self._stats["total_processing_time"]

                    if events_processed_val > 0:
                        self._stats["avg_processing_time"] = total_processing_time_val / events_processed_val
                    else:
                         self._stats["avg_processing_time"] = 0.0


                self._event_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Event processing loop cancelled.")
                break # Exit loop when cancelled
            except Exception as e:
                logger.error(
                    f"Event processing error: {e}\n{traceback.format_exc()}")
                with self._lock:
                    self._stats["processing_errors"] = cast(int, self._stats.get("processing_errors", 0)) + 1

    async def _process_single_event(self, event: Event):
        """Process a single event"""
        handlers_for_event: List[Tuple[str, EventCallback]] = []
        with self._lock: # Access _handlers under lock
            if event.name in self._handlers:
                handlers_for_event = list(self._handlers[event.name]) # Create a copy

        if handlers_for_event:
            tasks: List[Awaitable[None]] = []
            for subscriber_id, handler_func in handlers_for_event:
                tasks.append(self._execute_handler(
                    event, handler_func, subscriber_id))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        sub_id, _ = handlers_for_event[i]
                        logger.error(f"Exception in handler for event '{event.name}' by subscriber '{sub_id}': {result}", exc_info=result)


    async def _execute_handler(self, event: Event, handler: EventCallback, subscriber_id: str):
        """Execute a single event handler"""
        start_time = time.monotonic()
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                # Run synchronous handler in a thread pool executor to avoid blocking event loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, handler, event)


            execution_time = time.monotonic() - start_time
            with self._lock:
                handler_stats_dict = self._stats["handler_stats"]
                if isinstance(handler_stats_dict, dict):
                    stats = handler_stats_dict.get(subscriber_id)
                    if isinstance(stats, dict):
                        stats["total_calls"] = cast(int, stats.get("total_calls", 0)) + 1
                        stats["total_time"] = cast(float, stats.get("total_time", 0.0)) + execution_time
                        total_calls_val = stats["total_calls"]
                        total_time_val = stats["total_time"]
                        if total_calls_val > 0:
                             stats["avg_time"] = total_time_val / total_calls_val
                        # No need to reassign stats to handler_stats_dict[subscriber_id] if modifying in place

        except Exception as e:
            # execution_time = time.monotonic() - start_time # Already calculated or not relevant if error before call
            logger.error(
                f"Event handler error (event: {event.name}, handler: {subscriber_id}): {e}", exc_info=True)

            with self._lock:
                handler_stats_dict = self._stats["handler_stats"]
                if isinstance(handler_stats_dict, dict):
                    stats = handler_stats_dict.get(subscriber_id)
                    if isinstance(stats, dict): # Ensure stats is a dict
                        stats["total_calls"] = cast(int, stats.get("total_calls", 0)) + 1 # Still count as a call
                        stats["errors"] = cast(int, stats.get("errors", 0)) + 1
                        # Optionally add execution_time to total_time even on error, or handle differently
                        # stats["total_time"] += execution_time
                        # if stats["total_calls"] > 0:
                        #     stats["avg_time"] = stats["total_time"] / stats["total_calls"]


    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics"""
        with self._lock:
            # Ensure handler_stats is a dict before calling len
            handler_stats_val = self._stats.get("handler_stats", {})
            num_subscribers = len(handler_stats_val) if isinstance(handler_stats_val, dict) else 0

            return {
                "events_processed": self._stats.get("events_processed", 0),
                "events_dropped": self._stats.get("events_dropped", 0),
                "processing_errors": self._stats.get("processing_errors", 0),
                "avg_processing_time": self._stats.get("avg_processing_time", 0.0),
                "subscribers_count": num_subscribers,
                "event_types_count": len(self._handlers),
                "queue_size": self._event_queue.qsize(),
                "queue_maxsize": self._event_queue.maxsize, # maxsize is an attribute, not a method
                "history_size": len(self._event_history)
            }

    def get_subscriber_stats(self, subscriber_id: Optional[str] = None) -> Dict[str, Any]:
        """Get subscriber statistics"""
        with self._lock:
            handler_stats_val = self._stats.get("handler_stats", {})
            if not isinstance(handler_stats_val, dict): # Should be a dict
                return {}

            if subscriber_id:
                return handler_stats_val.get(subscriber_id, {})
            else:
                return dict(handler_stats_val) # Return a copy

    def get_event_history(self, limit: Optional[int] = None, event_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get event history"""
        with self._lock:
            # Create a copy for safe iteration if needed, though not strictly necessary here
            history_copy = list(self._event_history)

            if event_name:
                filtered_history = [
                    event.to_dict() for event in history_copy if event.name == event_name]
            else:
                filtered_history = [event.to_dict()
                                    for event in history_copy]

            if limit is not None and limit > 0:
                return filtered_history[-limit:]
            return filtered_history

    def get_subscribers(self, event_name: Optional[str] = None) -> Dict[str, List[str]]:
        """Get subscriber list"""
        with self._lock:
            if event_name:
                return {event_name: list(self._subscribers.get(event_name, set()))}
            return {name: list(subs) for name, subs in self._subscribers.items()}

    def clear_history(self):
        """Clear event history"""
        with self._lock:
            self._event_history = []

    def reset_stats(self):
        """Reset statistics"""
        with self._lock:
            self._stats = {
                "events_processed": 0,
                "events_dropped": 0,
                "processing_errors": 0,
                "avg_processing_time": 0.0,
                "total_processing_time": 0.0,
                "handler_stats": {}
            }

    async def add_delayed_event(self, event: Union[Event, str], delay: float, data: Any = None, source: str = "system") -> bool:
        """Add delayed event, triggered after specified time. Returns True if task was scheduled."""
        async def delayed_emit_task():
            await asyncio.sleep(delay)
            await self.emit(event, data, source) # Assuming emit handles logging of its success/failure

        # Create and schedule task. The variable 'task' is not used after creation,
        # which is fine for fire-and-forget tasks.
        asyncio.create_task(delayed_emit_task())
        return True # Indicates scheduling, not successful emission

    async def emit_periodic(self, event_name: str, interval: float,
                            data_generator: Optional[Callable[[], Union[Awaitable[Any], Any]]] = None,
                            source: str = "system", max_count: Optional[int] = None) -> asyncio.Task[None]:
        """Emit periodic event, optionally using a data generator function"""
        async def periodic_emitter():
            count = 0
            while (max_count is None) or (count < max_count):
                try:
                    event_data: Any
                    if data_generator:
                        # Check if data_generator is an async function
                        if asyncio.iscoroutinefunction(data_generator):
                            event_data = await data_generator()
                        else: # Synchronous generator
                            event_data = data_generator()
                    else: # Default data if no generator
                        event_data = {"timestamp": time.time(), "count": count}

                    await self.emit(event_name, event_data, source)
                    count += 1
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    logger.info(f"Periodic emitter for '{event_name}' cancelled.")
                    break
                except Exception as e:
                    logger.error(f"Error in periodic event emitter for '{event_name}': {e}", exc_info=True)
                    # Decide if to continue or break on error. Current: continue after interval.
                    await asyncio.sleep(interval)

        # Create and return task so it can be managed (e.g., cancelled)
        task: asyncio.Task[None] = asyncio.create_task(periodic_emitter())
        return task
