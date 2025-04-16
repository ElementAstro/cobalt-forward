import asyncio
import time
import logging
import traceback
from typing import Dict, List, Any, Callable, Set, Tuple, Optional, Union
import json
from dataclasses import dataclass, field, asdict
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class Event:
    """Event class for plugin system event passing"""
    name: str
    data: Any = None
    source: str = "system"
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: f"{time.time():.6f}")
    
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
        self._handlers: Dict[str, List[Tuple[str, Callable]]] = {}
        self._subscribers: Dict[str, Set[str]] = {}
        self._event_queue = asyncio.Queue(maxsize=max_queue_size)
        self._event_history: List[Event] = []
        self._max_history = max_history
        
        self._lock = threading.RLock()
        self._processing = False
        self._processor_task: Optional[asyncio.Task] = None
        
        # Performance statistics
        self._stats = {
            "events_processed": 0,
            "events_dropped": 0,
            "processing_errors": 0,
            "avg_processing_time": 0,
            "total_processing_time": 0,
            "handler_stats": {}
        }
    
    def subscribe(self, event_name: str, handler: Callable, subscriber_id: str):
        """Subscribe to an event"""
        with self._lock:
            if event_name not in self._handlers:
                self._handlers[event_name] = []
                
            # Record subscriber
            if event_name not in self._subscribers:
                self._subscribers[event_name] = set()
            self._subscribers[event_name].add(subscriber_id)
            
            # Add handler
            handler_tuple = (subscriber_id, handler)
            if handler_tuple not in self._handlers[event_name]:
                self._handlers[event_name].append(handler_tuple)
                
                # Initialize handler statistics
                if subscriber_id not in self._stats["handler_stats"]:
                    self._stats["handler_stats"][subscriber_id] = {
                        "total_calls": 0,
                        "errors": 0,
                        "avg_time": 0,
                        "total_time": 0
                    }
    
    def unsubscribe(self, event_name: str, subscriber_id: str):
        """Unsubscribe from an event"""
        with self._lock:
            if event_name in self._handlers:
                self._handlers[event_name] = [
                    (sid, h) for sid, h in self._handlers[event_name] if sid != subscriber_id
                ]
                
            # Update subscriber records
            if event_name in self._subscribers:
                self._subscribers[event_name].discard(subscriber_id)
                if not self._subscribers[event_name]:
                    del self._subscribers[event_name]
    
    def unsubscribe_all(self, subscriber_id: str):
        """Unsubscribe from all events"""
        with self._lock:
            for event_name in list(self._handlers.keys()):
                self.unsubscribe(event_name, subscriber_id)
    
    async def emit(self, event: Union[Event, str], data: Any = None, source: str = "system") -> bool:
        """Emit an event"""
        try:
            if isinstance(event, str):
                event = Event(name=event, data=data, source=source)
            
            # Add to event history
            with self._lock:
                self._event_history.append(event)
                if len(self._event_history) > self._max_history:
                    self._event_history = self._event_history[-self._max_history:]
            
            # If queue is full, drop the event
            try:
                await asyncio.wait_for(
                    self._event_queue.put(event),
                    timeout=0.5
                )
                return True
            except asyncio.TimeoutError:
                logger.warning(f"Event queue is full, dropping event: {event.name}")
                with self._lock:
                    self._stats["events_dropped"] += 1
                return False
                
        except Exception as e:
            logger.error(f"Failed to emit event: {e}")
            return False
    
    async def emit_batch(self, events: List[Union[Event, Tuple[str, Any, str]]]) -> int:
        """Emit events in batch"""
        success_count = 0
        for event in events:
            if isinstance(event, tuple):
                event_name, data, source = event
                if await self.emit(event_name, data, source):
                    success_count += 1
            else:
                if await self.emit(event):
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
        if self._processor_task:
            self._processing = False
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
            self._processor_task = None
            logger.info("Event bus processor stopped")
    
    async def _process_events(self):
        """Task for processing the event queue"""
        while self._processing:
            try:
                event = await self._event_queue.get()
                start_time = time.time()
                
                await self._process_single_event(event)
                
                processing_time = time.time() - start_time
                with self._lock:
                    self._stats["events_processed"] += 1
                    self._stats["total_processing_time"] += processing_time
                    self._stats["avg_processing_time"] = (
                        self._stats["total_processing_time"] / 
                        self._stats["events_processed"]
                    )
                
                self._event_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event processing error: {e}\n{traceback.format_exc()}")
                with self._lock:
                    self._stats["processing_errors"] += 1
    
    async def _process_single_event(self, event: Event):
        """Process a single event"""
        if event.name in self._handlers:
            handlers = list(self._handlers[event.name])
            
            # Execute all handlers concurrently
            tasks = []
            for subscriber_id, handler in handlers:
                tasks.append(self._execute_handler(event, handler, subscriber_id))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_handler(self, event: Event, handler: Callable, subscriber_id: str):
        """Execute a single event handler"""
        start_time = time.time()
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
                
            # Update statistics
            execution_time = time.time() - start_time
            with self._lock:
                stats = self._stats["handler_stats"].get(subscriber_id, {
                    "total_calls": 0,
                    "errors": 0,
                    "avg_time": 0,
                    "total_time": 0
                })
                
                stats["total_calls"] += 1
                stats["total_time"] += execution_time
                stats["avg_time"] = stats["total_time"] / stats["total_calls"]
                self._stats["handler_stats"][subscriber_id] = stats
                
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Event handler error (event: {event.name}, handler: {subscriber_id}): {e}")
            
            # Update error statistics
            with self._lock:
                stats = self._stats["handler_stats"].get(subscriber_id, {
                    "total_calls": 0,
                    "errors": 0,
                    "avg_time": 0,
                    "total_time": 0
                })
                
                stats["total_calls"] += 1
                stats["errors"] += 1
                stats["total_time"] += execution_time
                stats["avg_time"] = stats["total_time"] / stats["total_calls"]
                self._stats["handler_stats"][subscriber_id] = stats
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics"""
        with self._lock:
            return {
                "events_processed": self._stats["events_processed"],
                "events_dropped": self._stats["events_dropped"],
                "processing_errors": self._stats["processing_errors"],
                "avg_processing_time": self._stats["avg_processing_time"],
                "subscribers_count": len(self._stats["handler_stats"]),
                "event_types_count": len(self._handlers),
                "queue_size": self._event_queue.qsize(),
                "queue_maxsize": self._event_queue.maxsize,
                "history_size": len(self._event_history)
            }
    
    def get_subscriber_stats(self, subscriber_id: str = None) -> Dict[str, Any]:
        """Get subscriber statistics"""
        with self._lock:
            if subscriber_id:
                return self._stats["handler_stats"].get(subscriber_id, {})
            else:
                return self._stats["handler_stats"]
    
    def get_event_history(self, limit: int = None, event_name: str = None) -> List[Dict[str, Any]]:
        """Get event history"""
        with self._lock:
            if event_name:
                filtered_history = [event.to_dict() for event in self._event_history if event.name == event_name]
            else:
                filtered_history = [event.to_dict() for event in self._event_history]
                
            if limit is not None and limit > 0:
                return filtered_history[-limit:]
            return filtered_history
    
    def get_subscribers(self, event_name: str = None) -> Dict[str, List[str]]:
        """Get subscriber list"""
        with self._lock:
            if event_name:
                return {event_name: list(self._subscribers.get(event_name, set()))}
            return {event_name: list(subscribers) for event_name, subscribers in self._subscribers.items()}
    
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
                "avg_processing_time": 0,
                "total_processing_time": 0,
                "handler_stats": {}
            }
            
    async def add_delayed_event(self, event: Union[Event, str], delay: float, data: Any = None, source: str = "system") -> bool:
        """Add delayed event, triggered after specified time"""
        async def delayed_emit():
            await asyncio.sleep(delay)
            return await self.emit(event, data, source)
            
        # Create and start task
        task = asyncio.create_task(delayed_emit())
        return True
        
    async def emit_periodic(self, event_name: str, interval: float, data_generator: Callable = None, 
                     source: str = "system", max_count: int = None) -> asyncio.Task:
        """Emit periodic event, optionally using a data generator function"""
        async def periodic_emitter():
            count = 0
            while max_count is None or count < max_count:
                try:
                    if data_generator:
                        if asyncio.iscoroutinefunction(data_generator):
                            data = await data_generator()
                        else:
                            data = data_generator()
                    else:
                        data = {"timestamp": time.time(), "count": count}
                    
                    await self.emit(event_name, data, source)
                    count += 1
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in periodic event emitter: {e}")
                    await asyncio.sleep(interval)  # Continue even with error
        
        # Create and start task
        task = asyncio.create_task(periodic_emitter())
        return task