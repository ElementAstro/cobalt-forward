"""
Event bus implementation for publish-subscribe messaging.

This module provides a high-performance event bus implementation with
priority queues, async processing, and subscription management.
"""

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, List, Set, Union, Optional
from weakref import WeakSet

from ..interfaces.messaging import IEventBus
from ..interfaces.lifecycle import IComponent
from ..domain.events import Event, EventPriority

logger = logging.getLogger(__name__)


class EventSubscription:
    """Represents an event subscription."""
    
    def __init__(self, subscription_id: str, event_pattern: str,
                 handler: Callable[[Event], Any], priority: EventPriority):
        self.subscription_id = subscription_id
        self.event_pattern = event_pattern
        self.handler = handler
        self.priority = priority
        self.created_at = time.time()
        self.call_count = 0
        self.last_called: Optional[float] = None
        self.error_count = 0


class EventBus(IComponent, IEventBus):
    """
    High-performance event bus implementation.
    
    Supports priority-based event processing, pattern matching for subscriptions,
    and comprehensive metrics collection.
    """
    
    def __init__(self, max_workers: int = 10, queue_size: int = 1000):
        self._subscriptions: Dict[str, List[EventSubscription]] = defaultdict(list)
        self._wildcard_subscriptions: List[EventSubscription] = []
        self._event_queue: asyncio.PriorityQueue[Any] = asyncio.PriorityQueue(maxsize=queue_size)
        self._workers: List[asyncio.Task[Any]] = []
        self._max_workers = max_workers
        self._running = False
        self._active_handlers: WeakSet[Any] = WeakSet()
        
        # Metrics
        self._metrics: Dict[str, Any] = {
            'events_published': 0,
            'events_processed': 0,
            'events_failed': 0,
            'subscriptions_count': 0,
            'processing_times': [],
            'queue_size': 0
        }
    
    @property
    def name(self) -> str:
        """Get component name."""
        return "EventBus"
    
    @property
    def version(self) -> str:
        """Get component version."""
        return "1.0.0"
    
    async def start(self) -> None:
        """Start the event bus and worker tasks."""
        if self._running:
            return
        
        logger.info(f"Starting event bus with {self._max_workers} workers")
        
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_process())
            for _ in range(self._max_workers)
        ]
        
        logger.info("Event bus started successfully")
    
    async def stop(self) -> None:
        """Stop the event bus and cleanup resources."""
        if not self._running:
            return
        
        logger.info("Stopping event bus...")
        
        self._running = False
        
        # Cancel all worker tasks
        for worker in self._workers:
            worker.cancel()
        
        # Wait for workers to finish
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        
        self._workers.clear()
        
        # Clear subscriptions
        self._subscriptions.clear()
        self._wildcard_subscriptions.clear()
        
        logger.info("Event bus stopped")
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the event bus."""
        max_workers = config.get('max_workers', self._max_workers)
        queue_size = config.get('queue_size', 1000)
        
        if max_workers != self._max_workers:
            logger.info(f"Updating max workers from {self._max_workers} to {max_workers}")
            self._max_workers = max_workers
            
            # Restart workers if running
            if self._running:
                await self.stop()
                await self.start()
    
    async def check_health(self) -> Dict[str, Any]:
        """Check event bus health."""
        return {
            'healthy': True,
            'status': 'running' if self._running else 'stopped',
            'details': {
                'workers_count': len(self._workers),
                'subscriptions_count': sum(len(subs) for subs in self._subscriptions.values()) + len(self._wildcard_subscriptions),
                'queue_size': self._event_queue.qsize(),
                'events_published': self._metrics['events_published'],
                'events_processed': self._metrics['events_processed'],
                'events_failed': self._metrics['events_failed']
            }
        }
    
    async def publish(self, event: Union[Event, str], data: Any = None, 
                     priority: EventPriority = EventPriority.NORMAL) -> str:
        """Publish an event to the event bus."""
        if not self._running:
            raise RuntimeError("Event bus is not running")
        
        # Create event object if string provided
        if isinstance(event, str):
            event = Event(
                name=event,
                data=data,
                priority=priority
            )
        
        # Add to queue
        try:
            await self._event_queue.put(event)
            self._metrics['events_published'] += 1
            self._metrics['queue_size'] = self._event_queue.qsize()
            
            logger.debug(f"Published event: {event.name} (ID: {event.event_id})")
            return event.event_id
        
        except asyncio.QueueFull:
            logger.error(f"Event queue full, dropping event: {event.name}")
            raise RuntimeError("Event queue is full")
    
    async def subscribe(self, event_name: str, handler: Callable[[Event], Any],
                       priority: EventPriority = EventPriority.NORMAL) -> str:
        """Subscribe to events with the given name pattern."""
        subscription_id = str(uuid.uuid4())
        subscription = EventSubscription(
            subscription_id=subscription_id,
            event_pattern=event_name,
            handler=handler,
            priority=priority
        )
        
        # Add to appropriate subscription list
        if '*' in event_name or '?' in event_name:
            self._wildcard_subscriptions.append(subscription)
        else:
            self._subscriptions[event_name].append(subscription)
        
        # Sort subscriptions by priority
        if event_name in self._subscriptions:
            self._subscriptions[event_name].sort(key=lambda s: s.priority.value, reverse=True)
        
        self._wildcard_subscriptions.sort(key=lambda s: s.priority.value, reverse=True)
        
        self._metrics['subscriptions_count'] += 1
        
        logger.debug(f"Added subscription for '{event_name}' (ID: {subscription_id})")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe using subscription ID."""
        # Search in regular subscriptions
        for event_name, subscriptions in self._subscriptions.items():
            for i, subscription in enumerate(subscriptions):
                if subscription.subscription_id == subscription_id:
                    subscriptions.pop(i)
                    self._metrics['subscriptions_count'] -= 1
                    logger.debug(f"Removed subscription {subscription_id} for '{event_name}'")
                    return True
        
        # Search in wildcard subscriptions
        for i, subscription in enumerate(self._wildcard_subscriptions):
            if subscription.subscription_id == subscription_id:
                self._wildcard_subscriptions.pop(i)
                self._metrics['subscriptions_count'] -= 1
                logger.debug(f"Removed wildcard subscription {subscription_id}")
                return True
        
        return False
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get event bus metrics."""
        avg_processing_time = 0
        if self._metrics['processing_times']:
            avg_processing_time = sum(self._metrics['processing_times']) / len(self._metrics['processing_times'])
        
        return {
            **self._metrics,
            'avg_processing_time': avg_processing_time,
            'active_handlers': len(self._active_handlers)
        }
    
    async def _worker_process(self) -> None:
        """Worker process for handling events."""
        while self._running:
            try:
                # Get event from queue with timeout
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                
                await self._process_event(event)
                self._event_queue.task_done()
                
            except asyncio.TimeoutError:
                # Timeout is normal, continue loop
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}")
    
    async def _process_event(self, event: Event) -> None:
        """Process a single event by calling all matching handlers."""
        start_time = time.time()
        
        try:
            # Find matching subscriptions
            matching_subscriptions = []
            
            # Exact match subscriptions
            if event.name in self._subscriptions:
                matching_subscriptions.extend(self._subscriptions[event.name])
            
            # Wildcard subscriptions
            for subscription in self._wildcard_subscriptions:
                if self._matches_pattern(event.name, subscription.event_pattern):
                    matching_subscriptions.append(subscription)
            
            # Sort by priority
            matching_subscriptions.sort(key=lambda s: s.priority.value, reverse=True)
            
            # Call handlers
            for subscription in matching_subscriptions:
                try:
                    self._active_handlers.add(subscription.handler)
                    
                    if asyncio.iscoroutinefunction(subscription.handler):
                        await subscription.handler(event)
                    else:
                        subscription.handler(event)
                    
                    subscription.call_count += 1
                    subscription.last_called = time.time()
                
                except Exception as e:
                    subscription.error_count += 1
                    logger.error(f"Handler error for event {event.name}: {e}")
                    self._metrics['events_failed'] += 1
                
                finally:
                    self._active_handlers.discard(subscription.handler)
            
            self._metrics['events_processed'] += 1
            
        except Exception as e:
            logger.error(f"Event processing error: {e}")
            self._metrics['events_failed'] += 1
        
        finally:
            # Record processing time
            processing_time = time.time() - start_time
            self._metrics['processing_times'].append(processing_time)
            
            # Keep only last 1000 processing times
            if len(self._metrics['processing_times']) > 1000:
                self._metrics['processing_times'] = self._metrics['processing_times'][-1000:]
    
    def _matches_pattern(self, event_name: str, pattern: str) -> bool:
        """Check if event name matches a pattern with wildcards."""
        import fnmatch
        return fnmatch.fnmatch(event_name, pattern)
