"""
Tests for the event bus implementation.

This module tests event publishing, subscription, and processing
functionality of the event bus.
"""

import asyncio
import pytest
from typing import AsyncGenerator

from cobalt_forward.core.services.event_bus import EventBus
from cobalt_forward.core.domain.events import Event, EventPriority


class TestEventBus:
    """Test cases for the event bus."""
    
    @pytest.fixture
    async def event_bus(self) -> AsyncGenerator[EventBus, None]:
        """Create and start an event bus for testing."""
        bus = EventBus(max_workers=2, queue_size=100)
        await bus.start()
        yield bus
        await bus.stop()
    
    @pytest.mark.asyncio
    async def test_publish_and_subscribe(self, event_bus: EventBus) -> None:
        """Test basic event publishing and subscription."""
        received_events = []

        async def handler(event: Event) -> None:
            received_events.append(event)
        
        # Subscribe to events
        subscription_id = await event_bus.subscribe("test.event", handler)
        
        # Publish event
        event_id = await event_bus.publish("test.event", {"message": "hello"})
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify event was received
        assert len(received_events) == 1
        assert received_events[0].name == "test.event"
        assert received_events[0].data == {"message": "hello"}
        assert received_events[0].event_id == event_id
        
        # Unsubscribe
        success = await event_bus.unsubscribe(subscription_id)
        assert success
    
    @pytest.mark.asyncio
    async def test_event_priority_ordering(self, event_bus: EventBus) -> None:
        """Test that events are processed in priority order."""
        received_events = []

        async def handler(event: Event) -> None:
            received_events.append(event)
        
        # Subscribe to events
        await event_bus.subscribe("test.*", handler)
        
        # Publish events with different priorities
        await event_bus.publish("test.low", "low", EventPriority.LOW)
        await event_bus.publish("test.high", "high", EventPriority.HIGH)
        await event_bus.publish("test.normal", "normal", EventPriority.NORMAL)
        await event_bus.publish("test.critical", "critical", EventPriority.CRITICAL)
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Verify events were processed in priority order
        assert len(received_events) == 4
        priorities = [event.priority for event in received_events]
        
        # Should be in descending priority order
        expected_order = [EventPriority.CRITICAL, EventPriority.HIGH, EventPriority.NORMAL, EventPriority.LOW]
        assert priorities == expected_order
    
    @pytest.mark.asyncio
    async def test_wildcard_subscriptions(self, event_bus):
        """Test wildcard pattern subscriptions."""
        received_events = []
        
        async def handler(event: Event):
            received_events.append(event)
        
        # Subscribe with wildcard pattern
        await event_bus.subscribe("system.*", handler)
        
        # Publish matching events
        await event_bus.publish("system.startup", "startup")
        await event_bus.publish("system.shutdown", "shutdown")
        await event_bus.publish("user.login", "login")  # Should not match
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify only matching events were received
        assert len(received_events) == 2
        event_names = [event.name for event in received_events]
        assert "system.startup" in event_names
        assert "system.shutdown" in event_names
        assert "user.login" not in event_names
    
    @pytest.mark.asyncio
    async def test_multiple_subscribers(self, event_bus):
        """Test multiple subscribers for the same event."""
        received_by_handler1 = []
        received_by_handler2 = []
        
        async def handler1(event: Event):
            received_by_handler1.append(event)
        
        async def handler2(event: Event):
            received_by_handler2.append(event)
        
        # Subscribe with both handlers
        await event_bus.subscribe("test.event", handler1)
        await event_bus.subscribe("test.event", handler2)
        
        # Publish event
        await event_bus.publish("test.event", "data")
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify both handlers received the event
        assert len(received_by_handler1) == 1
        assert len(received_by_handler2) == 1
        assert received_by_handler1[0].data == "data"
        assert received_by_handler2[0].data == "data"
    
    @pytest.mark.asyncio
    async def test_event_object_publishing(self, event_bus):
        """Test publishing Event objects directly."""
        received_events = []
        
        async def handler(event: Event):
            received_events.append(event)
        
        # Subscribe to events
        await event_bus.subscribe("custom.event", handler)
        
        # Create and publish event object
        custom_event = Event(
            name="custom.event",
            data={"custom": "data"},
            priority=EventPriority.HIGH,
            source="test",
            correlation_id="test-123"
        )
        
        event_id = await event_bus.publish(custom_event)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify event was received with all properties
        assert len(received_events) == 1
        received = received_events[0]
        assert received.name == "custom.event"
        assert received.data == {"custom": "data"}
        assert received.priority == EventPriority.HIGH
        assert received.source == "test"
        assert received.correlation_id == "test-123"
        assert received.event_id == event_id
    
    @pytest.mark.asyncio
    async def test_health_check(self, event_bus):
        """Test event bus health check."""
        health = await event_bus.check_health()
        
        assert health['healthy'] is True
        assert health['status'] == 'running'
        assert 'workers_count' in health['details']
        assert 'subscriptions_count' in health['details']
        assert 'events_published' in health['details']
    
    @pytest.mark.asyncio
    async def test_metrics_collection(self, event_bus):
        """Test metrics collection."""
        # Publish some events
        await event_bus.publish("test.metric1", "data1")
        await event_bus.publish("test.metric2", "data2")
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Get metrics
        metrics = await event_bus.get_metrics()
        
        assert metrics['events_published'] >= 2
        assert 'events_processed' in metrics
        assert 'processing_times' in metrics
