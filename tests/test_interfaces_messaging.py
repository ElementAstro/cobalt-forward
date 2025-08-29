"""
Tests for messaging interfaces.

This module tests the messaging interfaces including IEventBus and IMessageBus.
"""

import pytest
import asyncio
from typing import Any, Dict, List, Callable
from unittest.mock import AsyncMock, Mock

from cobalt_forward.core.interfaces.messaging import IEventBus, IMessageBus
from cobalt_forward.core.domain.events import Event, EventPriority
from cobalt_forward.core.domain.messages import Message, MessageType, MessagePriority


class MockEventBus(IEventBus):
    """Mock implementation of IEventBus for testing."""
    
    def __init__(self):
        self.published_events: List[Event] = []
        self.subscriptions: Dict[str, List[Callable[[Event], Any]]] = {}
        self.subscription_counter = 0
        self.metrics = {
            'events_published': 0,
            'events_processed': 0,
            'subscriptions_count': 0
        }
    
    async def publish(self, event: Event | str, data: Any = None,
                      priority: EventPriority = EventPriority.NORMAL) -> str:
        """Publish an event to the event bus."""
        if isinstance(event, str):
            event = Event(name=event, data=data, priority=priority)
        
        self.published_events.append(event)
        self.metrics['events_published'] += 1
        
        # Simulate event processing
        for pattern, handlers in self.subscriptions.items():
            if self._matches_pattern(event.name, pattern):
                for handler in handlers:
                    try:
                        await handler(event)
                        self.metrics['events_processed'] += 1
                    except Exception:
                        pass  # Ignore handler errors in mock
        
        return event.event_id
    
    async def subscribe(self, event_name: str, handler: Callable[[Event], Any],
                        priority: EventPriority = EventPriority.NORMAL) -> str:
        """Subscribe to events with the given name."""
        if event_name not in self.subscriptions:
            self.subscriptions[event_name] = []
        
        self.subscriptions[event_name].append(handler)
        self.subscription_counter += 1
        self.metrics['subscriptions_count'] = len(self.subscriptions)
        
        return f"sub_{self.subscription_counter}"
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events using subscription ID."""
        # Simplified implementation for testing
        return True
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get event bus metrics."""
        return self.metrics.copy()
    
    def _matches_pattern(self, event_name: str, pattern: str) -> bool:
        """Simple pattern matching for testing."""
        if pattern == event_name:
            return True
        if pattern.endswith('*'):
            return event_name.startswith(pattern[:-1])
        return False


class MockMessageBus(IMessageBus):
    """Mock implementation of IMessageBus for testing."""
    
    def __init__(self):
        self.sent_messages: List[Message] = []
        self.subscriptions: Dict[str, List[Callable[[Message], Any]]] = {}
        self.subscription_counter = 0
        self.pending_responses: Dict[str, Message] = {}
        self.metrics = {
            'messages_sent': 0,
            'messages_delivered': 0,
            'requests_sent': 0,
            'responses_received': 0,
            'subscriptions_count': 0
        }
    
    async def send(self, message: Message) -> None:
        """Send a message through the bus."""
        self.sent_messages.append(message)
        self.metrics['messages_sent'] += 1
        
        # Simulate message delivery
        for pattern, handlers in self.subscriptions.items():
            if self._matches_pattern(message.topic, pattern):
                for handler in handlers:
                    try:
                        await handler(message)
                        self.metrics['messages_delivered'] += 1
                    except Exception:
                        pass  # Ignore handler errors in mock
    
    async def request(self, message: Message, timeout: float = 30.0) -> Message:
        """Send a request message and wait for response."""
        await self.send(message)
        self.metrics['requests_sent'] += 1
        
        # Check if we have a pre-configured response
        if message.message_id in self.pending_responses:
            response = self.pending_responses.pop(message.message_id)
            self.metrics['responses_received'] += 1
            return response
        
        # Simulate timeout
        await asyncio.sleep(0.01)  # Small delay for testing
        if timeout < 0.1:  # If very short timeout, simulate timeout
            raise TimeoutError("Request timed out")
        
        # Return a default response
        response = Message(
            topic=message.reply_to or "response.topic",
            data={"status": "success", "request_id": message.message_id},
            message_type=MessageType.RESPONSE,
            correlation_id=message.correlation_id
        )
        self.metrics['responses_received'] += 1
        return response
    
    async def subscribe(self, topic: str, handler: Callable[[Message], Any]) -> str:
        """Subscribe to messages on a topic."""
        if topic not in self.subscriptions:
            self.subscriptions[topic] = []
        
        self.subscriptions[topic].append(handler)
        self.subscription_counter += 1
        self.metrics['subscriptions_count'] = len(self.subscriptions)
        
        return f"sub_{self.subscription_counter}"
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from messages using subscription ID."""
        # Simplified implementation for testing
        return True
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get message bus metrics."""
        return self.metrics.copy()
    
    def set_response(self, request_id: str, response: Message) -> None:
        """Set a pre-configured response for testing."""
        self.pending_responses[request_id] = response
    
    def _matches_pattern(self, topic: str, pattern: str) -> bool:
        """Simple pattern matching for testing."""
        if pattern == topic:
            return True
        if pattern.endswith('*'):
            return topic.startswith(pattern[:-1])
        return False


class TestIEventBus:
    """Test cases for IEventBus interface."""
    
    @pytest.fixture
    def event_bus(self) -> MockEventBus:
        """Create a mock event bus."""
        return MockEventBus()
    
    @pytest.fixture
    def sample_event(self) -> Event:
        """Create a sample event for testing."""
        return Event(
            name="test.event",
            data={"message": "test data"},
            priority=EventPriority.NORMAL
        )
    
    @pytest.mark.asyncio
    async def test_publish_event_object(self, event_bus: MockEventBus, sample_event: Event) -> None:
        """Test publishing an Event object."""
        event_id = await event_bus.publish(sample_event)
        
        assert event_id == sample_event.event_id
        assert len(event_bus.published_events) == 1
        assert event_bus.published_events[0] == sample_event
        assert event_bus.metrics['events_published'] == 1
    
    @pytest.mark.asyncio
    async def test_publish_event_string(self, event_bus: MockEventBus) -> None:
        """Test publishing an event by name string."""
        event_name = "user.login"
        event_data = {"user_id": "123"}
        priority = EventPriority.HIGH
        
        event_id = await event_bus.publish(event_name, event_data, priority)
        
        assert isinstance(event_id, str)
        assert len(event_bus.published_events) == 1
        
        published_event = event_bus.published_events[0]
        assert published_event.name == event_name
        assert published_event.data == event_data
        assert published_event.priority == priority
        assert event_bus.metrics['events_published'] == 1
    
    @pytest.mark.asyncio
    async def test_subscribe_and_handle_event(self, event_bus: MockEventBus) -> None:
        """Test subscribing to events and handling them."""
        received_events: List[Event] = []
        
        async def event_handler(event: Event) -> None:
            received_events.append(event)
        
        # Subscribe to events
        subscription_id = await event_bus.subscribe("test.event", event_handler)
        assert isinstance(subscription_id, str)
        assert event_bus.metrics['subscriptions_count'] == 1
        
        # Publish an event
        await event_bus.publish("test.event", {"data": "test"})
        
        # Verify event was handled
        assert len(received_events) == 1
        assert received_events[0].name == "test.event"
        assert received_events[0].data == {"data": "test"}
        assert event_bus.metrics['events_processed'] == 1
    
    @pytest.mark.asyncio
    async def test_subscribe_with_wildcard(self, event_bus: MockEventBus) -> None:
        """Test subscribing to events with wildcard patterns."""
        received_events: List[Event] = []
        
        async def event_handler(event: Event) -> None:
            received_events.append(event)
        
        # Subscribe to wildcard pattern
        await event_bus.subscribe("user.*", event_handler)
        
        # Publish matching events
        await event_bus.publish("user.login", {"user": "alice"})
        await event_bus.publish("user.logout", {"user": "bob"})
        await event_bus.publish("system.start", {"component": "server"})  # Should not match
        
        # Verify only matching events were handled
        assert len(received_events) == 2
        assert received_events[0].name == "user.login"
        assert received_events[1].name == "user.logout"
    
    @pytest.mark.asyncio
    async def test_unsubscribe(self, event_bus: MockEventBus) -> None:
        """Test unsubscribing from events."""
        async def event_handler(event: Event) -> None:
            pass
        
        # Subscribe and then unsubscribe
        subscription_id = await event_bus.subscribe("test.event", event_handler)
        result = await event_bus.unsubscribe(subscription_id)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_get_metrics(self, event_bus: MockEventBus) -> None:
        """Test getting event bus metrics."""
        # Initial metrics
        metrics = await event_bus.get_metrics()
        assert metrics['events_published'] == 0
        assert metrics['events_processed'] == 0
        assert metrics['subscriptions_count'] == 0
        
        # Add subscription and publish event
        async def handler(event: Event) -> None:
            pass
        
        await event_bus.subscribe("test.event", handler)
        await event_bus.publish("test.event", {"data": "test"})
        
        # Check updated metrics
        metrics = await event_bus.get_metrics()
        assert metrics['events_published'] == 1
        assert metrics['events_processed'] == 1
        assert metrics['subscriptions_count'] == 1
    
    @pytest.mark.asyncio
    async def test_multiple_handlers_same_event(self, event_bus: MockEventBus) -> None:
        """Test multiple handlers for the same event."""
        handler1_calls = []
        handler2_calls = []
        
        async def handler1(event: Event) -> None:
            handler1_calls.append(event)
        
        async def handler2(event: Event) -> None:
            handler2_calls.append(event)
        
        # Subscribe both handlers
        await event_bus.subscribe("test.event", handler1)
        await event_bus.subscribe("test.event", handler2)
        
        # Publish event
        await event_bus.publish("test.event", {"data": "test"})
        
        # Both handlers should be called
        assert len(handler1_calls) == 1
        assert len(handler2_calls) == 1
        assert handler1_calls[0].name == "test.event"
        assert handler2_calls[0].name == "test.event"
    
    @pytest.mark.asyncio
    async def test_event_priority_handling(self, event_bus: MockEventBus) -> None:
        """Test event priority in subscription."""
        # Test that priority parameter is accepted
        async def handler(event: Event) -> None:
            pass
        
        subscription_id = await event_bus.subscribe(
            "test.event", 
            handler, 
            EventPriority.HIGH
        )
        
        assert isinstance(subscription_id, str)


class TestIMessageBus:
    """Test cases for IMessageBus interface."""

    @pytest.fixture
    def message_bus(self) -> MockMessageBus:
        """Create a mock message bus."""
        return MockMessageBus()

    @pytest.fixture
    def sample_message(self) -> Message:
        """Create a sample message for testing."""
        return Message(
            topic="test.topic",
            data={"content": "test message"},
            message_type=MessageType.DATA
        )

    @pytest.mark.asyncio
    async def test_send_message(self, message_bus: MockMessageBus, sample_message: Message) -> None:
        """Test sending a message through the bus."""
        await message_bus.send(sample_message)

        assert len(message_bus.sent_messages) == 1
        assert message_bus.sent_messages[0] == sample_message
        assert message_bus.metrics['messages_sent'] == 1

    @pytest.mark.asyncio
    async def test_send_multiple_messages(self, message_bus: MockMessageBus) -> None:
        """Test sending multiple messages."""
        messages = [
            Message(topic="topic1", data={"id": 1}),
            Message(topic="topic2", data={"id": 2}),
            Message(topic="topic3", data={"id": 3})
        ]

        for message in messages:
            await message_bus.send(message)

        assert len(message_bus.sent_messages) == 3
        assert message_bus.metrics['messages_sent'] == 3

        for i, sent_message in enumerate(message_bus.sent_messages):
            assert sent_message.data == {"id": i + 1}

    @pytest.mark.asyncio
    async def test_request_response(self, message_bus: MockMessageBus) -> None:
        """Test request-response pattern."""
        request = Message(
            topic="service.request",
            data={"action": "process"},
            message_type=MessageType.REQUEST,
            reply_to="client.response"
        )

        response = await message_bus.request(request, timeout=1.0)

        assert isinstance(response, Message)
        assert response.message_type == MessageType.RESPONSE
        assert response.topic == "client.response"
        assert response.correlation_id == request.correlation_id
        assert message_bus.metrics['requests_sent'] == 1
        assert message_bus.metrics['responses_received'] == 1

    @pytest.mark.asyncio
    async def test_request_with_preconfigured_response(self, message_bus: MockMessageBus) -> None:
        """Test request with pre-configured response."""
        request = Message(
            topic="service.request",
            data={"action": "custom"},
            message_type=MessageType.REQUEST
        )

        # Pre-configure response
        custom_response = Message(
            topic="custom.response",
            data={"result": "custom result"},
            message_type=MessageType.RESPONSE
        )
        message_bus.set_response(request.message_id, custom_response)

        response = await message_bus.request(request)

        assert response == custom_response
        assert message_bus.metrics['requests_sent'] == 1
        assert message_bus.metrics['responses_received'] == 1

    @pytest.mark.asyncio
    async def test_request_timeout(self, message_bus: MockMessageBus) -> None:
        """Test request timeout."""
        request = Message(
            topic="service.request",
            data={"action": "timeout_test"},
            message_type=MessageType.REQUEST
        )

        with pytest.raises(TimeoutError, match="Request timed out"):
            await message_bus.request(request, timeout=0.05)  # Very short timeout

    @pytest.mark.asyncio
    async def test_subscribe_and_handle_message(self, message_bus: MockMessageBus) -> None:
        """Test subscribing to messages and handling them."""
        received_messages: List[Message] = []

        async def message_handler(message: Message) -> None:
            received_messages.append(message)

        # Subscribe to messages
        subscription_id = await message_bus.subscribe("test.topic", message_handler)
        assert isinstance(subscription_id, str)
        assert message_bus.metrics['subscriptions_count'] == 1

        # Send a message
        message = Message(topic="test.topic", data={"content": "test"})
        await message_bus.send(message)

        # Verify message was handled
        assert len(received_messages) == 1
        assert received_messages[0].topic == "test.topic"
        assert received_messages[0].data == {"content": "test"}
        assert message_bus.metrics['messages_delivered'] == 1

    @pytest.mark.asyncio
    async def test_subscribe_with_wildcard(self, message_bus: MockMessageBus) -> None:
        """Test subscribing to messages with wildcard patterns."""
        received_messages: List[Message] = []

        async def message_handler(message: Message) -> None:
            received_messages.append(message)

        # Subscribe to wildcard pattern
        await message_bus.subscribe("service.*", message_handler)

        # Send matching messages
        await message_bus.send(Message(topic="service.user", data={"user": "alice"}))
        await message_bus.send(Message(topic="service.auth", data={"token": "xyz"}))
        await message_bus.send(Message(topic="system.health", data={"status": "ok"}))  # Should not match

        # Verify only matching messages were handled
        assert len(received_messages) == 2
        assert received_messages[0].topic == "service.user"
        assert received_messages[1].topic == "service.auth"

    @pytest.mark.asyncio
    async def test_unsubscribe(self, message_bus: MockMessageBus) -> None:
        """Test unsubscribing from messages."""
        async def message_handler(message: Message) -> None:
            pass

        # Subscribe and then unsubscribe
        subscription_id = await message_bus.subscribe("test.topic", message_handler)
        result = await message_bus.unsubscribe(subscription_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_get_metrics(self, message_bus: MockMessageBus) -> None:
        """Test getting message bus metrics."""
        # Initial metrics
        metrics = await message_bus.get_metrics()
        assert metrics['messages_sent'] == 0
        assert metrics['messages_delivered'] == 0
        assert metrics['requests_sent'] == 0
        assert metrics['responses_received'] == 0
        assert metrics['subscriptions_count'] == 0

        # Add subscription and send message
        async def handler(message: Message) -> None:
            pass

        await message_bus.subscribe("test.topic", handler)
        await message_bus.send(Message(topic="test.topic", data={"test": "data"}))

        # Check updated metrics
        metrics = await message_bus.get_metrics()
        assert metrics['messages_sent'] == 1
        assert metrics['messages_delivered'] == 1
        assert metrics['subscriptions_count'] == 1
