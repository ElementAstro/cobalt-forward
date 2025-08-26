"""
Messaging interfaces for event and message bus systems.

These interfaces define the contracts for publish-subscribe messaging
and event handling throughout the application.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union, AsyncIterator
from ..domain.events import Event, EventPriority
from ..domain.messages import Message


class IEventBus(ABC):
    """Interface for event bus implementations."""
    
    @abstractmethod
    async def publish(self, event: Union[Event, str], data: Any = None, 
                     priority: EventPriority = EventPriority.NORMAL) -> str:
        """
        Publish an event to the event bus.
        
        Args:
            event: Event object or event name string
            data: Event data (if event is a string)
            priority: Event priority (if event is a string)
            
        Returns:
            Event ID for tracking
        """
        pass
    
    @abstractmethod
    async def subscribe(self, event_name: str, handler: Callable[[Event], Any],
                       priority: EventPriority = EventPriority.NORMAL) -> str:
        """
        Subscribe to events with the given name.
        
        Args:
            event_name: Name of events to subscribe to (supports wildcards)
            handler: Async function to handle events
            priority: Handler priority for ordering
            
        Returns:
            Subscription ID for unsubscribing
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from events using subscription ID.
        
        Args:
            subscription_id: ID returned from subscribe()
            
        Returns:
            True if successfully unsubscribed
        """
        pass
    
    @abstractmethod
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get event bus metrics.
        
        Returns:
            Dictionary containing metrics like event counts, processing times, etc.
        """
        pass


class IMessageBus(ABC):
    """Interface for message bus implementations."""
    
    @abstractmethod
    async def send(self, message: Message) -> None:
        """
        Send a message through the bus.
        
        Args:
            message: Message to send
            
        Raises:
            MessageDeliveryException: If message cannot be delivered
        """
        pass
    
    @abstractmethod
    async def request(self, message: Message, timeout: float = 30.0) -> Message:
        """
        Send a request message and wait for response.
        
        Args:
            message: Request message
            timeout: Maximum time to wait for response
            
        Returns:
            Response message
            
        Raises:
            TimeoutError: If no response received within timeout
            MessageDeliveryException: If request cannot be sent
        """
        pass
    
    @abstractmethod
    async def subscribe(self, topic: str, handler: Callable[[Message], Any]) -> str:
        """
        Subscribe to messages on a topic.
        
        Args:
            topic: Topic pattern to subscribe to (supports wildcards)
            handler: Async function to handle messages
            
        Returns:
            Subscription ID for unsubscribing
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from messages using subscription ID.
        
        Args:
            subscription_id: ID returned from subscribe()
            
        Returns:
            True if successfully unsubscribed
        """
        pass
    
    @abstractmethod
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get message bus metrics.
        
        Returns:
            Dictionary containing metrics like message counts, latency, etc.
        """
        pass


class IMessageTransformer(ABC):
    """Interface for message transformation components."""
    
    @abstractmethod
    async def transform(self, message: Message) -> Message:
        """
        Transform a message.
        
        Args:
            message: Input message
            
        Returns:
            Transformed message
        """
        pass
    
    @abstractmethod
    def can_transform(self, message: Message) -> bool:
        """
        Check if this transformer can handle the given message.
        
        Args:
            message: Message to check
            
        Returns:
            True if transformer can handle this message
        """
        pass


class IMessageFilter(ABC):
    """Interface for message filtering components."""
    
    @abstractmethod
    def should_process(self, message: Message) -> bool:
        """
        Determine if a message should be processed.
        
        Args:
            message: Message to check
            
        Returns:
            True if message should be processed
        """
        pass


class IMessageSerializer(ABC):
    """Interface for message serialization."""
    
    @abstractmethod
    def serialize(self, message: Message) -> bytes:
        """
        Serialize a message to bytes.
        
        Args:
            message: Message to serialize
            
        Returns:
            Serialized message bytes
        """
        pass
    
    @abstractmethod
    def deserialize(self, data: bytes) -> Message:
        """
        Deserialize bytes to a message.
        
        Args:
            data: Serialized message bytes
            
        Returns:
            Deserialized message
        """
        pass
