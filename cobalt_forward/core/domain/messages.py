"""
Message domain models for the messaging system.

This module defines the core message structures used for communication
between different parts of the system and external services.
"""

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Optional


class MessageType(Enum):
    """Types of messages in the system."""
    COMMAND = auto()     # Command messages
    EVENT = auto()       # Event messages
    REQUEST = auto()     # Request messages
    RESPONSE = auto()    # Response messages
    NOTIFICATION = auto()  # Notification messages
    DATA = auto()        # Data messages
    HEARTBEAT = auto()   # Heartbeat/keepalive messages
    ERROR = auto()       # Error messages


class MessagePriority(Enum):
    """Message priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class Message:
    """
    Message representing data being transmitted through the system.

    Messages are the fundamental unit of communication in the messaging
    system and can represent various types of data and operations.
    """

    topic: str
    """Message topic/destination."""

    data: Any
    """Message payload data."""

    message_type: MessageType = MessageType.DATA
    """Type of message."""

    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    """Unique message identifier."""

    timestamp: float = field(default_factory=time.time)
    """Unix timestamp when message was created."""

    source: Optional[str] = None
    """Source component or service."""

    destination: Optional[str] = None
    """Destination component or service."""

    correlation_id: Optional[str] = None
    """ID for correlating related messages."""

    reply_to: Optional[str] = None
    """Topic to send reply to (for request messages)."""

    priority: MessagePriority = MessagePriority.NORMAL
    """Message priority."""

    ttl: Optional[float] = None
    """Time-to-live in seconds."""

    headers: Dict[str, Any] = field(default_factory=dict)
    """Message headers/metadata."""

    def __post_init__(self) -> None:
        """Validate message after creation."""
        if not self.topic:
            raise ValueError("Message topic cannot be empty")

        if not isinstance(self.message_type, MessageType):
            raise ValueError("Message type must be a MessageType enum value")

        if not isinstance(self.priority, MessagePriority):
            raise ValueError("Priority must be a MessagePriority enum value")

        if self.ttl is not None and self.ttl <= 0:
            raise ValueError("TTL must be positive")

    def __lt__(self, other: 'Message') -> bool:
        """
        Compare messages for priority queue ordering.

        Higher priority messages come first, then by timestamp (FIFO).
        """
        if not isinstance(other, Message):
            return NotImplemented

        # Higher priority value = higher priority
        if self.priority.value != other.priority.value:
            return self.priority.value > other.priority.value

        # Same priority, order by timestamp (earlier first)
        return self.timestamp < other.timestamp

    @property
    def is_expired(self) -> bool:
        """Check if message has expired based on TTL."""
        if self.ttl is None:
            return False

        return (time.time() - self.timestamp) > self.ttl

    @property
    def age(self) -> float:
        """Get message age in seconds."""
        return time.time() - self.timestamp

    def with_headers(self, **headers: Any) -> 'Message':
        """
        Create a new message with additional headers.

        Args:
            **headers: Additional headers to add

        Returns:
            New message with merged headers
        """
        new_headers = {**self.headers, **headers}
        return Message(
            topic=self.topic,
            data=self.data,
            message_type=self.message_type,
            message_id=self.message_id,
            timestamp=self.timestamp,
            source=self.source,
            destination=self.destination,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            priority=self.priority,
            ttl=self.ttl,
            headers=new_headers
        )

    def with_correlation_id(self, correlation_id: str) -> 'Message':
        """
        Create a new message with a correlation ID.

        Args:
            correlation_id: Correlation ID to set

        Returns:
            New message with correlation ID
        """
        return Message(
            topic=self.topic,
            data=self.data,
            message_type=self.message_type,
            message_id=self.message_id,
            timestamp=self.timestamp,
            source=self.source,
            destination=self.destination,
            correlation_id=correlation_id,
            reply_to=self.reply_to,
            priority=self.priority,
            ttl=self.ttl,
            headers=self.headers
        )

    def create_reply(self, data: Any, message_type: MessageType = MessageType.RESPONSE) -> 'Message':
        """
        Create a reply message to this message.

        Args:
            data: Reply data
            message_type: Type of reply message

        Returns:
            Reply message
        """
        if not self.reply_to:
            raise ValueError(
                "Cannot create reply: no reply_to topic specified")

        return Message(
            topic=self.reply_to,
            data=data,
            message_type=message_type,
            source=self.destination,
            destination=self.source,
            correlation_id=self.correlation_id,
            priority=self.priority,
            headers={'in_reply_to': self.message_id}
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert message to dictionary representation.

        Returns:
            Dictionary representation of the message
        """
        return {
            'topic': self.topic,
            'data': self.data,
            'message_type': self.message_type.name,
            'message_id': self.message_id,
            'timestamp': self.timestamp,
            'source': self.source,
            'destination': self.destination,
            'correlation_id': self.correlation_id,
            'reply_to': self.reply_to,
            'priority': self.priority.name,
            'ttl': self.ttl,
            'headers': self.headers
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """
        Create message from dictionary representation.

        Args:
            data: Dictionary containing message data

        Returns:
            Message instance
        """
        message_type = MessageType[data.get('message_type', 'DATA')]
        priority = MessagePriority[data.get('priority', 'NORMAL')]

        return cls(
            topic=data['topic'],
            data=data.get('data'),
            message_type=message_type,
            message_id=data.get('message_id', str(uuid.uuid4())),
            timestamp=data.get('timestamp', time.time()),
            source=data.get('source'),
            destination=data.get('destination'),
            correlation_id=data.get('correlation_id'),
            reply_to=data.get('reply_to'),
            priority=priority,
            ttl=data.get('ttl'),
            headers=data.get('headers', {})
        )
