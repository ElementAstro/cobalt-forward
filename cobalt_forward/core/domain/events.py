"""
Event domain models for the event-driven architecture.

This module defines the core event structures used throughout the application
for decoupled communication between components.
"""

import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum, auto
from typing import Any, Dict, Optional


class EventPriority(IntEnum):
    """Event priority levels for processing order."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass(frozen=True)
class Event:
    """
    Immutable event representing something that happened in the system.

    Events are the primary mechanism for decoupled communication between
    components in the application.
    """

    name: str
    """Event name/type identifier."""

    data: Any = None
    """Event payload data."""

    priority: EventPriority = EventPriority.NORMAL
    """Event processing priority."""

    timestamp: float = field(default_factory=time.time)
    """Unix timestamp when event was created."""

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    """Unique event identifier."""

    source: Optional[str] = None
    """Component or service that generated the event."""

    correlation_id: Optional[str] = None
    """ID for correlating related events."""

    metadata: Dict[str, Any] = field(default_factory=dict)
    """Additional event metadata."""

    def __post_init__(self) -> None:
        """Validate event after creation."""
        if not self.name:
            raise ValueError("Event name cannot be empty")

        if not isinstance(self.priority, EventPriority):
            raise ValueError("Priority must be an EventPriority enum value")

    def __lt__(self, other: 'Event') -> bool:
        """
        Compare events for priority queue ordering.

        Higher priority events come first, then by timestamp (FIFO).
        """
        if not isinstance(other, Event):
            return NotImplemented

        # Higher priority value = higher priority
        if self.priority.value != other.priority.value:
            return self.priority.value > other.priority.value

        # Same priority, order by timestamp (earlier first)
        return self.timestamp < other.timestamp

    def with_metadata(self, **metadata: Any) -> 'Event':
        """
        Create a new event with additional metadata.

        Args:
            **metadata: Additional metadata to add

        Returns:
            New event with merged metadata
        """
        new_metadata = {**self.metadata, **metadata}
        return Event(
            name=self.name,
            data=self.data,
            priority=self.priority,
            timestamp=self.timestamp,
            event_id=self.event_id,
            source=self.source,
            correlation_id=self.correlation_id,
            metadata=new_metadata
        )

    def with_correlation_id(self, correlation_id: str) -> 'Event':
        """
        Create a new event with a correlation ID.

        Args:
            correlation_id: Correlation ID to set

        Returns:
            New event with correlation ID
        """
        return Event(
            name=self.name,
            data=self.data,
            priority=self.priority,
            timestamp=self.timestamp,
            event_id=self.event_id,
            source=self.source,
            correlation_id=correlation_id,
            metadata=self.metadata
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert event to dictionary representation.

        Returns:
            Dictionary representation of the event
        """
        return {
            'name': self.name,
            'data': self.data,
            'priority': self.priority.name,
            'timestamp': self.timestamp,
            'event_id': self.event_id,
            'source': self.source,
            'correlation_id': self.correlation_id,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """
        Create event from dictionary representation.

        Args:
            data: Dictionary containing event data

        Returns:
            Event instance
        """
        priority = EventPriority[data.get('priority', 'NORMAL')]

        return cls(
            name=data['name'],
            data=data.get('data'),
            priority=priority,
            timestamp=data.get('timestamp', time.time()),
            event_id=data.get('event_id', str(uuid.uuid4())),
            source=data.get('source'),
            correlation_id=data.get('correlation_id'),
            metadata=data.get('metadata', {})
        )


# Common system events
class SystemEvents:
    """Constants for common system events."""

    STARTUP_COMPLETE = "system.startup.complete"
    SHUTDOWN_INITIATED = "system.shutdown.initiated"
    SHUTDOWN_COMPLETE = "system.shutdown.complete"
    COMPONENT_STARTED = "system.component.started"
    COMPONENT_STOPPED = "system.component.stopped"
    COMPONENT_FAILED = "system.component.failed"
    CONFIGURATION_CHANGED = "system.configuration.changed"
    ERROR_OCCURRED = "system.error.occurred"
    HEALTH_CHECK_FAILED = "system.health.check.failed"
    PLUGIN_LOADED = "system.plugin.loaded"
    PLUGIN_UNLOADED = "system.plugin.unloaded"
    PLUGIN_FAILED = "system.plugin.failed"
