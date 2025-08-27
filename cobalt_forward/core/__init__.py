"""
Core module containing business logic, domain models, and service interfaces.

This module defines the core abstractions and business logic of the Cobalt Forward
application, independent of external frameworks and infrastructure concerns.
"""

from .interfaces.lifecycle import IStartable, IStoppable, IHealthCheckable
from .interfaces.messaging import IEventBus, IMessageBus
from .interfaces.commands import ICommandDispatcher, ICommandHandler
from .domain.events import Event, EventPriority
from .domain.commands import Command, CommandResult, CommandStatus
from .domain.messages import Message, MessageType

__all__ = [
    "IStartable",
    "IStoppable",
    "IHealthCheckable",
    "IEventBus",
    "IMessageBus",
    "ICommandDispatcher",
    "ICommandHandler",
    "Event",
    "EventPriority",
    "Command",
    "CommandResult",
    "CommandStatus",
    "Message",
    "MessageType",
]
