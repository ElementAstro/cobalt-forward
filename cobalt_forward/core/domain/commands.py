"""
Command domain models for the command pattern implementation.

This module defines the core command structures used for request-response
operations and command processing throughout the application.
"""

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Generic, Optional, TypeVar


T = TypeVar('T')  # Command data type
R = TypeVar('R')  # Result data type


class CommandType(Enum):
    """Types of commands in the system."""
    SYSTEM = auto()      # System management commands
    USER = auto()        # User-initiated commands
    DEVICE = auto()      # Device control commands
    DATA = auto()        # Data query/manipulation commands
    PLUGIN = auto()      # Plugin-related commands
    NETWORK = auto()     # Network operation commands


class CommandStatus(Enum):
    """Command execution status."""
    PENDING = auto()     # Command created but not yet processed
    RUNNING = auto()     # Command is currently being processed
    COMPLETED = auto()   # Command completed successfully
    FAILED = auto()      # Command failed with error
    CANCELLED = auto()   # Command was cancelled
    TIMEOUT = auto()     # Command timed out


@dataclass
class Command(Generic[T]):
    """
    Command representing a request for some operation to be performed.

    Commands encapsulate all information needed to perform an operation
    and follow the command pattern for request-response operations.
    """

    name: str
    """Command name/type identifier."""

    data: T
    """Command payload data."""

    command_type: CommandType = CommandType.USER
    """Type of command."""

    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    """Unique command identifier."""

    timestamp: float = field(default_factory=time.time)
    """Unix timestamp when command was created."""

    user_id: Optional[str] = None
    """ID of user who initiated the command."""

    session_id: Optional[str] = None
    """Session ID for the command."""

    correlation_id: Optional[str] = None
    """ID for correlating related commands/events."""

    timeout: Optional[float] = None
    """Command timeout in seconds."""

    priority: int = 0
    """Command priority (higher = more important)."""

    metadata: Dict[str, Any] = field(default_factory=dict)
    """Additional command metadata."""

    def __post_init__(self) -> None:
        """Validate command after creation."""
        if not self.name:
            raise ValueError("Command name cannot be empty")

        if not isinstance(self.command_type, CommandType):
            raise ValueError("Command type must be a CommandType enum value")

        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("Timeout must be positive")

    def __lt__(self, other: 'Command[Any]') -> bool:
        """
        Compare commands for priority queue ordering.

        Higher priority commands come first, then by timestamp (FIFO).
        """
        if not isinstance(other, Command):
            return NotImplemented

        # Higher priority value = higher priority
        if self.priority != other.priority:
            return self.priority > other.priority

        # Same priority, order by timestamp (earlier first)
        return self.timestamp < other.timestamp

    def with_metadata(self, **metadata: Any) -> 'Command[T]':
        """
        Create a new command with additional metadata.

        Args:
            **metadata: Additional metadata to add

        Returns:
            New command with merged metadata
        """
        new_metadata = {**self.metadata, **metadata}
        return Command(
            name=self.name,
            data=self.data,
            command_type=self.command_type,
            command_id=self.command_id,
            timestamp=self.timestamp,
            user_id=self.user_id,
            session_id=self.session_id,
            correlation_id=self.correlation_id,
            timeout=self.timeout,
            priority=self.priority,
            metadata=new_metadata
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert command to dictionary representation.

        Returns:
            Dictionary representation of the command
        """
        return {
            'name': self.name,
            'data': self.data,
            'command_type': self.command_type.name,
            'command_id': self.command_id,
            'timestamp': self.timestamp,
            'user_id': self.user_id,
            'session_id': self.session_id,
            'correlation_id': self.correlation_id,
            'timeout': self.timeout,
            'priority': self.priority,
            'metadata': self.metadata
        }


@dataclass
class CommandResult(Generic[R]):
    """
    Result of command execution.

    Contains the outcome of processing a command, including success/failure
    status and any result data or error information.
    """

    command_id: str
    """ID of the command this result is for."""

    status: CommandStatus
    """Execution status."""

    result: Optional[R] = None
    """Result data if successful."""

    error: Optional[str] = None
    """Error message if failed."""

    error_code: Optional[str] = None
    """Error code for programmatic handling."""

    execution_time: Optional[float] = None
    """Time taken to execute command in seconds."""

    timestamp: float = field(default_factory=time.time)
    """Unix timestamp when result was created."""

    metadata: Dict[str, Any] = field(default_factory=dict)
    """Additional result metadata."""

    def __post_init__(self) -> None:
        """Validate result after creation."""
        if not self.command_id:
            raise ValueError("Command ID cannot be empty")

        if not isinstance(self.status, CommandStatus):
            raise ValueError("Status must be a CommandStatus enum value")

        if self.execution_time is not None and self.execution_time < 0:
            raise ValueError("Execution time cannot be negative")

    @property
    def is_success(self) -> bool:
        """Check if command was successful."""
        return self.status == CommandStatus.COMPLETED

    @property
    def is_failure(self) -> bool:
        """Check if command failed."""
        return self.status in (CommandStatus.FAILED, CommandStatus.TIMEOUT, CommandStatus.CANCELLED)

    @classmethod
    def success(cls, command_id: str, result: R, execution_time: Optional[float] = None) -> 'CommandResult[R]':
        """
        Create a successful command result.

        Args:
            command_id: ID of the command
            result: Result data
            execution_time: Time taken to execute

        Returns:
            Successful command result
        """
        return cls(
            command_id=command_id,
            status=CommandStatus.COMPLETED,
            result=result,
            execution_time=execution_time
        )

    @classmethod
    def failure(cls, command_id: str, error: str, error_code: Optional[str] = None,
                execution_time: Optional[float] = None) -> 'CommandResult[Any]':
        """
        Create a failed command result.

        Args:
            command_id: ID of the command
            error: Error message
            error_code: Error code
            execution_time: Time taken before failure

        Returns:
            Failed command result
        """
        return cls(
            command_id=command_id,
            status=CommandStatus.FAILED,
            error=error,
            error_code=error_code,
            execution_time=execution_time
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert result to dictionary representation.

        Returns:
            Dictionary representation of the result
        """
        return {
            'command_id': self.command_id,
            'status': self.status.name,
            'result': self.result,
            'error': self.error,
            'error_code': self.error_code,
            'execution_time': self.execution_time,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }
