"""
Command handling interfaces for the command pattern implementation.

These interfaces define the contracts for command processing, including
command handlers, dispatchers, and result handling.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar
from ..domain.commands import Command, CommandResult


T = TypeVar('T')  # Command data type
R = TypeVar('R')  # Result data type


class ICommandHandler(ABC, Generic[T, R]):
    """Interface for command handlers."""
    
    @abstractmethod
    async def handle(self, command: Command[T]) -> CommandResult[R]:
        """
        Handle a command and return a result.
        
        Args:
            command: Command to handle
            
        Returns:
            Command execution result
            
        Raises:
            CommandHandlerException: If command handling fails
        """
        pass
    
    @abstractmethod
    def can_handle(self, command: Command[Any]) -> bool:
        """
        Check if this handler can process the given command.
        
        Args:
            command: Command to check
            
        Returns:
            True if handler can process this command
        """
        pass
    
    @property
    @abstractmethod
    def supported_commands(self) -> List[Type[Command[Any]]]:
        """
        Get list of command types this handler supports.
        
        Returns:
            List of supported command types
        """
        pass


class ICommandDispatcher(ABC):
    """Interface for command dispatchers."""
    
    @abstractmethod
    async def dispatch(self, command: Command[Any]) -> CommandResult[Any]:
        """
        Dispatch a command to appropriate handler.
        
        Args:
            command: Command to dispatch
            
        Returns:
            Command execution result
            
        Raises:
            CommandDispatchException: If no handler found or dispatch fails
        """
        pass
    
    @abstractmethod
    async def register_handler(self, handler: ICommandHandler[Any, Any]) -> None:
        """
        Register a command handler.
        
        Args:
            handler: Handler to register
            
        Raises:
            ValueError: If handler is invalid or conflicts with existing handler
        """
        pass
    
    @abstractmethod
    async def unregister_handler(self, handler: ICommandHandler[Any, Any]) -> bool:
        """
        Unregister a command handler.
        
        Args:
            handler: Handler to unregister
            
        Returns:
            True if handler was successfully unregistered
        """
        pass
    
    @abstractmethod
    async def get_handlers(self, command_type: Type[Command[Any]]) -> List[ICommandHandler[Any, Any]]:
        """
        Get all handlers for a specific command type.
        
        Args:
            command_type: Type of command
            
        Returns:
            List of handlers that can process this command type
        """
        pass
    
    @abstractmethod
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get command dispatcher metrics.
        
        Returns:
            Dictionary containing metrics like command counts, processing times, etc.
        """
        pass


class ICommandValidator(ABC):
    """Interface for command validation."""
    
    @abstractmethod
    async def validate(self, command: Command[Any]) -> bool:
        """
        Validate a command.
        
        Args:
            command: Command to validate
            
        Returns:
            True if command is valid
            
        Raises:
            CommandValidationException: If command is invalid
        """
        pass


class ICommandInterceptor(ABC):
    """Interface for command interceptors (middleware)."""
    
    @abstractmethod
    async def before_handle(self, command: Command[Any]) -> Command[Any]:
        """
        Process command before handling.
        
        Args:
            command: Original command
            
        Returns:
            Potentially modified command
        """
        pass
    
    @abstractmethod
    async def after_handle(self, command: Command[Any], 
                          result: CommandResult[Any]) -> CommandResult[Any]:
        """
        Process result after handling.
        
        Args:
            command: Original command
            result: Handler result
            
        Returns:
            Potentially modified result
        """
        pass
    
    @abstractmethod
    async def on_error(self, command: Command[Any], error: Exception) -> Optional[CommandResult[Any]]:
        """
        Handle errors during command processing.
        
        Args:
            command: Command that caused error
            error: Exception that occurred
            
        Returns:
            Optional result to use instead of error, or None to propagate error
        """
        pass


class ICommandQueue(ABC):
    """Interface for command queuing systems."""
    
    @abstractmethod
    async def enqueue(self, command: Command[Any], priority: int = 0) -> str:
        """
        Add command to queue.
        
        Args:
            command: Command to queue
            priority: Command priority (higher = more important)
            
        Returns:
            Queue item ID for tracking
        """
        pass
    
    @abstractmethod
    async def dequeue(self, timeout: Optional[float] = None) -> Optional[Command[Any]]:
        """
        Get next command from queue.
        
        Args:
            timeout: Maximum time to wait for command
            
        Returns:
            Next command or None if timeout
        """
        pass
    
    @abstractmethod
    async def get_queue_size(self) -> int:
        """
        Get current queue size.
        
        Returns:
            Number of commands in queue
        """
        pass
