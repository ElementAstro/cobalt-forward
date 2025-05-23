from abc import abstractmethod
from typing import Any, Dict, Optional, List, Protocol, Generic, TypeVar, Union, Sequence, cast
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import asyncio
import uuid
import time
import logging
from datetime import datetime
import traceback

from app.core.base import BaseComponent
from app.core.shared_services import SharedServices
from app.utils.error_handler import error_boundary

logger = logging.getLogger(__name__)

# Type definitions for commands
T = TypeVar('T')
R = TypeVar('R')


class CommandType(Enum):
    """Command type enumeration"""
    USER = auto()           # User initiated commands
    SYSTEM = auto()         # System automatically initiated commands
    SCHEDULED = auto()      # Scheduled commands
    BATCH = auto()          # Batch processing commands
    PLUGIN = auto()         # Plugin initiated commands
    REMOTE = auto()         # Remote call commands


class CommandPriority(Enum):
    """Command priority enumeration"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class CommandStatus(Enum):
    """Command status enumeration"""
    PENDING = auto()        # Waiting for execution
    EXECUTING = auto()      # In execution
    COMPLETED = auto()      # Execution completed
    FAILED = auto()         # Execution failed
    CANCELED = auto()       # Canceled
    TIMEOUT = auto()        # Timeout
    RETRY = auto()          # Retrying


@dataclass
class Command(Generic[T]):
    """Base command class"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    command_type: CommandType = CommandType.USER
    name: str = ""
    data: Optional[T] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    priority: CommandPriority = CommandPriority.NORMAL
    created_at: float = field(default_factory=time.time)
    timeout: float = 30.0  # Default 30 seconds timeout
    retry_count: int = 0
    max_retries: int = 0
    source: str = "user"
    target: str = "system"
    correlation_id: Optional[str] = None

    def __lt__(self, other: Any) -> bool:
        """Support comparison based on priority, for priority queues"""
        if not isinstance(other, Command):
            return NotImplemented
        # Higher priority at front of queue, same priority first-in-first-out
        return (self.priority.value > other.priority.value or
                (self.priority == other.priority and self.created_at < other.created_at))


@dataclass
class CommandResult(Generic[R]):
    """Command execution result"""
    command_id: str
    status: CommandStatus
    result: Optional[R] = None
    error: Optional[str] = None
    execution_time: float = 0.0  # Execution time
    completed_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class CommandHandler(Protocol, Generic[T, R]):
    """Command handler protocol, defines command handling interface"""

    @abstractmethod
    async def handle(self, command: Command[T]) -> CommandResult[R]:
        """Handle command"""
        pass


class UserCommand(Command[Dict[str, Any]]):
    """User command with user information"""
    def __init__(self, name: str, data: Dict[str, Any], user_id: str, **kwargs: Any):
        super().__init__(name=name, data=data, command_type=CommandType.USER, **kwargs)
        self.metadata["user_id"] = user_id
        self.metadata["session_id"] = kwargs.get("session_id", str(uuid.uuid4()))


class ScheduledCommand(Command[Dict[str, Any]]):
    """Scheduled command"""
    def __init__(self, name: str, data: Dict[str, Any], schedule_id: str, **kwargs: Any):
        super().__init__(name=name, data=data, command_type=CommandType.SCHEDULED, **kwargs)
        self.metadata["schedule_id"] = schedule_id
        self.metadata["execution_count"] = kwargs.get("execution_count", 0)


class BatchCommand(Command[List[Dict[str, Any]]]):
    """Batch command containing multiple sub-commands"""
    def __init__(self, name: str, commands: List[Dict[str, Any]], **kwargs: Any):
        super().__init__(name=name, data=commands, command_type=CommandType.BATCH, **kwargs)
        self.metadata["command_count"] = len(commands)
        self.metadata["batch_id"] = kwargs.get("batch_id", str(uuid.uuid4()))


class MessageBus:
    """Message bus interface for type checking"""
    async def subscribe(self, topic: str) -> Any:
        """Subscribe to a topic"""
        pass
    
    async def publish(self, topic: str, data: Any) -> None:
        """Publish data to a topic"""
        pass


class EventBus:
    """Event bus interface for type checking"""
    async def publish(self, topic: str, data: Any) -> None:
        """Publish event to a topic"""
        pass


class CommandTransmitter(BaseComponent):
    """
    Command transmitter responsible for sending commands to the message bus
    
    Used by clients to send command requests to the server
    """
    
    def __init__(self, message_bus: Optional[MessageBus] = None, name: str = "command_transmitter"):
        """
        Initialize command transmitter
        
        Args:
            message_bus: Message bus
            name: Component name
        """
        super().__init__(name=name)
        self._message_bus = message_bus
        self._pending_responses: Dict[str, asyncio.Future[Any]] = {}
        self._response_timeout = 30.0  # Default response timeout
        self._default_retry = 3  # Default retry count
        self._response_queue: Any = None
        self._response_task: Optional[asyncio.Task[None]] = None
    
    async def _start_impl(self) -> None:
        """Start command transmitter"""
        if self._message_bus:
            # Subscribe to command response topic
            self._response_queue = await self._message_bus.subscribe("command.response")
            # Start response processing task
            self._response_task = asyncio.create_task(self._process_responses())
    
    async def _stop_impl(self) -> None:
        """Stop command transmitter"""
        if hasattr(self, '_response_task') and self._response_task:
            self._response_task.cancel()
            try:
                await self._response_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all pending command responses
        for _cmd_id, future in self._pending_responses.items():
            if not future.done():
                future.set_exception(Exception("Command transmitter stopped"))
    
    async def _process_responses(self) -> None:
        """Process command response messages"""
        while self._running:
            try:
                message = await self._response_queue.get()
                result = message.data
                
                # Find matching command ID
                cmd_id = result.get("command_id")
                if cmd_id and cmd_id in self._pending_responses:
                    future = self._pending_responses[cmd_id]
                    if not future.done():
                        future.set_result(result)
                    
                    # Clean up completed command
                    del self._pending_responses[cmd_id]
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing command response: {e}")
    
    @error_boundary(error_handler=None)
    async def send(self, command: Command[Any]) -> CommandResult[Any]:
        """
        Send command and wait for response
        
        Args:
            command: Command to send
            
        Returns:
            Command execution result
            
        Raises:
            asyncio.TimeoutError: If command response times out
            Exception: Other sending errors
        """
        if not self._message_bus:
            raise Exception("Message bus not initialized")
        
        # Create response Future
        future: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._pending_responses[command.id] = future
        
        # Handle command timeout
        timeout = command.timeout or self._response_timeout
        
        # Send command
        start_time = time.time()
        try:
            # Send to command request topic
            topic = f"command.request.{command.name}"
            
            # Prepare data to send
            message_data: Dict[str, Any] = {
                "id": command.id,
                "name": command.name,
                "type": command.command_type.name,
                "data": command.data,
                "metadata": command.metadata,
                "priority": command.priority.name,
                "source": command.source,
                "target": command.target,
                "correlation_id": command.correlation_id,
                "created_at": command.created_at
            }
            
            # Send command
            await self._message_bus.publish(topic, message_data)
            
            # Wait for response with timeout
            result_data = await asyncio.wait_for(future, timeout)
            
            # Build command result
            execution_time = time.time() - start_time
            result = CommandResult(
                command_id=command.id,
                status=CommandStatus[result_data.get("status", "COMPLETED")],
                result=result_data.get("result"),
                error=result_data.get("error"),
                execution_time=execution_time,
                completed_at=time.time(),
                metadata=result_data.get("metadata", {})
            )
            
            # Record command execution metrics
            self._component_metrics.record_message(execution_time)
            
            return result
            
        except asyncio.TimeoutError:
            # Clean up timed out command's Future
            if command.id in self._pending_responses:
                del self._pending_responses[command.id]
            
            # Return timeout result
            self._component_metrics.record_error()
            return CommandResult(
                command_id=command.id,
                status=CommandStatus.TIMEOUT,
                error="Command timed out",
                execution_time=time.time() - start_time,
                completed_at=time.time(),
                metadata={"timeout": timeout}
            )
            
        except Exception as e:
            # Clean up error command's Future
            if command.id in self._pending_responses:
                del self._pending_responses[command.id]
            
            # Record error
            self._component_metrics.record_error()
            logger.error(f"Failed to send command {command.name}: {e}")
            
            # Return failed result
            return CommandResult(
                command_id=command.id,
                status=CommandStatus.FAILED,
                error=str(e),
                execution_time=time.time() - start_time,
                completed_at=time.time(),
                metadata={"exception": traceback.format_exc()}
            )
    
    async def send_batch(self, commands: List[Command[Any]], wait_all: bool = True) -> Sequence[CommandResult[Any]]:
        """
        Send multiple commands in batch
        
        Args:
            commands: List of commands to send
            wait_all: Whether to wait for all commands to complete
            
        Returns:
            List of command results
        """
        if wait_all:
            # Wait for all commands to complete
            results: List[CommandResult[Any]] = []
            for cmd in commands:
                result = await self.send(cmd)
                results.append(result)
            return results
        else:
            # Send all commands in parallel
            tasks = [self.send(cmd) for cmd in commands]
            return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def create_and_send(self, name: str, data: Any, command_type: CommandType = CommandType.USER, 
                              **kwargs: Any) -> CommandResult[Any]:
        """
        Convenience method to create and send a command
        
        Args:
            name: Command name
            data: Command data
            command_type: Command type
            **kwargs: Other command parameters
            
        Returns:
            Command execution result
        """
        cmd = Command(
            name=name,
            data=data,
            command_type=command_type,
            **kwargs
        )
        return await self.send(cmd)
    
    def register_handler(self, command_name: str, handler: CommandHandler[Any, Any]) -> None:
        """
        Register command handler
        
        Even though command transmitter is mainly for sending commands,
        it can register some basic command handlers locally
        
        Args:
            command_name: Command name
            handler: Command handler
        """
        # This method is mainly for registering local handlers to command service, not implemented in detail
        logger.info(f"Registering local command handler: {command_name}")


class UserCommandHandler(BaseComponent):
    """User command handler base class for handling user interactive commands"""
    
    def __init__(self, name: Optional[str] = None):
        """Initialize user command handler"""
        super().__init__(name=name)
        # Command authorization level
        self.required_permission = None
        # Command handling timeout
        self.timeout = 30.0
    
    @abstractmethod
    async def handle_command(self, command: UserCommand) -> CommandResult[Any]:
        """
        Handle user command, to be implemented by subclass
        
        Args:
            command: User command
            
        Returns:
            Command execution result
        """
        pass


class CommandDispatcher(BaseComponent):
    """
    Command dispatcher responsible for receiving and dispatching commands to appropriate handlers
    
    Used by the server to receive and process commands sent by clients
    """
    
    def __init__(self, name: str = "command_dispatcher", max_workers: int = 10):
        """
        Initialize command dispatcher
        
        Args:
            name: Component name
            max_workers: Maximum worker threads
        """
        super().__init__(name=name)
        self._handlers: Dict[str, Dict[CommandType, List[CommandHandler[Any, Any]]]] = defaultdict(
            lambda: {ct: [] for ct in CommandType}
        )
        self._message_bus: Optional[MessageBus] = None
        self._event_bus: Optional[EventBus] = None
        self._running = False
        self._command_queue: asyncio.PriorityQueue[Any] = asyncio.PriorityQueue()
        self._result_cache: Dict[str, CommandResult[Any]] = {}
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._worker_semaphore = asyncio.Semaphore(max_workers)
        self._worker_tasks: List[asyncio.Task[None]] = []
        self._active_commands: Dict[str, Command[Any]] = {}
        self._active_command_futures: Dict[str, asyncio.Future[CommandResult[Any]]] = {}
        self._command_subscription: Any = None
        self._metrics.update({
            'commands_processed': 0,
            'commands_failed': 0,
            'avg_processing_time': 0,
            'active_commands': 0,
            'queue_size': 0
        })
    
    async def _start_impl(self) -> None:
        """Start command dispatcher"""
        self._running = True
        
        # If there's a message bus, subscribe to command request topics
        if self._message_bus:
            self._command_subscription = await self._message_bus.subscribe("command.request.#")
            # Start command receiving task
            asyncio.create_task(self._receive_commands())
        
        # Start worker threads
        self._worker_tasks = [
            asyncio.create_task(self._worker_process())
            for _ in range(10)  # Fixed number of workers to avoid accessing private attribute
        ]
        
        logger.info(f"Command dispatcher started with {len(self._worker_tasks)} worker threads")
    
    async def _stop_impl(self) -> None:
        """Stop command dispatcher"""
        self._running = False
        
        # Cancel all active commands
        for _cmd_id, future in self._active_command_futures.items():
            if not future.done():
                future.set_exception(Exception("CommandDispatcher stopped"))
        
        # Cancel all worker threads
        for task in self._worker_tasks:
            task.cancel()
        
        # Wait for all worker threads to finish
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks = []
        
        logger.info("Command dispatcher stopped")
    
    async def set_message_bus(self, message_bus: MessageBus) -> None:
        """Set message bus reference"""
        self._message_bus = message_bus
        # If dispatcher is already running, immediately subscribe to command topics
        if self._running:
            self._command_subscription = await self._message_bus.subscribe("command.request.#")
            asyncio.create_task(self._receive_commands())
        logger.info("Message bus integrated with command dispatcher")
    
    async def set_event_bus(self, event_bus: EventBus) -> None:
        """Set event bus reference"""
        self._event_bus = event_bus
        logger.info("Event bus integrated with command dispatcher")
    
    async def _receive_commands(self) -> None:
        """Receive command requests from message bus"""
        while self._running:
            try:
                message = await self._command_subscription.get()
                topic = message.topic
                data = message.data
                
                # Parse command name
                cmd_name = topic.split('.')[-1] if '.' in topic else topic
                
                # Build command object
                try:
                    cmd_type = CommandType[data.get("type", "USER")]
                    cmd_priority = CommandPriority[data.get("priority", "NORMAL")]
                    
                    command = Command(
                        id=data.get("id", str(uuid.uuid4())),
                        command_type=cmd_type,
                        name=cmd_name,
                        data=data.get("data"),
                        metadata=data.get("metadata", {}),
                        priority=cmd_priority,
                        created_at=data.get("created_at", time.time()),
                        source=data.get("source", "remote"),
                        target=data.get("target", "system"),
                        correlation_id=data.get("correlation_id")
                    )
                    
                    # Send command to queue
                    await self._command_queue.put((command.priority.value, command))
                    
                    # Update metrics
                    self._metrics['queue_size'] = self._command_queue.qsize()
                    logger.debug(f"Received command: {cmd_name}, ID: {command.id}")
                    
                except Exception as e:
                    logger.error(f"Failed to parse command data: {e}, data: {data}")
                    # Send error response
                    if self._message_bus:
                        error_response: Dict[str, Any] = {
                            "command_id": data.get("id", "unknown"),
                            "status": "FAILED",
                            "error": f"Invalid command format: {str(e)}",
                            "completed_at": time.time()
                        }
                        await self._message_bus.publish("command.response", error_response)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error receiving command: {e}")
    
    async def _worker_process(self) -> None:
        """Worker thread to process command queue"""
        while self._running:
            try:
                # Get next command
                _, command = await self._command_queue.get()
                
                # Use worker semaphore to limit concurrency
                async with self._worker_semaphore:
                    try:
                        # Record active command
                        self._active_commands[command.id] = command
                        self._metrics['active_commands'] = len(self._active_commands)
                        
                        # Process command
                        result = await self._dispatch_command(command)
                        
                        # Cache result
                        self._result_cache[command.id] = result
                        
                        # If remote command, send response
                        if command.source != "local" and self._message_bus:
                            response: Dict[str, Any] = {
                                "command_id": result.command_id,
                                "status": result.status.name,
                                "result": result.result,
                                "error": result.error,
                                "execution_time": result.execution_time,
                                "completed_at": result.completed_at,
                                "metadata": result.metadata
                            }
                            await self._message_bus.publish("command.response", response)
                        
                        # Handle command result Future
                        if command.id in self._active_command_futures:
                            future = self._active_command_futures[command.id]
                            if not future.done():
                                future.set_result(result)
                            del self._active_command_futures[command.id]
                        
                    except Exception as e:
                        logger.error(f"Failed to process command {command.name}: {e}")
                        
                        # Build error result
                        error_result = CommandResult(
                            command_id=command.id,
                            status=CommandStatus.FAILED,
                            error=str(e),
                            completed_at=time.time(),
                            metadata={"exception": traceback.format_exc()}
                        )
                        
                        # Cache error result
                        self._result_cache[command.id] = error_result
                        
                        # Send error response
                        if command.source != "local" and self._message_bus:
                            error_response: Dict[str, Any] = {
                                "command_id": command.id,
                                "status": "FAILED",
                                "error": str(e),
                                "completed_at": time.time(),
                                "metadata": {"exception": traceback.format_exc()}
                            }
                            await self._message_bus.publish("command.response", error_response)
                        
                        # Handle command result Future
                        if command.id in self._active_command_futures:
                            future = self._active_command_futures[command.id]
                            if not future.done():
                                future.set_exception(e)
                            del self._active_command_futures[command.id]
                    
                    finally:
                        # Remove active command
                        if command.id in self._active_commands:
                            del self._active_commands[command.id]
                            self._metrics['active_commands'] = len(self._active_commands)
                        
                        # Mark task as done
                        self._command_queue.task_done()
                        
                        # Update queue size metric
                        self._metrics['queue_size'] = self._command_queue.qsize()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Command worker thread error: {e}")
    
    async def _dispatch_command(self, command: Command[Any]) -> CommandResult[Any]:
        """
        Dispatch command to appropriate handler based on command type and name
        
        Args:
            command: Command to process
            
        Returns:
            Command execution result
        """
        start_time = time.time()
        
        try:
            # Find matching handlers
            handlers = self._handlers.get(command.name, {}).get(command.command_type, [])
            
            # If no handler found, try generic handler
            if not handlers and command.command_type in self._handlers.get("*", {}):
                handlers = self._handlers.get("*", {}).get(command.command_type, [])
            
            # If still no handler found, try default handler
            if not handlers:
                handlers = self._handlers.get("*", {}).get(CommandType.USER, [])
            
            # If no handler found at all, return failure
            if not handlers:
                raise Exception(f"No handler found for command: {command.name} of type {command.command_type.name}")
            
            # Choose first handler to process command
            handler = handlers[0]
            
            # Call handler with timeout mechanism
            try:
                result = await asyncio.wait_for(
                    handler.handle(command),
                    timeout=command.timeout
                )
            except asyncio.TimeoutError:
                result = CommandResult(
                    command_id=command.id,
                    status=CommandStatus.TIMEOUT,
                    error=f"Command execution timed out after {command.timeout} seconds",
                    completed_at=time.time(),
                    execution_time=time.time() - start_time
                )
            
            # Publish command executed event
            if self._event_bus:
                await self._event_bus.publish(
                    f"command.executed.{command.name}",
                    {
                        "command_id": command.id,
                        "name": command.name,
                        "success": result.status == CommandStatus.COMPLETED,
                        "execution_time": result.execution_time
                    }
                )
            
            # Update metrics
            self._metrics['commands_processed'] += 1
            if result.status != CommandStatus.COMPLETED:
                self._metrics['commands_failed'] += 1
            
            # Update average processing time
            execution_time = time.time() - start_time
            if 'processing_times' not in self._metrics:
                self._metrics['processing_times'] = []
            
            self._metrics['processing_times'].append(execution_time)
            if len(self._metrics['processing_times']) > 100:  # Keep last 100 command processing times
                self._metrics['processing_times'].pop(0)
            
            # Calculate new average processing time
            if self._metrics['processing_times']:
                self._metrics['avg_processing_time'] = sum(self._metrics['processing_times']) / len(self._metrics['processing_times'])
            
            return result
            
        except Exception as e:
            # Update error metrics
            self._metrics['commands_failed'] += 1
            self._component_metrics.record_error()
            
            # Log error details
            logger.error(f"Error processing command {command.name}: {e}")
            logger.debug(traceback.format_exc())
            
            # Build error result
            error_result = CommandResult(
                command_id=command.id,
                status=CommandStatus.FAILED,
                error=str(e),
                completed_at=time.time(),
                execution_time=time.time() - start_time,
                metadata={"exception": traceback.format_exc()}
            )
            
            # Publish command failed event
            if self._event_bus:
                await self._event_bus.publish(
                    "command.failed",
                    {
                        "command_id": command.id,
                        "name": command.name,
                        "error": str(e),
                        "execution_time": error_result.execution_time
                    }
                )
            
            return error_result
    
    async def dispatch(self, command_name: str, data: Any, **kwargs: Any) -> CommandResult[Any]:
        """
        Dispatch command, can be called directly from within the component
        
        Args:
            command_name: Command name
            data: Command data
            **kwargs: Other command parameters
            
        Returns:
            Command execution result
        """
        # Create command
        command = Command(
            name=command_name,
            data=data,
            source="local",  # Mark as local call
            **kwargs
        )
        
        # Create Future to wait for result
        future: asyncio.Future[CommandResult[Any]] = asyncio.get_event_loop().create_future()
        self._active_command_futures[command.id] = future
        
        # Add command to queue
        await self._command_queue.put((command.priority.value, command))
        
        try:
            # Wait for command execution result
            return await asyncio.wait_for(future, command.timeout)
        except asyncio.TimeoutError:
            # Command execution timeout
            if command.id in self._active_command_futures:
                del self._active_command_futures[command.id]
                
            return CommandResult(
                command_id=command.id,
                status=CommandStatus.TIMEOUT,
                error=f"Command dispatch timed out after {command.timeout} seconds",
                completed_at=time.time()
            )
    
    def register_handler(self, command_name: str, handler: CommandHandler[Any, Any],
                         command_type: CommandType = CommandType.USER) -> None:
        """
        Register command handler
        
        Args:
            command_name: Command name, can be specific name or wildcard "*"
            handler: Command handler
            command_type: Command type
        """
        self._handlers[command_name][command_type].append(handler)
        logger.debug(f"Registered command handler: {command_name}, type: {command_type.name}")
    
    def unregister_handler(self, command_name: str, handler: CommandHandler[Any, Any],
                          command_type: CommandType = CommandType.USER) -> bool:
        """
        Unregister command handler
        
        Args:
            command_name: Command name
            handler: Command handler
            command_type: Command type
            
        Returns:
            Whether unregistration was successful
        """
        if (command_name in self._handlers and 
            command_type in self._handlers[command_name] and
            handler in self._handlers[command_name][command_type]):
            
            self._handlers[command_name][command_type].remove(handler)
            return True
            
        return False
