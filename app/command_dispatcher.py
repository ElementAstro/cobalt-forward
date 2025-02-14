from abc import abstractmethod
from types import coroutine
from typing import Any, Dict, Type, Optional, List, Callable, Protocol, Generic, TypeVar
from dataclasses import dataclass
from enum import Enum, auto
import asyncio
import time
from collections import defaultdict
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from loguru import logger

from message_bus import MessageBus

# Type variables for generic command handling
T_Payload = TypeVar('T_Payload')
T_Result = TypeVar('T_Result')


class CommandStatus(Enum):
    """Command execution status"""
    PENDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class CommandContext:
    """Context information for command execution"""
    command_id: str
    timestamp: float
    metadata: Dict[str, Any]
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 30.0


class CommandResult(Generic[T_Result]):
    """Result of command execution"""

    def __init__(
        self,
        status: CommandStatus,
        result: Optional[T_Result] = None,
        error: Optional[Exception] = None
    ):
        self.status = status
        self.result = result
        self.error = error
        self.completion_time = time.time()


class Command(Generic[T_Payload, T_Result]):
    """Base command class"""

    def __init__(self, payload: T_Payload):
        self.payload = payload
        self.context: Optional[CommandContext] = None

    @abstractmethod
    async def execute(self) -> T_Result:
        pass

    @abstractmethod
    async def validate(self) -> bool:
        pass

    async def rollback(self) -> None:
        """Optional rollback implementation"""
        pass


class CommandHandler(Protocol[T_Payload, T_Result]):
    """Protocol for command handlers"""

    async def handle(self, command: Command[T_Payload, T_Result]) -> CommandResult[T_Result]:
        pass


class CommandTransmitter:
    """High-performance command transmitter"""

    def __init__(self, message_bus: MessageBus):
        self._message_bus = message_bus
        self._handlers: Dict[Type[Command], CommandHandler] = {}
        self._active_commands: Dict[str, Command] = {}
        self._results_cache: Dict[str, CommandResult] = {}
        self._command_queues: Dict[str,
                                   asyncio.Queue] = defaultdict(asyncio.Queue)
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._metrics = CommandMetrics()

        logger.info("Initializing CommandTransmitter")
        logger.debug(f"ThreadPoolExecutor created with {10} workers")

        self._middleware: List[Callable] = [
            self._logging_middleware,
            self._validation_middleware,
            self._retry_middleware,
            self._timing_middleware
        ]
        logger.debug(
            f"Registered {len(self._middleware)} middleware functions")

    async def register_handler(self, command_type: Type[Command], handler: CommandHandler) -> None:
        """Register a command handler"""
        self._handlers[command_type] = handler
        logger.info(
            f"Registered handler for command type: {command_type.__name__}")
        logger.debug(f"Total registered handlers: {len(self._handlers)}")

    async def send(
        self,
        command: Command[T_Payload, T_Result],
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> CommandResult[T_Result]:
        """Send a command for execution"""
        command.context = CommandContext(
            command_id=str(uuid.uuid4()),
            timestamp=time.time(),
            metadata=metadata or {}
        )

        logger.debug(
            f"Sending command: {type(command).__name__} [ID: {command.context.command_id}]")
        logger.trace(f"Command payload: {command.payload}")

        try:
            # Apply middleware pipeline
            logger.debug("Applying middleware pipeline")
            for middleware in self._middleware:
                command = await middleware(command)
                logger.trace(f"Applied middleware: {middleware.__name__}")

            # Get appropriate handler
            handler = self._handlers.get(type(command))
            if not handler:
                error_msg = f"No handler registered for command type: {type(command)}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Execute command
            logger.info(
                f"Executing command [ID: {command.context.command_id}]")
            self._active_commands[command.context.command_id] = command
            result = await self._execute_with_timeout(handler.handle(command))

            # Cache result
            self._results_cache[command.context.command_id] = result
            logger.debug(
                f"Command result cached [ID: {command.context.command_id}]")

            # Update metrics
            self._metrics.record_execution(result.status)
            logger.trace(
                f"Updated metrics for command [ID: {command.context.command_id}]")

            if result.status == CommandStatus.COMPLETED:
                logger.success(
                    f"Command executed successfully [ID: {command.context.command_id}]")
            else:
                logger.warning(
                    f"Command completed with status: {result.status} [ID: {command.context.command_id}]")

            return result

        except Exception as e:
            logger.exception(
                f"Command execution failed [ID: {command.context.command_id}]")
            return CommandResult(CommandStatus.FAILED, error=e)
        finally:
            self._active_commands.pop(command.context.command_id, None)
            logger.debug(
                f"Command removed from active commands [ID: {command.context.command_id}]")

    async def send_batch(
        self,
        commands: List[Command],
        parallel: bool = True
    ) -> List[CommandResult]:
        """Send multiple commands for execution"""
        logger.info(
            f"Processing batch of {len(commands)} commands (parallel={parallel})")

        if parallel:
            logger.debug("Executing commands in parallel")
            tasks = [self.send(cmd) for cmd in commands]
            results = await asyncio.gather(*tasks)
        else:
            logger.debug("Executing commands sequentially")
            results = []
            for cmd in commands:
                results.append(await self.send(cmd))

        success_count = sum(1 for r in results if r.status ==
                            CommandStatus.COMPLETED)
        logger.info(
            f"Batch processing completed. Success: {success_count}/{len(commands)}")
        return results

    async def _execute_with_timeout(self, coro: coroutine) -> CommandResult:
        """Execute coroutine with timeout"""
        try:
            logger.debug("Executing command with timeout")
            return await asyncio.wait_for(coro, timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("Command execution timed out")
            return CommandResult(CommandStatus.FAILED, error=TimeoutError())

    # Middleware implementations
    async def _logging_middleware(self, command: Command) -> Command:
        """Log command execution"""
        logger.debug(
            f"[Middleware] Logging command execution: {type(command).__name__}")
        return command

    async def _validation_middleware(self, command: Command) -> Command:
        """Validate command before execution"""
        logger.debug(
            f"[Middleware] Validating command: {type(command).__name__}")
        if not await command.validate():
            logger.error(
                f"Command validation failed: {type(command).__name__}")
            raise ValueError("Command validation failed")
        logger.debug("Command validation successful")
        return command

    async def _retry_middleware(self, command: Command) -> Command:
        """Handle command retry logic"""
        if command.context.retry_count > 0:
            logger.info(
                f"Retrying command (attempt {command.context.retry_count})")
            await asyncio.sleep(1.0 * command.context.retry_count)
        return command

    async def _timing_middleware(self, command: Command) -> Command:
        """Track command execution time"""
        command.context.metadata['start_time'] = time.time()
        logger.trace(
            f"Command start time recorded: {command.context.metadata['start_time']}")
        return command


class CommandMetrics:
    """Metrics tracking for command execution"""

    def __init__(self):
        self._execution_times: List[float] = []
        self._status_counts = defaultdict(int)
        self._total_commands = 0

    def record_execution(self, status: CommandStatus, execution_time: float = 0.0):
        """Record command execution metrics"""
        self._status_counts[status] += 1
        self._total_commands += 1
        self._execution_times.append(execution_time)

    @property
    def metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return {
            'total_commands': self._total_commands,
            'status_counts': dict(self._status_counts),
            'avg_execution_time': sum(self._execution_times) / len(self._execution_times) if self._execution_times else 0
        }

# Example command implementations


class UserCommand(Command[Dict[str, Any], Dict[str, Any]]):
    """Example user management command"""

    async def validate(self) -> bool:
        required_fields = ['user_id', 'action']
        return all(field in self.payload for field in required_fields)

    async def execute(self) -> Dict[str, Any]:
        # Implementation details
        pass


class UserCommandHandler(CommandHandler[Dict[str, Any], Dict[str, Any]]):
    """Handler for user commands"""

    async def handle(self, command: UserCommand) -> CommandResult[Dict[str, Any]]:
        try:
            result = await command.execute()
            return CommandResult(CommandStatus.COMPLETED, result=result)
        except Exception as e:
            return CommandResult(CommandStatus.FAILED, error=e)


class BatchCommand(Command[List[Dict[str, Any]], List[Dict[str, Any]]]):
    """批量处理命令"""

    async def validate(self) -> bool:
        if not isinstance(self.payload, list):
            return False
        return all(isinstance(item, dict) for item in self.payload)

    async def execute(self) -> List[Dict[str, Any]]:
        results = []
        for item in self.payload:
            # 处理每个批次项
            results.append({"status": "processed", "data": item})
        return results


class ScheduledCommand(Command[Dict[str, Any], None]):
    """计划执行的命令"""

    async def validate(self) -> bool:
        required = ['schedule_time', 'command_data']
        return all(key in self.payload for key in required)

    async def execute(self) -> None:
        schedule_time = self.payload['schedule_time']
        command_data = self.payload['command_data']
        # 实现计划执行逻辑
        await asyncio.sleep(0)  # 占位符


class RetryableCommand(Command[Dict[str, Any], Any]):
    """支持重试的命令"""

    def __init__(self, payload: Dict[str, Any], max_retries: int = 3):
        super().__init__(payload)
        self.max_retries = max_retries
        self.current_retry = 0

    async def execute_with_retry(self) -> Any:
        while self.current_retry < self.max_retries:
            try:
                return await self.execute()
            except Exception as e:
                self.current_retry += 1
                if self.current_retry >= self.max_retries:
                    raise e
                await asyncio.sleep(1 * self.current_retry)
