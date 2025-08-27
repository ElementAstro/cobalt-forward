"""
Command dispatcher implementation for command pattern processing.

This module provides a command dispatcher that routes commands to appropriate
handlers with support for validation, interception, and result processing.
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Type, Optional

from ..interfaces.commands import ICommandDispatcher, ICommandHandler, ICommandValidator, ICommandInterceptor
from ..interfaces.lifecycle import IComponent
from ..domain.commands import Command, CommandResult, CommandStatus

logger = logging.getLogger(__name__)


class CommandDispatcher(IComponent, ICommandDispatcher):
    """
    Command dispatcher implementation with validation and interception support.

    Routes commands to registered handlers with support for middleware-style
    interceptors and comprehensive error handling.
    """

    def __init__(self) -> None:
        self._handlers: Dict[Type[Command[Any]],
                             List[ICommandHandler[Any, Any]]] = defaultdict(list)
        self._validators: List[ICommandValidator] = []
        self._interceptors: List[ICommandInterceptor] = []
        self._running = False

        # Metrics
        self._metrics: Dict[str, Any] = {
            'commands_dispatched': 0,
            'commands_completed': 0,
            'commands_failed': 0,
            'handlers_count': 0,
            'avg_processing_time': 0.0,
            'processing_times': []
        }

    @property
    def name(self) -> str:
        """Get component name."""
        return "CommandDispatcher"

    @property
    def version(self) -> str:
        """Get component version."""
        return "1.0.0"

    async def start(self) -> None:
        """Start the command dispatcher."""
        if self._running:
            return

        logger.info("Starting command dispatcher")
        self._running = True
        logger.info("Command dispatcher started successfully")

    async def stop(self) -> None:
        """Stop the command dispatcher."""
        if not self._running:
            return

        logger.info("Stopping command dispatcher...")

        self._running = False
        self._handlers.clear()
        self._validators.clear()
        self._interceptors.clear()

        logger.info("Command dispatcher stopped")

    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the command dispatcher."""
        # Configuration options can be added here
        pass

    async def check_health(self) -> Dict[str, Any]:
        """Check command dispatcher health."""
        return {
            'healthy': True,
            'status': 'running' if self._running else 'stopped',
            'details': {
                'handlers_count': sum(len(handlers) for handlers in self._handlers.values()),
                'validators_count': len(self._validators),
                'interceptors_count': len(self._interceptors),
                'commands_dispatched': self._metrics['commands_dispatched'],
                'commands_completed': self._metrics['commands_completed'],
                'commands_failed': self._metrics['commands_failed'],
                'avg_processing_time': self._metrics['avg_processing_time']
            }
        }

    async def dispatch(self, command: Command[Any]) -> CommandResult[Any]:
        """Dispatch a command to appropriate handler."""
        if not self._running:
            raise RuntimeError("Command dispatcher is not running")

        start_time = time.time()

        try:
            # Validate command
            await self._validate_command(command)

            # Apply before interceptors
            processed_command = await self._apply_before_interceptors(command)

            # Find and execute handler
            result = await self._execute_command(processed_command)

            # Apply after interceptors
            final_result = await self._apply_after_interceptors(processed_command, result)

            self._metrics['commands_dispatched'] += 1
            if final_result.is_success:
                self._metrics['commands_completed'] += 1
            else:
                self._metrics['commands_failed'] += 1

            return final_result

        except Exception as e:
            self._metrics['commands_failed'] += 1

            # Try error interceptors
            error_result = await self._apply_error_interceptors(command, e)
            if error_result:
                return error_result

            # Return error result
            return CommandResult.failure(
                command_id=command.command_id,
                error=str(e),
                execution_time=time.time() - start_time
            )

        finally:
            # Record processing time
            processing_time = time.time() - start_time
            self._metrics['processing_times'].append(processing_time)

            # Keep only last 1000 processing times
            if len(self._metrics['processing_times']) > 1000:
                self._metrics['processing_times'] = self._metrics['processing_times'][-1000:]

            # Update average
            if self._metrics['processing_times']:
                self._metrics['avg_processing_time'] = sum(
                    self._metrics['processing_times']) / len(self._metrics['processing_times'])

    async def register_handler(self, handler: ICommandHandler[Any, Any]) -> None:
        """Register a command handler."""
        for command_type in handler.supported_commands:
            self._handlers[command_type].append(handler)
            logger.debug(
                f"Registered handler {handler.__class__.__name__} for command {command_type.__name__}")

        self._metrics['handlers_count'] = sum(
            len(handlers) for handlers in self._handlers.values())

    async def unregister_handler(self, handler: ICommandHandler[Any, Any]) -> bool:
        """Unregister a command handler."""
        removed = False

        for command_type in handler.supported_commands:
            if command_type in self._handlers:
                try:
                    self._handlers[command_type].remove(handler)
                    removed = True
                    logger.debug(
                        f"Unregistered handler {handler.__class__.__name__} for command {command_type.__name__}")
                except ValueError:
                    pass  # Handler not in list

        self._metrics['handlers_count'] = sum(
            len(handlers) for handlers in self._handlers.values())
        return removed

    async def get_handlers(self, command_type: Type[Command[Any]]) -> List[ICommandHandler[Any, Any]]:
        """Get all handlers for a specific command type."""
        return self._handlers.get(command_type, []).copy()

    async def get_metrics(self) -> Dict[str, Any]:
        """Get command dispatcher metrics."""
        return self._metrics.copy()

    def add_validator(self, validator: ICommandValidator) -> None:
        """Add a command validator."""
        self._validators.append(validator)
        logger.debug(
            f"Added command validator: {validator.__class__.__name__}")

    def add_interceptor(self, interceptor: ICommandInterceptor) -> None:
        """Add a command interceptor."""
        self._interceptors.append(interceptor)
        logger.debug(
            f"Added command interceptor: {interceptor.__class__.__name__}")

    async def _validate_command(self, command: Command[Any]) -> None:
        """Validate command using all registered validators."""
        for validator in self._validators:
            if not await validator.validate(command):
                raise ValueError(f"Command validation failed: {command.name}")

    async def _apply_before_interceptors(self, command: Command[Any]) -> Command[Any]:
        """Apply before interceptors to the command."""
        processed_command = command

        for interceptor in self._interceptors:
            processed_command = await interceptor.before_handle(processed_command)

        return processed_command

    async def _apply_after_interceptors(self, command: Command[Any], result: CommandResult[Any]) -> CommandResult[Any]:
        """Apply after interceptors to the result."""
        processed_result = result

        for interceptor in self._interceptors:
            processed_result = await interceptor.after_handle(command, processed_result)

        return processed_result

    async def _apply_error_interceptors(self, command: Command[Any], error: Exception) -> Optional[CommandResult[Any]]:
        """Apply error interceptors."""
        for interceptor in self._interceptors:
            result = await interceptor.on_error(command, error)
            if result:
                return result

        return None

    async def _execute_command(self, command: Command[Any]) -> CommandResult[Any]:
        """Execute command using appropriate handler."""
        command_type = type(command)

        # Find handlers for this command type
        handlers = []
        for registered_type, handler_list in self._handlers.items():
            if issubclass(command_type, registered_type):
                handlers.extend(handler_list)

        if not handlers:
            raise ValueError(
                f"No handler found for command type: {command_type.__name__}")

        # Find the best matching handler
        best_handler = None
        for handler in handlers:
            if handler.can_handle(command):
                best_handler = handler
                break

        if not best_handler:
            raise ValueError(
                f"No suitable handler found for command: {command.name}")

        # Execute the command
        return await best_handler.handle(command)
