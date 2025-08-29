"""
Tests for command interfaces.

This module tests the command interfaces including ICommandHandler and ICommandDispatcher.
"""

import pytest
from typing import Any, Dict, List, Type
from unittest.mock import AsyncMock, Mock

from cobalt_forward.core.interfaces.commands import ICommandHandler, ICommandDispatcher
from cobalt_forward.core.domain.commands import Command, CommandResult, CommandStatus, CommandType


class TestCommand(Command[Dict[str, Any]]):
    """Test command implementation."""
    
    def __init__(self, data: Dict[str, Any], name: str = "test.command"):
        super().__init__(name=name, data=data)


class AnotherTestCommand(Command[str]):
    """Another test command implementation."""
    
    def __init__(self, data: str, name: str = "another.command"):
        super().__init__(name=name, data=data)


class MockCommandHandler(ICommandHandler[Dict[str, Any], str]):
    """Mock implementation of ICommandHandler for testing."""
    
    def __init__(self, supported_commands: List[Type[Command[Any]]] = None):
        self._supported_commands = supported_commands or [TestCommand]
        self.handled_commands: List[Command[Any]] = []
        self.handle_called_count = 0
        self.can_handle_called_count = 0
        self.should_handle = True
        self.result_to_return = "mock_result"
        self.should_fail = False
    
    async def handle(self, command: Command[Dict[str, Any]]) -> CommandResult[str]:
        """Handle a command and return a result."""
        self.handled_commands.append(command)
        self.handle_called_count += 1
        
        if self.should_fail:
            return CommandResult.failure(
                command_id=command.command_id,
                error="Mock handler failure"
            )
        
        return CommandResult.success(
            command_id=command.command_id,
            result=self.result_to_return
        )
    
    def can_handle(self, command: Command[Any]) -> bool:
        """Check if this handler can process the given command."""
        self.can_handle_called_count += 1
        return self.should_handle and type(command) in self._supported_commands
    
    @property
    def supported_commands(self) -> List[Type[Command[Any]]]:
        """Get list of command types this handler supports."""
        return self._supported_commands


class MockCommandDispatcher(ICommandDispatcher):
    """Mock implementation of ICommandDispatcher for testing."""
    
    def __init__(self):
        self.handlers: List[ICommandHandler[Any, Any]] = []
        self.dispatched_commands: List[Command[Any]] = []
        self.dispatch_called_count = 0
        self.register_handler_called_count = 0
        self.unregister_handler_called_count = 0
        self.get_handlers_called_count = 0
        self.get_metrics_called_count = 0
        self.should_fail_dispatch = False
        self.metrics = {
            'commands_dispatched': 0,
            'commands_completed': 0,
            'commands_failed': 0,
            'handlers_count': 0
        }
    
    async def dispatch(self, command: Command[Any]) -> CommandResult[Any]:
        """Dispatch a command to appropriate handler."""
        self.dispatched_commands.append(command)
        self.dispatch_called_count += 1
        self.metrics['commands_dispatched'] += 1
        
        if self.should_fail_dispatch:
            self.metrics['commands_failed'] += 1
            return CommandResult.failure(
                command_id=command.command_id,
                error="Mock dispatcher failure"
            )
        
        # Find handler for command
        for handler in self.handlers:
            if handler.can_handle(command):
                result = await handler.handle(command)
                if result.is_success:
                    self.metrics['commands_completed'] += 1
                else:
                    self.metrics['commands_failed'] += 1
                return result
        
        # No handler found
        self.metrics['commands_failed'] += 1
        return CommandResult.failure(
            command_id=command.command_id,
            error="No handler found for command"
        )
    
    async def register_handler(self, handler: ICommandHandler[Any, Any]) -> None:
        """Register a command handler."""
        self.handlers.append(handler)
        self.register_handler_called_count += 1
        self.metrics['handlers_count'] = len(self.handlers)
    
    async def unregister_handler(self, handler: ICommandHandler[Any, Any]) -> bool:
        """Unregister a command handler."""
        self.unregister_handler_called_count += 1
        if handler in self.handlers:
            self.handlers.remove(handler)
            self.metrics['handlers_count'] = len(self.handlers)
            return True
        return False
    
    async def get_handlers(self, command_type: Type[Command[Any]]) -> List[ICommandHandler[Any, Any]]:
        """Get all handlers for a specific command type."""
        self.get_handlers_called_count += 1
        return [h for h in self.handlers if command_type in h.supported_commands]
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get command dispatcher metrics."""
        self.get_metrics_called_count += 1
        return self.metrics.copy()


class TestICommandHandler:
    """Test cases for ICommandHandler interface."""
    
    @pytest.fixture
    def command_handler(self) -> MockCommandHandler:
        """Create a mock command handler."""
        return MockCommandHandler()
    
    @pytest.fixture
    def test_command(self) -> TestCommand:
        """Create a test command."""
        return TestCommand(data={"action": "test", "value": 42})
    
    @pytest.fixture
    def another_command(self) -> AnotherTestCommand:
        """Create another test command."""
        return AnotherTestCommand(data="test data")
    
    @pytest.mark.asyncio
    async def test_handle_method_exists(self, command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test that handle method exists and can be called."""
        assert hasattr(command_handler, 'handle')
        assert callable(command_handler.handle)
        
        result = await command_handler.handle(test_command)
        
        assert isinstance(result, CommandResult)
        assert result.command_id == test_command.command_id
        assert command_handler.handle_called_count == 1
        assert len(command_handler.handled_commands) == 1
        assert command_handler.handled_commands[0] == test_command
    
    @pytest.mark.asyncio
    async def test_handle_success_result(self, command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test handling command with success result."""
        command_handler.result_to_return = "success_result"
        
        result = await command_handler.handle(test_command)
        
        assert result.is_success is True
        assert result.is_failure is False
        assert result.status == CommandStatus.COMPLETED
        assert result.result == "success_result"
        assert result.error is None
    
    @pytest.mark.asyncio
    async def test_handle_failure_result(self, command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test handling command with failure result."""
        command_handler.should_fail = True
        
        result = await command_handler.handle(test_command)
        
        assert result.is_success is False
        assert result.is_failure is True
        assert result.status == CommandStatus.FAILED
        assert result.result is None
        assert result.error == "Mock handler failure"
    
    def test_can_handle_method_exists(self, command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test that can_handle method exists and can be called."""
        assert hasattr(command_handler, 'can_handle')
        assert callable(command_handler.can_handle)
        
        result = command_handler.can_handle(test_command)
        
        assert isinstance(result, bool)
        assert command_handler.can_handle_called_count == 1
    
    def test_can_handle_supported_command(self, command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test can_handle returns True for supported command."""
        result = command_handler.can_handle(test_command)
        assert result is True
    
    def test_can_handle_unsupported_command(self, command_handler: MockCommandHandler, another_command: AnotherTestCommand) -> None:
        """Test can_handle returns False for unsupported command."""
        result = command_handler.can_handle(another_command)
        assert result is False
    
    def test_can_handle_when_disabled(self, command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test can_handle returns False when handler is disabled."""
        command_handler.should_handle = False
        
        result = command_handler.can_handle(test_command)
        assert result is False
    
    def test_supported_commands_property(self, command_handler: MockCommandHandler) -> None:
        """Test supported_commands property."""
        assert hasattr(command_handler, 'supported_commands')
        
        supported = command_handler.supported_commands
        
        assert isinstance(supported, list)
        assert TestCommand in supported
        assert len(supported) == 1
    
    def test_supported_commands_multiple_types(self) -> None:
        """Test handler with multiple supported command types."""
        handler = MockCommandHandler([TestCommand, AnotherTestCommand])
        
        supported = handler.supported_commands
        
        assert len(supported) == 2
        assert TestCommand in supported
        assert AnotherTestCommand in supported
    
    @pytest.mark.asyncio
    async def test_handle_multiple_commands(self, command_handler: MockCommandHandler) -> None:
        """Test handling multiple commands."""
        commands = [
            TestCommand({"id": 1}),
            TestCommand({"id": 2}),
            TestCommand({"id": 3})
        ]
        
        results = []
        for command in commands:
            result = await command_handler.handle(command)
            results.append(result)
        
        assert len(results) == 3
        assert command_handler.handle_called_count == 3
        assert len(command_handler.handled_commands) == 3
        
        for i, result in enumerate(results):
            assert result.is_success is True
            assert result.command_id == commands[i].command_id


class TestICommandDispatcher:
    """Test cases for ICommandDispatcher interface."""
    
    @pytest.fixture
    def command_dispatcher(self) -> MockCommandDispatcher:
        """Create a mock command dispatcher."""
        return MockCommandDispatcher()
    
    @pytest.fixture
    def command_handler(self) -> MockCommandHandler:
        """Create a mock command handler."""
        return MockCommandHandler()
    
    @pytest.fixture
    def test_command(self) -> TestCommand:
        """Create a test command."""
        return TestCommand(data={"action": "dispatch_test"})
    
    @pytest.mark.asyncio
    async def test_dispatch_method_exists(self, command_dispatcher: MockCommandDispatcher, test_command: TestCommand) -> None:
        """Test that dispatch method exists and can be called."""
        assert hasattr(command_dispatcher, 'dispatch')
        assert callable(command_dispatcher.dispatch)
        
        result = await command_dispatcher.dispatch(test_command)
        
        assert isinstance(result, CommandResult)
        assert command_dispatcher.dispatch_called_count == 1
        assert len(command_dispatcher.dispatched_commands) == 1
        assert command_dispatcher.dispatched_commands[0] == test_command
    
    @pytest.mark.asyncio
    async def test_dispatch_with_handler(self, command_dispatcher: MockCommandDispatcher, 
                                        command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test dispatching command with registered handler."""
        await command_dispatcher.register_handler(command_handler)
        
        result = await command_dispatcher.dispatch(test_command)
        
        assert result.is_success is True
        assert result.result == "mock_result"
        assert command_handler.handle_called_count == 1
        assert command_dispatcher.metrics['commands_completed'] == 1
    
    @pytest.mark.asyncio
    async def test_dispatch_no_handler(self, command_dispatcher: MockCommandDispatcher, test_command: TestCommand) -> None:
        """Test dispatching command with no registered handler."""
        result = await command_dispatcher.dispatch(test_command)
        
        assert result.is_success is False
        assert result.error == "No handler found for command"
        assert command_dispatcher.metrics['commands_failed'] == 1
    
    @pytest.mark.asyncio
    async def test_dispatch_failure(self, command_dispatcher: MockCommandDispatcher, test_command: TestCommand) -> None:
        """Test dispatch failure."""
        command_dispatcher.should_fail_dispatch = True
        
        result = await command_dispatcher.dispatch(test_command)
        
        assert result.is_success is False
        assert result.error == "Mock dispatcher failure"
        assert command_dispatcher.metrics['commands_failed'] == 1

    @pytest.mark.asyncio
    async def test_register_handler(self, command_dispatcher: MockCommandDispatcher, command_handler: MockCommandHandler) -> None:
        """Test registering a command handler."""
        assert hasattr(command_dispatcher, 'register_handler')
        assert callable(command_dispatcher.register_handler)

        await command_dispatcher.register_handler(command_handler)

        assert command_handler in command_dispatcher.handlers
        assert command_dispatcher.register_handler_called_count == 1
        assert command_dispatcher.metrics['handlers_count'] == 1

    @pytest.mark.asyncio
    async def test_register_multiple_handlers(self, command_dispatcher: MockCommandDispatcher) -> None:
        """Test registering multiple handlers."""
        handler1 = MockCommandHandler([TestCommand])
        handler2 = MockCommandHandler([AnotherTestCommand])

        await command_dispatcher.register_handler(handler1)
        await command_dispatcher.register_handler(handler2)

        assert len(command_dispatcher.handlers) == 2
        assert handler1 in command_dispatcher.handlers
        assert handler2 in command_dispatcher.handlers
        assert command_dispatcher.metrics['handlers_count'] == 2

    @pytest.mark.asyncio
    async def test_unregister_handler(self, command_dispatcher: MockCommandDispatcher, command_handler: MockCommandHandler) -> None:
        """Test unregistering a command handler."""
        assert hasattr(command_dispatcher, 'unregister_handler')
        assert callable(command_dispatcher.unregister_handler)

        # Register first
        await command_dispatcher.register_handler(command_handler)
        assert len(command_dispatcher.handlers) == 1

        # Then unregister
        result = await command_dispatcher.unregister_handler(command_handler)

        assert result is True
        assert command_handler not in command_dispatcher.handlers
        assert command_dispatcher.unregister_handler_called_count == 1
        assert command_dispatcher.metrics['handlers_count'] == 0

    @pytest.mark.asyncio
    async def test_unregister_nonexistent_handler(self, command_dispatcher: MockCommandDispatcher, command_handler: MockCommandHandler) -> None:
        """Test unregistering a handler that was never registered."""
        result = await command_dispatcher.unregister_handler(command_handler)

        assert result is False
        assert command_dispatcher.unregister_handler_called_count == 1

    @pytest.mark.asyncio
    async def test_get_handlers(self, command_dispatcher: MockCommandDispatcher) -> None:
        """Test getting handlers for a specific command type."""
        assert hasattr(command_dispatcher, 'get_handlers')
        assert callable(command_dispatcher.get_handlers)

        handler1 = MockCommandHandler([TestCommand])
        handler2 = MockCommandHandler([AnotherTestCommand])
        handler3 = MockCommandHandler([TestCommand, AnotherTestCommand])

        await command_dispatcher.register_handler(handler1)
        await command_dispatcher.register_handler(handler2)
        await command_dispatcher.register_handler(handler3)

        # Get handlers for TestCommand
        test_handlers = await command_dispatcher.get_handlers(TestCommand)
        assert len(test_handlers) == 2
        assert handler1 in test_handlers
        assert handler3 in test_handlers
        assert handler2 not in test_handlers

        # Get handlers for AnotherTestCommand
        another_handlers = await command_dispatcher.get_handlers(AnotherTestCommand)
        assert len(another_handlers) == 2
        assert handler2 in another_handlers
        assert handler3 in another_handlers
        assert handler1 not in another_handlers

        assert command_dispatcher.get_handlers_called_count == 2

    @pytest.mark.asyncio
    async def test_get_handlers_no_matches(self, command_dispatcher: MockCommandDispatcher) -> None:
        """Test getting handlers when no handlers match the command type."""
        # Register handler for TestCommand
        handler = MockCommandHandler([TestCommand])
        await command_dispatcher.register_handler(handler)

        # Try to get handlers for AnotherTestCommand
        handlers = await command_dispatcher.get_handlers(AnotherTestCommand)

        assert len(handlers) == 0
        assert isinstance(handlers, list)

    @pytest.mark.asyncio
    async def test_get_metrics(self, command_dispatcher: MockCommandDispatcher) -> None:
        """Test getting dispatcher metrics."""
        assert hasattr(command_dispatcher, 'get_metrics')
        assert callable(command_dispatcher.get_metrics)

        # Initial metrics
        metrics = await command_dispatcher.get_metrics()

        assert isinstance(metrics, dict)
        assert 'commands_dispatched' in metrics
        assert 'commands_completed' in metrics
        assert 'commands_failed' in metrics
        assert 'handlers_count' in metrics
        assert command_dispatcher.get_metrics_called_count == 1

        # Verify initial values
        assert metrics['commands_dispatched'] == 0
        assert metrics['commands_completed'] == 0
        assert metrics['commands_failed'] == 0
        assert metrics['handlers_count'] == 0

    @pytest.mark.asyncio
    async def test_metrics_updated_after_operations(self, command_dispatcher: MockCommandDispatcher,
                                                   command_handler: MockCommandHandler, test_command: TestCommand) -> None:
        """Test that metrics are updated after operations."""
        # Register handler and dispatch command
        await command_dispatcher.register_handler(command_handler)
        await command_dispatcher.dispatch(test_command)

        metrics = await command_dispatcher.get_metrics()

        assert metrics['commands_dispatched'] == 1
        assert metrics['commands_completed'] == 1
        assert metrics['commands_failed'] == 0
        assert metrics['handlers_count'] == 1
