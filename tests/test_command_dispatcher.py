import pytest
from unittest.mock import Mock, AsyncMock, patch
import asyncio
from typing import Dict, Any

from app.core.command_dispatcher import (
    CommandTransmitter,
    Command,
    CommandHandler,
    CommandResult,
    CommandStatus,
    MessageBus
)

class TestCommand(Command[Dict[str, Any], str]):
    async def validate(self) -> bool:
        return True
        
    async def execute(self) -> str:
        return "test_result"

class FailingCommand(Command[Dict[str, Any], str]):
    async def validate(self) -> bool:
        return False
        
    async def execute(self) -> str:
        raise Exception("Test error")

class TestHandler(CommandHandler[Dict[str, Any], str]):
    async def handle(self, command: Command) -> CommandResult[str]:
        result = await command.execute()
        return CommandResult(CommandStatus.COMPLETED, result=result)

@pytest.fixture
def message_bus():
    return Mock(spec=MessageBus)

@pytest.fixture
def command_transmitter(message_bus):
    return CommandTransmitter(message_bus)

@pytest.fixture
def test_command():
    return TestCommand({"test": "data"})

@pytest.fixture
def failing_command():
    return FailingCommand({"test": "data"})

@pytest.fixture
def test_handler():
    return TestHandler()

@pytest.mark.asyncio
async def test_register_handler(command_transmitter, test_handler):
    await command_transmitter.register_handler(TestCommand, test_handler)
    assert command_transmitter._handlers[TestCommand] == test_handler

@pytest.mark.asyncio
async def test_send_command_success(command_transmitter, test_command, test_handler):
    await command_transmitter.register_handler(TestCommand, test_handler)
    
    result = await command_transmitter.send(test_command)
    
    assert result.status == CommandStatus.COMPLETED
    assert result.result == "test_result"
    assert result.error is None

@pytest.mark.asyncio
async def test_send_command_validation_failure(command_transmitter, failing_command, test_handler):
    await command_transmitter.register_handler(FailingCommand, test_handler)
    
    result = await command_transmitter.send(failing_command)
    
    assert result.status == CommandStatus.FAILED
    assert isinstance(result.error, ValueError)

@pytest.mark.asyncio
async def test_send_batch_parallel(command_transmitter, test_command, test_handler):
    await command_transmitter.register_handler(TestCommand, test_handler)
    
    commands = [test_command, test_command]
    results = await command_transmitter.send_batch(commands, parallel=True)
    
    assert len(results) == 2
    assert all(r.status == CommandStatus.COMPLETED for r in results)
    assert all(r.result == "test_result" for r in results)

@pytest.mark.asyncio
async def test_send_batch_sequential(command_transmitter, test_command, test_handler):
    await command_transmitter.register_handler(TestCommand, test_handler)
    
    commands = [test_command, test_command]
    results = await command_transmitter.send_batch(commands, parallel=False)
    
    assert len(results) == 2
    assert all(r.status == CommandStatus.COMPLETED for r in results)
    assert all(r.result == "test_result" for r in results)

@pytest.mark.asyncio
async def test_command_timeout(command_transmitter, test_handler):
    class SlowCommand(TestCommand):
        async def execute(self) -> str:
            await asyncio.sleep(35)  # Longer than default timeout
            return "slow_result"
            
    await command_transmitter.register_handler(SlowCommand, test_handler)
    
    result = await command_transmitter.send(SlowCommand({"test": "data"}))
    
    assert result.status == CommandStatus.FAILED
    assert isinstance(result.error, TimeoutError)

@pytest.mark.asyncio
async def test_metrics_recording(command_transmitter, test_command, test_handler):
    await command_transmitter.register_handler(TestCommand, test_handler)
    
    await command_transmitter.send(test_command)
    
    metrics = command_transmitter._metrics.metrics
    assert metrics['total_commands'] == 1
    assert metrics['status_counts'][CommandStatus.COMPLETED] == 1
    assert metrics['avg_execution_time'] >= 0

@pytest.mark.asyncio
async def test_handler_not_found(command_transmitter, test_command):
    result = await command_transmitter.send(test_command)
    
    assert result.status == CommandStatus.FAILED
    assert isinstance(result.error, ValueError)