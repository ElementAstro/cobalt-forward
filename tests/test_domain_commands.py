"""
Tests for Command domain model.

This module tests the Command and CommandResult domain models including
validation, serialization, and all methods.
"""

import pytest
import time
from typing import Any, Dict

from cobalt_forward.core.domain.commands import (
    Command, CommandResult, CommandType, CommandStatus
)


class TestCommandType:
    """Test cases for CommandType enum."""
    
    def test_command_types_exist(self):
        """Test that all expected command types exist."""
        expected_types = {
            'SYSTEM', 'USER', 'DEVICE', 'DATA', 'PLUGIN', 'NETWORK'
        }
        actual_types = {ct.name for ct in CommandType}
        assert actual_types == expected_types


class TestCommandStatus:
    """Test cases for CommandStatus enum."""
    
    def test_command_statuses_exist(self):
        """Test that all expected command statuses exist."""
        expected_statuses = {
            'PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT'
        }
        actual_statuses = {cs.name for cs in CommandStatus}
        assert actual_statuses == expected_statuses


class TestCommand:
    """Test cases for Command domain model."""
    
    @pytest.fixture
    def sample_command_data(self) -> Dict[str, Any]:
        """Sample command data for testing."""
        return {
            "target": "server1",
            "action": "restart",
            "parameters": {"force": True}
        }
    
    @pytest.fixture
    def sample_metadata(self) -> Dict[str, Any]:
        """Sample metadata for testing."""
        return {
            "version": "1.0",
            "environment": "test"
        }
    
    def test_command_creation_minimal(self, sample_command_data):
        """Test creating command with minimal required fields."""
        command = Command(name="system.restart", data=sample_command_data)
        
        assert command.name == "system.restart"
        assert command.data == sample_command_data
        assert command.command_type == CommandType.USER
        assert command.user_id is None
        assert command.session_id is None
        assert command.correlation_id is None
        assert command.timeout is None
        assert command.priority == 0
        assert isinstance(command.timestamp, float)
        assert isinstance(command.command_id, str)
        assert len(command.command_id) > 0
        assert isinstance(command.metadata, dict)
        assert len(command.metadata) == 0
    
    def test_command_creation_full(self, sample_command_data, sample_metadata):
        """Test creating command with all fields."""
        timestamp = time.time()
        command_id = "cmd-123"
        
        command = Command(
            name="system.shutdown",
            data=sample_command_data,
            command_type=CommandType.SYSTEM,
            command_id=command_id,
            timestamp=timestamp,
            user_id="user-456",
            session_id="session-789",
            correlation_id="corr-abc",
            timeout=30.0,
            priority=5,
            metadata=sample_metadata
        )
        
        assert command.name == "system.shutdown"
        assert command.data == sample_command_data
        assert command.command_type == CommandType.SYSTEM
        assert command.command_id == command_id
        assert command.timestamp == timestamp
        assert command.user_id == "user-456"
        assert command.session_id == "session-789"
        assert command.correlation_id == "corr-abc"
        assert command.timeout == 30.0
        assert command.priority == 5
        assert command.metadata == sample_metadata
    
    def test_command_validation_empty_name(self, sample_command_data):
        """Test validation fails for empty command name."""
        with pytest.raises(ValueError, match="Command name cannot be empty"):
            Command(name="", data=sample_command_data)
    
    def test_command_validation_invalid_command_type(self, sample_command_data):
        """Test validation fails for invalid command type."""
        with pytest.raises(ValueError, match="Command type must be a CommandType enum value"):
            command = Command.__new__(Command)
            command.name = "test.command"
            command.data = sample_command_data
            command.command_type = "invalid"  # type: ignore
            command.timeout = None
            command.__post_init__()
    
    def test_command_validation_negative_timeout(self, sample_command_data):
        """Test validation fails for negative timeout."""
        with pytest.raises(ValueError, match="Timeout must be positive"):
            Command(
                name="test.command",
                data=sample_command_data,
                timeout=-1.0
            )
    
    def test_command_validation_zero_timeout(self, sample_command_data):
        """Test validation fails for zero timeout."""
        with pytest.raises(ValueError, match="Timeout must be positive"):
            Command(
                name="test.command",
                data=sample_command_data,
                timeout=0.0
            )
    
    def test_command_priority_ordering(self, sample_command_data):
        """Test command priority ordering for queue."""
        # Higher priority value = higher priority
        high_priority = Command(
            name="high.priority",
            data=sample_command_data,
            priority=10,
            timestamp=1000.0
        )
        
        low_priority = Command(
            name="low.priority",
            data=sample_command_data,
            priority=1,
            timestamp=1000.0
        )
        
        # High priority should come before low priority
        assert high_priority < low_priority
        assert not (low_priority < high_priority)
    
    def test_command_timestamp_ordering_same_priority(self, sample_command_data):
        """Test timestamp ordering for same priority commands."""
        earlier = Command(
            name="earlier.command",
            data=sample_command_data,
            priority=5,
            timestamp=1000.0
        )
        
        later = Command(
            name="later.command",
            data=sample_command_data,
            priority=5,
            timestamp=2000.0
        )
        
        # Earlier timestamp should come first for same priority
        assert earlier < later
        assert not (later < earlier)
    
    def test_command_ordering_not_implemented_for_non_command(self, sample_command_data):
        """Test that ordering returns NotImplemented for non-Command objects."""
        command = Command(name="test.command", data=sample_command_data)
        result = command.__lt__("not a command")
        assert result is NotImplemented
    
    def test_with_metadata(self, sample_command_data):
        """Test adding metadata to command."""
        original_command = Command(
            name="test.command",
            data=sample_command_data,
            metadata={"original": "value"}
        )
        
        new_command = original_command.with_metadata(
            additional="data",
            version="2.0"
        )
        
        # Original command unchanged
        assert original_command.metadata == {"original": "value"}
        
        # New command has merged metadata
        expected_metadata = {
            "original": "value",
            "additional": "data",
            "version": "2.0"
        }
        assert new_command.metadata == expected_metadata
        
        # Other fields unchanged
        assert new_command.name == original_command.name
        assert new_command.command_id == original_command.command_id
        assert new_command.timestamp == original_command.timestamp
        assert new_command.data == original_command.data
    
    def test_with_metadata_override(self, sample_command_data):
        """Test that new metadata overrides existing keys."""
        original_command = Command(
            name="test.command",
            data=sample_command_data,
            metadata={"key": "original", "other": "value"}
        )
        
        new_command = original_command.with_metadata(key="updated")
        
        assert new_command.metadata == {"key": "updated", "other": "value"}
    
    def test_to_dict(self, sample_command_data, sample_metadata):
        """Test converting command to dictionary."""
        command = Command(
            name="system.action",
            data=sample_command_data,
            command_type=CommandType.SYSTEM,
            command_id="cmd-123",
            timestamp=1234567890.0,
            user_id="user-456",
            session_id="session-789",
            correlation_id="corr-abc",
            timeout=30.0,
            priority=5,
            metadata=sample_metadata
        )
        
        result = command.to_dict()
        
        expected = {
            'name': 'system.action',
            'data': sample_command_data,
            'command_type': 'SYSTEM',
            'command_id': 'cmd-123',
            'timestamp': 1234567890.0,
            'user_id': 'user-456',
            'session_id': 'session-789',
            'correlation_id': 'corr-abc',
            'timeout': 30.0,
            'priority': 5,
            'metadata': sample_metadata
        }
        
        assert result == expected
    
    def test_to_dict_minimal(self, sample_command_data):
        """Test converting minimal command to dictionary."""
        command = Command(name="simple.command", data=sample_command_data)
        result = command.to_dict()
        
        assert result['name'] == 'simple.command'
        assert result['data'] == sample_command_data
        assert result['command_type'] == 'USER'
        assert result['user_id'] is None
        assert result['session_id'] is None
        assert result['correlation_id'] is None
        assert result['timeout'] is None
        assert result['priority'] == 0
        assert isinstance(result['timestamp'], float)
        assert isinstance(result['command_id'], str)
        assert result['metadata'] == {}
    
    def test_default_command_id_unique(self, sample_command_data):
        """Test that default command IDs are unique."""
        command1 = Command(name="test.command1", data=sample_command_data)
        command2 = Command(name="test.command2", data=sample_command_data)
        
        assert command1.command_id != command2.command_id
        assert len(command1.command_id) > 0
        assert len(command2.command_id) > 0


class TestCommandResult:
    """Test cases for CommandResult domain model."""
    
    @pytest.fixture
    def sample_result_data(self) -> Dict[str, Any]:
        """Sample result data for testing."""
        return {
            "output": "Operation completed successfully",
            "affected_rows": 5
        }
    
    def test_command_result_creation_minimal(self):
        """Test creating command result with minimal fields."""
        result = CommandResult(
            command_id="cmd-123",
            status=CommandStatus.COMPLETED
        )
        
        assert result.command_id == "cmd-123"
        assert result.status == CommandStatus.COMPLETED
        assert result.result is None
        assert result.error is None
        assert result.error_code is None
        assert result.execution_time is None
        assert isinstance(result.timestamp, float)
        assert isinstance(result.metadata, dict)
        assert len(result.metadata) == 0
    
    def test_command_result_creation_full(self, sample_result_data):
        """Test creating command result with all fields."""
        timestamp = time.time()
        metadata = {"version": "1.0"}
        
        result = CommandResult(
            command_id="cmd-456",
            status=CommandStatus.FAILED,
            result=sample_result_data,
            error="Operation failed",
            error_code="ERR_001",
            execution_time=2.5,
            timestamp=timestamp,
            metadata=metadata
        )
        
        assert result.command_id == "cmd-456"
        assert result.status == CommandStatus.FAILED
        assert result.result == sample_result_data
        assert result.error == "Operation failed"
        assert result.error_code == "ERR_001"
        assert result.execution_time == 2.5
        assert result.timestamp == timestamp
        assert result.metadata == metadata
    
    def test_command_result_validation_empty_command_id(self):
        """Test validation fails for empty command ID."""
        with pytest.raises(ValueError, match="Command ID cannot be empty"):
            CommandResult(command_id="", status=CommandStatus.COMPLETED)
    
    def test_command_result_validation_invalid_status(self):
        """Test validation fails for invalid status."""
        with pytest.raises(ValueError, match="Status must be a CommandStatus enum value"):
            result = CommandResult.__new__(CommandResult)
            result.command_id = "cmd-123"
            result.status = "invalid"  # type: ignore
            result.execution_time = None
            result.__post_init__()
    
    def test_command_result_validation_negative_execution_time(self):
        """Test validation fails for negative execution time."""
        with pytest.raises(ValueError, match="Execution time cannot be negative"):
            CommandResult(
                command_id="cmd-123",
                status=CommandStatus.COMPLETED,
                execution_time=-1.0
            )
    
    def test_is_success_property(self):
        """Test is_success property."""
        success_result = CommandResult(
            command_id="cmd-123",
            status=CommandStatus.COMPLETED
        )
        assert success_result.is_success is True
        
        failed_result = CommandResult(
            command_id="cmd-456",
            status=CommandStatus.FAILED
        )
        assert failed_result.is_success is False
    
    def test_is_failure_property(self):
        """Test is_failure property."""
        failure_statuses = [
            CommandStatus.FAILED,
            CommandStatus.TIMEOUT,
            CommandStatus.CANCELLED
        ]
        
        for status in failure_statuses:
            result = CommandResult(command_id="cmd-123", status=status)
            assert result.is_failure is True
        
        success_result = CommandResult(
            command_id="cmd-456",
            status=CommandStatus.COMPLETED
        )
        assert success_result.is_failure is False
        
        pending_result = CommandResult(
            command_id="cmd-789",
            status=CommandStatus.PENDING
        )
        assert pending_result.is_failure is False

    def test_success_factory_method(self, sample_result_data):
        """Test CommandResult.success factory method."""
        result = CommandResult.success(
            command_id="cmd-success",
            result=sample_result_data,
            execution_time=1.5
        )

        assert result.command_id == "cmd-success"
        assert result.status == CommandStatus.COMPLETED
        assert result.result == sample_result_data
        assert result.error is None
        assert result.error_code is None
        assert result.execution_time == 1.5
        assert result.is_success is True
        assert result.is_failure is False

    def test_failure_factory_method(self):
        """Test CommandResult.failure factory method."""
        result = CommandResult.failure(
            command_id="cmd-failure",
            error="Something went wrong",
            error_code="ERR_500",
            execution_time=0.8
        )

        assert result.command_id == "cmd-failure"
        assert result.status == CommandStatus.FAILED
        assert result.result is None
        assert result.error == "Something went wrong"
        assert result.error_code == "ERR_500"
        assert result.execution_time == 0.8
        assert result.is_success is False
        assert result.is_failure is True

    def test_to_dict(self, sample_result_data):
        """Test converting command result to dictionary."""
        metadata = {"trace_id": "trace-123"}

        result = CommandResult(
            command_id="cmd-dict",
            status=CommandStatus.COMPLETED,
            result=sample_result_data,
            error="No error",
            error_code="SUCCESS",
            execution_time=3.2,
            timestamp=1234567890.0,
            metadata=metadata
        )

        result_dict = result.to_dict()

        expected = {
            'command_id': 'cmd-dict',
            'status': 'COMPLETED',
            'result': sample_result_data,
            'error': 'No error',
            'error_code': 'SUCCESS',
            'execution_time': 3.2,
            'timestamp': 1234567890.0,
            'metadata': metadata
        }

        assert result_dict == expected

    def test_to_dict_minimal(self):
        """Test converting minimal command result to dictionary."""
        result = CommandResult(
            command_id="cmd-minimal",
            status=CommandStatus.PENDING
        )

        result_dict = result.to_dict()

        assert result_dict['command_id'] == 'cmd-minimal'
        assert result_dict['status'] == 'PENDING'
        assert result_dict['result'] is None
        assert result_dict['error'] is None
        assert result_dict['error_code'] is None
        assert result_dict['execution_time'] is None
        assert isinstance(result_dict['timestamp'], float)
        assert result_dict['metadata'] == {}
