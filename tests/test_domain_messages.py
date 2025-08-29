"""
Tests for Message domain model.

This module tests the Message domain model including validation,
serialization, and all methods.
"""

import pytest
import time
from typing import Any, Dict

from cobalt_forward.core.domain.messages import Message, MessageType, MessagePriority


class TestMessageType:
    """Test cases for MessageType enum."""
    
    def test_message_types_exist(self) -> None:
        """Test that all expected message types exist."""
        expected_types = {
            'COMMAND', 'EVENT', 'REQUEST', 'RESPONSE', 
            'NOTIFICATION', 'DATA', 'HEARTBEAT', 'ERROR'
        }
        actual_types = {mt.name for mt in MessageType}
        assert actual_types == expected_types


class TestMessagePriority:
    """Test cases for MessagePriority enum."""
    
    def test_priority_values(self) -> None:
        """Test that priority values are correct."""
        assert MessagePriority.LOW.value == 1
        assert MessagePriority.NORMAL.value == 2
        assert MessagePriority.HIGH.value == 3
        assert MessagePriority.CRITICAL.value == 4
    
    def test_priority_ordering(self) -> None:
        """Test that priorities can be compared."""
        assert MessagePriority.LOW < MessagePriority.NORMAL
        assert MessagePriority.NORMAL < MessagePriority.HIGH
        assert MessagePriority.HIGH < MessagePriority.CRITICAL


class TestMessage:
    """Test cases for Message domain model."""
    
    @pytest.fixture
    def sample_message_data(self) -> Dict[str, Any]:
        """Sample message data for testing."""
        return {
            "user_id": "123",
            "action": "process_data",
            "payload": {"items": [1, 2, 3]}
        }
    
    @pytest.fixture
    def sample_headers(self) -> Dict[str, Any]:
        """Sample headers for testing."""
        return {
            "content-type": "application/json",
            "version": "1.0"
        }
    
    def test_message_creation_minimal(self, sample_message_data) -> None:
        """Test creating message with minimal required fields."""
        message = Message(topic="test.topic", data=sample_message_data)
        
        assert message.topic == "test.topic"
        assert message.data == sample_message_data
        assert message.message_type == MessageType.DATA
        assert message.source is None
        assert message.destination is None
        assert message.correlation_id is None
        assert message.reply_to is None
        assert message.priority == MessagePriority.NORMAL
        assert message.ttl is None
        assert isinstance(message.timestamp, float)
        assert isinstance(message.message_id, str)
        assert len(message.message_id) > 0
        assert isinstance(message.headers, dict)
        assert len(message.headers) == 0
    
    def test_message_creation_full(self, sample_message_data, sample_headers) -> None:
        """Test creating message with all fields."""
        timestamp = time.time()
        message_id = "msg-123"
        
        message = Message(
            topic="user.action",
            data=sample_message_data,
            message_type=MessageType.COMMAND,
            message_id=message_id,
            timestamp=timestamp,
            source="client_service",
            destination="server_service",
            correlation_id="corr-456",
            reply_to="client.response",
            priority=MessagePriority.HIGH,
            ttl=300.0,
            headers=sample_headers
        )
        
        assert message.topic == "user.action"
        assert message.data == sample_message_data
        assert message.message_type == MessageType.COMMAND
        assert message.message_id == message_id
        assert message.timestamp == timestamp
        assert message.source == "client_service"
        assert message.destination == "server_service"
        assert message.correlation_id == "corr-456"
        assert message.reply_to == "client.response"
        assert message.priority == MessagePriority.HIGH
        assert message.ttl == 300.0
        assert message.headers == sample_headers
    
    def test_message_validation_empty_topic(self, sample_message_data) -> None:
        """Test validation fails for empty topic."""
        with pytest.raises(ValueError, match="Topic cannot be empty"):
            Message(topic="", data=sample_message_data)
    
    def test_message_validation_invalid_message_type(self, sample_message_data) -> None:
        """Test validation fails for invalid message type."""
        with pytest.raises(ValueError, match="Message type must be a MessageType enum value"):
            message = Message.__new__(Message)
            message.topic = "test.topic"
            message.data = sample_message_data
            message.message_type = "invalid"  # type: ignore
            message.priority = MessagePriority.NORMAL
            message.ttl = None
            message.__post_init__()
    
    def test_message_validation_invalid_priority(self, sample_message_data) -> None:
        """Test validation fails for invalid priority."""
        with pytest.raises(ValueError, match="Priority must be a MessagePriority enum value"):
            message = Message.__new__(Message)
            message.topic = "test.topic"
            message.data = sample_message_data
            message.message_type = MessageType.DATA
            message.priority = "invalid"  # type: ignore
            message.ttl = None
            message.__post_init__()
    
    def test_message_validation_negative_ttl(self, sample_message_data) -> None:
        """Test validation fails for negative TTL."""
        with pytest.raises(ValueError, match="TTL must be positive"):
            Message(
                topic="test.topic",
                data=sample_message_data,
                ttl=-1.0
            )
    
    def test_message_validation_zero_ttl(self, sample_message_data) -> None:
        """Test validation fails for zero TTL."""
        with pytest.raises(ValueError, match="TTL must be positive"):
            Message(
                topic="test.topic",
                data=sample_message_data,
                ttl=0.0
            )
    
    def test_message_default_timestamp(self, sample_message_data) -> None:
        """Test that default timestamp is current time."""
        before = time.time()
        message = Message(topic="test.topic", data=sample_message_data)
        after = time.time()
        
        assert before <= message.timestamp <= after
    
    def test_message_default_message_id_unique(self, sample_message_data) -> None:
        """Test that default message IDs are unique."""
        message1 = Message(topic="test.topic1", data=sample_message_data)
        message2 = Message(topic="test.topic2", data=sample_message_data)
        
        assert message1.message_id != message2.message_id
        assert len(message1.message_id) > 0
        assert len(message2.message_id) > 0
    
    def test_is_expired_no_ttl(self, sample_message_data) -> None:
        """Test is_expired returns False when no TTL is set."""
        message = Message(topic="test.topic", data=sample_message_data)
        assert message.is_expired is False
    
    def test_is_expired_not_expired(self, sample_message_data) -> None:
        """Test is_expired returns False when message is not expired."""
        message = Message(
            topic="test.topic",
            data=sample_message_data,
            ttl=300.0,  # 5 minutes
            timestamp=time.time()  # Current time
        )
        assert message.is_expired is False
    
    def test_is_expired_expired(self, sample_message_data) -> None:
        """Test is_expired returns True when message is expired."""
        message = Message(
            topic="test.topic",
            data=sample_message_data,
            ttl=1.0,  # 1 second
            timestamp=time.time() - 2.0  # 2 seconds ago
        )
        assert message.is_expired is True
    
    def test_with_headers(self, sample_message_data) -> None:
        """Test adding headers to message."""
        original_message = Message(
            topic="test.topic",
            data=sample_message_data,
            headers={"original": "value"}
        )
        
        new_message = original_message.with_headers(
            additional="data",
            version="2.0"
        )
        
        # Original message unchanged
        assert original_message.headers == {"original": "value"}
        
        # New message has merged headers
        expected_headers = {
            "original": "value",
            "additional": "data",
            "version": "2.0"
        }
        assert new_message.headers == expected_headers
        
        # Other fields unchanged
        assert new_message.topic == original_message.topic
        assert new_message.message_id == original_message.message_id
        assert new_message.timestamp == original_message.timestamp
        assert new_message.data == original_message.data
    
    def test_with_headers_override(self, sample_message_data) -> None:
        """Test that new headers override existing keys."""
        original_message = Message(
            topic="test.topic",
            data=sample_message_data,
            headers={"key": "original", "other": "value"}
        )
        
        new_message = original_message.with_headers(key="updated")
        
        assert new_message.headers == {"key": "updated", "other": "value"}
    
    def test_with_correlation_id(self, sample_message_data) -> None:
        """Test setting correlation ID."""
        original_message = Message(topic="test.topic", data=sample_message_data)
        new_message = original_message.with_correlation_id("corr-789")
        
        # Original message unchanged
        assert original_message.correlation_id is None
        
        # New message has correlation ID
        assert new_message.correlation_id == "corr-789"
        
        # Other fields unchanged
        assert new_message.topic == original_message.topic
        assert new_message.message_id == original_message.message_id
        assert new_message.timestamp == original_message.timestamp
        assert new_message.data == original_message.data
        assert new_message.headers == original_message.headers
    
    def test_create_reply_success(self, sample_message_data) -> None:
        """Test creating reply message."""
        original_message = Message(
            topic="request.topic",
            data=sample_message_data,
            source="client",
            destination="server",
            reply_to="response.topic",
            correlation_id="corr-123"
        )
        
        reply_data = {"status": "success", "result": "processed"}
        reply_message = original_message.create_reply(reply_data)
        
        assert reply_message.topic == "response.topic"
        assert reply_message.data == reply_data
        assert reply_message.message_type == MessageType.RESPONSE
        assert reply_message.source == "server"  # Swapped
        assert reply_message.destination == "client"  # Swapped
        assert reply_message.correlation_id == "corr-123"  # Preserved
        assert reply_message.priority == original_message.priority
        assert reply_message.headers["in_reply_to"] == original_message.message_id
    
    def test_create_reply_custom_type(self, sample_message_data) -> None:
        """Test creating reply message with custom type."""
        original_message = Message(
            topic="request.topic",
            data=sample_message_data,
            reply_to="error.topic"
        )
        
        error_data = {"error": "Something went wrong"}
        reply_message = original_message.create_reply(
            error_data, 
            MessageType.ERROR
        )
        
        assert reply_message.message_type == MessageType.ERROR
        assert reply_message.data == error_data
    
    def test_create_reply_no_reply_to(self, sample_message_data) -> None:
        """Test create_reply fails when no reply_to is set."""
        message = Message(topic="test.topic", data=sample_message_data)
        
        with pytest.raises(ValueError, match="Cannot create reply: no reply_to topic specified"):
            message.create_reply({"response": "data"})

    def test_to_dict(self, sample_message_data, sample_headers) -> None:
        """Test converting message to dictionary."""
        message = Message(
            topic="user.action",
            data=sample_message_data,
            message_type=MessageType.COMMAND,
            message_id="msg-123",
            timestamp=1234567890.0,
            source="client_service",
            destination="server_service",
            correlation_id="corr-456",
            reply_to="client.response",
            priority=MessagePriority.HIGH,
            ttl=300.0,
            headers=sample_headers
        )

        result = message.to_dict()

        expected = {
            'topic': 'user.action',
            'data': sample_message_data,
            'message_type': 'COMMAND',
            'message_id': 'msg-123',
            'timestamp': 1234567890.0,
            'source': 'client_service',
            'destination': 'server_service',
            'correlation_id': 'corr-456',
            'reply_to': 'client.response',
            'priority': 'HIGH',
            'ttl': 300.0,
            'headers': sample_headers
        }

        assert result == expected

    def test_to_dict_minimal(self, sample_message_data) -> None:
        """Test converting minimal message to dictionary."""
        message = Message(topic="simple.topic", data=sample_message_data)
        result = message.to_dict()

        assert result['topic'] == 'simple.topic'
        assert result['data'] == sample_message_data
        assert result['message_type'] == 'DATA'
        assert result['source'] is None
        assert result['destination'] is None
        assert result['correlation_id'] is None
        assert result['reply_to'] is None
        assert result['priority'] == 'NORMAL'
        assert result['ttl'] is None
        assert isinstance(result['timestamp'], float)
        assert isinstance(result['message_id'], str)
        assert result['headers'] == {}

    def test_from_dict_full(self, sample_message_data, sample_headers) -> None:
        """Test creating message from dictionary."""
        data = {
            'topic': 'user.action',
            'data': sample_message_data,
            'message_type': 'COMMAND',
            'message_id': 'msg-123',
            'timestamp': 1234567890.0,
            'source': 'client_service',
            'destination': 'server_service',
            'correlation_id': 'corr-456',
            'reply_to': 'client.response',
            'priority': 'HIGH',
            'ttl': 300.0,
            'headers': sample_headers
        }

        message = Message.from_dict(data)

        assert message.topic == 'user.action'
        assert message.data == sample_message_data
        assert message.message_type == MessageType.COMMAND
        assert message.message_id == 'msg-123'
        assert message.timestamp == 1234567890.0
        assert message.source == 'client_service'
        assert message.destination == 'server_service'
        assert message.correlation_id == 'corr-456'
        assert message.reply_to == 'client.response'
        assert message.priority == MessagePriority.HIGH
        assert message.ttl == 300.0
        assert message.headers == sample_headers

    def test_from_dict_minimal(self, sample_message_data) -> None:
        """Test creating message from minimal dictionary."""
        data = {'topic': 'simple.topic', 'data': sample_message_data}
        message = Message.from_dict(data)

        assert message.topic == 'simple.topic'
        assert message.data == sample_message_data
        assert message.message_type == MessageType.DATA
        assert message.source is None
        assert message.destination is None
        assert message.correlation_id is None
        assert message.reply_to is None
        assert message.priority == MessagePriority.NORMAL
        assert message.ttl is None
        assert isinstance(message.timestamp, float)
        assert isinstance(message.message_id, str)
        assert message.headers == {}

    def test_from_dict_defaults(self) -> None:
        """Test from_dict uses correct defaults."""
        data = {'topic': 'test.topic'}
        message = Message.from_dict(data)

        assert message.message_type == MessageType.DATA
        assert message.priority == MessagePriority.NORMAL

    def test_from_dict_invalid_message_type(self) -> None:
        """Test from_dict handles invalid message type."""
        data = {'topic': 'test.topic', 'message_type': 'INVALID'}

        with pytest.raises(KeyError):
            Message.from_dict(data)

    def test_from_dict_invalid_priority(self) -> None:
        """Test from_dict handles invalid priority."""
        data = {'topic': 'test.topic', 'priority': 'INVALID'}

        with pytest.raises(KeyError):
            Message.from_dict(data)

    def test_roundtrip_serialization(self, sample_message_data, sample_headers) -> None:
        """Test that to_dict/from_dict roundtrip works correctly."""
        original_message = Message(
            topic="roundtrip.test",
            data=sample_message_data,
            message_type=MessageType.EVENT,
            source="test_source",
            destination="test_destination",
            correlation_id="corr-roundtrip",
            reply_to="reply.topic",
            priority=MessagePriority.CRITICAL,
            ttl=600.0,
            headers=sample_headers
        )

        # Convert to dict and back
        message_dict = original_message.to_dict()
        restored_message = Message.from_dict(message_dict)

        # Should be equal (except potentially timestamp precision)
        assert restored_message.topic == original_message.topic
        assert restored_message.data == original_message.data
        assert restored_message.message_type == original_message.message_type
        assert restored_message.message_id == original_message.message_id
        assert restored_message.source == original_message.source
        assert restored_message.destination == original_message.destination
        assert restored_message.correlation_id == original_message.correlation_id
        assert restored_message.reply_to == original_message.reply_to
        assert restored_message.priority == original_message.priority
        assert restored_message.ttl == original_message.ttl
        assert restored_message.headers == original_message.headers
        assert abs(restored_message.timestamp - original_message.timestamp) < 0.001
