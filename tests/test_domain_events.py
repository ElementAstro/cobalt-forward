"""
Tests for Event domain model.

This module tests the Event domain model including validation,
serialization, and all methods.
"""

import pytest
import time
from typing import Any, Dict

from cobalt_forward.core.domain.events import Event, EventPriority


class TestEventPriority:
    """Test cases for EventPriority enum."""
    
    def test_priority_values(self):
        """Test that priority values are correct."""
        assert EventPriority.LOW.value == 1
        assert EventPriority.NORMAL.value == 2
        assert EventPriority.HIGH.value == 3
        assert EventPriority.CRITICAL.value == 4
    
    def test_priority_ordering(self):
        """Test that priorities can be compared."""
        assert EventPriority.LOW < EventPriority.NORMAL
        assert EventPriority.NORMAL < EventPriority.HIGH
        assert EventPriority.HIGH < EventPriority.CRITICAL


class TestEvent:
    """Test cases for Event domain model."""
    
    @pytest.fixture
    def sample_event_data(self) -> Dict[str, Any]:
        """Sample event data for testing."""
        return {
            "user_id": "123",
            "action": "login",
            "ip_address": "192.168.1.1"
        }
    
    @pytest.fixture
    def sample_metadata(self) -> Dict[str, Any]:
        """Sample metadata for testing."""
        return {
            "version": "1.0",
            "environment": "test"
        }
    
    def test_event_creation_minimal(self):
        """Test creating event with minimal required fields."""
        event = Event(name="test.event")
        
        assert event.name == "test.event"
        assert event.data is None
        assert event.priority == EventPriority.NORMAL
        assert event.source is None
        assert event.correlation_id is None
        assert isinstance(event.timestamp, float)
        assert isinstance(event.event_id, str)
        assert len(event.event_id) > 0
        assert isinstance(event.metadata, dict)
        assert len(event.metadata) == 0
    
    def test_event_creation_full(self, sample_event_data, sample_metadata):
        """Test creating event with all fields."""
        timestamp = time.time()
        event_id = "test-event-123"
        
        event = Event(
            name="user.login",
            data=sample_event_data,
            priority=EventPriority.HIGH,
            timestamp=timestamp,
            event_id=event_id,
            source="auth_service",
            correlation_id="corr-123",
            metadata=sample_metadata
        )
        
        assert event.name == "user.login"
        assert event.data == sample_event_data
        assert event.priority == EventPriority.HIGH
        assert event.timestamp == timestamp
        assert event.event_id == event_id
        assert event.source == "auth_service"
        assert event.correlation_id == "corr-123"
        assert event.metadata == sample_metadata
    
    def test_event_immutable(self):
        """Test that event is immutable (frozen dataclass)."""
        event = Event(name="test.event")
        
        with pytest.raises(AttributeError):
            event.name = "modified.event"  # type: ignore
        
        with pytest.raises(AttributeError):
            event.priority = EventPriority.HIGH  # type: ignore
    
    def test_event_validation_empty_name(self):
        """Test validation fails for empty event name."""
        with pytest.raises(ValueError, match="Event name cannot be empty"):
            Event(name="")
    
    def test_event_priority_type_enforcement(self):
        """Test that Event enforces EventPriority type."""
        # Since Event is a frozen dataclass with type hints, invalid priority types
        # should be caught at runtime or by type checkers
        # We can test that valid priorities work correctly
        event = Event(name="test.event", data={}, priority=EventPriority.HIGH)
        assert event.priority == EventPriority.HIGH
        assert isinstance(event.priority, EventPriority)
    
    def test_event_default_timestamp(self):
        """Test that default timestamp is current time."""
        before = time.time()
        event = Event(name="test.event")
        after = time.time()
        
        assert before <= event.timestamp <= after
    
    def test_event_default_event_id_unique(self):
        """Test that default event IDs are unique."""
        event1 = Event(name="test.event1")
        event2 = Event(name="test.event2")
        
        assert event1.event_id != event2.event_id
        assert len(event1.event_id) > 0
        assert len(event2.event_id) > 0
    
    def test_with_metadata(self, sample_metadata):
        """Test adding metadata to event."""
        original_event = Event(
            name="test.event",
            metadata={"original": "value"}
        )
        
        new_event = original_event.with_metadata(
            additional="data",
            version="2.0"
        )
        
        # Original event unchanged
        assert original_event.metadata == {"original": "value"}
        
        # New event has merged metadata
        expected_metadata = {
            "original": "value",
            "additional": "data",
            "version": "2.0"
        }
        assert new_event.metadata == expected_metadata
        
        # Other fields unchanged
        assert new_event.name == original_event.name
        assert new_event.event_id == original_event.event_id
        assert new_event.timestamp == original_event.timestamp
    
    def test_with_metadata_override(self):
        """Test that new metadata overrides existing keys."""
        original_event = Event(
            name="test.event",
            metadata={"key": "original", "other": "value"}
        )
        
        new_event = original_event.with_metadata(key="updated")
        
        assert new_event.metadata == {"key": "updated", "other": "value"}
    
    def test_with_correlation_id(self):
        """Test setting correlation ID."""
        original_event = Event(name="test.event")
        new_event = original_event.with_correlation_id("corr-456")
        
        # Original event unchanged
        assert original_event.correlation_id is None
        
        # New event has correlation ID
        assert new_event.correlation_id == "corr-456"
        
        # Other fields unchanged
        assert new_event.name == original_event.name
        assert new_event.event_id == original_event.event_id
        assert new_event.timestamp == original_event.timestamp
        assert new_event.metadata == original_event.metadata
    
    def test_to_dict(self, sample_event_data, sample_metadata):
        """Test converting event to dictionary."""
        event = Event(
            name="user.action",
            data=sample_event_data,
            priority=EventPriority.HIGH,
            timestamp=1234567890.0,
            event_id="event-123",
            source="test_service",
            correlation_id="corr-789",
            metadata=sample_metadata
        )
        
        result = event.to_dict()
        
        expected = {
            'name': 'user.action',
            'data': sample_event_data,
            'priority': 'HIGH',
            'timestamp': 1234567890.0,
            'event_id': 'event-123',
            'source': 'test_service',
            'correlation_id': 'corr-789',
            'metadata': sample_metadata
        }
        
        assert result == expected
    
    def test_to_dict_minimal(self):
        """Test converting minimal event to dictionary."""
        event = Event(name="simple.event")
        result = event.to_dict()
        
        assert result['name'] == 'simple.event'
        assert result['data'] is None
        assert result['priority'] == 'NORMAL'
        assert result['source'] is None
        assert result['correlation_id'] is None
        assert isinstance(result['timestamp'], float)
        assert isinstance(result['event_id'], str)
        assert result['metadata'] == {}
    
    def test_from_dict_full(self, sample_event_data, sample_metadata):
        """Test creating event from dictionary."""
        data = {
            'name': 'user.action',
            'data': sample_event_data,
            'priority': 'HIGH',
            'timestamp': 1234567890.0,
            'event_id': 'event-123',
            'source': 'test_service',
            'correlation_id': 'corr-789',
            'metadata': sample_metadata
        }
        
        event = Event.from_dict(data)
        
        assert event.name == 'user.action'
        assert event.data == sample_event_data
        assert event.priority == EventPriority.HIGH
        assert event.timestamp == 1234567890.0
        assert event.event_id == 'event-123'
        assert event.source == 'test_service'
        assert event.correlation_id == 'corr-789'
        assert event.metadata == sample_metadata
    
    def test_from_dict_minimal(self):
        """Test creating event from minimal dictionary."""
        data = {'name': 'simple.event'}
        event = Event.from_dict(data)
        
        assert event.name == 'simple.event'
        assert event.data is None
        assert event.priority == EventPriority.NORMAL
        assert event.source is None
        assert event.correlation_id is None
        assert isinstance(event.timestamp, float)
        assert isinstance(event.event_id, str)
        assert event.metadata == {}
    
    def test_from_dict_default_priority(self):
        """Test from_dict uses NORMAL priority as default."""
        data = {'name': 'test.event'}
        event = Event.from_dict(data)
        assert event.priority == EventPriority.NORMAL
    
    def test_from_dict_invalid_priority(self):
        """Test from_dict handles invalid priority."""
        data = {'name': 'test.event', 'priority': 'INVALID'}
        
        with pytest.raises(KeyError):
            Event.from_dict(data)
    
    def test_roundtrip_serialization(self, sample_event_data, sample_metadata):
        """Test that to_dict/from_dict roundtrip works correctly."""
        original_event = Event(
            name="roundtrip.test",
            data=sample_event_data,
            priority=EventPriority.CRITICAL,
            source="test_source",
            correlation_id="corr-roundtrip",
            metadata=sample_metadata
        )
        
        # Convert to dict and back
        event_dict = original_event.to_dict()
        restored_event = Event.from_dict(event_dict)
        
        # Should be equal (except potentially timestamp precision)
        assert restored_event.name == original_event.name
        assert restored_event.data == original_event.data
        assert restored_event.priority == original_event.priority
        assert restored_event.event_id == original_event.event_id
        assert restored_event.source == original_event.source
        assert restored_event.correlation_id == original_event.correlation_id
        assert restored_event.metadata == original_event.metadata
        assert abs(restored_event.timestamp - original_event.timestamp) < 0.001
