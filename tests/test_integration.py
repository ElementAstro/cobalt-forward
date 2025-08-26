"""
Integration tests for the complete application architecture.

This module tests the integration between different components
and validates the overall system behavior.
"""

import asyncio
import pytest
from typing import Any, Dict, List, AsyncGenerator

# Use app package instead of cobalt_forward for now
from app.core.event_bus import EventBus, Event
from app.core.message_bus import MessageBus, Message, MessageType
from app.core.command_dispatcher import CommandDispatcher
from app.core.shared_services import SharedServices
from app.config.config_manager import ConfigManager

# Mock interfaces for testing
class IEventBus:
    pass

class IMessageBus:
    pass

class ICommandDispatcher:
    pass

# Mock container for testing
class Container:
    def __init__(self) -> None:
        self._services: Dict[Any, Any] = {}

    def register(self, interface: Any, implementation: Any) -> None:
        self._services[interface] = implementation

    def resolve(self, interface: Any) -> Any:
        return self._services[interface]()

# Mock startup for testing
class ApplicationStartup:
    def __init__(self, container: Container) -> None:
        self.container = container

    async def configure_services(self, config: Any) -> None:
        pass

# Mock config
class ApplicationConfig:
    def to_dict(self) -> Dict[str, Any]:
        return {}


class TestApplicationIntegration:
    """Integration tests for the complete application."""
    
    @pytest.fixture
    async def application_setup(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Set up a complete application for testing."""
        # Create container
        container = Container()
        
        # Create minimal configuration
        config = ApplicationConfig()
        
        # Register core services
        container.register(IEventBus, EventBus)
        container.register(IMessageBus, MessageBus)
        container.register(ICommandDispatcher, CommandDispatcher)
        
        # Create startup manager
        startup = ApplicationStartup(container)
        
        # Configure services (simplified for testing)
        await startup.configure_services(config.to_dict())
        
        # Start core components
        event_bus = container.resolve(IEventBus)
        message_bus = container.resolve(IMessageBus)
        command_dispatcher = container.resolve(ICommandDispatcher)
        
        await event_bus.start()
        await message_bus.start()
        await command_dispatcher.start()
        
        yield {
            'container': container,
            'startup': startup,
            'config': config,
            'event_bus': event_bus,
            'message_bus': message_bus,
            'command_dispatcher': command_dispatcher
        }
        
        # Cleanup
        await event_bus.stop()
        await message_bus.stop()
        await command_dispatcher.stop()
    
    @pytest.mark.asyncio
    async def test_event_bus_integration(self, application_setup: Dict[str, Any]) -> None:
        """Test event bus integration with the application."""
        event_bus = application_setup['event_bus']
        received_events: List[Event] = []

        async def event_handler(event: Event) -> None:
            received_events.append(event)
        
        # Subscribe to events
        await event_bus.subscribe("integration.test", event_handler)
        
        # Publish event
        await event_bus.publish("integration.test", {"test": "data"})
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify event was processed
        assert len(received_events) == 1
        assert received_events[0].name == "integration.test"
        assert received_events[0].data == {"test": "data"}
    
    @pytest.mark.asyncio
    async def test_message_bus_integration(self, application_setup: Dict[str, Any]) -> None:
        """Test message bus integration with the application."""
        message_bus = application_setup['message_bus']
        received_messages: List[Message[Dict[str, str]]] = []
        
        async def message_handler(message: Message[Dict[str, str]]) -> None:
            received_messages.append(message)
        
        # Subscribe to messages
        await message_bus.subscribe("integration.topic", message_handler)
        
        # Send message
        test_message = Message(
            topic="integration.topic",
            data={"test": "message"},
            message_type=MessageType.DATA
        )
        
        await message_bus.send(test_message)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify message was processed
        assert len(received_messages) == 1
        assert received_messages[0].topic == "integration.topic"
        assert received_messages[0].data == {"test": "message"}
    
    @pytest.mark.asyncio
    async def test_container_service_resolution(self, application_setup: Dict[str, Any]) -> None:
        """Test that all services can be resolved from the container."""
        container = application_setup['container']
        
        # Resolve all core services
        event_bus = container.resolve(IEventBus)
        message_bus = container.resolve(IMessageBus)
        command_dispatcher = container.resolve(ICommandDispatcher)
        
        # Verify services are properly instantiated
        assert event_bus is not None
        assert message_bus is not None
        assert command_dispatcher is not None
        
        # Verify services are singletons
        event_bus2 = container.resolve(IEventBus)
        assert event_bus is event_bus2
    
    @pytest.mark.asyncio
    async def test_component_health_checks(self, application_setup: Dict[str, Any]) -> None:
        """Test health checks for all components."""
        event_bus = application_setup['event_bus']
        message_bus = application_setup['message_bus']
        command_dispatcher = application_setup['command_dispatcher']
        
        # Check health of all components
        event_health = await event_bus.check_health()
        message_health = await message_bus.check_health()
        command_health = await command_dispatcher.check_health()
        
        # Verify all components are healthy
        assert event_health['healthy'] is True
        assert event_health['status'] == 'running'
        
        assert message_health['healthy'] is True
        assert message_health['status'] == 'running'
        
        assert command_health['healthy'] is True
        assert command_health['status'] == 'running'
    
    @pytest.mark.asyncio
    async def test_cross_component_communication(self, application_setup: Dict[str, Any]) -> None:
        """Test communication between different components."""
        event_bus = application_setup['event_bus']
        message_bus = application_setup['message_bus']
        
        # Set up cross-component communication
        events_received: List[Event] = []
        messages_received: List[Message[Dict[str, Any]]] = []

        async def event_to_message_handler(event: Event) -> None:
            # Convert event to message
            message = Message(
                topic="converted.message",
                data=event.data,
                message_type=MessageType.EVENT,
                correlation_id=event.correlation_id
            )
            await message_bus.send(message)
        
        async def message_handler(message: Message[Dict[str, Any]]) -> None:
            messages_received.append(message)
        
        # Set up subscriptions
        await event_bus.subscribe("cross.component", event_to_message_handler)
        await message_bus.subscribe("converted.message", message_handler)
        
        # Publish event
        await event_bus.publish("cross.component", {"cross": "component", "test": True})
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Verify cross-component communication worked
        assert len(messages_received) == 1
        assert messages_received[0].data == {"cross": "component", "test": True}
        assert messages_received[0].message_type == MessageType.EVENT
    
    @pytest.mark.asyncio
    async def test_application_lifecycle(self, application_setup: Dict[str, Any]) -> None:
        """Test complete application lifecycle."""
        startup = application_setup['startup']
        
        # Application should already be started by fixture
        # Test that we can stop and restart
        await startup.stop_application()
        
        # Verify components are stopped
        event_bus = application_setup['event_bus']
        health = await event_bus.check_health()
        assert health['status'] == 'stopped'
        
        # Restart application
        await startup.start_application()
        
        # Verify components are running again
        health = await event_bus.check_health()
        assert health['status'] == 'running'


class TestConfigurationIntegration:
    """Integration tests for configuration management."""
    
    def test_configuration_model_validation(self) -> None:
        """Test configuration model validation."""
        # Valid configuration
        config = ApplicationConfig(
            name="Test App",
            version="1.0.0",
            debug=True
        )
        
        assert config.name == "Test App"
        assert config.version == "1.0.0"
        assert config.debug is True
        
        # Test nested configuration
        assert config.tcp.host == "localhost"
        assert config.tcp.port == 8080
        assert config.websocket.port == 8000
    
    def test_configuration_to_dict_conversion(self):
        """Test configuration serialization."""
        config = ApplicationConfig(name="Test", debug=True)
        config_dict = config.to_dict()
        
        assert isinstance(config_dict, dict)
        assert config_dict['name'] == "Test"
        assert config_dict['debug'] is True
        assert 'tcp' in config_dict
        assert 'websocket' in config_dict
    
    def test_configuration_from_dict_creation(self):
        """Test configuration deserialization."""
        config_data = {
            'name': 'Test App',
            'debug': True,
            'tcp': {'host': 'example.com', 'port': 9000},
            'websocket': {'port': 8080}
        }
        
        config = ApplicationConfig.from_dict(config_data)
        
        assert config.name == 'Test App'
        assert config.debug is True
        assert config.tcp.host == 'example.com'
        assert config.tcp.port == 9000
        assert config.websocket.port == 8080
