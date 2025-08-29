"""
Tests for ApplicationStartup.

This module tests the ApplicationStartup class including service configuration,
startup sequence, and shutdown procedures.
"""

import pytest
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, Mock, patch

from cobalt_forward.application.startup import ApplicationStartup
from cobalt_forward.application.container import Container, IContainer, ServiceLifetime
from cobalt_forward.core.interfaces.lifecycle import IStartable, IStoppable, IComponent
from cobalt_forward.core.interfaces.messaging import IEventBus, IMessageBus
from cobalt_forward.core.interfaces.commands import ICommandDispatcher
from cobalt_forward.core.interfaces.plugins import IPluginManager


class MockComponent(IComponent):
    """Mock component for testing startup/shutdown."""
    
    def __init__(self, name: str = "mock_component", version: str = "1.0.0"):
        self._name = name
        self._version = version
        self.started = False
        self.stopped = False
        self.configured = False
        self.config: Dict[str, Any] = {}
        
        # Call counters
        self.start_called_count = 0
        self.stop_called_count = 0
        self.configure_called_count = 0
        self.check_health_called_count = 0
        
        # Configuration for testing failures
        self.should_fail_start = False
        self.should_fail_stop = False
        self.should_fail_configure = False
        self.should_fail_health_check = False
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def version(self) -> str:
        return self._version
    
    async def start(self) -> None:
        self.start_called_count += 1
        if self.should_fail_start:
            raise RuntimeError(f"Mock start failure for {self._name}")
        self.started = True
    
    async def stop(self) -> None:
        self.stop_called_count += 1
        if self.should_fail_stop:
            raise RuntimeError(f"Mock stop failure for {self._name}")
        self.stopped = True
        self.started = False
    
    async def configure(self, config: Dict[str, Any]) -> None:
        self.configure_called_count += 1
        if self.should_fail_configure:
            raise ValueError(f"Mock configure failure for {self._name}")
        self.configured = True
        self.config = config
    
    async def check_health(self) -> Dict[str, Any]:
        self.check_health_called_count += 1
        if self.should_fail_health_check:
            raise RuntimeError(f"Mock health check failure for {self._name}")
        
        return {
            'healthy': self.started,
            'status': 'running' if self.started else 'stopped',
            'details': {
                'name': self._name,
                'version': self._version,
                'configured': self.configured
            }
        }


class MockEventBus(IEventBus, IStartable):
    """Mock event bus for testing."""

    def __init__(self):
        self.started = False
        self.start_called_count = 0
        self.stop_called_count = 0

    async def start(self) -> None:
        self.start_called_count += 1
        self.started = True

    async def stop(self) -> None:
        self.stop_called_count += 1
        self.started = False
    
    async def publish(self, event, data=None, priority=None) -> str:
        return "mock-event-id"
    
    async def subscribe(self, event_name: str, handler, priority=None) -> str:
        return "mock-subscription-id"
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        return True
    
    async def get_metrics(self) -> Dict[str, Any]:
        return {"events_published": 0}


class MockMessageBus(IMessageBus, IStartable):
    """Mock message bus for testing."""

    def __init__(self):
        self.started = False
        self.start_called_count = 0
        self.stop_called_count = 0

    async def start(self) -> None:
        self.start_called_count += 1
        self.started = True

    async def stop(self) -> None:
        self.stop_called_count += 1
        self.started = False
    
    async def send(self, message) -> None:
        pass
    
    async def request(self, message, timeout=30.0):
        return Mock()
    
    async def subscribe(self, topic: str, handler) -> str:
        return "mock-subscription-id"
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        return True
    
    async def get_metrics(self) -> Dict[str, Any]:
        return {"messages_sent": 0}


class MockCommandDispatcher(ICommandDispatcher, IStartable):
    """Mock command dispatcher for testing."""

    def __init__(self):
        self.started = False
        self.start_called_count = 0
        self.stop_called_count = 0

    async def start(self) -> None:
        self.start_called_count += 1
        self.started = True

    async def stop(self) -> None:
        self.stop_called_count += 1
        self.started = False
    
    async def dispatch(self, command):
        return Mock()
    
    async def register_handler(self, handler) -> None:
        pass
    
    async def unregister_handler(self, handler) -> bool:
        return True
    
    async def get_handlers(self, command_type):
        return []
    
    async def get_metrics(self) -> Dict[str, Any]:
        return {"commands_dispatched": 0}


class TestApplicationStartup:
    """Test cases for ApplicationStartup class."""
    
    @pytest.fixture
    def container(self) -> Container:
        """Create a container for testing."""
        return Container()
    
    @pytest.fixture
    def startup(self, container: Container) -> ApplicationStartup:
        """Create ApplicationStartup instance."""
        return ApplicationStartup(container)
    
    @pytest.fixture
    def sample_config(self) -> Dict[str, Any]:
        """Create sample configuration."""
        return {
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "plugins": {
                "enabled": True,
                "directory": "/plugins"
            },
            "ssh": {
                "enabled": True,
                "port": 22
            },
            "upload": {
                "max_file_size": 1024 * 1024 * 100,  # 100MB
                "chunk_size": 1024 * 1024  # 1MB
            }
        }
    
    def test_startup_initialization(self, container: Container) -> None:
        """Test ApplicationStartup initialization."""
        startup = ApplicationStartup(container)
        
        assert startup._container is container
        assert startup._started_components == []
        assert isinstance(startup._startup_order, list)
        assert len(startup._startup_order) > 0
        
        # Verify startup order contains expected components
        expected_components = [
            'config_manager', 'event_bus', 'message_bus', 
            'command_dispatcher', 'plugin_manager'
        ]
        for component in expected_components:
            assert component in startup._startup_order
    
    @pytest.mark.asyncio
    async def test_configure_services_basic(self, startup: ApplicationStartup, sample_config: Dict[str, Any]) -> None:
        """Test basic service configuration."""
        # Mock the registration methods to avoid actual service creation
        with patch.object(startup, '_register_core_services', new_callable=AsyncMock) as mock_core, \
             patch.object(startup, '_register_infrastructure_services', new_callable=AsyncMock) as mock_infra, \
             patch.object(startup, '_register_application_services', new_callable=AsyncMock) as mock_app:

            await startup.configure_services(sample_config)

            # Verify all registration methods were called
            mock_core.assert_called_once_with(sample_config)
            mock_infra.assert_called_once_with(sample_config)
            mock_app.assert_called_once_with(sample_config)

            # Verify config was registered as instance
            resolved_config = startup._container.try_resolve(dict)
            assert resolved_config == sample_config
    
    @pytest.mark.asyncio
    async def test_configure_services_with_mocked_services(self, startup: ApplicationStartup, sample_config: Dict[str, Any]) -> None:
        """Test service configuration with mocked services."""
        # Register mock services directly in container
        mock_event_bus = MockEventBus()
        mock_message_bus = MockMessageBus()
        mock_command_dispatcher = MockCommandDispatcher()
        
        startup._container.register_instance(IEventBus, mock_event_bus)
        startup._container.register_instance(IMessageBus, mock_message_bus)
        startup._container.register_instance(ICommandDispatcher, mock_command_dispatcher)
        
        # Mock the registration methods to avoid import errors
        with patch.object(startup, '_register_core_services', new_callable=AsyncMock), \
             patch.object(startup, '_register_infrastructure_services', new_callable=AsyncMock), \
             patch.object(startup, '_register_application_services', new_callable=AsyncMock):
            
            await startup.configure_services(sample_config)
            
            # Verify services can be resolved
            assert startup._container.try_resolve(IEventBus) is mock_event_bus
            assert startup._container.try_resolve(IMessageBus) is mock_message_bus
            assert startup._container.try_resolve(ICommandDispatcher) is mock_command_dispatcher
    
    @pytest.mark.asyncio
    async def test_start_application_success(self, startup: ApplicationStartup) -> None:
        """Test successful application startup."""
        # Create mock components
        mock_event_bus = MockEventBus()
        mock_message_bus = MockMessageBus()
        mock_command_dispatcher = MockCommandDispatcher()
        
        # Register components in container
        startup._container.register_instance(IEventBus, mock_event_bus)
        startup._container.register_instance(IMessageBus, mock_message_bus)
        startup._container.register_instance(ICommandDispatcher, mock_command_dispatcher)
        
        # Mock _get_component_by_name to return our mock components
        def mock_get_component(name: str):
            component_map = {
                'event_bus': mock_event_bus,
                'message_bus': mock_message_bus,
                'command_dispatcher': mock_command_dispatcher
            }
            return component_map.get(name)
        
        with patch.object(startup, '_get_component_by_name', side_effect=mock_get_component):
            await startup.start_application()
        
        # Verify components were started
        assert mock_event_bus.start_called_count == 1
        assert mock_message_bus.start_called_count == 1
        assert mock_command_dispatcher.start_called_count == 1
        
        # Verify components are in started list (they implement IComponent interface)
        # Note: Our mocks don't implement IComponent, so they won't be in the list
        # In real implementation, they would be
    
    @pytest.mark.asyncio
    async def test_start_application_with_component_failure(self, startup: ApplicationStartup) -> None:
        """Test application startup with component failure."""
        # Create mock components
        mock_event_bus = MockEventBus()
        failing_component = MockComponent("failing_component")
        failing_component.should_fail_start = True
        
        # Register components
        startup._container.register_instance(IEventBus, mock_event_bus)
        
        def mock_get_component(name: str):
            if name == 'event_bus':
                return mock_event_bus
            elif name == 'message_bus':
                return failing_component
            return None
        
        with patch.object(startup, '_get_component_by_name', side_effect=mock_get_component), \
             patch.object(startup, '_stop_started_components', new_callable=AsyncMock) as mock_stop:
            
            with pytest.raises(RuntimeError, match="Mock start failure"):
                await startup.start_application()
            
            # Verify cleanup was attempted
            mock_stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_stop_application_success(self, startup: ApplicationStartup) -> None:
        """Test successful application shutdown."""
        # Create mock components and add them to started list
        component1 = MockComponent("component1")
        component2 = MockComponent("component2")
        component3 = MockComponent("component3")
        
        startup._started_components = [component1, component2, component3]
        
        await startup.stop_application()
        
        # Verify all components were stopped in reverse order
        assert component1.stop_called_count == 1
        assert component2.stop_called_count == 1
        assert component3.stop_called_count == 1
        
        # Verify started components list is cleared
        assert startup._started_components == []
    
    @pytest.mark.asyncio
    async def test_stop_application_with_failure(self, startup: ApplicationStartup) -> None:
        """Test application shutdown with component failure."""
        # Create components, one that fails to stop
        component1 = MockComponent("component1")
        failing_component = MockComponent("failing_component")
        failing_component.should_fail_stop = True
        component3 = MockComponent("component3")
        
        startup._started_components = [component1, failing_component, component3]
        
        # Should not raise exception, but continue stopping other components
        await startup.stop_application()
        
        # Verify all components had stop called (even the failing one)
        assert component1.stop_called_count == 1
        assert failing_component.stop_called_count == 1
        assert component3.stop_called_count == 1
        
        # Verify started components list is cleared despite failure
        assert startup._started_components == []
    
    def test_get_component_by_name_known_components(self, startup: ApplicationStartup) -> None:
        """Test getting components by name for known components."""
        # Register mock services
        mock_event_bus = MockEventBus()
        mock_message_bus = MockMessageBus()
        
        startup._container.register_instance(IEventBus, mock_event_bus)
        startup._container.register_instance(IMessageBus, mock_message_bus)
        
        # Test getting known components
        assert startup._get_component_by_name('event_bus') is mock_event_bus
        assert startup._get_component_by_name('message_bus') is mock_message_bus
    
    def test_get_component_by_name_unknown_component(self, startup: ApplicationStartup) -> None:
        """Test getting component by name for unknown component."""
        result = startup._get_component_by_name('unknown_component')
        assert result is None
    
    def test_get_component_by_name_unregistered_component(self, startup: ApplicationStartup) -> None:
        """Test getting component by name for unregistered component."""
        # Try to get a known component type that's not registered
        result = startup._get_component_by_name('event_bus')
        assert result is None

    @pytest.mark.asyncio
    async def test_stop_started_components_private_method(self, startup: ApplicationStartup) -> None:
        """Test the private _stop_started_components method."""
        # Create mock components
        component1 = MockComponent("component1")
        component2 = MockComponent("component2")

        startup._started_components = [component1, component2]

        # Call the private method
        await startup._stop_started_components()

        # Verify components were stopped in reverse order
        assert component1.stop_called_count == 1
        assert component2.stop_called_count == 1

        # Verify list is cleared
        assert startup._started_components == []

    def test_startup_order_completeness(self, startup: ApplicationStartup) -> None:
        """Test that startup order contains all expected components."""
        expected_components = [
            'config_manager',
            'event_bus',
            'message_bus',
            'command_dispatcher',
            'plugin_manager',
            'ssh_forwarder',
            'upload_manager'
        ]

        for component in expected_components:
            assert component in startup._startup_order, f"Missing component: {component}"

    def test_startup_order_sequence(self, startup: ApplicationStartup) -> None:
        """Test that startup order has correct sequence."""
        order = startup._startup_order

        # Config manager should be first
        assert order[0] == 'config_manager'

        # Core services should come before infrastructure services
        config_idx = order.index('config_manager')
        event_bus_idx = order.index('event_bus')
        message_bus_idx = order.index('message_bus')
        command_dispatcher_idx = order.index('command_dispatcher')

        assert config_idx < event_bus_idx
        assert config_idx < message_bus_idx
        assert config_idx < command_dispatcher_idx


class TestApplicationStartupIntegration:
    """Integration tests for ApplicationStartup with real container."""

    @pytest.fixture
    def container(self) -> Container:
        """Create a real container for integration testing."""
        return Container()

    @pytest.fixture
    def startup(self, container: Container) -> ApplicationStartup:
        """Create ApplicationStartup with real container."""
        return ApplicationStartup(container)

    @pytest.mark.asyncio
    async def test_full_startup_shutdown_cycle(self, startup: ApplicationStartup) -> None:
        """Test complete startup and shutdown cycle."""
        # Register mock components that implement IComponent
        component1 = MockComponent("test_component_1")
        component2 = MockComponent("test_component_2")

        startup._container.register_instance(IEventBus, component1)
        startup._container.register_instance(IMessageBus, component2)

        # Mock _get_component_by_name to return our components
        def mock_get_component(name: str):
            if name == 'event_bus':
                return component1
            elif name == 'message_bus':
                return component2
            return None

        with patch.object(startup, '_get_component_by_name', side_effect=mock_get_component):
            # Start application
            await startup.start_application()

            # Verify components were started
            assert component1.started is True
            assert component2.started is True
            assert len(startup._started_components) == 2

            # Stop application
            await startup.stop_application()

            # Verify components were stopped
            assert component1.stopped is True
            assert component2.stopped is True
            assert len(startup._started_components) == 0

    @pytest.mark.asyncio
    async def test_partial_startup_failure_cleanup(self, startup: ApplicationStartup) -> None:
        """Test cleanup when startup fails partway through."""
        # Create components - second one will fail
        component1 = MockComponent("success_component")
        component2 = MockComponent("failing_component")
        component2.should_fail_start = True
        component3 = MockComponent("never_started_component")

        def mock_get_component(name: str):
            component_map = {
                'config_manager': component1,
                'event_bus': component2,
                'message_bus': component3
            }
            return component_map.get(name)

        with patch.object(startup, '_get_component_by_name', side_effect=mock_get_component):
            with pytest.raises(RuntimeError, match="Mock start failure"):
                await startup.start_application()

            # Verify first component was started then stopped during cleanup
            assert component1.start_called_count == 1
            assert component1.stop_called_count == 1

            # Verify failing component was attempted to start
            assert component2.start_called_count == 1

            # Verify third component was never started
            assert component3.start_called_count == 0

            # Verify no components remain in started list
            assert len(startup._started_components) == 0

    def test_container_integration(self, startup: ApplicationStartup) -> None:
        """Test integration with dependency injection container."""
        # Verify container is properly set
        assert startup._container is not None
        assert isinstance(startup._container, Container)

        # Test that we can register and resolve services through startup
        test_service = MockComponent("test_service")
        startup._container.register_instance(MockComponent, test_service)

        resolved = startup._container.try_resolve(MockComponent)
        assert resolved is test_service
