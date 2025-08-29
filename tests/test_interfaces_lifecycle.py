"""
Tests for lifecycle interfaces.

This module tests the lifecycle interfaces including IStartable, IStoppable,
IHealthCheckable, IConfigurable, and IComponent.
"""

import pytest
from typing import Any, Dict
from unittest.mock import AsyncMock, Mock

from cobalt_forward.core.interfaces.lifecycle import (
    IStartable, IStoppable, IHealthCheckable, IConfigurable, IComponent
)


class MockStartableComponent(IStartable):
    """Mock implementation of IStartable for testing."""
    
    def __init__(self):
        self.started = False
        self.start_called_count = 0
    
    async def start(self) -> None:
        """Start the component."""
        self.started = True
        self.start_called_count += 1


class MockStoppableComponent(IStoppable):
    """Mock implementation of IStoppable for testing."""
    
    def __init__(self):
        self.stopped = False
        self.stop_called_count = 0
    
    async def stop(self) -> None:
        """Stop the component."""
        self.stopped = True
        self.stop_called_count += 1


class MockHealthCheckableComponent(IHealthCheckable):
    """Mock implementation of IHealthCheckable for testing."""
    
    def __init__(self, healthy: bool = True):
        self.healthy = healthy
        self.check_health_called_count = 0
    
    async def check_health(self) -> Dict[str, Any]:
        """Check component health."""
        self.check_health_called_count += 1
        return {
            'healthy': self.healthy,
            'status': 'running' if self.healthy else 'error',
            'details': {
                'uptime': 3600,
                'connections': 5 if self.healthy else 0,
                'last_error': None if self.healthy else 'Test error'
            }
        }


class MockConfigurableComponent(IConfigurable):
    """Mock implementation of IConfigurable for testing."""
    
    def __init__(self):
        self.configured = False
        self.config: Dict[str, Any] = {}
        self.configure_called_count = 0
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the component."""
        self.configured = True
        self.config = config
        self.configure_called_count += 1


class MockComponent(IComponent):
    """Mock implementation of IComponent for testing."""
    
    def __init__(self, name: str = "test_component", version: str = "1.0.0", healthy: bool = True):
        self._name = name
        self._version = version
        self.healthy = healthy
        
        # State tracking
        self.started = False
        self.stopped = False
        self.configured = False
        self.config: Dict[str, Any] = {}
        
        # Call counters
        self.start_called_count = 0
        self.stop_called_count = 0
        self.check_health_called_count = 0
        self.configure_called_count = 0
    
    @property
    def name(self) -> str:
        """Get component name."""
        return self._name
    
    @property
    def version(self) -> str:
        """Get component version."""
        return self._version
    
    async def start(self) -> None:
        """Start the component."""
        self.started = True
        self.start_called_count += 1
    
    async def stop(self) -> None:
        """Stop the component."""
        self.stopped = True
        self.stop_called_count += 1
    
    async def check_health(self) -> Dict[str, Any]:
        """Check component health."""
        self.check_health_called_count += 1
        return {
            'healthy': self.healthy,
            'status': 'running' if self.healthy else 'error',
            'details': {
                'name': self._name,
                'version': self._version,
                'uptime': 3600,
                'last_error': None if self.healthy else 'Test error'
            }
        }
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the component."""
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")
        
        self.configured = True
        self.config = config
        self.configure_called_count += 1


class FailingComponent(IComponent):
    """Component that fails operations for testing error scenarios."""
    
    def __init__(self, fail_start: bool = False, fail_stop: bool = False, 
                 fail_health: bool = False, fail_configure: bool = False):
        self.fail_start = fail_start
        self.fail_stop = fail_stop
        self.fail_health = fail_health
        self.fail_configure = fail_configure
    
    @property
    def name(self) -> str:
        return "failing_component"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    async def start(self) -> None:
        if self.fail_start:
            raise RuntimeError("Failed to start component")
    
    async def stop(self) -> None:
        if self.fail_stop:
            raise RuntimeError("Failed to stop component")
    
    async def check_health(self) -> Dict[str, Any]:
        if self.fail_health:
            raise RuntimeError("Health check failed")
        return {'healthy': False, 'status': 'error', 'details': {}}
    
    async def configure(self, config: Dict[str, Any]) -> None:
        if self.fail_configure:
            raise ValueError("Configuration failed")


class TestIStartable:
    """Test cases for IStartable interface."""
    
    @pytest.fixture
    def startable_component(self) -> MockStartableComponent:
        """Create a mock startable component."""
        return MockStartableComponent()
    
    @pytest.mark.asyncio
    async def test_start_method_exists(self, startable_component: MockStartableComponent) -> None:
        """Test that start method exists and can be called."""
        assert hasattr(startable_component, 'start')
        assert callable(startable_component.start)
        
        await startable_component.start()
        assert startable_component.started is True
        assert startable_component.start_called_count == 1
    
    @pytest.mark.asyncio
    async def test_start_multiple_calls(self, startable_component: MockStartableComponent) -> None:
        """Test calling start multiple times."""
        await startable_component.start()
        await startable_component.start()
        
        assert startable_component.started is True
        assert startable_component.start_called_count == 2


class TestIStoppable:
    """Test cases for IStoppable interface."""
    
    @pytest.fixture
    def stoppable_component(self) -> MockStoppableComponent:
        """Create a mock stoppable component."""
        return MockStoppableComponent()
    
    @pytest.mark.asyncio
    async def test_stop_method_exists(self, stoppable_component: MockStoppableComponent) -> None:
        """Test that stop method exists and can be called."""
        assert hasattr(stoppable_component, 'stop')
        assert callable(stoppable_component.stop)
        
        await stoppable_component.stop()
        assert stoppable_component.stopped is True
        assert stoppable_component.stop_called_count == 1
    
    @pytest.mark.asyncio
    async def test_stop_multiple_calls(self, stoppable_component: MockStoppableComponent) -> None:
        """Test calling stop multiple times."""
        await stoppable_component.stop()
        await stoppable_component.stop()
        
        assert stoppable_component.stopped is True
        assert stoppable_component.stop_called_count == 2


class TestIHealthCheckable:
    """Test cases for IHealthCheckable interface."""
    
    @pytest.fixture
    def healthy_component(self) -> MockHealthCheckableComponent:
        """Create a healthy mock component."""
        return MockHealthCheckableComponent(healthy=True)
    
    @pytest.fixture
    def unhealthy_component(self) -> MockHealthCheckableComponent:
        """Create an unhealthy mock component."""
        return MockHealthCheckableComponent(healthy=False)
    
    @pytest.mark.asyncio
    async def test_check_health_method_exists(self, healthy_component: MockHealthCheckableComponent) -> None:
        """Test that check_health method exists and can be called."""
        assert hasattr(healthy_component, 'check_health')
        assert callable(healthy_component.check_health)
        
        health = await healthy_component.check_health()
        assert isinstance(health, dict)
        assert healthy_component.check_health_called_count == 1
    
    @pytest.mark.asyncio
    async def test_healthy_component_response(self, healthy_component: MockHealthCheckableComponent) -> None:
        """Test health check response for healthy component."""
        health = await healthy_component.check_health()
        
        # Required fields
        assert 'healthy' in health
        assert 'status' in health
        assert 'details' in health
        
        # Values for healthy component
        assert health['healthy'] is True
        assert health['status'] == 'running'
        assert isinstance(health['details'], dict)
        
        # Details should contain useful information
        details = health['details']
        assert 'uptime' in details
        assert 'connections' in details
        assert 'last_error' in details
        assert details['connections'] == 5
        assert details['last_error'] is None
    
    @pytest.mark.asyncio
    async def test_unhealthy_component_response(self, unhealthy_component: MockHealthCheckableComponent) -> None:
        """Test health check response for unhealthy component."""
        health = await unhealthy_component.check_health()
        
        # Required fields
        assert 'healthy' in health
        assert 'status' in health
        assert 'details' in health
        
        # Values for unhealthy component
        assert health['healthy'] is False
        assert health['status'] == 'error'
        
        # Details should show error information
        details = health['details']
        assert details['connections'] == 0
        assert details['last_error'] == 'Test error'


class TestIConfigurable:
    """Test cases for IConfigurable interface."""
    
    @pytest.fixture
    def configurable_component(self) -> MockConfigurableComponent:
        """Create a mock configurable component."""
        return MockConfigurableComponent()
    
    @pytest.mark.asyncio
    async def test_configure_method_exists(self, configurable_component: MockConfigurableComponent) -> None:
        """Test that configure method exists and can be called."""
        assert hasattr(configurable_component, 'configure')
        assert callable(configurable_component.configure)
        
        config = {"setting1": "value1", "setting2": 42}
        await configurable_component.configure(config)
        
        assert configurable_component.configured is True
        assert configurable_component.config == config
        assert configurable_component.configure_called_count == 1
    
    @pytest.mark.asyncio
    async def test_configure_empty_config(self, configurable_component: MockConfigurableComponent) -> None:
        """Test configuring with empty configuration."""
        config = {}
        await configurable_component.configure(config)
        
        assert configurable_component.configured is True
        assert configurable_component.config == config
    
    @pytest.mark.asyncio
    async def test_configure_multiple_times(self, configurable_component: MockConfigurableComponent) -> None:
        """Test configuring multiple times."""
        config1 = {"setting": "value1"}
        config2 = {"setting": "value2"}
        
        await configurable_component.configure(config1)
        await configurable_component.configure(config2)
        
        assert configurable_component.configured is True
        assert configurable_component.config == config2  # Should have latest config
        assert configurable_component.configure_called_count == 2


class TestIComponent:
    """Test cases for IComponent interface."""

    @pytest.fixture
    def component(self) -> MockComponent:
        """Create a mock component."""
        return MockComponent()

    @pytest.fixture
    def failing_component(self) -> FailingComponent:
        """Create a component that fails operations."""
        return FailingComponent(fail_start=True, fail_stop=True, fail_health=True, fail_configure=True)

    def test_component_properties(self, component: MockComponent) -> None:
        """Test component name and version properties."""
        assert component.name == "test_component"
        assert component.version == "1.0.0"

        # Test with custom values
        custom_component = MockComponent("custom_name", "2.0.0")
        assert custom_component.name == "custom_name"
        assert custom_component.version == "2.0.0"

    @pytest.mark.asyncio
    async def test_component_full_lifecycle(self, component: MockComponent) -> None:
        """Test complete component lifecycle."""
        # Initial state
        assert component.started is False
        assert component.stopped is False
        assert component.configured is False

        # Configure
        config = {"host": "localhost", "port": 8080}
        await component.configure(config)
        assert component.configured is True
        assert component.config == config

        # Start
        await component.start()
        assert component.started is True

        # Health check
        health = await component.check_health()
        assert health['healthy'] is True
        assert health['status'] == 'running'
        assert health['details']['name'] == "test_component"
        assert health['details']['version'] == "1.0.0"

        # Stop
        await component.stop()
        assert component.stopped is True

    @pytest.mark.asyncio
    async def test_component_start_failure(self) -> None:
        """Test component start failure."""
        failing_component = FailingComponent(fail_start=True)

        with pytest.raises(RuntimeError, match="Failed to start component"):
            await failing_component.start()

    @pytest.mark.asyncio
    async def test_component_stop_failure(self) -> None:
        """Test component stop failure."""
        failing_component = FailingComponent(fail_stop=True)

        with pytest.raises(RuntimeError, match="Failed to stop component"):
            await failing_component.stop()

    @pytest.mark.asyncio
    async def test_component_health_check_failure(self) -> None:
        """Test component health check failure."""
        failing_component = FailingComponent(fail_health=True)

        with pytest.raises(RuntimeError, match="Health check failed"):
            await failing_component.check_health()

    @pytest.mark.asyncio
    async def test_component_configure_failure(self) -> None:
        """Test component configuration failure."""
        failing_component = FailingComponent(fail_configure=True)

        with pytest.raises(ValueError, match="Configuration failed"):
            await failing_component.configure({})

    @pytest.mark.asyncio
    async def test_component_configure_invalid_input(self, component: MockComponent) -> None:
        """Test component configuration with invalid input."""
        # The mock component validates that config is a dict
        with pytest.raises(ValueError, match="Configuration must be a dictionary"):
            await component.configure("invalid_config")  # type: ignore

    @pytest.mark.asyncio
    async def test_component_unhealthy_state(self) -> None:
        """Test component in unhealthy state."""
        unhealthy_component = MockComponent(healthy=False)

        health = await unhealthy_component.check_health()
        assert health['healthy'] is False
        assert health['status'] == 'error'
        assert health['details']['last_error'] == 'Test error'

    @pytest.mark.asyncio
    async def test_component_call_counters(self, component: MockComponent) -> None:
        """Test that method call counters work correctly."""
        # Initial state
        assert component.start_called_count == 0
        assert component.stop_called_count == 0
        assert component.check_health_called_count == 0
        assert component.configure_called_count == 0

        # Call methods multiple times
        await component.start()
        await component.start()
        assert component.start_called_count == 2

        await component.stop()
        assert component.stop_called_count == 1

        await component.check_health()
        await component.check_health()
        await component.check_health()
        assert component.check_health_called_count == 3

        await component.configure({"test": "config"})
        assert component.configure_called_count == 1


class TestInterfaceInheritance:
    """Test cases for interface inheritance and composition."""

    @pytest.fixture
    def component(self) -> MockComponent:
        """Create a MockComponent instance."""
        return MockComponent()

    def test_component_inherits_all_interfaces(self) -> None:
        """Test that IComponent inherits from all lifecycle interfaces."""
        component = MockComponent()

        # Should be instance of all interfaces
        assert isinstance(component, IStartable)
        assert isinstance(component, IStoppable)
        assert isinstance(component, IHealthCheckable)
        assert isinstance(component, IConfigurable)
        assert isinstance(component, IComponent)

    def test_interface_method_signatures(self) -> None:
        """Test that interface methods have correct signatures."""
        # This test ensures the interfaces are properly defined
        # by checking that our mock implementations satisfy the interfaces

        startable: IStartable = MockStartableComponent()
        stoppable: IStoppable = MockStoppableComponent()
        health_checkable: IHealthCheckable = MockHealthCheckableComponent()
        configurable: IConfigurable = MockConfigurableComponent()
        component: IComponent = MockComponent()

        # If these assignments work, the interfaces are correctly implemented
        assert startable is not None
        assert stoppable is not None
        assert health_checkable is not None
        assert configurable is not None
        assert component is not None

    @pytest.mark.asyncio
    async def test_component_interface_methods_callable(self, component: MockComponent) -> None:
        """Test that all interface methods are callable on component."""
        # Test IStartable
        await component.start()

        # Test IStoppable
        await component.stop()

        # Test IHealthCheckable
        health = await component.check_health()
        assert isinstance(health, dict)

        # Test IConfigurable
        await component.configure({"test": "value"})

        # Test IComponent properties
        assert isinstance(component.name, str)
        assert isinstance(component.version, str)
