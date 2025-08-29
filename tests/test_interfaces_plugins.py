"""
Tests for plugin interfaces.

This module tests the plugin interfaces including IPlugin, IPluginManager, and IPluginContext.
"""

import pytest
from typing import Any, Dict, List, Set, Type, Optional
from unittest.mock import AsyncMock, Mock

from cobalt_forward.core.interfaces.plugins import IPlugin, IPluginManager, IPluginContext
from cobalt_forward.core.interfaces.lifecycle import IComponent


class MockPlugin(IPlugin):
    """Mock implementation of IPlugin for testing."""
    
    def __init__(self, name: str = "test_plugin", version: str = "1.0.0"):
        self._name = name
        self._version = version
        self._metadata = {
            "name": name,
            "version": version,
            "description": "Test plugin",
            "author": "Test Author",
            "dependencies": [],
            "permissions": set()
        }
        self._dependencies = []
        self._permissions = set()
        self._context: Optional[IPluginContext] = None
        self._initialized = False
        self._started = False
        self._configured = False
        self._shutdown = False
        
        # Call counters
        self.initialize_called_count = 0
        self.shutdown_called_count = 0
        self.start_called_count = 0
        self.stop_called_count = 0
        self.configure_called_count = 0
        self.check_health_called_count = 0
        
        # Configuration
        self.should_fail_initialize = False
        self.should_fail_shutdown = False
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
    
    @property
    def metadata(self) -> Dict[str, Any]:
        return self._metadata.copy()
    
    @property
    def dependencies(self) -> List[str]:
        return self._dependencies.copy()
    
    @property
    def permissions(self) -> Set[str]:
        return self._permissions.copy()
    
    async def initialize(self, context: IPluginContext) -> None:
        """Initialize the plugin with the given context."""
        self.initialize_called_count += 1
        if self.should_fail_initialize:
            raise RuntimeError("Plugin initialization failed")
        
        self._context = context
        self._initialized = True
    
    async def shutdown(self) -> None:
        """Shutdown the plugin and clean up resources."""
        self.shutdown_called_count += 1
        if self.should_fail_shutdown:
            raise RuntimeError("Plugin shutdown failed")
        
        self._shutdown = True
        self._context = None
    
    async def start(self) -> None:
        """Start the plugin."""
        self.start_called_count += 1
        if self.should_fail_start:
            raise RuntimeError("Plugin start failed")
        
        self._started = True
    
    async def stop(self) -> None:
        """Stop the plugin."""
        self.stop_called_count += 1
        if self.should_fail_stop:
            raise RuntimeError("Plugin stop failed")
        
        self._started = False
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the plugin."""
        self.configure_called_count += 1
        if self.should_fail_configure:
            raise ValueError("Plugin configuration failed")
        
        self._configured = True
    
    async def check_health(self) -> Dict[str, Any]:
        """Check plugin health."""
        self.check_health_called_count += 1
        if self.should_fail_health_check:
            raise RuntimeError("Plugin health check failed")
        
        return {
            'healthy': True,
            'status': 'running' if self._started else 'stopped',
            'details': {
                'name': self._name,
                'version': self._version,
                'initialized': self._initialized,
                'configured': self._configured
            }
        }
    
    def set_dependencies(self, dependencies: List[str]) -> None:
        """Set plugin dependencies for testing."""
        self._dependencies = dependencies
        self._metadata['dependencies'] = dependencies
    
    def set_permissions(self, permissions: Set[str]) -> None:
        """Set plugin permissions for testing."""
        self._permissions = permissions
        self._metadata['permissions'] = permissions


class MockPluginContext(IPluginContext):
    """Mock implementation of IPluginContext for testing."""
    
    def __init__(self):
        self.services: Dict[Type[Any], Any] = {}
        self.config: Dict[str, Any] = {}
        self.emitted_events: List[tuple] = []
        self.loggers: Dict[str, Any] = {}
        
        # Call counters
        self.get_service_called_count = 0
        self.get_config_called_count = 0
        self.emit_event_called_count = 0
        self.get_logger_called_count = 0
    
    def get_service(self, service_type: Type[Any]) -> Optional[Any]:
        """Get a service from the application container."""
        self.get_service_called_count += 1
        return self.services.get(service_type)
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value for the plugin."""
        self.get_config_called_count += 1
        return self.config.get(key, default)
    
    async def emit_event(self, event_name: str, data: Any = None) -> None:
        """Emit an event through the event bus."""
        self.emit_event_called_count += 1
        self.emitted_events.append((event_name, data))
    
    def get_logger(self, name: Optional[str] = None) -> Any:
        """Get a logger for the plugin."""
        self.get_logger_called_count += 1
        logger_name = name or "default"
        if logger_name not in self.loggers:
            self.loggers[logger_name] = Mock()
        return self.loggers[logger_name]
    
    def set_service(self, service_type: Type[Any], service: Any) -> None:
        """Set a service for testing."""
        self.services[service_type] = service
    
    def set_config(self, key: str, value: Any) -> None:
        """Set a config value for testing."""
        self.config[key] = value


class MockPluginManager(IPluginManager):
    """Mock implementation of IPluginManager for testing."""
    
    def __init__(self):
        self.loaded_plugins: Dict[str, IPlugin] = {}
        self.plugin_paths: Dict[str, str] = {}
        self.discovered_plugins: List[str] = []
        
        # Call counters
        self.load_plugin_called_count = 0
        self.unload_plugin_called_count = 0
        self.reload_plugin_called_count = 0
        self.get_plugin_called_count = 0
        self.list_plugins_called_count = 0
        self.discover_plugins_called_count = 0
        self.is_plugin_loaded_called_count = 0
        self.get_plugin_dependencies_called_count = 0
        self.start_called_count = 0
        self.stop_called_count = 0
        self.configure_called_count = 0
        self.check_health_called_count = 0
        
        # Configuration
        self.should_fail_load = False
        self.should_fail_unload = False
        self.should_fail_reload = False
        self._started = False
        self._configured = False
    
    @property
    def name(self) -> str:
        return "MockPluginManager"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    async def load_plugin(self, plugin_path: str) -> bool:
        """Load a plugin from the given path."""
        self.load_plugin_called_count += 1
        if self.should_fail_load:
            return False
        
        # Extract plugin name from path for testing
        plugin_name = plugin_path.split('/')[-1].replace('.py', '')
        plugin = MockPlugin(plugin_name)
        
        self.loaded_plugins[plugin_name] = plugin
        self.plugin_paths[plugin_name] = plugin_path
        return True
    
    async def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a plugin by name."""
        self.unload_plugin_called_count += 1
        if self.should_fail_unload:
            return False
        
        if plugin_name in self.loaded_plugins:
            plugin = self.loaded_plugins[plugin_name]
            await plugin.shutdown()
            del self.loaded_plugins[plugin_name]
            del self.plugin_paths[plugin_name]
            return True
        return False
    
    async def reload_plugin(self, plugin_name: str) -> bool:
        """Reload a plugin by name."""
        self.reload_plugin_called_count += 1
        if self.should_fail_reload:
            return False
        
        if plugin_name in self.loaded_plugins:
            plugin_path = self.plugin_paths[plugin_name]
            await self.unload_plugin(plugin_name)
            return await self.load_plugin(plugin_path)
        return False
    
    def get_plugin(self, plugin_name: str) -> Optional[IPlugin]:
        """Get a loaded plugin by name."""
        self.get_plugin_called_count += 1
        return self.loaded_plugins.get(plugin_name)
    
    def get_all_plugins(self) -> Dict[str, IPlugin]:
        """Get all loaded plugins."""
        return self.loaded_plugins.copy()
    
    def list_plugins(self) -> List[str]:
        """List all loaded plugin names."""
        self.list_plugins_called_count += 1
        return list(self.loaded_plugins.keys())
    
    async def discover_plugins(self, directory: str) -> List[str]:
        """Discover plugins in the given directory."""
        self.discover_plugins_called_count += 1
        # Return pre-configured discovered plugins for testing
        return self.discovered_plugins.copy()
    
    def is_plugin_loaded(self, plugin_name: str) -> bool:
        """Check if a plugin is loaded."""
        self.is_plugin_loaded_called_count += 1
        return plugin_name in self.loaded_plugins
    
    async def get_plugin_dependencies(self, plugin_name: str) -> List[str]:
        """Get dependencies for a plugin."""
        self.get_plugin_dependencies_called_count += 1
        plugin = self.loaded_plugins.get(plugin_name)
        return plugin.dependencies if plugin else []
    
    async def start(self) -> None:
        """Start the plugin manager."""
        self.start_called_count += 1
        self._started = True
    
    async def stop(self) -> None:
        """Stop the plugin manager."""
        self.stop_called_count += 1
        self._started = False
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the plugin manager."""
        self.configure_called_count += 1
        self._configured = True
    
    async def check_health(self) -> Dict[str, Any]:
        """Check plugin manager health."""
        self.check_health_called_count += 1
        return {
            'healthy': True,
            'status': 'running' if self._started else 'stopped',
            'details': {
                'loaded_plugins': len(self.loaded_plugins),
                'plugin_names': list(self.loaded_plugins.keys())
            }
        }
    
    def set_discovered_plugins(self, plugins: List[str]) -> None:
        """Set discovered plugins for testing."""
        self.discovered_plugins = plugins


class TestIPlugin:
    """Test cases for IPlugin interface."""
    
    @pytest.fixture
    def plugin(self) -> MockPlugin:
        """Create a mock plugin."""
        return MockPlugin()
    
    @pytest.fixture
    def plugin_context(self) -> MockPluginContext:
        """Create a mock plugin context."""
        return MockPluginContext()
    
    def test_plugin_properties(self, plugin: MockPlugin) -> None:
        """Test plugin properties."""
        assert plugin.name == "test_plugin"
        assert plugin.version == "1.0.0"
        
        metadata = plugin.metadata
        assert metadata['name'] == "test_plugin"
        assert metadata['version'] == "1.0.0"
        assert metadata['description'] == "Test plugin"
        assert metadata['author'] == "Test Author"
        
        assert plugin.dependencies == []
        assert plugin.permissions == set()
    
    def test_plugin_with_dependencies_and_permissions(self) -> None:
        """Test plugin with dependencies and permissions."""
        plugin = MockPlugin()
        dependencies = ["plugin1", "plugin2"]
        permissions = {"read", "write", "execute"}
        
        plugin.set_dependencies(dependencies)
        plugin.set_permissions(permissions)
        
        assert plugin.dependencies == dependencies
        assert plugin.permissions == permissions
        assert plugin.metadata['dependencies'] == dependencies
        assert plugin.metadata['permissions'] == permissions
    
    @pytest.mark.asyncio
    async def test_plugin_initialization(self, plugin: MockPlugin, plugin_context: MockPluginContext) -> None:
        """Test plugin initialization."""
        assert hasattr(plugin, 'initialize')
        assert callable(plugin.initialize)
        
        await plugin.initialize(plugin_context)
        
        assert plugin.initialize_called_count == 1
        assert plugin._initialized is True
        assert plugin._context is plugin_context
    
    @pytest.mark.asyncio
    async def test_plugin_initialization_failure(self, plugin: MockPlugin, plugin_context: MockPluginContext) -> None:
        """Test plugin initialization failure."""
        plugin.should_fail_initialize = True
        
        with pytest.raises(RuntimeError, match="Plugin initialization failed"):
            await plugin.initialize(plugin_context)
        
        assert plugin.initialize_called_count == 1
        assert plugin._initialized is False
    
    @pytest.mark.asyncio
    async def test_plugin_shutdown(self, plugin: MockPlugin, plugin_context: MockPluginContext) -> None:
        """Test plugin shutdown."""
        assert hasattr(plugin, 'shutdown')
        assert callable(plugin.shutdown)
        
        # Initialize first
        await plugin.initialize(plugin_context)
        
        # Then shutdown
        await plugin.shutdown()
        
        assert plugin.shutdown_called_count == 1
        assert plugin._shutdown is True
        assert plugin._context is None
    
    @pytest.mark.asyncio
    async def test_plugin_shutdown_failure(self, plugin: MockPlugin, plugin_context: MockPluginContext) -> None:
        """Test plugin shutdown failure."""
        plugin.should_fail_shutdown = True
        
        with pytest.raises(RuntimeError, match="Plugin shutdown failed"):
            await plugin.shutdown()
        
        assert plugin.shutdown_called_count == 1

    @pytest.mark.asyncio
    async def test_plugin_lifecycle_methods(self, plugin: MockPlugin) -> None:
        """Test plugin lifecycle methods from IComponent."""
        # Test start
        await plugin.start()
        assert plugin.start_called_count == 1
        assert plugin._started is True

        # Test configure
        config = {"setting": "value"}
        await plugin.configure(config)
        assert plugin.configure_called_count == 1
        assert plugin._configured is True

        # Test health check
        health = await plugin.check_health()
        assert plugin.check_health_called_count == 1
        assert health['healthy'] is True
        assert health['status'] == 'running'
        assert health['details']['name'] == "test_plugin"

        # Test stop
        await plugin.stop()
        assert plugin.stop_called_count == 1
        assert plugin._started is False

    @pytest.mark.asyncio
    async def test_plugin_lifecycle_failures(self, plugin: MockPlugin) -> None:
        """Test plugin lifecycle method failures."""
        # Test start failure
        plugin.should_fail_start = True
        with pytest.raises(RuntimeError, match="Plugin start failed"):
            await plugin.start()

        # Test configure failure
        plugin.should_fail_configure = True
        with pytest.raises(ValueError, match="Plugin configuration failed"):
            await plugin.configure({})

        # Test health check failure
        plugin.should_fail_health_check = True
        with pytest.raises(RuntimeError, match="Plugin health check failed"):
            await plugin.check_health()

        # Test stop failure
        plugin.should_fail_stop = True
        with pytest.raises(RuntimeError, match="Plugin stop failed"):
            await plugin.stop()

    def test_plugin_inherits_from_icomponent(self, plugin: MockPlugin) -> None:
        """Test that IPlugin inherits from IComponent."""
        assert isinstance(plugin, IComponent)
        assert isinstance(plugin, IPlugin)


class TestIPluginContext:
    """Test cases for IPluginContext interface."""

    @pytest.fixture
    def context(self) -> MockPluginContext:
        """Create a mock plugin context."""
        return MockPluginContext()

    def test_get_service(self, context: MockPluginContext) -> None:
        """Test getting service from context."""
        assert hasattr(context, 'get_service')
        assert callable(context.get_service)

        # Test with no service registered
        service = context.get_service(str)
        assert service is None
        assert context.get_service_called_count == 1

        # Test with service registered
        mock_service = Mock()
        context.set_service(str, mock_service)
        service = context.get_service(str)
        assert service is mock_service
        assert context.get_service_called_count == 2

    def test_get_config(self, context: MockPluginContext) -> None:
        """Test getting configuration from context."""
        assert hasattr(context, 'get_config')
        assert callable(context.get_config)

        # Test with no config set
        value = context.get_config("test_key", "default")
        assert value == "default"
        assert context.get_config_called_count == 1

        # Test with config set
        context.set_config("test_key", "test_value")
        value = context.get_config("test_key")
        assert value == "test_value"
        assert context.get_config_called_count == 2

    @pytest.mark.asyncio
    async def test_emit_event(self, context: MockPluginContext) -> None:
        """Test emitting event through context."""
        assert hasattr(context, 'emit_event')
        assert callable(context.emit_event)

        await context.emit_event("test.event", {"data": "test"})

        assert context.emit_event_called_count == 1
        assert len(context.emitted_events) == 1
        assert context.emitted_events[0] == ("test.event", {"data": "test"})

    def test_get_logger(self, context: MockPluginContext) -> None:
        """Test getting logger from context."""
        assert hasattr(context, 'get_logger')
        assert callable(context.get_logger)

        # Test with default name
        logger1 = context.get_logger()
        assert logger1 is not None
        assert context.get_logger_called_count == 1

        # Test with specific name
        logger2 = context.get_logger("test_plugin")
        assert logger2 is not None
        assert context.get_logger_called_count == 2

        # Test that same name returns same logger
        logger3 = context.get_logger("test_plugin")
        assert logger3 is logger2
        assert context.get_logger_called_count == 3


class TestIPluginManager:
    """Test cases for IPluginManager interface."""

    @pytest.fixture
    def plugin_manager(self) -> MockPluginManager:
        """Create a mock plugin manager."""
        return MockPluginManager()

    @pytest.fixture
    def sample_plugin(self) -> MockPlugin:
        """Create a sample plugin."""
        return MockPlugin("sample_plugin", "2.0.0")

    @pytest.mark.asyncio
    async def test_load_plugin(self, plugin_manager: MockPluginManager) -> None:
        """Test loading a plugin."""
        assert hasattr(plugin_manager, 'load_plugin')
        assert callable(plugin_manager.load_plugin)

        result = await plugin_manager.load_plugin("/path/to/test_plugin.py")

        assert result is True
        assert plugin_manager.load_plugin_called_count == 1
        assert "test_plugin" in plugin_manager.loaded_plugins
        assert plugin_manager.plugin_paths["test_plugin"] == "/path/to/test_plugin.py"

    @pytest.mark.asyncio
    async def test_load_plugin_failure(self, plugin_manager: MockPluginManager) -> None:
        """Test plugin loading failure."""
        plugin_manager.should_fail_load = True

        result = await plugin_manager.load_plugin("/path/to/failing_plugin.py")

        assert result is False
        assert plugin_manager.load_plugin_called_count == 1
        assert len(plugin_manager.loaded_plugins) == 0

    @pytest.mark.asyncio
    async def test_unload_plugin(self, plugin_manager: MockPluginManager) -> None:
        """Test unloading a plugin."""
        assert hasattr(plugin_manager, 'unload_plugin')
        assert callable(plugin_manager.unload_plugin)

        # Load plugin first
        await plugin_manager.load_plugin("/path/to/test_plugin.py")
        assert "test_plugin" in plugin_manager.loaded_plugins

        # Then unload
        result = await plugin_manager.unload_plugin("test_plugin")

        assert result is True
        assert plugin_manager.unload_plugin_called_count == 1
        assert "test_plugin" not in plugin_manager.loaded_plugins
        assert "test_plugin" not in plugin_manager.plugin_paths

    @pytest.mark.asyncio
    async def test_unload_nonexistent_plugin(self, plugin_manager: MockPluginManager) -> None:
        """Test unloading a plugin that doesn't exist."""
        result = await plugin_manager.unload_plugin("nonexistent_plugin")

        assert result is False
        assert plugin_manager.unload_plugin_called_count == 1

    @pytest.mark.asyncio
    async def test_unload_plugin_failure(self, plugin_manager: MockPluginManager) -> None:
        """Test plugin unloading failure."""
        plugin_manager.should_fail_unload = True

        result = await plugin_manager.unload_plugin("test_plugin")

        assert result is False
        assert plugin_manager.unload_plugin_called_count == 1
