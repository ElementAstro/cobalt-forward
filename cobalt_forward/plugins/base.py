"""
Base plugin classes and utilities.

This module provides base classes and utilities for creating plugins
that integrate with the Cobalt Forward application.
"""

import logging
from abc import abstractmethod
from typing import Any, Dict, List, Set, Optional

from ..core.interfaces.plugins import IPlugin, IPluginContext

logger = logging.getLogger(__name__)


class BasePlugin(IPlugin):
    """
    Base plugin class providing common functionality.
    
    Plugin developers should inherit from this class and implement
    the required abstract methods.
    """
    
    def __init__(self) -> None:
        self._context: Optional[IPluginContext] = None
        self._started = False
        self._logger = None
    
    @property
    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """Get plugin metadata."""
        pass
    
    @property
    @abstractmethod
    def dependencies(self) -> List[str]:
        """Get list of plugin dependencies."""
        pass
    
    @property
    @abstractmethod
    def permissions(self) -> Set[str]:
        """Get set of required permissions."""
        pass
    
    @property
    def name(self) -> str:
        """Get plugin name from metadata."""
        return str(self.metadata.get('name', self.__class__.__name__))

    @property
    def version(self) -> str:
        """Get plugin version from metadata."""
        return str(self.metadata.get('version', '1.0.0'))
    
    @property
    def context(self) -> Optional[IPluginContext]:
        """Get plugin context."""
        return self._context
    
    @property
    def logger(self) -> Any:
        """Get plugin logger."""
        if not self._logger and self._context:
            self._logger = self._context.get_logger(self.name)
        return self._logger or logger
    
    async def start(self) -> None:
        """Start the plugin."""
        if self._started:
            return
        
        self.logger.info(f"Starting plugin: {self.name}")
        await self.on_start()
        self._started = True
        self.logger.info(f"Plugin started: {self.name}")
    
    async def stop(self) -> None:
        """Stop the plugin."""
        if not self._started:
            return
        
        self.logger.info(f"Stopping plugin: {self.name}")
        await self.on_stop()
        self._started = False
        self.logger.info(f"Plugin stopped: {self.name}")
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the plugin."""
        await self.on_configure(config)
    
    async def check_health(self) -> Dict[str, Any]:
        """Check plugin health."""
        health_info = await self.on_health_check()
        
        return {
            'healthy': health_info.get('healthy', True),
            'status': 'running' if self._started else 'stopped',
            'details': {
                'name': self.name,
                'version': self.version,
                'dependencies': self.dependencies,
                'permissions': list(self.permissions),
                **health_info.get('details', {})
            }
        }
    
    async def initialize(self, context: IPluginContext) -> None:
        """Initialize the plugin with context."""
        self._context = context
        self._logger = context.get_logger(self.name)
        
        self.logger.info(f"Initializing plugin: {self.name}")
        await self.on_initialize()
        self.logger.info(f"Plugin initialized: {self.name}")
    
    async def shutdown(self) -> None:
        """Shutdown the plugin."""
        await self.stop()
        await self.on_shutdown()
        self._context = None
        self._logger = None
    
    # Hook methods for subclasses to override
    
    async def on_initialize(self) -> None:
        """Called when plugin is initialized with context."""
        pass
    
    async def on_start(self) -> None:
        """Called when plugin is started."""
        pass
    
    async def on_stop(self) -> None:
        """Called when plugin is stopped."""
        pass
    
    async def on_configure(self, config: Dict[str, Any]) -> None:
        """Called when plugin is configured."""
        pass
    
    async def on_health_check(self) -> Dict[str, Any]:
        """Called during health checks."""
        return {'healthy': True, 'details': {}}
    
    async def on_shutdown(self) -> None:
        """Called when plugin is shutdown."""
        pass
    
    # Utility methods
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        if self._context:
            return self._context.get_config(key, default)
        return default
    
    def get_service(self, service_type: type) -> Any:
        """Get service from application container."""
        if self._context:
            return self._context.get_service(service_type)
        return None
    
    async def emit_event(self, event_name: str, data: Any = None) -> None:
        """Emit an event."""
        if self._context:
            await self._context.emit_event(event_name, data)


class PluginContext(IPluginContext):
    """
    Plugin execution context implementation.
    
    Provides plugins with access to application services and configuration.
    """
    
    def __init__(self, container: Any, config_manager: Any, event_bus: Any) -> None:
        self._container = container
        self._config_manager = config_manager
        self._event_bus = event_bus
    
    def get_service(self, service_type: type) -> Any:
        """Get a service from the application container."""
        try:
            return self._container.resolve(service_type)
        except Exception:
            return None
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        try:
            return self._config_manager.get_value(key, default)
        except Exception:
            return default
    
    async def emit_event(self, event_name: str, data: Any = None) -> None:
        """Emit an event through the event bus."""
        try:
            await self._event_bus.publish(event_name, data)
        except Exception as e:
            logger.error(f"Failed to emit event {event_name}: {e}")
    
    def get_logger(self, name: Optional[str] = None) -> Any:
        """Get a logger for the plugin."""
        return logging.getLogger(f"plugin.{name}" if name else "plugin")
