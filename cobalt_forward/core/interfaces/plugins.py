"""
Plugin system interfaces for extensible architecture.

These interfaces define the contracts for plugin loading, management,
and lifecycle operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Type, TypeVar
from .lifecycle import IComponent

T = TypeVar('T')


class IPlugin(IComponent):
    """Interface for plugin implementations."""

    @property
    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """
        Get plugin metadata.

        Returns:
            Dictionary containing plugin information like:
            - name: Plugin name
            - version: Plugin version
            - description: Plugin description
            - author: Plugin author
            - dependencies: List of required plugins
            - permissions: List of required permissions
        """
        pass

    @property
    @abstractmethod
    def dependencies(self) -> List[str]:
        """
        Get list of plugin dependencies.

        Returns:
            List of plugin names this plugin depends on
        """
        pass

    @property
    @abstractmethod
    def permissions(self) -> Set[str]:
        """
        Get set of permissions required by this plugin.

        Returns:
            Set of permission names
        """
        pass

    @abstractmethod
    async def initialize(self, context: 'IPluginContext') -> None:
        """
        Initialize the plugin with the given context.

        Args:
            context: Plugin execution context

        Raises:
            PluginInitializationException: If initialization fails
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """
        Shutdown the plugin and clean up resources.

        Raises:
            PluginShutdownException: If shutdown fails
        """
        pass


class IPluginContext(ABC):
    """Interface for plugin execution context."""

    @abstractmethod
    def get_service(self, service_type: Type[T]) -> Optional[T]:
        """
        Get a service from the application container.

        Args:
            service_type: Type of service to retrieve

        Returns:
            Service instance or None if not found
        """
        pass

    @abstractmethod
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value for the plugin.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        pass

    @abstractmethod
    async def emit_event(self, event_name: str, data: Any = None) -> None:
        """
        Emit an event through the event bus.

        Args:
            event_name: Name of event to emit
            data: Event data
        """
        pass

    @abstractmethod
    def get_logger(self, name: Optional[str] = None) -> Any:
        """
        Get a logger for the plugin.

        Args:
            name: Logger name (defaults to plugin name)

        Returns:
            Logger instance
        """
        pass


class IPluginManager(IComponent):
    """Interface for plugin manager implementations."""

    @abstractmethod
    async def load_plugin(self, plugin_path: str) -> bool:
        """
        Load a plugin from the given path.

        Args:
            plugin_path: Path to plugin file or directory

        Returns:
            True if plugin loaded successfully

        Raises:
            PluginLoadException: If plugin loading fails
        """
        pass

    @abstractmethod
    async def unload_plugin(self, plugin_name: str) -> bool:
        """
        Unload a plugin by name.

        Args:
            plugin_name: Name of plugin to unload

        Returns:
            True if plugin unloaded successfully
        """
        pass

    @abstractmethod
    async def reload_plugin(self, plugin_name: str) -> bool:
        """
        Reload a plugin by name.

        Args:
            plugin_name: Name of plugin to reload

        Returns:
            True if plugin reloaded successfully
        """
        pass

    @abstractmethod
    def get_plugin(self, plugin_name: str) -> Optional[IPlugin]:
        """
        Get a loaded plugin by name.

        Args:
            plugin_name: Name of plugin

        Returns:
            Plugin instance or None if not found
        """
        pass

    @abstractmethod
    def get_all_plugins(self) -> Dict[str, IPlugin]:
        """
        Get all loaded plugins.

        Returns:
            Dictionary mapping plugin names to plugin instances
        """
        pass

    @abstractmethod
    async def discover_plugins(self, directory: str) -> List[str]:
        """
        Discover plugins in the given directory.

        Args:
            directory: Directory to search for plugins

        Returns:
            List of discovered plugin paths
        """
        pass

    @abstractmethod
    def is_plugin_loaded(self, plugin_name: str) -> bool:
        """
        Check if a plugin is loaded.

        Args:
            plugin_name: Name of plugin to check

        Returns:
            True if plugin is loaded
        """
        pass

    @abstractmethod
    async def get_plugin_dependencies(self, plugin_name: str) -> List[str]:
        """
        Get dependencies for a plugin.

        Args:
            plugin_name: Name of plugin

        Returns:
            List of dependency plugin names
        """
        pass
