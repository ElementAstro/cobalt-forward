from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable, Union, Type
from enum import Enum, auto
import asyncio
from loguru import logger


class PluginPriority(Enum):
    """Plugin execution priority levels"""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()


class WebSocketPlugin(ABC):
    """Base WebSocket Plugin Interface
    
    Abstract class defining the interface for WebSocket client plugins.
    All plugins must inherit from this class and implement required methods.
    
    Plugins can:
    - Process incoming and outgoing messages
    - Handle connection events (connect, disconnect, etc.)
    - Extend client functionality
    - Add custom commands
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Plugin unique identifier
        
        Returns:
            str: Plugin name
        """
        pass

    @property
    def description(self) -> str:
        """Plugin description
        
        Returns:
            str: Description of plugin functionality
        """
        return "WebSocket plugin"

    @property
    def version(self) -> str:
        """Plugin version
        
        Returns:
            str: Version string (semver recommended)
        """
        return "1.0.0"

    @property
    def priority(self) -> PluginPriority:
        """Plugin execution priority
        
        Returns:
            PluginPriority: Priority level
        """
        return PluginPriority.MEDIUM

    def dependencies(self) -> List[str]:
        """List of plugin dependencies
        
        Returns:
            List[str]: Names of required plugins
        """
        return []

    async def initialize(self, client: Any) -> None:
        """Initialize plugin
        
        Called when the plugin is loaded. Use this to register event handlers
        and set up required resources.
        
        Args:
            client: WebSocketClient instance
        """
        self.client = client
        logger.info(f"Initialized plugin: {self.name}")

    async def shutdown(self) -> None:
        """Cleanup plugin resources
        
        Called when the plugin is being unloaded. Clean up event handlers
        and release any resources.
        """
        logger.info(f"Shutting down plugin: {self.name}")

    async def on_connect(self) -> None:
        """Handle connection established event"""
        pass

    async def on_disconnect(self) -> None:
        """Handle connection closed event"""
        pass

    async def on_error(self, error: Exception) -> None:
        """Handle error events
        
        Args:
            error: Exception that occurred
        """
        pass

    async def pre_send(self, message: Any) -> Any:
        """Pre-process outgoing messages
        
        Modify or filter outgoing messages before they are sent.
        Return None to prevent message from being sent.
        
        Args:
            message: Original outgoing message
            
        Returns:
            Any: Processed message or None to cancel sending
        """
        return message

    async def post_receive(self, message: Any) -> Any:
        """Process incoming messages
        
        Modify or filter incoming messages after they are received.
        Return None to prevent message from being processed further.
        
        Args:
            message: Original incoming message
            
        Returns:
            Any: Processed message or None to stop processing chain
        """
        return message


class PluginManager:
    """WebSocket Plugin Manager
    
    Manages plugin lifecycle and execution chain for WebSocket clients.
    """

    def __init__(self):
        self.plugins: Dict[str, WebSocketPlugin] = {}
        self._sorted_plugins: List[WebSocketPlugin] = []
        self._initialized = False
        self._client = None

    def register(self, plugin_class: Type[WebSocketPlugin]) -> bool:
        """Register a plugin class
        
        Args:
            plugin_class: WebSocketPlugin class to register
            
        Returns:
            bool: True if registration was successful
        """
        try:
            plugin_instance = plugin_class()
            plugin_name = plugin_instance.name
            
            if plugin_name in self.plugins:
                logger.warning(f"Plugin '{plugin_name}' already registered")
                return False
                
            self.plugins[plugin_name] = plugin_instance
            self._sort_plugins()
            
            logger.info(f"Registered plugin: {plugin_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register plugin: {e}")
            return False

    def unregister(self, plugin_name: str) -> bool:
        """Unregister a plugin by name
        
        Args:
            plugin_name: Name of the plugin to unregister
            
        Returns:
            bool: True if successfully unregistered
        """
        if plugin_name not in self.plugins:
            logger.warning(f"Plugin '{plugin_name}' not found")
            return False
            
        asyncio.create_task(self.plugins[plugin_name].shutdown())
        del self.plugins[plugin_name]
        self._sort_plugins()
        
        logger.info(f"Unregistered plugin: {plugin_name}")
        return True

    def _sort_plugins(self) -> None:
        """Sort plugins by priority
        
        Ensures plugins are executed in the correct order.
        """
        self._sorted_plugins = sorted(
            self.plugins.values(),
            key=lambda p: p.priority.value,
            reverse=True  # Higher priority first
        )

    async def initialize(self, client: Any) -> None:
        """Initialize all plugins
        
        Args:
            client: WebSocketClient instance
        """
        if self._initialized:
            return
            
        self._client = client
        
        # Check dependencies
        for plugin in self._sorted_plugins:
            for dep in plugin.dependencies():
                if dep not in self.plugins:
                    logger.warning(f"Missing dependency '{dep}' for plugin '{plugin.name}'")

        # Initialize plugins with validated dependencies
        for plugin in self._sorted_plugins:
            try:
                await plugin.initialize(client)
            except Exception as e:
                logger.error(f"Failed to initialize plugin '{plugin.name}': {e}")
                
        self._initialized = True

    async def shutdown(self) -> None:
        """Shutdown all plugins"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down plugin '{plugin.name}': {e}")
                
        self._initialized = False
        self._client = None

    async def pre_send_chain(self, message: Any) -> Any:
        """Process outgoing message through plugin chain
        
        Args:
            message: Original message
            
        Returns:
            Any: Processed message or None if cancelled
        """
        processed_message = message
        for plugin in self._sorted_plugins:
            try:
                processed_message = await plugin.pre_send(processed_message)
                if processed_message is None:
                    logger.debug(f"Message sending cancelled by plugin: {plugin.name}")
                    break
            except Exception as e:
                logger.error(f"Error in pre_send for plugin '{plugin.name}': {e}")
                
        return processed_message

    async def post_receive_chain(self, message: Any) -> Any:
        """Process incoming message through plugin chain
        
        Args:
            message: Original message
            
        Returns:
            Any: Processed message or None if cancelled
        """
        processed_message = message
        for plugin in self._sorted_plugins:
            try:
                processed_message = await plugin.post_receive(processed_message)
                if processed_message is None:
                    logger.debug(f"Message processing cancelled by plugin: {plugin.name}")
                    break
            except Exception as e:
                logger.error(f"Error in post_receive for plugin '{plugin.name}': {e}")
                
        return processed_message

    async def on_connect(self) -> None:
        """Notify all plugins about connection event"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_connect()
            except Exception as e:
                logger.error(f"Error in on_connect for plugin '{plugin.name}': {e}")

    async def on_disconnect(self) -> None:
        """Notify all plugins about disconnection event"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_disconnect()
            except Exception as e:
                logger.error(f"Error in on_disconnect for plugin '{plugin.name}': {e}")

    async def on_error(self, error: Exception) -> None:
        """Notify all plugins about error event
        
        Args:
            error: Exception that occurred
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_error(error)
            except Exception as e:
                logger.error(f"Error in on_error for plugin '{plugin.name}': {e}")