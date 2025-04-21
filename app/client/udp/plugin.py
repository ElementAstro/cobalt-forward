from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Type, Dict, Any, Optional
import asyncio
from loguru import logger


class PluginPriority(Enum):
    """Plugin priority enumeration"""
    LOWEST = 0   # Lowest priority
    LOW = 10     # Low priority
    NORMAL = 20  # Normal priority
    HIGH = 30    # High priority
    HIGHEST = 40  # Highest priority


class UDPPlugin(ABC):
    """Base interface class for UDP plugins

    Abstract class defining the interface for UDP client plugins.
    All plugins must inherit from this class and implement the required methods.

    Plugins can:
    - Handle packet sending and receiving
    - Implement packet transformation and parsing
    - Handle connection and disconnection events
    - Add custom protocol implementations
    - Provide reliability and ordering guarantees
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Plugin name

        Returns:
            str: Unique plugin name
        """
        pass

    @property
    def description(self) -> str:
        """Plugin description

        Returns:
            str: Description of the plugin's functionality
        """
        return "UDP Plugin"

    @property
    def version(self) -> str:
        """Plugin version

        Returns:
            str: Version number
        """
        return "1.0.0"

    @property
    def priority(self) -> PluginPriority:
        """Plugin priority

        Affects the execution order in the processing chain

        Returns:
            PluginPriority: Plugin priority
        """
        return PluginPriority.NORMAL

    def dependencies(self) -> List[str]:
        """Get plugin dependencies

        Lists the names of other plugins this plugin depends on

        Returns:
            List[str]: List of plugin names
        """
        return []

    async def initialize(self, client: Any) -> None:
        """Initialize the plugin

        Called when the plugin is loaded. Used to register event handlers
        and set up required resources.

        Args:
            client: UDPClient instance
        """
        self.client = client
        logger.info(f"Initialized plugin: {self.name}")

    async def shutdown(self) -> None:
        """Clean up plugin resources

        Called when the plugin is unloaded. Cleans up event handlers
        and releases any resources.
        """
        logger.info(f"Shutting down plugin: {self.name}")

    async def on_connect(self) -> None:
        """Handle connection established event"""
        pass

    async def on_disconnect(self) -> None:
        """Handle connection closed event"""
        pass

    async def pre_send(self, data: bytes) -> bytes:
        """Process data before sending

        Args:
            data: Raw data to be sent

        Returns:
            bytes: Processed data
            If None is returned, sending is prevented
        """
        return data

    async def post_send(self, data: bytes, success: bool) -> None:
        """Process data after sending

        Args:
            data: Data that was sent
            success: Whether the send was successful
        """
        pass

    async def pre_receive(self, data: bytes) -> bytes:
        """Process data before receiving

        Args:
            data: Raw data received

        Returns:
            bytes: Processed data
            If None is returned, further processing is prevented
        """
        return data

    async def post_receive(self, data: bytes) -> None:
        """Process data after receiving

        Args:
            data: Processed received data
        """
        pass

    async def pre_heartbeat(self) -> bool:
        """Process before sending heartbeat

        Returns:
            bool: Whether to allow sending the heartbeat
        """
        return True

    async def post_heartbeat(self, success: bool) -> None:
        """Process after sending heartbeat

        Args:
            success: Whether the heartbeat send was successful
        """
        pass

    async def on_error(self, error: Exception) -> None:
        """Handle error event

        Args:
            error: The exception that occurred
        """
        pass

    async def on_idle(self, idle_time: float) -> None:
        """Handle connection idle event

        Args:
            idle_time: Connection idle time in seconds
        """
        pass

    async def on_packet_loss(self, sequence: int) -> None:
        """Handle packet loss event

        Args:
            sequence: Sequence number of the lost packet
        """
        pass

    async def on_out_of_order(self, expected: int, received: int) -> None:
        """Handle out-of-order packet event

        Args:
            expected: Expected sequence number
            received: Received sequence number
        """
        pass


class UDPPluginManager:
    """UDP Plugin Manager

    Manages the lifecycle and execution chain of plugins for the UDP client.
    """

    def __init__(self):
        self.plugins: Dict[str, UDPPlugin] = {}
        self._sorted_plugins: List[UDPPlugin] = []
        self._initialized = False
        self._client = None

    def register(self, plugin_class: Type[UDPPlugin]) -> bool:
        """Register a plugin class

        Args:
            plugin_class: The UDPPlugin class to register

        Returns:
            bool: True if registration was successful
        """
        try:
            plugin_instance = plugin_class()
            plugin_name = plugin_instance.name

            if plugin_name in self.plugins:
                logger.warning(f"Plugin '{plugin_name}' is already registered")
                return False

            self.plugins[plugin_name] = plugin_instance
            self._sort_plugins()

            logger.info(f"Registered plugin: {plugin_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register plugin: {e}")
            return False

    def unregister(self, plugin_name: str) -> bool:
        """Unregister a plugin

        Args:
            plugin_name: The name of the plugin to unregister

        Returns:
            bool: True if unregistration was successful
        """
        if plugin_name not in self.plugins:
            logger.warning(f"Plugin '{plugin_name}' is not registered")
            return False

        del self.plugins[plugin_name]
        self._sort_plugins()

        logger.info(f"Unregistered plugin: {plugin_name}")
        return True

    def _sort_plugins(self) -> None:
        """Sort plugins based on priority"""
        self._sorted_plugins = sorted(
            self.plugins.values(),
            key=lambda p: p.priority.value,
            reverse=True
        )

    async def initialize(self, client: Any) -> None:
        """Initialize all plugins

        Args:
            client: UDPClient instance
        """
        if self._initialized:
            return

        self._client = client

        # Check dependencies
        for plugin in self._sorted_plugins:
            for dep in plugin.dependencies():
                if dep not in self.plugins:
                    logger.warning(
                        f"Plugin '{plugin.name}' is missing dependency '{dep}'")

        # Initialize plugins with verified dependencies
        for plugin in self._sorted_plugins:
            try:
                await plugin.initialize(client)
            except Exception as e:
                logger.error(
                    f"Failed to initialize plugin '{plugin.name}': {e}")

        self._initialized = True

    async def shutdown(self) -> None:
        """Shutdown all plugins"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.shutdown()
            except Exception as e:
                logger.error(
                    f"Error shutting down plugin '{plugin.name}': {e}")

        self._initialized = False
        self._client = None

    async def run_on_connect(self) -> None:
        """Run the connection event processing chain"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_connect()
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' on_connect: {e}")

    async def run_on_disconnect(self) -> None:
        """Run the disconnection event processing chain"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_disconnect()
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' on_disconnect: {e}")

    async def run_pre_send(self, data: bytes) -> bytes:
        """Run the pre-send processing chain

        Args:
            data: Raw data to be sent

        Returns:
            bytes: Processed data
            If None is returned, sending is prevented
        """
        current_data = data

        for plugin in self._sorted_plugins:
            try:
                current_data = await plugin.pre_send(current_data)
                if current_data is None:
                    logger.debug(
                        f"Data sending cancelled by plugin: {plugin.name}")
                    return None
            except Exception as e:
                logger.error(f"Error in plugin '{plugin.name}' pre_send: {e}")

        return current_data

    async def run_post_send(self, data: bytes, success: bool) -> None:
        """Run the post-send processing chain

        Args:
            data: Data that was sent
            success: Whether the send was successful
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_send(data, success)
            except Exception as e:
                logger.error(f"Error in plugin '{plugin.name}' post_send: {e}")

    async def run_pre_receive(self, data: bytes) -> bytes:
        """Run the pre-receive processing chain

        Args:
            data: Raw data received

        Returns:
            bytes: Processed data
            If None is returned, further processing is prevented
        """
        current_data = data

        for plugin in self._sorted_plugins:
            try:
                current_data = await plugin.pre_receive(current_data)
                if current_data is None:
                    logger.debug(
                        f"Data receive processing cancelled by plugin: {plugin.name}")
                    return None
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' pre_receive: {e}")

        return current_data

    async def run_post_receive(self, data: bytes) -> None:
        """Run the post-receive processing chain

        Args:
            data: Processed received data
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_receive(data)
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' post_receive: {e}")

    async def run_pre_heartbeat(self) -> bool:
        """Run the pre-heartbeat processing chain

        Returns:
            bool: Whether to allow sending the heartbeat
        """
        allow_heartbeat = True

        for plugin in self._sorted_plugins:
            try:
                result = await plugin.pre_heartbeat()
                if not result:
                    logger.debug(
                        f"Heartbeat sending cancelled by plugin: {plugin.name}")
                    allow_heartbeat = False
                    break
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' pre_heartbeat: {e}")

        return allow_heartbeat

    async def run_post_heartbeat(self, success: bool) -> None:
        """Run the post-heartbeat processing chain

        Args:
            success: Whether the heartbeat send was successful
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_heartbeat(success)
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' post_heartbeat: {e}")

    async def run_on_error(self, error: Exception) -> None:
        """Run the error handling chain

        Args:
            error: The exception that occurred
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_error(error)
            except Exception as e:
                logger.error(f"Error in plugin '{plugin.name}' on_error: {e}")

    async def run_on_idle(self, idle_time: float) -> None:
        """Run the connection idle event processing chain

        Args:
            idle_time: Connection idle time in seconds
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_idle(idle_time)
            except Exception as e:
                logger.error(f"Error in plugin '{plugin.name}' on_idle: {e}")

    async def run_on_packet_loss(self, sequence: int) -> None:
        """Run the packet loss event processing chain

        Args:
            sequence: Sequence number of the lost packet
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_packet_loss(sequence)
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' on_packet_loss: {e}")

    async def run_on_out_of_order(self, expected: int, received: int) -> None:
        """Run the out-of-order packet event processing chain

        Args:
            expected: Expected sequence number
            received: Received sequence number
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_out_of_order(expected, received)
            except Exception as e:
                logger.error(
                    f"Error in plugin '{plugin.name}' on_out_of_order: {e}")
