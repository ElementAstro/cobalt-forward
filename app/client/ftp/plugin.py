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


class FTPPlugin(ABC):
    """
    Abstract base class for FTP plugins.

    This abstract class defines the interface for FTP client plugins.
    All plugins must inherit from this class and implement the required methods.

    Plugins can:
    - Handle upload and download operations
    - Extend file listing and directory operation functionalities
    - Add custom commands and operations
    - Implement file conversion and preprocessing
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Unique identifier for the plugin.

        Returns:
            str: The name of the plugin.
        """
        pass

    @property
    def description(self) -> str:
        """
        Description of the plugin.

        Returns:
            str: A description of the plugin's functionality.
        """
        return "FTP Plugin"

    @property
    def version(self) -> str:
        """
        Version of the plugin.

        Returns:
            str: Version string (semantic versioning recommended).
        """
        return "1.0.0"

    @property
    def priority(self) -> PluginPriority:
        """
        Execution priority of the plugin.

        Returns:
            PluginPriority: The priority level.
        """
        return PluginPriority.MEDIUM

    def dependencies(self) -> List[str]:
        """
        List of plugin dependencies.

        Returns:
            List[str]: Names of required plugins.
        """
        return []

    async def initialize(self, client: Any) -> None:
        """
        Initialize the plugin.

        Called when the plugin is loaded. Used to register event handlers
        and set up required resources.

        Args:
            client: The FTPClient instance.
        """
        self.client = client
        logger.info(f"Initialized plugin: {self.name}")

    async def shutdown(self) -> None:
        """
        Clean up plugin resources.

        Called when the plugin is unloaded. Clean up event handlers
        and release any resources.
        """
        logger.info(f"Shutting down plugin: {self.name}")

    async def pre_list_directory(self, path: str) -> str:
        """
        Pre-processing for directory listing operation.

        Args:
            path: The original directory path.

        Returns:
            str: The processed path or the original path.
        """
        return path

    async def post_list_directory(self, path: str, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Post-processing for directory listing operation.

        Args:
            path: The directory path.
            files: The list of files result.

        Returns:
            List[Dict[str, Any]]: The processed list of files.
        """
        return files

    async def pre_upload(self, local_path: str, remote_path: str) -> tuple[str, str]:
        """
        Pre-processing before uploading a file.

        Args:
            local_path: The local file path.
            remote_path: The remote file path.

        Returns:
            tuple[str, str]: The processed local and remote paths.
        """
        return local_path, remote_path

    async def post_upload(self, local_path: str, remote_path: str, success: bool) -> None:
        """
        Post-processing after uploading a file.

        Args:
            local_path: The local file path.
            remote_path: The remote file path.
            success: Whether the upload was successful.
        """
        pass

    async def pre_download(self, remote_path: str, local_path: str) -> tuple[str, str]:
        """
        Pre-processing before downloading a file.

        Args:
            remote_path: The remote file path.
            local_path: The local file path.

        Returns:
            tuple[str, str]: The processed remote and local paths.
        """
        return remote_path, local_path

    async def post_download(self, remote_path: str, local_path: str, success: bool) -> None:
        """
        Post-processing after downloading a file.

        Args:
            remote_path: The remote file path.
            local_path: The local file path.
            success: Whether the download was successful.
        """
        pass

    async def pre_mkdir(self, path: str) -> str:
        """
        Pre-processing before creating a directory.

        Args:
            path: The directory path.

        Returns:
            str: The processed directory path.
        """
        return path

    async def post_mkdir(self, path: str, success: bool) -> None:
        """
        Post-processing after creating a directory.

        Args:
            path: The directory path.
            success: Whether the operation was successful.
        """
        pass

    async def on_error(self, operation: str, error: Exception) -> None:
        """
        Handle operation error events.

        Args:
            operation: The name of the operation where the error occurred.
            error: The exception that occurred.
        """
        pass


class FTPPluginManager:
    """
    FTP Plugin Manager.

    Manages the lifecycle and execution chain of plugins for the FTP client.
    """

    def __init__(self):
        self.plugins: Dict[str, FTPPlugin] = {}
        self._sorted_plugins: List[FTPPlugin] = []
        self._initialized = False
        self._client = None

    def register(self, plugin_class: Type[FTPPlugin]) -> bool:
        """
        Register a plugin class.

        Args:
            plugin_class: The FTPPlugin class to register.

        Returns:
            bool: True if registration was successful.
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
        """
        Unregister a plugin by name.

        Args:
            plugin_name: The name of the plugin to unregister.

        Returns:
            bool: True if unregistration was successful.
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
        """
        Sort plugins by priority.

        Ensures that plugins are executed in the correct order.
        """
        self._sorted_plugins = sorted(
            self.plugins.values(),
            key=lambda p: p.priority.value,
            reverse=True  # Higher priority first
        )

    async def initialize(self, client: Any) -> None:
        """
        Initialize all plugins.

        Args:
            client: The FTPClient instance.
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

        # Initialize plugins with validated dependencies
        for plugin in self._sorted_plugins:
            try:
                await plugin.initialize(client)
            except Exception as e:
                logger.error(
                    f"Failed to initialize plugin '{plugin.name}': {e}")

        self._initialized = True

    async def shutdown(self) -> None:
        """
        Shutdown all plugins.
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.shutdown()
            except Exception as e:
                logger.error(
                    f"Error shutting down plugin '{plugin.name}': {e}")

        self._initialized = False
        self._client = None

    async def run_pre_list_directory(self, path: str) -> str:
        """
        Run the pre-processing chain for directory listing.

        Args:
            path: The original directory path.

        Returns:
            str: The processed path.
        """
        processed_path = path
        for plugin in self._sorted_plugins:
            try:
                processed_path = await plugin.pre_list_directory(processed_path)
            except Exception as e:
                logger.error(
                    f"Error in pre_list_directory of plugin '{plugin.name}': {e}")

        return processed_path

    async def run_post_list_directory(self, path: str, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Run the post-processing chain for directory listing.

        Args:
            path: The directory path.
            files: The list of files result.

        Returns:
            List[Dict[str, Any]]: The processed list of files.
        """
        processed_files = files
        for plugin in self._sorted_plugins:
            try:
                processed_files = await plugin.post_list_directory(path, processed_files)
                if processed_files is None:
                    logger.debug(
                        f"File list processing cancelled by plugin: {plugin.name}")
                    break
            except Exception as e:
                logger.error(
                    f"Error in post_list_directory of plugin '{plugin.name}': {e}")

        return processed_files

    async def run_pre_upload(self, local_path: str, remote_path: str) -> tuple[str, str]:
        """
        Run the pre-processing chain for file upload.

        Args:
            local_path: The local file path.
            remote_path: The remote file path.

        Returns:
            tuple[str, str]: The processed local and remote paths.
        """
        current_local = local_path
        current_remote = remote_path

        for plugin in self._sorted_plugins:
            try:
                current_local, current_remote = await plugin.pre_upload(current_local, current_remote)
            except Exception as e:
                logger.error(
                    f"Error in pre_upload of plugin '{plugin.name}': {e}")

        return current_local, current_remote

    async def run_post_upload(self, local_path: str, remote_path: str, success: bool) -> None:
        """
        Run the post-processing chain for file upload.

        Args:
            local_path: The local file path.
            remote_path: The remote file path.
            success: Whether the upload was successful.
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_upload(local_path, remote_path, success)
            except Exception as e:
                logger.error(
                    f"Error in post_upload of plugin '{plugin.name}': {e}")

    async def run_pre_download(self, remote_path: str, local_path: str) -> tuple[str, str]:
        """
        Run the pre-processing chain for file download.

        Args:
            remote_path: The remote file path.
            local_path: The local file path.

        Returns:
            tuple[str, str]: The processed remote and local paths.
        """
        current_remote = remote_path
        current_local = local_path

        for plugin in self._sorted_plugins:
            try:
                current_remote, current_local = await plugin.pre_download(current_remote, current_local)
            except Exception as e:
                logger.error(
                    f"Error in pre_download of plugin '{plugin.name}': {e}")

        return current_remote, current_local

    async def run_post_download(self, remote_path: str, local_path: str, success: bool) -> None:
        """
        Run the post-processing chain for file download.

        Args:
            remote_path: The remote file path.
            local_path: The local file path.
            success: Whether the download was successful.
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_download(remote_path, local_path, success)
            except Exception as e:
                logger.error(
                    f"Error in post_download of plugin '{plugin.name}': {e}")

    async def run_pre_mkdir(self, path: str) -> str:
        """
        Run the pre-processing chain for directory creation.

        Args:
            path: The directory path.

        Returns:
            str: The processed directory path.
        """
        processed_path = path
        for plugin in self._sorted_plugins:
            try:
                processed_path = await plugin.pre_mkdir(processed_path)
            except Exception as e:
                logger.error(
                    f"Error in pre_mkdir of plugin '{plugin.name}': {e}")

        return processed_path

    async def run_post_mkdir(self, path: str, success: bool) -> None:
        """
        Run the post-processing chain for directory creation.

        Args:
            path: The directory path.
            success: Whether the operation was successful.
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_mkdir(path, success)
            except Exception as e:
                logger.error(
                    f"Error in post_mkdir of plugin '{plugin.name}': {e}")

    async def run_on_error(self, operation: str, error: Exception) -> None:
        """
        Run the error handling chain.

        Args:
            operation: The name of the operation where the error occurred.
            error: The exception that occurred.
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_error(operation, error)
            except Exception as e:
                logger.error(
                    f"Error in on_error of plugin '{plugin.name}': {e}")
