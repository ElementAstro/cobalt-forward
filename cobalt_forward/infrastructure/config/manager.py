"""
Configuration manager for runtime configuration management.

This module provides configuration management capabilities including
hot reloading, validation, and change notifications.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

from ...core.interfaces.lifecycle import IComponent
from .loader import ConfigLoader
from .models import ApplicationConfig

logger = logging.getLogger(__name__)


if WATCHDOG_AVAILABLE:
    from watchdog.events import FileSystemEventHandler

    class ConfigChangeHandler(FileSystemEventHandler):
        """File system event handler for configuration file changes."""

        def __init__(self, config_manager: 'ConfigManager') -> None:
            self._config_manager = config_manager

        def on_modified(self, event: Any) -> None:
            """Handle file modification events."""
            if not event.is_directory:
                file_path = Path(event.src_path)
                if file_path == self._config_manager._config_file_path:
                    logger.info(f"Configuration file changed: {file_path}")
                    asyncio.create_task(self._config_manager._reload_config())
else:
    ConfigChangeHandler = None  # type: ignore


class ConfigManager(IComponent):
    """
    Configuration manager with hot reloading and change notification support.
    
    This class manages the application configuration, provides hot reloading
    capabilities, and notifies observers of configuration changes.
    """
    
    def __init__(self, initial_config: Dict[str, Any]) -> None:
        self._config = ApplicationConfig.from_dict(initial_config)
        self._config_loader = ConfigLoader()
        self._config_file_path: Optional[Path] = None
        self._observers: List[Callable[[ApplicationConfig], None]] = []
        self._file_observer: Optional[Any] = None
        self._hot_reload_enabled = False
        self._started = False
        
        if self._config.config_file_path:
            self._config_file_path = Path(self._config.config_file_path)
    
    @property
    def name(self) -> str:
        """Get component name."""
        return "ConfigManager"
    
    @property
    def version(self) -> str:
        """Get component version."""
        return "1.0.0"
    
    @property
    def config(self) -> ApplicationConfig:
        """Get current configuration."""
        return self._config
    
    async def start(self) -> None:
        """Start the configuration manager."""
        if self._started:
            return
        
        logger.info("Starting configuration manager")
        
        # Start hot reload if enabled and config file exists
        if self._config.debug and self._config_file_path and self._config_file_path.exists():
            await self.enable_hot_reload()
        
        self._started = True
        logger.info("Configuration manager started")
    
    async def stop(self) -> None:
        """Stop the configuration manager."""
        if not self._started:
            return
        
        logger.info("Stopping configuration manager")
        
        await self.disable_hot_reload()
        self._observers.clear()
        
        self._started = False
        logger.info("Configuration manager stopped")
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the component."""
        # Configuration manager configures itself
        pass
    
    async def check_health(self) -> Dict[str, Any]:
        """Check component health."""
        return {
            'healthy': True,
            'status': 'running' if self._started else 'stopped',
            'details': {
                'hot_reload_enabled': self._hot_reload_enabled,
                'config_file': str(self._config_file_path) if self._config_file_path else None,
                'observers_count': len(self._observers),
                'environment': self._config.environment,
                'debug': self._config.debug
            }
        }
    
    async def enable_hot_reload(self) -> None:
        """Enable hot reloading of configuration file."""
        if self._hot_reload_enabled or not self._config_file_path:
            return

        if not WATCHDOG_AVAILABLE:
            logger.warning("Hot reload not available: watchdog package not installed")
            return

        logger.info("Enabling configuration hot reload")

        try:
            self._file_observer = Observer()
            event_handler = ConfigChangeHandler(self)

            # Watch the directory containing the config file
            watch_dir = self._config_file_path.parent
            self._file_observer.schedule(event_handler, str(watch_dir), recursive=False)
            self._file_observer.start()

            self._hot_reload_enabled = True
            logger.info(f"Hot reload enabled for {self._config_file_path}")

        except Exception as e:
            logger.error(f"Failed to enable hot reload: {e}")
            raise
    
    async def disable_hot_reload(self) -> None:
        """Disable hot reloading."""
        if not self._hot_reload_enabled or not self._file_observer:
            return
        
        logger.info("Disabling configuration hot reload")
        
        try:
            self._file_observer.stop()
            self._file_observer.join(timeout=5.0)
            self._file_observer = None
            self._hot_reload_enabled = False
            
            logger.info("Hot reload disabled")
        
        except Exception as e:
            logger.error(f"Error disabling hot reload: {e}")
    
    def register_observer(self, observer: Callable[[ApplicationConfig], None]) -> None:
        """
        Register an observer for configuration changes.
        
        Args:
            observer: Function to call when configuration changes
        """
        if observer not in self._observers:
            self._observers.append(observer)
            logger.debug(f"Registered configuration observer: {observer.__name__}")
    
    def unregister_observer(self, observer: Callable[[ApplicationConfig], None]) -> None:
        """
        Unregister a configuration change observer.
        
        Args:
            observer: Observer function to remove
        """
        if observer in self._observers:
            self._observers.remove(observer)
            logger.debug(f"Unregistered configuration observer: {observer.__name__}")
    
    async def reload_config(self) -> bool:
        """
        Manually reload configuration from file.
        
        Returns:
            True if configuration was successfully reloaded
        """
        return await self._reload_config()
    
    async def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration with new values.
        
        Args:
            updates: Dictionary of configuration updates
        """
        try:
            # Merge updates with current configuration
            current_dict = self._config.to_dict()
            merged_dict = self._merge_configs(current_dict, updates)
            
            # Create new configuration and validate
            new_config = ApplicationConfig.from_dict(merged_dict)
            
            # Update current configuration
            old_config = self._config
            self._config = new_config
            
            # Notify observers
            await self._notify_observers(old_config, new_config)
            
            logger.info("Configuration updated successfully")
        
        except Exception as e:
            logger.error(f"Failed to update configuration: {e}")
            raise
    
    def get_section(self, section: str) -> Any:
        """
        Get a configuration section.
        
        Args:
            section: Section name (e.g., 'tcp', 'websocket')
            
        Returns:
            Configuration section
        """
        return getattr(self._config, section, None)
    
    def get_value(self, path: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            path: Configuration path (e.g., 'tcp.host', 'websocket.port')
            default: Default value if path not found
            
        Returns:
            Configuration value
        """
        try:
            keys = path.split('.')
            value = self._config
            
            for key in keys:
                value = getattr(value, key)
            
            return value
        
        except (AttributeError, KeyError):
            return default
    
    async def _reload_config(self) -> bool:
        """Internal method to reload configuration from file."""
        if not self._config_file_path or not self._config_file_path.exists():
            logger.warning("Cannot reload: configuration file not found")
            return False
        
        try:
            logger.info(f"Reloading configuration from {self._config_file_path}")
            
            # Load new configuration
            new_config = self._config_loader.load_config(str(self._config_file_path))
            
            # Validate new configuration
            old_config = self._config
            self._config = new_config
            
            # Notify observers
            await self._notify_observers(old_config, new_config)
            
            logger.info("Configuration reloaded successfully")
            return True
        
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            return False
    
    async def _notify_observers(self, old_config: ApplicationConfig, new_config: ApplicationConfig) -> None:
        """Notify all observers of configuration changes."""
        for observer in self._observers:
            try:
                if asyncio.iscoroutinefunction(observer):
                    await observer(new_config)
                else:
                    observer(new_config)
            except Exception as e:
                logger.error(f"Error in configuration observer {observer.__name__}: {e}")
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge two configuration dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
