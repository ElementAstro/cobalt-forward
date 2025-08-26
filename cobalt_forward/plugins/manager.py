"""
Plugin manager implementation for loading and managing plugins.

This module provides plugin discovery, loading, dependency resolution,
and lifecycle management capabilities.
"""

import importlib.util
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core.interfaces.plugins import IPluginManager, IPlugin
from ..core.interfaces.lifecycle import IComponent
from .base import PluginContext

logger = logging.getLogger(__name__)


class PluginManager(IPluginManager):
    """
    Plugin manager implementation with dependency resolution and sandboxing.
    
    Manages the complete plugin lifecycle including discovery, loading,
    dependency resolution, and execution.
    """
    
    def __init__(self, config: Dict[str, Any], container: Any) -> None:
        self._config = config
        self._container = container
        self._plugins: Dict[str, IPlugin] = {}
        self._plugin_paths: Dict[str, str] = {}
        self._plugin_context: Optional[PluginContext] = None
        self._running = False
        
        # Plugin configuration
        self._plugin_directory = config.get('plugin_directory', 'plugins')
        self._auto_load = config.get('auto_load', True)
        self._sandbox_enabled = config.get('sandbox_enabled', True)
        self._allowed_modules = set(config.get('allowed_modules', []))
        self._blocked_modules = set(config.get('blocked_modules', []))
    
    @property
    def name(self) -> str:
        """Get component name."""
        return "PluginManager"
    
    @property
    def version(self) -> str:
        """Get component version."""
        return "1.0.0"
    
    async def start(self) -> None:
        """Start the plugin manager."""
        if self._running:
            return
        
        logger.info("Starting plugin manager")
        
        # Create plugin context
        try:
            config_manager = self._container.resolve("ConfigManager")
            event_bus = self._container.resolve("IEventBus")
            self._plugin_context = PluginContext(self._container, config_manager, event_bus)
        except Exception as e:
            logger.warning(f"Failed to create plugin context: {e}")
            self._plugin_context = None
        
        # Auto-load plugins if enabled
        if self._auto_load:
            await self._auto_load_plugins()
        
        self._running = True
        logger.info("Plugin manager started successfully")
    
    async def stop(self) -> None:
        """Stop the plugin manager."""
        if not self._running:
            return
        
        logger.info("Stopping plugin manager...")
        
        # Stop all plugins
        for plugin_name in list(self._plugins.keys()):
            await self.unload_plugin(plugin_name)
        
        self._running = False
        logger.info("Plugin manager stopped")
    
    async def configure(self, config: Dict[str, Any]) -> None:
        """Configure the plugin manager."""
        self._config.update(config)
        self._plugin_directory = config.get('plugin_directory', self._plugin_directory)
        self._auto_load = config.get('auto_load', self._auto_load)
    
    async def check_health(self) -> Dict[str, Any]:
        """Check plugin manager health."""
        plugin_health = {}
        overall_healthy = True
        
        for name, plugin in self._plugins.items():
            try:
                health = await plugin.check_health()
                plugin_health[name] = health
                if not health.get('healthy', True):
                    overall_healthy = False
            except Exception as e:
                plugin_health[name] = {
                    'healthy': False,
                    'error': str(e)
                }
                overall_healthy = False
        
        return {
            'healthy': overall_healthy,
            'status': 'running' if self._running else 'stopped',
            'details': {
                'loaded_plugins': len(self._plugins),
                'plugin_directory': self._plugin_directory,
                'auto_load': self._auto_load,
                'sandbox_enabled': self._sandbox_enabled,
                'plugins': plugin_health
            }
        }
    
    async def load_plugin(self, plugin_path: str) -> bool:
        """Load a plugin from the given path."""
        try:
            logger.info(f"Loading plugin from: {plugin_path}")
            
            # Load plugin module
            plugin_module = await self._load_plugin_module(plugin_path)
            
            # Find plugin class
            plugin_class = self._find_plugin_class(plugin_module)
            if not plugin_class:
                logger.error(f"No plugin class found in {plugin_path}")
                return False
            
            # Create plugin instance
            plugin = plugin_class()
            plugin_name = plugin.name
            
            # Check dependencies
            if not await self._check_dependencies(plugin):
                logger.error(f"Plugin dependencies not satisfied: {plugin_name}")
                return False
            
            # Initialize plugin
            if self._plugin_context:
                await plugin.initialize(self._plugin_context)
            
            # Start plugin
            await plugin.start()
            
            # Register plugin
            self._plugins[plugin_name] = plugin
            self._plugin_paths[plugin_name] = plugin_path
            
            logger.info(f"Plugin loaded successfully: {plugin_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to load plugin from {plugin_path}: {e}")
            return False
    
    async def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a plugin by name."""
        if plugin_name not in self._plugins:
            return False
        
        try:
            logger.info(f"Unloading plugin: {plugin_name}")
            
            plugin = self._plugins[plugin_name]
            
            # Stop and shutdown plugin
            await plugin.shutdown()
            
            # Remove from registry
            del self._plugins[plugin_name]
            self._plugin_paths.pop(plugin_name, None)
            
            logger.info(f"Plugin unloaded successfully: {plugin_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to unload plugin {plugin_name}: {e}")
            return False
    
    async def reload_plugin(self, plugin_name: str) -> bool:
        """Reload a plugin by name."""
        if plugin_name not in self._plugins:
            return False
        
        plugin_path = self._plugin_paths.get(plugin_name)
        if not plugin_path:
            return False
        
        # Unload and reload
        if await self.unload_plugin(plugin_name):
            return await self.load_plugin(plugin_path)
        
        return False
    
    def get_plugin(self, plugin_name: str) -> Optional[IPlugin]:
        """Get a loaded plugin by name."""
        return self._plugins.get(plugin_name)
    
    def get_all_plugins(self) -> Dict[str, IPlugin]:
        """Get all loaded plugins."""
        return self._plugins.copy()
    
    async def discover_plugins(self, directory: str) -> List[str]:
        """Discover plugins in the given directory."""
        plugin_paths: List[str] = []
        plugin_dir = Path(directory)
        
        if not plugin_dir.exists():
            logger.warning(f"Plugin directory does not exist: {directory}")
            return plugin_paths
        
        # Look for Python files
        for file_path in plugin_dir.rglob("*.py"):
            if file_path.name.startswith("__"):
                continue
            
            plugin_paths.append(str(file_path))
        
        logger.info(f"Discovered {len(plugin_paths)} potential plugins in {directory}")
        return plugin_paths
    
    def is_plugin_loaded(self, plugin_name: str) -> bool:
        """Check if a plugin is loaded."""
        return plugin_name in self._plugins
    
    async def get_plugin_dependencies(self, plugin_name: str) -> List[str]:
        """Get dependencies for a plugin."""
        plugin = self._plugins.get(plugin_name)
        if plugin:
            return plugin.dependencies
        return []
    
    async def _auto_load_plugins(self) -> None:
        """Auto-load plugins from the plugin directory."""
        plugin_paths = await self.discover_plugins(self._plugin_directory)
        
        for plugin_path in plugin_paths:
            await self.load_plugin(plugin_path)
    
    async def _load_plugin_module(self, plugin_path: str) -> Any:
        """Load a plugin module from file."""
        spec = importlib.util.spec_from_file_location("plugin", plugin_path)
        if not spec or not spec.loader:
            raise ImportError(f"Cannot load plugin from {plugin_path}")
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        return module
    
    def _find_plugin_class(self, module: Any) -> Optional[type]:
        """Find the plugin class in a module."""
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            
            if (isinstance(attr, type) and 
                issubclass(attr, IPlugin) and 
                attr is not IPlugin):
                return attr
        
        return None
    
    async def _check_dependencies(self, plugin: IPlugin) -> bool:
        """Check if plugin dependencies are satisfied."""
        for dependency in plugin.dependencies:
            if not self.is_plugin_loaded(dependency):
                logger.warning(f"Plugin dependency not loaded: {dependency}")
                return False
        
        return True
