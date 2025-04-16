"""
Plugin manager for loading, unloading and managing plugins.

This module provides the primary interface for the plugin system,
including plugin discovery, lifecycle management, dependency injection,
and versioning.
"""
import os
import sys
import importlib
import inspect
import logging
import asyncio
import time
import traceback
import json
from typing import Dict, List, Any, Optional, Set, Tuple, Callable, Union, Type
from pathlib import Path
import concurrent.futures
import threading

from .base import Plugin
from .models import PluginMetadata, PluginState
from .permissions import PermissionManager
from .function import FunctionRegistry
from .event import EventBus
from .sandbox import PluginSandbox

logger = logging.getLogger(__name__)


class PluginLoadError(Exception):
    """Exception raised when a plugin fails to load"""
    pass


class PluginDependencyError(Exception):
    """Exception raised when plugin dependencies cannot be satisfied"""
    pass


class PluginVersionError(Exception):
    """Exception raised when plugin versions are incompatible"""
    pass


class PluginManager:
    """
    Plugin manager responsible for loading, unloading, and managing plugins
    """
    
    def __init__(self, 
                 plugin_dir: str = "plugins", 
                 config_dir: str = "config/plugins",
                 event_bus: Optional[EventBus] = None,
                 function_registry: Optional[FunctionRegistry] = None,
                 permission_manager: Optional[PermissionManager] = None):
        """
        Initialize plugin manager
        
        Args:
            plugin_dir: Directory containing plugins
            config_dir: Directory containing plugin configurations
            event_bus: Event bus instance for plugin events
            function_registry: Function registry for plugin functions
            permission_manager: Permission manager for plugin permissions
        """
        self.plugin_dir = os.path.abspath(plugin_dir)
        self.config_dir = os.path.abspath(config_dir)
        
        # Plugin registry
        self.plugins: Dict[str, Plugin] = {}
        self.plugin_metadata: Dict[str, PluginMetadata] = {}
        self.plugin_paths: Dict[str, str] = {}
        self.plugin_modules: Dict[str, Any] = {}
        self.plugin_configs: Dict[str, Dict[str, Any]] = {}
        self.plugin_load_order: List[str] = []
        
        # Plugin dependencies
        self.plugin_dependencies: Dict[str, Set[str]] = {}
        self.plugin_dependents: Dict[str, Set[str]] = {}
        
        # Component references
        self.event_bus = event_bus
        self.function_registry = function_registry
        self.permission_manager = permission_manager or PermissionManager()
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Thread pool for background tasks
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=4, 
            thread_name_prefix="plugin-manager"
        )
        
        # Shared resources for plugins
        self.shared_resources: Dict[str, Any] = {}
        
        # Plugin sandbox
        self.sandbox = PluginSandbox()

    async def start(self) -> None:
        """Start the plugin manager and initialize built-in plugins"""
        logger.info("Starting plugin manager")
        await self.discover_plugins()
        await self.load_plugin_configs()
        await self.load_enabled_plugins()
        
        if self.event_bus:
            await self.event_bus.emit("plugin_manager.started", {
                "plugin_count": len(self.plugins),
                "enabled_plugins": [p for p in self.plugins if self.plugins[p].state == PluginState.ACTIVE]
            })
            
        logger.info(f"Plugin manager started with {len(self.plugins)} plugins")
    
    async def stop(self) -> None:
        """Stop the plugin manager and unload all plugins"""
        logger.info("Stopping plugin manager")
        
        # Unload plugins in reverse load order
        plugins_to_unload = list(reversed(self.plugin_load_order))
        for plugin_name in plugins_to_unload:
            await self.unload_plugin(plugin_name)
            
        # Shutdown thread pool
        self.thread_pool.shutdown(wait=False)
        
        if self.event_bus:
            await self.event_bus.emit("plugin_manager.stopped", {})
            
        logger.info("Plugin manager stopped")
    
    async def discover_plugins(self) -> Dict[str, PluginMetadata]:
        """
        Discover available plugins and their metadata
        
        Returns:
            Dictionary mapping plugin names to metadata
        """
        logger.info(f"Discovering plugins in {self.plugin_dir}")
        async with self._lock:
            discovered_plugins = {}
            plugin_files = []
            
            # Ensure plugin directory exists
            os.makedirs(self.plugin_dir, exist_ok=True)
            
            # Find potential plugin files
            for root, dirs, files in os.walk(self.plugin_dir):
                for file in files:
                    if file == "__init__.py" or file.endswith(".py"):
                        plugin_files.append(os.path.join(root, file))
            
            # Import each potential plugin
            for plugin_file in plugin_files:
                try:
                    # Convert file path to module path
                    rel_path = os.path.relpath(plugin_file, start=os.path.dirname(self.plugin_dir))
                    module_path = rel_path.replace(os.path.sep, ".").replace(".py", "")
                    
                    # Skip __pycache__ and similar directories
                    if "__pycache__" in module_path:
                        continue
                        
                    # Import module
                    module = importlib.import_module(module_path)
                    
                    # Look for plugin class
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        
                        # Check if it's a plugin class (but not Plugin itself)
                        if (inspect.isclass(attr) and 
                            issubclass(attr, Plugin) and 
                            attr is not Plugin and 
                            hasattr(attr, 'initialize')):
                            
                            # Create instance to get metadata
                            plugin_instance = attr()
                            
                            if hasattr(plugin_instance, 'metadata') and plugin_instance.metadata:
                                metadata = plugin_instance.metadata
                                plugin_name = metadata.name
                                
                                # Store plugin info
                                discovered_plugins[plugin_name] = metadata
                                self.plugin_metadata[plugin_name] = metadata
                                self.plugin_paths[plugin_name] = plugin_file
                                self.plugin_modules[plugin_name] = module
                                
                                # Store dependencies
                                self.plugin_dependencies[plugin_name] = set(metadata.dependencies or [])
                                
                                logger.info(f"Discovered plugin: {plugin_name} v{metadata.version}")
                
                except Exception as e:
                    logger.error(f"Error discovering plugin in {plugin_file}: {e}")
                    logger.debug(f"Plugin discovery error details: {traceback.format_exc()}")
            
            # Build reverse dependency map
            for plugin_name, deps in self.plugin_dependencies.items():
                for dep_name in deps:
                    if dep_name not in self.plugin_dependents:
                        self.plugin_dependents[dep_name] = set()
                    self.plugin_dependents[dep_name].add(plugin_name)
            
            return discovered_plugins
    
    async def load_plugin_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Load configurations for all discovered plugins
        
        Returns:
            Dictionary of plugin configurations
        """
        logger.info("Loading plugin configurations")
        async with self._lock:
            # Ensure config directory exists
            os.makedirs(self.config_dir, exist_ok=True)
            
            # Load each plugin's config
            for plugin_name, metadata in self.plugin_metadata.items():
                try:
                    config_file = os.path.join(self.config_dir, f"{plugin_name}.json")
                    
                    # Create default config if it doesn't exist
                    if not os.path.exists(config_file):
                        default_config = metadata.config_schema.get("default", {}) if hasattr(metadata, "config_schema") else {}
                        os.makedirs(os.path.dirname(config_file), exist_ok=True)
                        with open(config_file, 'w') as f:
                            json.dump(default_config, f, indent=2)
                            
                    # Load config
                    with open(config_file, 'r') as f:
                        config = json.load(f)
                        
                    self.plugin_configs[plugin_name] = config
                    logger.debug(f"Loaded configuration for plugin {plugin_name}")
                    
                except Exception as e:
                    logger.error(f"Error loading configuration for plugin {plugin_name}: {e}")
                    # Use empty config as fallback
                    self.plugin_configs[plugin_name] = {}
            
            return self.plugin_configs
    
    async def load_enabled_plugins(self) -> List[str]:
        """
        Load all enabled plugins respecting dependencies
        
        Returns:
            List of successfully loaded plugins
        """
        logger.info("Loading enabled plugins")
        loaded_plugins = []
        
        # Get list of enabled plugins
        enabled_plugins = await self._get_enabled_plugins()
        
        # Sort plugins by dependencies and priority
        load_order = await self._calculate_load_order(enabled_plugins)
        
        # Load plugins in order
        for plugin_name in load_order:
            try:
                success = await self.load_plugin(plugin_name)
                if success:
                    loaded_plugins.append(plugin_name)
            except Exception as e:
                logger.error(f"Failed to load plugin {plugin_name}: {e}")
                logger.debug(f"Plugin load error details: {traceback.format_exc()}")
                
        self.plugin_load_order = loaded_plugins
        return loaded_plugins
    
    async def _get_enabled_plugins(self) -> List[str]:
        """
        Get list of plugins that should be enabled
        
        Returns:
            List of enabled plugin names
        """
        # For now, all discovered plugins are considered enabled
        # This could be enhanced with a config option
        return list(self.plugin_metadata.keys())
    
    async def _calculate_load_order(self, plugin_names: List[str]) -> List[str]:
        """
        Calculate plugin load order based on dependencies and priorities
        
        Args:
            plugin_names: List of plugin names to load
            
        Returns:
            Sorted list of plugin names in load order
            
        Raises:
            PluginDependencyError: If circular dependencies are detected
        """
        # Dependency resolution using topological sort
        result = []
        temp_marks = set()
        perm_marks = set()
        
        async def visit(plugin):
            """Recursive visiting function for topological sort"""
            if plugin in perm_marks:
                return
            if plugin in temp_marks:
                raise PluginDependencyError(f"Circular dependency detected involving {plugin}")
                
            temp_marks.add(plugin)
            
            # Visit dependencies
            if plugin in self.plugin_dependencies:
                for dep in self.plugin_dependencies[plugin]:
                    # Skip if dependency is not in the provided list
                    if dep not in plugin_names:
                        logger.warning(f"Plugin {plugin} depends on {dep}, but it's not enabled")
                        continue
                    await visit(dep)
                    
            temp_marks.remove(plugin)
            perm_marks.add(plugin)
            result.append(plugin)
            
        # Visit all plugins
        for plugin in plugin_names:
            if plugin not in perm_marks:
                await visit(plugin)
                
        # Sort by priority within dependency constraints
        # Higher priority (lower number) loads first
        def get_priority(plugin_name):
            metadata = self.plugin_metadata.get(plugin_name)
            return metadata.load_priority if metadata else 100
            
        # Group plugins by their depth in the dependency graph
        layers = []
        current_layer = [p for p in result if not any(
            d in result for d in self.plugin_dependencies.get(p, set())
        )]
        
        while current_layer:
            # Sort current layer by priority
            current_layer.sort(key=get_priority)
            layers.append(current_layer)
            
            # Remove current layer from result
            for p in current_layer:
                result.remove(p)
                
            # Find next layer
            current_layer = [p for p in result if not any(
                d in result for d in self.plugin_dependencies.get(p, set())
            )]
            
        # Flatten layers back into a single list
        return [p for layer in layers for p in layer]
    
    async def load_plugin(self, plugin_name: str) -> bool:
        """
        Load and initialize a specific plugin
        
        Args:
            plugin_name: Name of the plugin to load
            
        Returns:
            True if plugin was loaded successfully, False otherwise
            
        Raises:
            ValueError: If plugin is not found
            PluginLoadError: If plugin fails to load
        """
        if plugin_name not in self.plugin_metadata:
            raise ValueError(f"Plugin {plugin_name} not found")
            
        if plugin_name in self.plugins:
            logger.info(f"Plugin {plugin_name} is already loaded")
            return True
            
        logger.info(f"Loading plugin {plugin_name}")
        async with self._lock:
            try:
                # Check dependencies
                for dep_name in self.plugin_dependencies.get(plugin_name, set()):
                    if dep_name not in self.plugins or self.plugins[dep_name].state != PluginState.ACTIVE:
                        raise PluginDependencyError(
                            f"Plugin {plugin_name} depends on {dep_name}, which is not loaded"
                        )
                
                # Import plugin module
                module = self.plugin_modules.get(plugin_name)
                if not module:
                    raise PluginLoadError(f"Plugin module for {plugin_name} not found")
                    
                # Find plugin class
                plugin_class = None
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (inspect.isclass(attr) and 
                        issubclass(attr, Plugin) and 
                        attr is not Plugin and
                        hasattr(attr, 'initialize')):
                        plugin_class = attr
                        break
                        
                if not plugin_class:
                    raise PluginLoadError(f"No Plugin subclass found in module for {plugin_name}")
                    
                # Create plugin instance
                plugin = plugin_class()
                
                # Set metadata
                if not hasattr(plugin, 'metadata') or not plugin.metadata:
                    plugin.metadata = self.plugin_metadata.get(plugin_name)
                    
                # Set config
                plugin.config = self.plugin_configs.get(plugin_name, {})
                
                # Connect to event bus and plugin manager
                plugin._event_bus = self.event_bus
                plugin._plugin_manager = self
                
                # Add to plugin registry
                self.plugins[plugin_name] = plugin
                
                # Initialize plugin
                plugin.state = PluginState.LOADING
                success = await plugin.safe_initialize()
                
                if not success:
                    raise PluginLoadError(f"Plugin {plugin_name} failed to initialize")
                    
                # Register plugin functions
                if self.function_registry:
                    await self._register_plugin_functions(plugin)
                    
                # Register event handlers
                if self.event_bus:
                    await self._register_plugin_event_handlers(plugin)
                    
                # Set default permissions
                if self.permission_manager:
                    # Basic read permissions by default
                    self.permission_manager.add_plugin_role(plugin_name, "basic")
                    
                # Emit plugin loaded event
                if self.event_bus:
                    await self.event_bus.emit("plugin.loaded", {
                        "plugin_name": plugin_name,
                        "version": plugin.metadata.version if plugin.metadata else "unknown"
                    })
                    
                logger.info(f"Plugin {plugin_name} loaded successfully")
                return True
                
            except Exception as e:
                logger.error(f"Error loading plugin {plugin_name}: {e}")
                logger.debug(f"Plugin load error details: {traceback.format_exc()}")
                
                # Clean up if plugin was partially loaded
                if plugin_name in self.plugins:
                    plugin = self.plugins[plugin_name]
                    await plugin.safe_shutdown()
                    del self.plugins[plugin_name]
                    
                # Set error state in metadata
                if plugin_name in self.plugin_metadata:
                    metadata = self.plugin_metadata[plugin_name]
                    metadata.state = PluginState.ERROR
                    
                if self.event_bus:
                    await self.event_bus.emit("plugin.load_error", {
                        "plugin_name": plugin_name,
                        "error": str(e)
                    })
                    
                # Re-raise as PluginLoadError
                if not isinstance(e, (PluginLoadError, PluginDependencyError, PluginVersionError)):
                    raise PluginLoadError(f"Failed to load plugin {plugin_name}: {e}") from e
                raise
                
        return False
    
    async def unload_plugin(self, plugin_name: str) -> bool:
        """
        Unload a specific plugin
        
        Args:
            plugin_name: Name of the plugin to unload
            
        Returns:
            True if plugin was unloaded successfully, False otherwise
        """
        if plugin_name not in self.plugins:
            logger.warning(f"Plugin {plugin_name} is not loaded")
            return False
            
        logger.info(f"Unloading plugin {plugin_name}")
        async with self._lock:
            # Check for dependents
            if plugin_name in self.plugin_dependents:
                active_dependents = [
                    dep for dep in self.plugin_dependents[plugin_name]
                    if dep in self.plugins and self.plugins[dep].state == PluginState.ACTIVE
                ]
                if active_dependents:
                    logger.warning(
                        f"Cannot unload plugin {plugin_name} because it is required by: "
                        f"{', '.join(active_dependents)}"
                    )
                    return False
            
            try:
                plugin = self.plugins[plugin_name]
                
                # Unregister functions
                if self.function_registry:
                    await self.function_registry.unregister_plugin(plugin_name)
                    
                # Unregister event handlers
                if self.event_bus and hasattr(plugin, '_event_handlers'):
                    for event_name in plugin._event_handlers:
                        for handler in plugin._event_handlers[event_name]:
                            self.event_bus.unsubscribe(event_name, plugin_name)
                
                # Shutdown plugin
                success = await plugin.safe_shutdown()
                if not success:
                    logger.warning(f"Plugin {plugin_name} did not shut down cleanly")
                    
                # Remove from plugin registry
                del self.plugins[plugin_name]
                
                # Update plugin load order
                if plugin_name in self.plugin_load_order:
                    self.plugin_load_order.remove(plugin_name)
                    
                # Emit plugin unloaded event
                if self.event_bus:
                    await self.event_bus.emit("plugin.unloaded", {
                        "plugin_name": plugin_name
                    })
                    
                logger.info(f"Plugin {plugin_name} unloaded successfully")
                return True
                
            except Exception as e:
                logger.error(f"Error unloading plugin {plugin_name}: {e}")
                logger.debug(f"Plugin unload error details: {traceback.format_exc()}")
                
                # Force remove from registry in case of error
                if plugin_name in self.plugins:
                    del self.plugins[plugin_name]
                    
                if self.event_bus:
                    await self.event_bus.emit("plugin.unload_error", {
                        "plugin_name": plugin_name,
                        "error": str(e)
                    })
                    
                return False
    
    async def reload_plugin(self, plugin_name: str) -> bool:
        """
        Reload a specific plugin
        
        Args:
            plugin_name: Name of the plugin to reload
            
        Returns:
            True if plugin was reloaded successfully, False otherwise
        """
        logger.info(f"Reloading plugin {plugin_name}")
        
        # Remember the plugin's position in the load order
        load_position = self.plugin_load_order.index(plugin_name) if plugin_name in self.plugin_load_order else -1
        
        # Unload the plugin
        if plugin_name in self.plugins:
            unloaded = await self.unload_plugin(plugin_name)
            if not unloaded:
                logger.error(f"Failed to unload plugin {plugin_name} for reload")
                return False
                
        # Reload the module
        if plugin_name in self.plugin_modules:
            module = self.plugin_modules[plugin_name]
            try:
                # Reload the module
                importlib.reload(module)
                logger.debug(f"Reloaded module for plugin {plugin_name}")
            except Exception as e:
                logger.error(f"Failed to reload module for plugin {plugin_name}: {e}")
                return False
                
        # Load the plugin again
        try:
            loaded = await self.load_plugin(plugin_name)
            if loaded:
                # Restore load position if possible
                if load_position >= 0 and plugin_name in self.plugin_load_order:
                    self.plugin_load_order.remove(plugin_name)
                    self.plugin_load_order.insert(min(load_position, len(self.plugin_load_order)), plugin_name)
                    
                logger.info(f"Plugin {plugin_name} reloaded successfully")
                
                # Emit plugin reloaded event
                if self.event_bus:
                    await self.event_bus.emit("plugin.reloaded", {
                        "plugin_name": plugin_name
                    })
                    
                return True
        except Exception as e:
            logger.error(f"Failed to reload plugin {plugin_name}: {e}")
            
        return False
    
    async def update_plugin_config(self, plugin_name: str, config: Dict[str, Any]) -> bool:
        """
        Update configuration for a plugin
        
        Args:
            plugin_name: Name of the plugin
            config: New configuration dictionary
            
        Returns:
            True if configuration was updated successfully, False otherwise
        """
        if plugin_name not in self.plugin_metadata:
            logger.warning(f"Cannot update config for unknown plugin {plugin_name}")
            return False
            
        logger.info(f"Updating configuration for plugin {plugin_name}")
        
        try:
            # Update stored config
            self.plugin_configs[plugin_name] = config
            
            # Save to file
            config_file = os.path.join(self.config_dir, f"{plugin_name}.json")
            os.makedirs(os.path.dirname(config_file), exist_ok=True)
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
                
            # Update running plugin if loaded
            if plugin_name in self.plugins:
                plugin = self.plugins[plugin_name]
                if hasattr(plugin, 'on_config_change'):
                    success = await plugin.on_config_change(config)
                    if not success:
                        logger.warning(f"Plugin {plugin_name} rejected configuration change")
                        return False
                        
            logger.info(f"Configuration updated for plugin {plugin_name}")
            
            # Emit config changed event
            if self.event_bus:
                await self.event_bus.emit("plugin.config_changed", {
                    "plugin_name": plugin_name
                })
                
            return True
            
        except Exception as e:
            logger.error(f"Error updating configuration for plugin {plugin_name}: {e}")
            return False
    
    async def _register_plugin_functions(self, plugin: Plugin) -> None:
        """
        Register plugin functions with function registry
        
        Args:
            plugin: Plugin instance
        """
        if not self.function_registry or not hasattr(plugin, '_functions'):
            return
            
        plugin_name = plugin.metadata.name if plugin.metadata else "unknown"
        
        for func_name, func_wrapper in plugin._functions.items():
            if hasattr(func_wrapper, 'func'):
                await self.function_registry.register_function(
                    plugin_name,
                    func_wrapper.func,
                    func_wrapper.name,
                    func_wrapper.description
                )
    
    async def _register_plugin_event_handlers(self, plugin: Plugin) -> None:
        """
        Register plugin event handlers with event bus
        
        Args:
            plugin: Plugin instance
        """
        if not self.event_bus or not hasattr(plugin, '_event_handlers'):
            return
            
        plugin_name = plugin.metadata.name if plugin.metadata else "unknown"
        
        for event_name, handlers in plugin._event_handlers.items():
            for handler in handlers:
                self.event_bus.subscribe(event_name, handler, plugin_name)
    
    def get_plugin(self, plugin_name: str) -> Optional[Plugin]:
        """
        Get a loaded plugin by name
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            Plugin instance or None if not found
        """
        return self.plugins.get(plugin_name)
    
    def get_plugin_metadata(self, plugin_name: str) -> Optional[PluginMetadata]:
        """
        Get metadata for a plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            PluginMetadata instance or None if not found
        """
        return self.plugin_metadata.get(plugin_name)
    
    def get_plugin_config(self, plugin_name: str) -> Dict[str, Any]:
        """
        Get configuration for a plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            Configuration dictionary, empty dict if not found
        """
        return self.plugin_configs.get(plugin_name, {})
    
    def get_plugin_status(self, plugin_name: str) -> Dict[str, Any]:
        """
        Get status information for a plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            Status dictionary, empty dict if not found
        """
        if plugin_name in self.plugins:
            plugin = self.plugins[plugin_name]
            return plugin.get_status()
            
        if plugin_name in self.plugin_metadata:
            metadata = self.plugin_metadata[plugin_name]
            return {
                "name": metadata.name,
                "version": metadata.version,
                "state": PluginState.UNLOADED,
                "initialized": False
            }
            
        return {}
    
    def get_all_plugins(self) -> List[str]:
        """
        Get list of all discovered plugins
        
        Returns:
            List of plugin names
        """
        return list(self.plugin_metadata.keys())
    
    def get_loaded_plugins(self) -> List[str]:
        """
        Get list of all loaded plugins
        
        Returns:
            List of loaded plugin names
        """
        return list(self.plugins.keys())
    
    def get_plugin_dependencies(self, plugin_name: str) -> List[str]:
        """
        Get dependencies for a plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            List of dependency plugin names
        """
        return list(self.plugin_dependencies.get(plugin_name, set()))
    
    def get_plugin_dependents(self, plugin_name: str) -> List[str]:
        """
        Get plugins that depend on a specific plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            List of dependent plugin names
        """
        return list(self.plugin_dependents.get(plugin_name, set()))
    
    async def send_plugin_message(self, source_plugin: str, target_plugin: str, message: Any) -> bool:
        """
        Send a message from one plugin to another
        
        Args:
            source_plugin: Name of the source plugin
            target_plugin: Name of the target plugin
            message: Message data
            
        Returns:
            True if message was delivered, False otherwise
        """
        if target_plugin not in self.plugins:
            logger.warning(f"Cannot send message to non-existent plugin {target_plugin}")
            return False
            
        target = self.plugins[target_plugin]
        
        try:
            # Wrap message with source information
            wrapped_message = {
                "source": source_plugin,
                "timestamp": time.time(),
                "data": message
            }
            
            # Add to target's message queue
            if hasattr(target, '_message_queue'):
                try:
                    # Add with timeout to avoid blocking
                    await asyncio.wait_for(
                        target._message_queue.put(wrapped_message),
                        timeout=0.5
                    )
                    return True
                except asyncio.TimeoutError:
                    logger.warning(f"Message queue full for plugin {target_plugin}")
                    return False
                    
            return False
            
        except Exception as e:
            logger.error(f"Error sending message to plugin {target_plugin}: {e}")
            return False
    
    def register_shared_resource(self, name: str, resource: Any) -> None:
        """
        Register a shared resource accessible to all plugins
        
        Args:
            name: Resource name
            resource: Resource object
        """
        self.shared_resources[name] = resource
        logger.debug(f"Registered shared resource: {name}")
    
    def unregister_shared_resource(self, name: str) -> bool:
        """
        Unregister a shared resource
        
        Args:
            name: Resource name
            
        Returns:
            True if resource was unregistered, False if not found
        """
        if name in self.shared_resources:
            del self.shared_resources[name]
            logger.debug(f"Unregistered shared resource: {name}")
            return True
        return False
    
    def get_shared_resource(self, name: str) -> Optional[Any]:
        """
        Get a shared resource
        
        Args:
            name: Resource name
            
        Returns:
            Resource object or None if not found
        """
        return self.shared_resources.get(name)
    
    def get_all_shared_resources(self) -> Dict[str, Any]:
        """
        Get all shared resources
        
        Returns:
            Dictionary of resource names to objects
        """
        return dict(self.shared_resources)
    
    async def check_plugin_health(self, plugin_name: str) -> Dict[str, Any]:
        """
        Check health of a specific plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            Health status dictionary
        """
        if plugin_name not in self.plugins:
            return {"status": "not_loaded", "plugin": plugin_name}
            
        plugin = self.plugins[plugin_name]
        
        try:
            if hasattr(plugin, 'check_health'):
                health = await plugin.check_health()
                return health
            return {"status": plugin.state, "plugin": plugin_name}
        except Exception as e:
            logger.error(f"Error checking health for plugin {plugin_name}: {e}")
            return {
                "status": "error",
                "plugin": plugin_name,
                "error": str(e)
            }
    
    async def check_all_plugins_health(self) -> Dict[str, Dict[str, Any]]:
        """
        Check health of all loaded plugins
        
        Returns:
            Dictionary mapping plugin names to health status
        """
        results = {}
        
        for plugin_name in self.plugins:
            results[plugin_name] = await self.check_plugin_health(plugin_name)
            
        return results