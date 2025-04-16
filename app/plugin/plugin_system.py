import os
import sys
import asyncio
import logging
import inspect
import threading
import pkg_resources
import importlib
import time
import shutil
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any, Optional, Type, Union, Set, Callable
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .plugin_manager import PluginManager
from .event import EventBus
from .utils import ensure_directory, version_to_tuple, check_version_compatibility
from .base import Plugin
from .models import PluginMetadata, PluginState

logger = logging.getLogger(__name__)


class PluginSystemConfig(BaseModel):
    """Configuration for the plugin system"""
    plugin_dir: str = Field("plugins", description="Directory where plugins are stored")
    config_dir: str = Field("config/plugins", description="Directory for plugin configuration files")
    cache_dir: str = Field("cache/plugins", description="Directory for plugin cache files")
    auto_reload: bool = Field(True, description="Whether to automatically reload modified plugins")
    sandbox_enabled: bool = Field(True, description="Whether to enable the plugin sandbox for security")
    health_check_interval: int = Field(60, description="Time between plugin health checks in seconds")
    log_level: str = Field("INFO", description="Logging level")
    max_threads: int = Field(10, description="Maximum number of threads for plugin operations")


class PluginSystem:
    """
    Core plugin system that integrates plugin manager, event bus, and 
    provides application lifecycle hooks.
    
    This class acts as the central coordination point for all plugin-related 
    functionality, including initialization, plugin lifecycle management,
    and system integration.
    """
    
    _instance: Optional['PluginSystem'] = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls, config: Optional[PluginSystemConfig] = None) -> 'PluginSystem':
        """
        Get or create singleton instance of the plugin system
        
        Args:
            config: Optional configuration for the plugin system
            
        Returns:
            The plugin system singleton instance
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(config or PluginSystemConfig())
            elif config is not None:
                logger.warning("Plugin system already initialized, config update ignored")
            return cls._instance
    
    def __init__(self, config: PluginSystemConfig):
        """
        Initialize the plugin system
        
        Args:
            config: Plugin system configuration
        """
        self.config = config
        
        # Setup logging
        logging_level = getattr(logging, config.log_level)
        logger.setLevel(logging_level)
        
        # Ensure directories exist
        ensure_directory(config.plugin_dir)
        ensure_directory(config.config_dir)
        ensure_directory(config.cache_dir)
        
        # Create core components
        self.event_bus = EventBus(max_queue_size=1000, max_history=500)
        self.plugin_manager = PluginManager(
            plugin_dir=config.plugin_dir,
            config_dir=config.config_dir,
            cache_dir=config.cache_dir
        )
        
        # Thread pool for background tasks
        self._thread_pool = ThreadPoolExecutor(
            max_workers=min(32, config.max_threads),
            thread_name_prefix="plugin_system_worker"
        )
        
        # Runtime state
        self._initialized = False
        self._shutting_down = False
        self._tasks: List[asyncio.Task] = []
        self._app: Optional[FastAPI] = None
        self._start_time = time.time()
        self._system_info = self._get_system_info()
        
        # API handlers for application controllers
        self._lifecycle_hooks = {
            "pre_startup": [],     # Before application startup
            "startup": [],         # During application startup
            "post_startup": [],    # After application startup
            "pre_shutdown": [],    # Before application shutdown
            "shutdown": [],        # During application shutdown
            "post_shutdown": [],   # After application shutdown
            "reload": [],          # During system reload
            "error": []            # On system error
        }
        
        # Integration points
        self._api_routes = {}  # Routes to install into FastAPI app
        self._message_handlers = {}  # Handlers for message bus integration
        
        # Version compatibility tracking
        self._version = "1.0.0"
        self._compatible_versions = ["0.9.0", "1.0.0"]
        
        logger.info("Plugin system created")
        
    def _get_system_info(self) -> Dict[str, Any]:
        """
        Get information about the system environment
        
        Returns:
            Dictionary with system information
        """
        import platform
        
        return {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "plugin_system_version": self._version,
            "start_time": time.time(),
            "process_id": os.getpid()
        }
    
    async def initialize(self, app: Optional[FastAPI] = None) -> bool:
        """
        Initialize the plugin system and all plugins
        
        Args:
            app: Optional FastAPI application to integrate with
            
        Returns:
            True if initialization was successful
        """
        if self._initialized:
            logger.warning("Plugin system already initialized")
            return True
        
        logger.info("Initializing plugin system...")
        
        try:
            # Store FastAPI app reference
            self._app = app
            
            # Initialize event bus
            await self.event_bus.start_processing()
            
            # Connect plugin manager with event bus
            await self.plugin_manager.set_event_bus(self.event_bus)
            
            # Initialize plugin manager
            await self.plugin_manager.initialize()
            
            # Register API routes if app provided
            if app:
                self._register_api_routes(app)
                
                # Store plugin manager in app state
                app.state.plugin_manager = self.plugin_manager
                app.state.event_bus = self.event_bus
                app.state.plugin_system = self
                
            # Trigger lifecycle hooks
            await self._run_lifecycle_hooks("startup")
                
            self._initialized = True
            logger.info("Plugin system initialized successfully")
            
            # Emit system ready event
            await self.event_bus.emit(
                "plugin_system.ready",
                {
                    "timestamp": time.time(),
                    "version": self._version,
                }
            )
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to initialize plugin system: {e}")
            logger.debug(f"Initialization error details: {traceback.format_exc()}")
            
            # Trigger error hooks
            await self._run_lifecycle_hooks("error", {"error": str(e)})
            
            return False
    
    def _register_api_routes(self, app: FastAPI) -> None:
        """
        Register plugin system API routes with FastAPI app
        
        Args:
            app: FastAPI application to register routes with
        """
        from .plugin_api import router as plugin_router
        app.include_router(plugin_router)
        logger.info("Plugin API routes registered")
    
    async def _run_lifecycle_hooks(self, hook_name: str, data: Dict[str, Any] = None) -> None:
        """
        Run lifecycle hooks for given stage
        
        Args:
            hook_name: Name of the hook to run
            data: Optional data to pass to hooks
        """
        if hook_name not in self._lifecycle_hooks:
            logger.warning(f"Unknown lifecycle hook: {hook_name}")
            return
        
        hooks = self._lifecycle_hooks[hook_name]
        if not hooks:
            return
            
        logger.debug(f"Running {len(hooks)} lifecycle hooks for stage: {hook_name}")
        
        for hook in hooks:
            try:
                if asyncio.iscoroutinefunction(hook):
                    await hook(data or {})
                else:
                    hook(data or {})
            except Exception as e:
                logger.error(f"Error in lifecycle hook {hook_name}: {e}")
                logger.debug(f"Hook error details: {traceback.format_exc()}")
    
    def add_lifecycle_hook(self, stage: str, callback: Callable) -> bool:
        """
        Add a lifecycle hook
        
        Args:
            stage: Lifecycle stage to hook into
            callback: Function to call during that lifecycle stage
            
        Returns:
            True if hook was added, False otherwise
        """
        if stage not in self._lifecycle_hooks:
            logger.warning(f"Unknown lifecycle stage: {stage}")
            return False
            
        self._lifecycle_hooks[stage].append(callback)
        return True
    
    async def shutdown(self) -> None:
        """
        Shut down the plugin system and all plugins
        
        This method ensures a graceful shutdown of all plugins and resources.
        """
        if self._shutting_down:
            logger.warning("Plugin system already shutting down")
            return
            
        self._shutting_down = True
        logger.info("Shutting down plugin system...")
        
        # Trigger pre-shutdown hooks
        await self._run_lifecycle_hooks("pre_shutdown")
        
        try:
            # Shutdown plugin manager
            if self.plugin_manager and hasattr(self.plugin_manager, 'shutdown_plugins'):
                await self.plugin_manager.shutdown_plugins()
            
            # Stop event bus
            if self.event_bus and hasattr(self.event_bus, 'stop_processing'):
                await self.event_bus.stop_processing()
                
            # Cancel tasks
            for task in self._tasks:
                if not task.done():
                    try:
                        task.cancel()
                        await asyncio.wait([task], timeout=2.0)
                    except asyncio.TimeoutError:
                        logger.warning(f"Task cancellation timed out for: {task.get_name()}")
                    except Exception as e:
                        logger.error(f"Error cancelling task: {e}")
            
            # Shutdown thread pool
            self._thread_pool.shutdown(wait=False)
            
            # Trigger shutdown hooks
            await self._run_lifecycle_hooks("shutdown")
            
            logger.info("Plugin system shut down successfully")
            
        except Exception as e:
            logger.error(f"Error during plugin system shutdown: {e}")
            logger.debug(f"Shutdown error details: {traceback.format_exc()}")
        finally:
            # Trigger post-shutdown hooks regardless of errors
            await self._run_lifecycle_hooks("post_shutdown")
    
    async def reload(self) -> bool:
        """
        Reload the plugin system and all plugins
        
        Returns:
            True if reload was successful
        """
        logger.info("Reloading plugin system...")
        
        try:
            # Trigger reload hooks
            await self._run_lifecycle_hooks("reload")
            
            # Reload plugins
            reload_results = {}
            for plugin_name in list(self.plugin_manager.plugins.keys()):
                reload_results[plugin_name] = await self.plugin_manager.reload_plugin(plugin_name)
            
            # Report reload results
            success_count = sum(1 for result in reload_results.values() if result)
            total_count = len(reload_results)
            
            if success_count == total_count:
                logger.info(f"Plugin system reloaded successfully ({success_count}/{total_count} plugins)")
                return True
            else:
                logger.warning(f"Plugin system reload partial success ({success_count}/{total_count} plugins)")
                for name, success in reload_results.items():
                    if not success:
                        logger.warning(f"Failed to reload plugin: {name}")
                return success_count > 0
                
        except Exception as e:
            logger.error(f"Error during plugin system reload: {e}")
            logger.debug(f"Reload error details: {traceback.format_exc()}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get plugin system status
        
        Returns:
            Dictionary with system status information
        """
        return {
            "initialized": self._initialized,
            "uptime": time.time() - self._start_time,
            "plugin_count": len(self.plugin_manager.plugins) if hasattr(self.plugin_manager, 'plugins') else 0,
            "event_bus": self.event_bus.get_stats() if hasattr(self.event_bus, 'get_stats') else {},
            "system_info": self._system_info,
            "config": self.config.dict(),
            "plugins": self.plugin_manager.get_plugin_status() if hasattr(self.plugin_manager, 'get_plugin_status') else {}
        }
    
    async def install_plugin(self, source_path: str) -> Optional[str]:
        """
        Install a plugin from source path (file or directory)
        
        Args:
            source_path: Path to plugin source
            
        Returns:
            Plugin name if installation succeeded, None otherwise
        """
        try:
            # Check if source exists
            if not os.path.exists(source_path):
                logger.error(f"Plugin source not found: {source_path}")
                return None
                
            # Determine plugin name from source
            plugin_name = os.path.splitext(os.path.basename(source_path))[0]
            target_path = os.path.join(self.config.plugin_dir, f"{plugin_name}.py")
            
            # Check if plugin already exists
            if os.path.exists(target_path):
                logger.warning(f"Plugin {plugin_name} already exists, it will be overwritten")
            
            # Copy plugin file
            if os.path.isfile(source_path):
                shutil.copy2(source_path, target_path)
                logger.info(f"Copied plugin file to {target_path}")
            elif os.path.isdir(source_path):
                # If directory, copy all contents to plugin directory
                target_dir = os.path.join(self.config.plugin_dir, plugin_name)
                ensure_directory(target_dir)
                shutil.copytree(source_path, target_dir, dirs_exist_ok=True)
                logger.info(f"Copied plugin directory to {target_dir}")
            
            logger.info(f"Plugin {plugin_name} installed successfully")
            
            # Reload plugin manager to detect new plugin
            await self.plugin_manager._scan_plugins()
            await self.plugin_manager.load_plugins()
            
            return plugin_name
            
        except Exception as e:
            logger.error(f"Error installing plugin: {e}")
            logger.debug(f"Installation error details: {traceback.format_exc()}")
            return None
    
    @staticmethod
    def create_plugin_template(output_path: str, plugin_name: str, author: str = "Unknown", 
                              description: str = "") -> bool:
        """
        Create a plugin template file
        
        Args:
            output_path: Where to save the template
            plugin_name: Name of the plugin
            author: Plugin author
            description: Plugin description
            
        Returns:
            True if template was created successfully
        """
        try:
            template = f'''"""
{plugin_name} - {description}

A plugin for Cobalt Forward.
"""
import logging
import time
from typing import Dict, List, Any, Optional
from app.plugin.base import Plugin
from app.plugin.models import PluginMetadata
from app.plugin.permissions import PluginPermission, require_permission

logger = logging.getLogger(__name__)


class {plugin_name.title().replace('_', '')}Plugin(Plugin):
    """
    {description}
    """
    
    def __init__(self):
        """Initialize plugin"""
        super().__init__()
        
        # Define plugin metadata
        self.metadata = PluginMetadata(
            name="{plugin_name}",
            version="0.1.0",
            author="{author}",
            description="{description}",
            dependencies=[],
            load_priority=100  # Lower values load first
        )
    
    async def initialize(self) -> None:
        """Called when plugin is loaded"""
        logger.info("Initializing {plugin_name} plugin")
        
        # Register event handlers
        self.register_event_handler("system.ping", self.handle_ping)
        
    async def shutdown(self) -> None:
        """Called when plugin is unloaded"""
        logger.info("Shutting down {plugin_name} plugin")
    
    @require_permission(PluginPermission.EVENT)
    async def handle_ping(self, event_data: Any) -> Dict[str, Any]:
        """Handle ping event"""
        return {{
            "plugin": "{plugin_name}",
            "timestamp": time.time(),
            "status": "active"
        }}
    
    @require_permission(PluginPermission.FILE_IO)
    def get_plugin_info(self) -> Dict[str, Any]:
        """Return plugin information"""
        return {{
            "name": self.metadata.name,
            "version": self.metadata.version,
            "author": self.metadata.author,
            "description": self.metadata.description
        }}
'''
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write template to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(template)
                
            logger.info(f"Plugin template created successfully at {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating plugin template: {e}")
            logger.debug(f"Template creation error details: {traceback.format_exc()}")
            return False
    
    async def run_in_thread(self, func, *args, **kwargs):
        """
        Run a function in the thread pool
        
        Args:
            func: Function to run
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            The result of the function
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._thread_pool, lambda: func(*args, **kwargs))


# Global accessor function
def get_plugin_system(config: Optional[PluginSystemConfig] = None) -> PluginSystem:
    """
    Get the plugin system singleton instance
    
    Args:
        config: Optional configuration for the plugin system
        
    Returns:
        The plugin system singleton instance
    """
    return PluginSystem.get_instance(config)
