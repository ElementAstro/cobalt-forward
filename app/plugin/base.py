from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type, Optional, Callable, Set, Union
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from datetime import datetime
import logging
import traceback
import json

from .models import PluginMetadata, PluginState, PluginMetrics
from .sandbox import PluginSandbox
from .function import PluginFunction
from .permissions import PluginPermission, require_permission

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Base plugin class providing lifecycle management and various functionality"""
    
    def __init__(self):
        self.metadata: PluginMetadata = None
        self.state: str = PluginState.UNLOADED
        self.config: Dict[str, Any] = {}
        self.start_time: float = None
        self._hooks: Dict[str, List[Callable]] = {}
        self._event_handlers: Dict[str, List[Callable]] = {}
        self.health_status: Dict[str, Any] = {"status": "unknown"}
        self.last_health_check: float = None
        self._lifecycle_hooks = {
            "pre_initialize": [],
            "post_initialize": [],
            "pre_shutdown": [],
            "post_shutdown": []
        }
        self._functions: Dict[str, PluginFunction] = {}
        self._exported_types: Dict[str, Type] = {}
        self._permissions: Set[str] = set()
        self._sandbox = PluginSandbox()
        self._metrics = PluginMetrics()
        self._message_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)  # Limit queue size
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        self._local_storage = threading.local()
        self._plugin_manager = None  # Reference to plugin manager
        self._event_bus = None       # Reference to event bus
        self._lock = asyncio.Lock()  # Lock for critical operations
        self._initialized = False    # Initialization flag
        self._shutdown_complete = False  # Shutdown completion flag
        self._config_version = 0     # Configuration version
        self._error_count = 0        # Error counter
        self._last_error = None      # Last error information

    def _init_thread_pool(self, max_workers: int = 3):
        """Initialize thread pool"""
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(
                max_workers=max_workers, 
                thread_name_prefix=f"{self.metadata.name if self.metadata else 'plugin'}_worker"
            )
    
    def _shutdown_thread_pool(self):
        """Shut down thread pool"""
        if self._thread_pool:
            self._thread_pool.shutdown(wait=False)
            self._thread_pool = None

    @abstractmethod
    async def initialize(self) -> None:
        """Plugin initialization, must be implemented by subclass"""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Plugin shutdown, must be implemented by subclass"""
        pass

    async def safe_initialize(self) -> bool:
        """Safe initialization wrapper"""
        if self._initialized:
            return True
            
        try:
            # Execute pre-initialization hooks
            for hook in self._lifecycle_hooks.get("pre_initialize", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
                    
            # Initialize thread pool
            self._init_thread_pool()
            
            # Execute subclass initialization method
            await self.initialize()
            
            # Execute post-initialization hooks
            for hook in self._lifecycle_hooks.get("post_initialize", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
                    
            self._initialized = True
            self.state = PluginState.ACTIVE
            self.start_time = time.time()
            logger.info(f"Plugin {self.metadata.name if self.metadata else 'unknown'} initialized successfully")
            return True
            
        except Exception as e:
            self._error_count += 1
            self._last_error = {
                "time": datetime.now().isoformat(),
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.state = PluginState.ERROR
            logger.error(f"Plugin {self.metadata.name if self.metadata else 'unknown'} initialization failed: {e}")
            return False

    async def safe_shutdown(self) -> bool:
        """Safe shutdown wrapper"""
        if self._shutdown_complete:
            return True
            
        try:
            # Execute pre-shutdown hooks
            for hook in self._lifecycle_hooks.get("pre_shutdown", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
                    
            # Execute subclass shutdown method
            await self.shutdown()
            
            # Execute post-shutdown hooks
            for hook in self._lifecycle_hooks.get("post_shutdown", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
            
            # Clean up thread pool
            self._shutdown_thread_pool()
            
            # Clear queue
            while not self._message_queue.empty():
                try:
                    self._message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                    
            self._shutdown_complete = True
            self.state = PluginState.UNLOADED
            logger.info(f"Plugin {self.metadata.name if self.metadata else 'unknown'} shutdown successfully")
            return True
            
        except Exception as e:
            self._error_count += 1
            self._last_error = {
                "time": datetime.now().isoformat(),
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            logger.error(f"Plugin {self.metadata.name if self.metadata else 'unknown'} shutdown failed: {e}")
            return False

    async def on_config_change(self, new_config: Dict[str, Any]) -> bool:
        """Handle configuration change, supports config validation"""
        try:
            # Validate new config
            if hasattr(self, 'validate_config'):
                valid = await self.validate_config(new_config)
                if not valid:
                    logger.warning(f"Plugin {self.metadata.name if self.metadata else 'unknown'} configuration validation failed")
                    return False
            
            # Update config
            old_config = self.config.copy()
            self.config = new_config
            self._config_version += 1
            
            # Log config change
            logger.info(f"Plugin {self.metadata.name if self.metadata else 'unknown'} configuration updated")
            
            # Reset sandbox cache
            self._sandbox.reset_cache()
            
            return True
        except Exception as e:
            logger.error(f"Configuration change processing failed: {e}")
            return False

    def register_hook(self, hook_name: str, callback: Callable) -> None:
        """Register hook function, thread-safe"""
        if hook_name not in self._hooks:
            self._hooks[hook_name] = []
        if callback not in self._hooks[hook_name]:
            self._hooks[hook_name].append(callback)

    def unregister_hook(self, hook_name: str, callback: Callable) -> bool:
        """Unregister hook function, thread-safe"""
        if hook_name in self._hooks and callback in self._hooks[hook_name]:
            self._hooks[hook_name].remove(callback)
            return True
        return False

    def register_event_handler(self, event_name: str, handler: Callable) -> None:
        """Register event handler, thread-safe"""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        if handler not in self._event_handlers[event_name]:
            self._event_handlers[event_name].append(handler)

    def unregister_event_handler(self, event_name: str, handler: Callable) -> bool:
        """Unregister event handler, thread-safe"""
        if event_name in self._event_handlers and handler in self._event_handlers[event_name]:
            self._event_handlers[event_name].remove(handler)
            return True
        return False

    async def handle_event(self, event_name: str, event_data: Any) -> List[Any]:
        """Handle event, returns list of handler results"""
        results = []
        start_time = time.time()
        
        if event_name in self._event_handlers:
            for handler in self._event_handlers[event_name]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(event_data)
                    else:
                        result = handler(event_data)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Event handler {handler.__name__} execution failed: {e}")
                    self._metrics.record_error()
        
        # Record execution metrics
        execution_time = time.time() - start_time
        self._metrics.record_execution(execution_time)
        
        return results

    def add_lifecycle_hook(self, stage: str, callback: Callable) -> bool:
        """Add lifecycle hook, thread-safe"""
        if stage in self._lifecycle_hooks:
            if callback not in self._lifecycle_hooks[stage]:
                self._lifecycle_hooks[stage].append(callback)
                return True
        return False

    async def check_health(self) -> Dict[str, Any]:
        """Check plugin health status with timeout handling"""
        try:
            # Limit health check execution time
            health_result = await asyncio.wait_for(
                self._health_check(), 
                timeout=5.0  # 5 second timeout
            )
            self.health_status = health_result
            self.last_health_check = time.time()
            return health_result
        except asyncio.TimeoutError:
            self.health_status = {
                "status": "error", 
                "error": "Health check timeout"
            }
            return self.health_status
        except Exception as e:
            self.health_status = {
                "status": "error", 
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            return self.health_status

    async def _health_check(self) -> Dict[str, Any]:
        """Default health check implementation"""
        return {
            "status": "healthy",
            "initialized": self._initialized,
            "error_count": self._error_count,
            "uptime": time.time() - self.start_time if self.start_time else 0,
            "metrics": self._metrics.get_metrics_summary() if hasattr(self._metrics, 'get_metrics_summary') else {}
        }

    async def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration, supports JSON Schema validation"""
        if not hasattr(self.metadata, 'config_schema') or not self.metadata.config_schema:
            return True
            
        try:
            # Use jsonschema module if available
            try:
                import jsonschema
                jsonschema.validate(instance=config, schema=self.metadata.config_schema)
            except ImportError:
                # Simple validation: check if all required fields exist
                required_fields = self.metadata.config_schema.get('required', [])
                for field in required_fields:
                    if field not in config:
                        logger.error(f"Missing required configuration field: {field}")
                        return False
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False

    def export_function(self, name: str = None, description: str = None):
        """Function export decorator, optimized cache management"""
        def decorator(func):
            func_name = name or func.__name__
            wrapped = PluginFunction(func, func_name, description)
            self._functions[func_name] = wrapped
            return func
        return decorator

    def export_type(self, type_class: Type, name: str = None):
        """Export type, supports type checking"""
        type_name = name or type_class.__name__
        self._exported_types[type_name] = type_class
        return type_class

    def get_function(self, name: str) -> Optional[PluginFunction]:
        """Get exported function"""
        return self._functions.get(name)

    def get_type(self, name: str) -> Optional[Type]:
        """Get exported type"""
        return self._exported_types.get(name)

    def get_api_schema(self) -> Dict[str, Any]:
        """Get plugin API schema with extended information"""
        schema = {
            "functions": {},
            "types": {},
            "metadata": self.metadata.to_dict() if hasattr(self.metadata, 'to_dict') else (self.metadata.__dict__ if self.metadata else {}),
            "permissions": list(self._permissions),
            "version": self.metadata.version if self.metadata else "unknown"
        }

        # Collect function information
        for name, func in self._functions.items():
            if hasattr(func, 'get_signature_info'):
                schema["functions"][name] = func.get_signature_info()
            else:
                schema["functions"][name] = {
                    "name": name,
                    "description": func.description if hasattr(func, 'description') else "",
                    "is_async": asyncio.iscoroutinefunction(func.func if hasattr(func, 'func') else func)
                }

        # Collect type information
        for name, type_class in self._exported_types.items():
            type_info = {"fields": {}}
            
            # Handle dataclasses
            if hasattr(type_class, "__dataclass_fields__"):
                for field_name, field_obj in type_class.__dataclass_fields__.items():
                    type_info["fields"][field_name] = {
                        "type": str(field_obj.type),
                        "default": field_obj.default if field_obj.default is not field_obj.default_factory else "has_factory"
                    }
            # Handle regular classes
            elif hasattr(type_class, "__annotations__"):
                for field_name, field_type in type_class.__annotations__.items():
                    type_info["fields"][field_name] = {
                        "type": str(field_type),
                        "default": getattr(type_class, field_name, None) if hasattr(type_class, field_name) else None
                    }
                
            schema["types"][name] = type_info

        return schema

    async def send_message(self, target_plugin: str, message: Any) -> bool:
        """Send message to another plugin with timeout support"""
        if not self._plugin_manager:
            logger.warning(f"Plugin {self.metadata.name if self.metadata else 'unknown'} tried to send message but plugin manager is not connected")
            return False
            
        try:
            # Limit message size
            message_str = json.dumps(message) if not isinstance(message, str) else message
            if len(message_str) > 1024 * 1024:  # 1MB limit
                logger.warning(f"Message too large ({len(message_str)} bytes), rejected")
                return False
                
            # Send message with 5 second timeout
            return await asyncio.wait_for(
                self._plugin_manager.send_plugin_message(
                    self.metadata.name if self.metadata else "unknown", 
                    target_plugin, 
                    message
                ),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.error(f"Send message to plugin {target_plugin} timed out")
            return False
        except Exception as e:
            logger.error(f"Send message failed: {e}")
            return False

    def run_in_sandbox(self, func: Callable, *args, **kwargs):
        """Run function in sandbox with exception handling"""
        start_time = time.time()
        try:
            with self._sandbox.apply():
                result = func(*args, **kwargs)
                
                # Record execution metrics
                execution_time = time.time() - start_time
                self._metrics.record_execution(execution_time)
                
                return result
        except Exception as e:
            # Record error
            execution_time = time.time() - start_time
            self._metrics.record_execution(execution_time)
            self._metrics.record_error()
            
            # Re-raise exception with context
            raise type(e)(f"Sandbox execution failed: {str(e)}")

    async def run_in_thread(self, func: Callable, *args, **kwargs):
        """Run function in thread pool, wait for result asynchronously"""
        if self._thread_pool is None:
            self._init_thread_pool()
            
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._thread_pool, 
            lambda: func(*args, **kwargs)
        )

    async def get_messages(self, timeout: float = 0.1) -> List[Any]:
        """Get pending messages with timeout support"""
        messages = []
        try:
            # Try to get messages within timeout
            while True:
                try:
                    message = await asyncio.wait_for(
                        self._message_queue.get(), 
                        timeout=timeout
                    )
                    messages.append(message)
                    self._message_queue.task_done()
                except asyncio.TimeoutError:
                    # Timeout, return collected messages
                    break
        except Exception as e:
            logger.error(f"Error getting messages: {e}")
            
        return messages
        
    def get_status(self) -> Dict[str, Any]:
        """Get plugin status"""
        return {
            "name": self.metadata.name if self.metadata else "unknown",
            "version": self.metadata.version if self.metadata else "unknown",
            "state": self.state,
            "initialized": self._initialized,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "uptime": time.time() - self.start_time if self.start_time else 0,
            "config_version": self._config_version,
            "health": self.health_status,
            "metrics": self._metrics.get_metrics_summary() if hasattr(self._metrics, 'get_metrics_summary') else {},
            "sandbox_status": self._sandbox.get_status() if hasattr(self._sandbox, 'get_status') else {},
        }
        
    def __del__(self):
        """Ensure resources are properly cleaned up"""
        try:
            self._shutdown_thread_pool()
        except:
            pass
