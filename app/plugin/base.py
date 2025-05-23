from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type, Optional, Callable, Set, Union, Coroutine, TypeVar, ParamSpec
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from datetime import datetime
import logging
import traceback
import json
# Assuming PluginMetricsCollector is the intended class for metrics
from .utils import PluginMetricsCollector
from .models import PluginMetadata, PluginState
# PluginSandbox and PluginFunction are assumed to be correctly defined
from .sandbox import PluginSandbox
from .function import PluginFunction
# Permissions might be used by subclasses or are part of the plugin's API
from .permissions import PluginPermission, require_permission # pylint: disable=unused-import

logger = logging.getLogger(__name__)

# Define ParamSpec and TypeVar for generic callables
P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T") # Generic type for Type[T]

# Type alias for lifecycle hooks
LifecycleHookType = Union[Callable[[], None], Callable[[], Coroutine[Any, Any, None]]]
# Type alias for event handlers (assuming they take event_data and can be sync or async)
EventHandlerType = Union[Callable[[Any], Any], Callable[[Any], Coroutine[Any, Any, Any]]]
# Type alias for general hooks if signature is very generic
GeneralHookType = Callable[..., Any]


class Plugin(ABC):
    """Base plugin class providing lifecycle management and various functionality"""
    
    def __init__(self):
        self.metadata: Optional[PluginMetadata] = None
        self.state: str = PluginState.UNLOADED
        self.config: Dict[str, Any] = {}
        self.start_time: Optional[float] = None
        self._hooks: Dict[str, List[GeneralHookType]] = {}
        self._event_handlers: Dict[str, List[EventHandlerType]] = {}
        self.health_status: Dict[str, Any] = {"status": "unknown"}
        self.last_health_check: Optional[float] = None
        self._lifecycle_hooks: Dict[str, List[LifecycleHookType]] = {
            "pre_initialize": [],
            "post_initialize": [],
            "pre_shutdown": [],
            "post_shutdown": []
        }
        self._functions: Dict[str, PluginFunction] = {}
        self._exported_types: Dict[str, Type[Any]] = {}
        self._permissions: Set[str] = set()
        self._sandbox: PluginSandbox = PluginSandbox()
        # Use PluginMetricsCollector from utils.py
        self._metrics: PluginMetricsCollector = PluginMetricsCollector(
            plugin_name=self.metadata.name if self.metadata else "unknown_plugin_at_init"
        )
        self._message_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1000)  # Limit queue size
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        self._local_storage = threading.local() # type: ignore[var-annotated] # threading.local is tricky to type precisely here
        self._plugin_manager: Optional[Any] = None  # Reference to plugin manager (type Any for now)
        self._event_bus: Optional[Any] = None       # Reference to event bus (type Any for now)
        self._lock: asyncio.Lock = asyncio.Lock()  # Lock for critical operations
        self._initialized: bool = False    # Initialization flag
        self._shutdown_complete: bool = False  # Shutdown completion flag
        self._config_version: int = 0     # Configuration version
        self._error_count: int = 0        # Error counter
        self._last_error: Optional[Dict[str, Any]] = None      # Last error information

    def _init_thread_pool(self, max_workers: int = 3) -> None:
        """Initialize thread pool"""
        if self._thread_pool is None:
            plugin_name_for_thread = "plugin"
            if self.metadata and hasattr(self.metadata, 'name') and self.metadata.name:
                plugin_name_for_thread = self.metadata.name
            self._thread_pool = ThreadPoolExecutor(
                max_workers=max_workers, 
                thread_name_prefix=f"{plugin_name_for_thread}_worker"
            )
    
    def _shutdown_thread_pool(self) -> None:
        """Shut down thread pool"""
        if self._thread_pool:
            self._thread_pool.shutdown(wait=False) # Consider wait=True for cleaner shutdown
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
        
        # Update metrics plugin name if metadata is now available
        if self.metadata and hasattr(self.metadata, 'name') and self.metadata.name:
            self._metrics.plugin_name = self.metadata.name
            
        try:
            # Execute pre-initialization hooks
            for hook in self._lifecycle_hooks.get("pre_initialize", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook() # type: ignore[operator] # Handled by Union type
                    
            # Initialize thread pool
            self._init_thread_pool()
            
            # Execute subclass initialization method
            await self.initialize()
            
            # Execute post-initialization hooks
            for hook in self._lifecycle_hooks.get("post_initialize", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook() # type: ignore[operator] # Handled by Union type
                    
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
                    hook() # type: ignore[operator] # Handled by Union type
                    
            # Execute subclass shutdown method
            await self.shutdown()
            
            # Execute post-shutdown hooks
            for hook in self._lifecycle_hooks.get("post_shutdown", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook() # type: ignore[operator] # Handled by Union type
            
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
            if hasattr(self, 'validate_config') and callable(self.validate_config):
                valid = await self.validate_config(new_config)
                if not valid:
                    logger.warning(f"Plugin {self.metadata.name if self.metadata else 'unknown'} configuration validation failed")
                    return False
            
            # Update config
            # old_config = self.config.copy() # Marked as unused
            self.config = new_config
            self._config_version += 1
            
            # Log config change
            logger.info(f"Plugin {self.metadata.name if self.metadata else 'unknown'} configuration updated")
            
            # Reset sandbox cache
            if hasattr(self._sandbox, 'reset_cache'):
                self._sandbox.reset_cache()
            
            return True
        except Exception as e:
            logger.error(f"Configuration change processing failed: {e}")
            return False

    def register_hook(self, hook_name: str, callback: GeneralHookType) -> None:
        """Register hook function, thread-safe"""
        if hook_name not in self._hooks:
            self._hooks[hook_name] = []
        if callback not in self._hooks[hook_name]:
            self._hooks[hook_name].append(callback)

    def unregister_hook(self, hook_name: str, callback: GeneralHookType) -> bool:
        """Unregister hook function, thread-safe"""
        if hook_name in self._hooks and callback in self._hooks[hook_name]:
            self._hooks[hook_name].remove(callback)
            return True
        return False

    def register_event_handler(self, event_name: str, handler: EventHandlerType) -> None:
        """Register event handler, thread-safe"""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        if handler not in self._event_handlers[event_name]:
            self._event_handlers[event_name].append(handler)

    def unregister_event_handler(self, event_name: str, handler: EventHandlerType) -> bool:
        """Unregister event handler, thread-safe"""
        if event_name in self._event_handlers and handler in self._event_handlers[event_name]:
            self._event_handlers[event_name].remove(handler)
            return True
        return False

    async def handle_event(self, event_name: str, event_data: Any) -> List[Any]:
        """Handle event, returns list of handler results"""
        results: List[Any] = []
        start_time_event = time.time() # Renamed to avoid conflict
        
        if event_name in self._event_handlers:
            for handler_item in self._event_handlers[event_name]: # Renamed to avoid conflict
                try:
                    if asyncio.iscoroutinefunction(handler_item):
                        result_item: Any = await handler_item(event_data) # type: ignore[operator]
                    else:
                        result_item = handler_item(event_data) # type: ignore[operator]
                    results.append(result_item)
                except Exception as e:
                    logger.error(f"Event handler {getattr(handler_item, '__name__', 'unknown_handler')} execution failed: {e}")
                    if hasattr(self._metrics, 'record_error'):
                        self._metrics.record_error()
        
        # Record execution metrics
        execution_time = time.time() - start_time_event
        if hasattr(self._metrics, 'record_execution'):
            self._metrics.record_execution(execution_time)
        
        return results

    def add_lifecycle_hook(self, stage: str, callback: LifecycleHookType) -> bool:
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
            self.last_health_check = time.time() # Record check time even on timeout
            return self.health_status
        except Exception as e:
            self.health_status = {
                "status": "error", 
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.last_health_check = time.time() # Record check time on error
            return self.health_status

    async def _health_check(self) -> Dict[str, Any]:
        """Default health check implementation"""
        metrics_summary = {}
        if hasattr(self._metrics, 'get_metrics_summary') and callable(self._metrics.get_metrics_summary):
            metrics_summary = self._metrics.get_metrics_summary()

        return {
            "status": "healthy",
            "initialized": self._initialized,
            "error_count": self._error_count,
            "uptime": time.time() - self.start_time if self.start_time is not None else 0,
            "metrics": metrics_summary
        }

    async def validate_config(self, config_to_validate: Dict[str, Any]) -> bool: # Renamed parameter
        """Validate configuration, supports JSON Schema validation"""
        if not self.metadata or not hasattr(self.metadata, 'config_schema') or not self.metadata.config_schema:
            return True # No schema to validate against
            
        try:
            # Use jsonschema module if available
            try:
                import jsonschema # pylint: disable=import-outside-toplevel
                jsonschema.validate(instance=config_to_validate, schema=self.metadata.config_schema)
            except ImportError:
                # Simple validation: check if all required fields exist
                required_fields = self.metadata.config_schema.get('required', [])
                if isinstance(required_fields, list):
                    for field in required_fields:
                        if field not in config_to_validate:
                            logger.error(f"Missing required configuration field: {field}")
                            return False
            return True
        except Exception as e: # Catch jsonschema.ValidationError specifically if jsonschema is used
            logger.error(f"Configuration validation failed: {e}")
            return False

    def export_function(self, name: Optional[str] = None, description: Optional[str] = None) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Function export decorator, optimized cache management"""
        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            func_name = name or func.__name__
            # Assuming PluginFunction is correctly defined and handles func, name, description
            wrapped = PluginFunction(func, func_name, description)
            self._functions[func_name] = wrapped
            return func
        return decorator

    def export_type(self, type_class: Type[T], name: Optional[str] = None) -> Type[T]:
        """Export type, supports type checking"""
        type_name = name or type_class.__name__
        self._exported_types[type_name] = type_class
        return type_class

    def get_function(self, name: str) -> Optional[PluginFunction]:
        """Get exported function"""
        return self._functions.get(name)

    def get_type(self, name: str) -> Optional[Type[Any]]:
        """Get exported type"""
        return self._exported_types.get(name)

    def get_api_schema(self) -> Dict[str, Any]:
        """Get plugin API schema with extended information"""
        plugin_metadata_dict: Dict[str, Any] = {}
        if self.metadata:
            if hasattr(self.metadata, 'to_dict') and callable(self.metadata.to_dict):
                plugin_metadata_dict = self.metadata.to_dict()
            elif hasattr(self.metadata, 'model_dump') and callable(self.metadata.model_dump): # For Pydantic models
                plugin_metadata_dict = self.metadata.model_dump()
            else:
                plugin_metadata_dict = self.metadata.__dict__ # Fallback

        schema: Dict[str, Any] = {
            "functions": {},
            "types": {},
            "metadata": plugin_metadata_dict,
            "permissions": list(self._permissions),
            "version": self.metadata.version if self.metadata else "unknown"
        }

        # Collect function information
        for func_name_item, func_item in self._functions.items(): # Renamed
            if hasattr(func_item, 'get_signature_info') and callable(func_item.get_signature_info):
                schema["functions"][func_name_item] = func_item.get_signature_info()
            else:
                # Fallback if get_signature_info is not available
                actual_callable = func_item.func if hasattr(func_item, 'func') else func_item
                schema["functions"][func_name_item] = {
                    "name": func_name_item,
                    "description": func_item.description if hasattr(func_item, 'description') else "",
                    "is_async": asyncio.iscoroutinefunction(actual_callable)
                }

        # Collect type information
        for type_name_item, type_class_item in self._exported_types.items(): # Renamed
            type_info: Dict[str, Any] = {"fields": {}}
            
            # Handle dataclasses
            if hasattr(type_class_item, "__dataclass_fields__") and isinstance(getattr(type_class_item, "__dataclass_fields__"), dict):
                dc_fields = getattr(type_class_item, "__dataclass_fields__")
                for field_name, field_obj in dc_fields.items():
                    # Ensure field_obj is a dataclasses.Field or similar structure
                    field_type_str = str(getattr(field_obj, 'type', 'Unknown'))
                    default_val = getattr(field_obj, 'default', 'NoDefault')
                    default_factory_val = getattr(field_obj, 'default_factory', 'NoFactory')

                    final_default: Any = default_val
                    if default_val is not default_factory_val and default_factory_val != 'NoFactory': # Comparing to specific sentinel
                         # Prefer default_factory if it's set and different from default
                         # This logic might need adjustment based on how dataclasses.Field works
                         final_default = "has_factory"
                    elif default_val == 'NoDefault' and default_factory_val != 'NoFactory':
                         final_default = "has_factory"
                    elif default_val == 'NoDefault' and default_factory_val == 'NoFactory':
                         final_default = None # Or some other indicator of no default

                    type_info["fields"][field_name] = {
                        "type": field_type_str,
                        "default": final_default
                    }
            # Handle regular classes with annotations
            elif hasattr(type_class_item, "__annotations__") and isinstance(getattr(type_class_item, "__annotations__"), dict):
                annotations = getattr(type_class_item, "__annotations__")
                for field_name, field_type in annotations.items():
                    type_info["fields"][field_name] = {
                        "type": str(field_type),
                        "default": getattr(type_class_item, field_name, None) # Class default
                    }
                
            schema["types"][type_name_item] = type_info

        return schema

    async def send_message(self, target_plugin: str, message_content: Any) -> bool: # Renamed parameter
        """Send message to another plugin with timeout support"""
        if not self._plugin_manager or not (self.metadata and self.metadata.name):
            logger.warning(f"Plugin {self.metadata.name if self.metadata else 'unknown'} tried to send message but plugin manager or metadata.name is not available")
            return False
            
        try:
            # Limit message size
            message_str = json.dumps(message_content) if not isinstance(message_content, str) else message_content
            if len(message_str) > 1024 * 1024:  # 1MB limit
                logger.warning(f"Message too large ({len(message_str)} bytes), rejected")
                return False
            
            # Ensure _plugin_manager has send_plugin_message
            if hasattr(self._plugin_manager, 'send_plugin_message') and callable(self._plugin_manager.send_plugin_message):
                return await asyncio.wait_for(
                    self._plugin_manager.send_plugin_message(
                        self.metadata.name, 
                        target_plugin, 
                        message_content
                    ),
                    timeout=5.0
                )
            else:
                logger.error(f"Plugin manager does not support 'send_plugin_message'. Cannot send message from {self.metadata.name} to {target_plugin}.")
                return False
        except asyncio.TimeoutError:
            logger.error(f"Send message from {self.metadata.name} to plugin {target_plugin} timed out")
            return False
        except Exception as e:
            logger.error(f"Send message from {self.metadata.name} failed: {e}")
            return False

    def run_in_sandbox(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """Run function in sandbox with exception handling"""
        start_time_sandbox = time.time() # Renamed
        try:
            # Assuming _sandbox.apply() is a context manager
            with self._sandbox.apply(): # type: ignore[attr-defined] # If apply is not recognized
                result: R = func(*args, **kwargs)
                
            execution_time_sandbox = time.time() - start_time_sandbox # Renamed
            if hasattr(self._metrics, 'record_execution'):
                self._metrics.record_execution(execution_time_sandbox)
            return result
        except Exception as e:
            execution_time_sandbox_err = time.time() - start_time_sandbox # Renamed
            if hasattr(self._metrics, 'record_execution'):
                self._metrics.record_execution(execution_time_sandbox_err)
            if hasattr(self._metrics, 'record_error'):
                self._metrics.record_error()
            
            # Re-raise exception with context
            raise type(e)(f"Sandbox execution failed for {getattr(func, '__name__', 'sandboxed_function')}: {str(e)}") from e

    async def run_in_thread(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """Run function in thread pool, wait for result asynchronously"""
        if self._thread_pool is None:
            self._init_thread_pool() # Ensure pool is initialized
            if self._thread_pool is None: # Still None after init attempt
                raise RuntimeError(f"Thread pool not initialized for plugin {self.metadata.name if self.metadata else 'unknown'}")

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._thread_pool, 
            lambda: func(*args, **kwargs)
        )

    async def get_messages(self, timeout: float = 0.1) -> List[Any]:
        """Get pending messages with timeout support"""
        messages_list: List[Any] = [] # Renamed
        try:
            # Try to get messages within timeout
            while True: # Loop to get multiple messages if available quickly
                try:
                    message_item = await asyncio.wait_for( # Renamed
                        self._message_queue.get(), 
                        timeout=timeout # Timeout for each get operation
                    )
                    messages_list.append(message_item)
                    self._message_queue.task_done()
                except asyncio.TimeoutError:
                    # Timeout for a single get, means no more messages within this timeframe
                    break
                except asyncio.QueueEmpty: # Should not happen with await get() unless queue is closed
                    break
        except Exception as e:
            logger.error(f"Error getting messages for plugin {self.metadata.name if self.metadata else 'unknown'}: {e}")
            
        return messages_list
        
    def get_status(self) -> Dict[str, Any]:
        """Get plugin status"""
        metrics_summary_status = {}
        if hasattr(self._metrics, 'get_metrics_summary') and callable(self._metrics.get_metrics_summary):
            metrics_summary_status = self._metrics.get_metrics_summary()

        sandbox_status_info = {}
        if hasattr(self._sandbox, 'get_status') and callable(self._sandbox.get_status):
            sandbox_status_info = self._sandbox.get_status() # type: ignore[attr-defined]

        return {
            "name": self.metadata.name if self.metadata else "unknown",
            "version": self.metadata.version if self.metadata else "unknown",
            "state": self.state,
            "initialized": self._initialized,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "uptime": time.time() - self.start_time if self.start_time is not None else 0,
            "config_version": self._config_version,
            "health": self.health_status,
            "metrics": metrics_summary_status,
            "sandbox_status": sandbox_status_info,
        }
        
    def __del__(self) -> None:
        """Ensure resources are properly cleaned up"""
        try:
            self._shutdown_thread_pool()
        except Exception: # pylint: disable=broad-except
            # Log error during __del__ if necessary, but avoid raising
            pass
