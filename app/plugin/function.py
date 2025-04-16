"""
Plugin function export and management system.

This module provides functionality for exporting functions from plugins,
making them available for external access through a standardized interface.
"""
import inspect
import asyncio
import time
import functools
import logging
import traceback
from typing import Dict, Any, List, Callable, Optional, Union, Set
from pydantic import create_model, ValidationError
from typing import get_type_hints
from functools import lru_cache, wraps
import hashlib
import json

logger = logging.getLogger(__name__)


class PluginFunction:
    """
    Decorator and wrapper for plugin exported functions with parameter validation and metrics tracking
    """
    
    _registered_functions: Dict[str, Dict[str, Any]] = {}
    
    def __init__(self, 
                 func: Callable = None, 
                 name: str = None, 
                 description: str = None,
                 plugin_name: str = None,
                 cache_size: int = 128):
        """
        Initialize a plugin function wrapper
        
        Args:
            func: Function to wrap
            name: Custom name for the function
            description: Description of the function
            plugin_name: Name of the plugin that owns this function
            cache_size: Size of the parameter validation cache
        """
        self.func = func
        self.name = name or (func.__name__ if func else None)
        self.description = description or (func.__doc__ or "" if func else "")
        self.plugin_name = plugin_name
        
        if func:
            self.type_hints = get_type_hints(func)
            self.signature = inspect.signature(func)
            self.is_coroutine = asyncio.iscoroutinefunction(func)
            self.param_model = self._create_param_model()
        
        self.cache_size = cache_size
        
        # Performance monitoring
        self.total_calls = 0
        self.total_errors = 0
        self.execution_times = []
        self.max_execution_samples = 100
        
        # Initialize cache
        if func:
            self._init_cache()
    
    def _create_param_model(self):
        """Create parameter validation model using Pydantic"""
        fields = {}
        for param_name, param in self.signature.parameters.items():
            if param.name == 'self':
                continue
            annotation = self.type_hints.get(param_name, Any)
            default = ... if param.default == inspect.Parameter.empty else param.default
            fields[param_name] = (annotation, default)
        return create_model(f'{self.name}_params', **fields)
    
    def _init_cache(self):
        """Initialize caching decorator"""
        @lru_cache(maxsize=self.cache_size)
        def cached_validate(param_hash):
            """Cache parameter validation results"""
            return True
        
        self._cached_validate = cached_validate
        
        # Parameter dictionary cache
        self._param_cache = {}
    
    def _get_param_hash(self, kwargs: Dict) -> str:
        """Get parameter hash value for cache key"""
        # Convert dictionary to sorted string to ensure consistent hashing
        param_str = json.dumps(kwargs, sort_keys=True)
        return hashlib.md5(param_str.encode()).hexdigest()
    
    def clear_cache(self):
        """Clear validation cache"""
        if hasattr(self, '_cached_validate'):
            self._cached_validate.cache_clear()
        if hasattr(self, '_param_cache'):
            self._param_cache.clear()
    
    def get_signature_info(self) -> Dict[str, Any]:
        """Get function signature information"""
        if not hasattr(self, 'signature'):
            return {"name": self.name, "description": self.description}
            
        result = {
            "name": self.name,
            "description": self.description,
            "is_async": self.is_coroutine,
            "parameters": {},
            "return_type": None
        }
        
        # Get return type
        if self.signature.return_annotation is not inspect.Signature.empty:
            result["return_type"] = str(self.signature.return_annotation)
            
        # Get parameters
        for param_name, param in self.signature.parameters.items():
            if param_name == 'self':
                continue
                
            param_info = {
                "required": param.default is inspect.Parameter.empty,
                "type": str(self.type_hints.get(param_name, Any)),
                "default": None if param.default is inspect.Parameter.empty else param.default
            }
            
            result["parameters"][param_name] = param_info
            
        return result
    
    @classmethod
    def register(cls, plugin_name: str, function: Callable, 
                name: Optional[str] = None, description: Optional[str] = None,
                schema: Optional[Dict[str, Any]] = None) -> Callable:
        """
        Register a function to make it available through the API
        
        Args:
            plugin_name: Name of the plugin that owns the function
            function: The function to register
            name: Optional custom name for the function
            description: Optional description of the function
            schema: Optional JSON schema for the function parameters
            
        Returns:
            The original function (decorated)
        """
        func_name = name or function.__name__
        qualified_name = f"{plugin_name}.{func_name}"
        
        # Get function signature and parameters
        sig = inspect.signature(function)
        params = {}
        
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
                
            param_info = {
                "name": param_name,
                "required": param.default == inspect.Parameter.empty
            }
            
            # Get parameter type info if available
            if param.annotation != inspect.Parameter.empty:
                param_info["type"] = str(param.annotation)
                
            # Get default value if available
            if param.default != inspect.Parameter.empty:
                param_info["default"] = param.default
                
            params[param_name] = param_info
        
        # Get return type hint if available
        return_type = None
        if sig.return_annotation != inspect.Signature.empty:
            return_type = str(sig.return_annotation)
            
        # Store function metadata
        cls._registered_functions[qualified_name] = {
            "plugin": plugin_name,
            "name": func_name,
            "qualified_name": qualified_name,
            "description": description or function.__doc__ or f"Function {func_name}",
            "params": params,
            "return_type": return_type,
            "is_async": asyncio.iscoroutinefunction(function),
            "schema": schema or {},
            "function": function
        }
        
        return function
    
    @classmethod
    def unregister(cls, plugin_name: str, function_name: Optional[str] = None) -> None:
        """
        Unregister a function or all functions from a plugin
        
        Args:
            plugin_name: Name of the plugin
            function_name: Optional name of the function to unregister
        """
        if function_name:
            qualified_name = f"{plugin_name}.{function_name}"
            if qualified_name in cls._registered_functions:
                del cls._registered_functions[qualified_name]
        else:
            # Remove all functions from the specified plugin
            to_remove = []
            for name, info in cls._registered_functions.items():
                if info["plugin"] == plugin_name:
                    to_remove.append(name)
                    
            for name in to_remove:
                del cls._registered_functions[name]
    
    @classmethod
    def get_function(cls, plugin_name: str, function_name: str) -> Optional[Callable]:
        """
        Get a registered function by name
        
        Args:
            plugin_name: Name of the plugin
            function_name: Name of the function
            
        Returns:
            The function if found, None otherwise
        """
        qualified_name = f"{plugin_name}.{function_name}"
        if qualified_name in cls._registered_functions:
            return cls._registered_functions[qualified_name]["function"]
        return None
    
    @classmethod
    def get_function_info(cls, plugin_name: str, function_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a registered function
        
        Args:
            plugin_name: Name of the plugin
            function_name: Name of the function
            
        Returns:
            Dictionary with function information if found, None otherwise
        """
        qualified_name = f"{plugin_name}.{function_name}"
        if qualified_name in cls._registered_functions:
            info = dict(cls._registered_functions[qualified_name])
            # Don't expose the actual function object
            if "function" in info:
                del info["function"]
            return info
        return None
    
    @classmethod
    def get_plugin_functions(cls, plugin_name: str) -> List[Dict[str, Any]]:
        """
        Get all registered functions for a plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            List of functions registered by the plugin
        """
        result = []
        for name, info in cls._registered_functions.items():
            if info["plugin"] == plugin_name:
                info_copy = dict(info)
                # Don't expose the actual function object
                if "function" in info_copy:
                    del info_copy["function"]
                result.append(info_copy)
        return result
    
    @classmethod
    def async_safe_call(cls, func: Callable, *args, **kwargs) -> Any:
        """
        Call a function safely, handling both async and sync functions
        
        Args:
            func: Function to call
            *args: Arguments to pass
            **kwargs: Keyword arguments to pass
            
        Returns:
            Function result
        """
        try:
            if asyncio.iscoroutinefunction(func):
                # Create a new event loop if we're not in one
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                return loop.run_until_complete(func(*args, **kwargs))
            else:
                # Call synchronous function directly
                return func(*args, **kwargs)
                
        except Exception as e:
            logger.error(f"Error calling function {func.__name__}: {e}")
            logger.debug(f"Exception details: {traceback.format_exc()}")
            raise
    
    def __call__(self, *args, **kwargs):
        """Call the wrapped function with metrics tracking"""
        start_time = time.time()
        self.total_calls += 1
        
        try:
            if self.is_coroutine:
                # Handle async functions
                async def wrapped():
                    try:
                        return await self.func(*args, **kwargs)
                    except Exception as e:
                        self.total_errors += 1
                        logger.error(f"Error in plugin function {self.name}: {e}")
                        logger.debug(f"Exception details: {traceback.format_exc()}")
                        raise
                
                return wrapped()
            else:
                # Handle sync functions
                try:
                    return self.func(*args, **kwargs)
                except Exception as e:
                    self.total_errors += 1
                    logger.error(f"Error in plugin function {self.name}: {e}")
                    logger.debug(f"Exception details: {traceback.format_exc()}")
                    raise
        finally:
            # Record execution time
            exec_time = time.time() - start_time
            if len(self.execution_times) >= self.max_execution_samples:
                self.execution_times.pop(0)
            self.execution_times.append(exec_time)

    def get_metrics(self) -> Dict[str, Any]:
        """Get function execution metrics"""
        avg_time = sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0
        max_time = max(self.execution_times) if self.execution_times else 0
        min_time = min(self.execution_times) if self.execution_times else 0
        
        return {
            "total_calls": self.total_calls,
            "total_errors": self.total_errors,
            "error_rate": self.total_errors / self.total_calls if self.total_calls > 0 else 0,
            "avg_execution_time": avg_time,
            "max_execution_time": max_time,
            "min_execution_time": min_time,
            "sample_count": len(self.execution_times),
            "cache_size": self.cache_size,
        }


def export_function(name: Optional[str] = None, description: Optional[str] = None,
                  schema: Optional[Dict[str, Any]] = None):
    """
    Decorator to export a plugin function, making it available through the API
    
    Args:
        name: Custom name for the function (defaults to function name)
        description: Description of what the function does
        schema: JSON Schema for function parameters
        
    Returns:
        Decorated function
    """
    def decorator(func):
        # The actual registration happens when the plugin loads and
        # scans for decorated functions
        func._export_info = {
            "name": name or func.__name__,
            "description": description or func.__doc__ or f"Function {func.__name__}",
            "schema": schema or {}
        }
        return func
    return decorator


class FunctionExecutionContext:
    """Context for function execution with metrics tracking"""
    
    def __init__(self, plugin_name: str, function_name: str):
        """
        Initialize execution context
        
        Args:
            plugin_name: Name of the plugin
            function_name: Name of the function
        """
        self.plugin_name = plugin_name
        self.function_name = function_name
        self.start_time = None
        self.end_time = None
        self.success = False
        self.error = None
    
    def __enter__(self):
        """Start tracking execution time"""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End tracking and record metrics"""
        self.end_time = time.time()
        self.success = exc_type is None
        self.error = exc_val
        
        # Record metrics in a central location if needed
        duration = self.end_time - self.start_time
        logger.debug(f"Function {self.plugin_name}.{self.function_name} executed in {duration:.4f}s")
        
        # Don't suppress exceptions
        return False
    
    @property
    def duration(self) -> float:
        """Get execution duration in seconds"""
        if self.start_time is None:
            return 0
        end = self.end_time or time.time()
        return end - self.start_time


class FunctionRegistry:
    """Central registry for plugin functions"""
    
    def __init__(self):
        """Initialize registry with empty collections"""
        self._functions: Dict[str, Dict[str, Any]] = {}
        self._metrics: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()  # For thread-safe operations
    
    async def register_function(self, plugin_name: str, func: Callable,
                        name: Optional[str] = None, 
                        description: Optional[str] = None,
                        schema: Optional[Dict[str, Any]] = None) -> str:
        """
        Register a function
        
        Args:
            plugin_name: Plugin that owns the function
            func: Function to register
            name: Custom name (defaults to function name)
            description: Function description
            schema: Parameter schema
            
        Returns:
            Qualified function name
        """
        func_name = name or func.__name__
        qualified_name = f"{plugin_name}.{func_name}"
        
        async with self._lock:
            self._functions[qualified_name] = {
                "plugin": plugin_name,
                "name": func_name,
                "description": description or func.__doc__,
                "schema": schema,
                "function": func,
                "is_async": asyncio.iscoroutinefunction(func)
            }
            
            # Initialize metrics
            self._metrics[qualified_name] = {
                "calls": 0,
                "errors": 0,
                "total_time": 0,
                "avg_time": 0,
                "last_call": None
            }
            
        return qualified_name
    
    async def unregister_plugin(self, plugin_name: str) -> List[str]:
        """
        Unregister all functions from a plugin
        
        Args:
            plugin_name: Name of plugin to unregister
            
        Returns:
            List of unregistered function names
        """
        to_remove = []
        
        async with self._lock:
            for name, info in self._functions.items():
                if info["plugin"] == plugin_name:
                    to_remove.append(name)
                    
            for name in to_remove:
                if name in self._functions:
                    del self._functions[name]
                if name in self._metrics:
                    del self._metrics[name]
                    
        return to_remove
    
    async def call_function(self, plugin_name: str, function_name: str,
                         *args, **kwargs) -> Any:
        """
        Call a registered function
        
        Args:
            plugin_name: Plugin that owns the function
            function_name: Name of the function
            *args: Arguments to pass
            **kwargs: Keyword arguments to pass
            
        Returns:
            Function result
            
        Raises:
            ValueError: If function not found
            Exception: Any exception raised by the function
        """
        qualified_name = f"{plugin_name}.{function_name}"
        
        # First check if function exists
        function_info = None
        async with self._lock:
            if qualified_name not in self._functions:
                raise ValueError(f"Function {qualified_name} not found")
            function_info = self._functions[qualified_name]
        
        func = function_info["function"]
        
        # Track execution
        with FunctionExecutionContext(plugin_name, function_name) as context:
            try:
                # Update metrics before call
                async with self._lock:
                    self._metrics[qualified_name]["calls"] += 1
                    self._metrics[qualified_name]["last_call"] = time.time()
                
                # Call function based on type
                if function_info["is_async"]:
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                    
                return result
                
            except Exception as e:
                # Update error metrics
                async with self._lock:
                    self._metrics[qualified_name]["errors"] += 1
                logger.error(f"Error calling function {qualified_name}: {e}")
                logger.debug(f"Exception details: {traceback.format_exc()}")
                raise
            finally:
                # Update timing metrics
                async with self._lock:
                    duration = context.duration
                    self._metrics[qualified_name]["total_time"] += duration
                    calls = max(1, self._metrics[qualified_name]["calls"])
                    self._metrics[qualified_name]["avg_time"] = (
                        self._metrics[qualified_name]["total_time"] / calls
                    )
    
    async def get_plugin_functions(self, plugin_name: str) -> List[Dict[str, Any]]:
        """
        Get all functions registered by a plugin
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            List of function information dictionaries
        """
        result = []
        async with self._lock:
            for name, info in self._functions.items():
                if info["plugin"] == plugin_name:
                    info_copy = dict(info)
                    # Don't include the actual function
                    if "function" in info_copy:
                        del info_copy["function"]
                    # Add metrics
                    if name in self._metrics:
                        info_copy["metrics"] = self._metrics[name]
                    result.append(info_copy)
        return result
    
    async def get_function_info(self, plugin_name: str, 
                        function_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a specific function
        
        Args:
            plugin_name: Name of the plugin
            function_name: Name of the function
            
        Returns:
            Function information or None if not found
        """
        qualified_name = f"{plugin_name}.{function_name}"
        async with self._lock:
            if qualified_name in self._functions:
                info = dict(self._functions[qualified_name])
                # Don't include the actual function
                if "function" in info:
                    del info["function"]
                # Add metrics
                if qualified_name in self._metrics:
                    info["metrics"] = self._metrics[qualified_name]
                return info
        return None
    
    def get_function(self, plugin_name: str, function_name: str) -> Optional[Callable]:
        """
        Get the actual function object
        
        Args:
            plugin_name: Name of the plugin
            function_name: Name of the function
            
        Returns:
            Function object or None if not found
        """
        qualified_name = f"{plugin_name}.{function_name}"
        function_info = self._functions.get(qualified_name)
        if function_info:
            return function_info.get("function")
        return None
    
    async def reset_metrics(self, plugin_name: Optional[str] = None) -> int:
        """
        Reset metrics for a plugin or all plugins
        
        Args:
            plugin_name: Name of the plugin, or None for all
            
        Returns:
            Number of function metrics reset
        """
        count = 0
        async with self._lock:
            if plugin_name:
                # Reset metrics for specific plugin
                for qualified_name, info in self._functions.items():
                    if info["plugin"] == plugin_name:
                        if qualified_name in self._metrics:
                            self._metrics[qualified_name] = {
                                "calls": 0,
                                "errors": 0,
                                "total_time": 0,
                                "avg_time": 0,
                                "last_call": None
                            }
                            count += 1
            else:
                # Reset all metrics
                for qualified_name in self._metrics.keys():
                    self._metrics[qualified_name] = {
                        "calls": 0,
                        "errors": 0,
                        "total_time": 0,
                        "avg_time": 0,
                        "last_call": None
                    }
                    count += 1
        
        return count
    
    async def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get metrics for all functions
        
        Returns:
            Dictionary of function metrics
        """
        async with self._lock:
            return dict(self._metrics)
