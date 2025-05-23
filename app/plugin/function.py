"""
Plugin function export and management system.

This module provides functionality for exporting functions from plugins,
making them available for external access through a standardized interface.
"""
import inspect
import asyncio
import time
import traceback
from typing import Dict, Any, List, Callable, Optional, TypeVar, Type
from pydantic import create_model
from typing import get_type_hints
from functools import lru_cache
import hashlib
import json
from types import TracebackType
from loguru import logger

T = TypeVar('T')
R = TypeVar('R')


class PluginFunction:
    """
    Decorator and wrapper for plugin exported functions with parameter validation and metrics tracking
    """

    _registered_functions: Dict[str, Dict[str, Any]] = {}

    def __init__(self,
                 func: Optional[Callable[..., Any]] = None,
                 name: Optional[str] = None,
                 description: Optional[str] = None,
                 plugin_name: Optional[str] = None,
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
        if func is not None and name is None:
            self.name = func.__name__
        else:
            self.name = name or ""

        if func is not None:
            if description is None:
                self.description = func.__doc__ or ""
            else:
                self.description = description
        else:
            self.description = description or ""

        self.plugin_name = plugin_name

        if func is not None:
            self.type_hints = get_type_hints(func)
            self.signature = inspect.signature(func)
            self.is_coroutine = asyncio.iscoroutinefunction(func)
            self.param_model = self._create_param_model()

        self.cache_size = cache_size

        # Performance monitoring
        self.total_calls = 0
        self.total_errors = 0
        self.execution_times: List[float] = []
        self.max_execution_samples = 100

        # Initialize cache
        if func is not None:
            self._init_cache()

    def _create_param_model(self) -> Any:
        """Create parameter validation model using Pydantic"""
        fields: Dict[str, Any] = {}
        if hasattr(self, 'signature'):
            for param_name, param in self.signature.parameters.items():
                if param.name == 'self':
                    continue
                annotation = self.type_hints.get(param_name, Any)
                default = ... if param.default == inspect.Parameter.empty else param.default
                fields[param_name] = (annotation, default)
        return create_model(f'{self.name}_params', **fields)

    def _init_cache(self) -> None:
        """Initialize caching decorator"""
        @lru_cache(maxsize=self.cache_size)
        def cached_validate(_param_hash: str) -> bool:
            """Cache parameter validation results"""
            # param_hash is not used in this simplified cache validation
            return True

        self._cached_validate = cached_validate

        # Parameter dictionary cache
        self._param_cache: Dict[str, Any] = {}

    def _get_param_hash(self, kwargs: Dict[str, Any]) -> str:
        """Get parameter hash value for cache key"""
        # Convert dictionary to sorted string to ensure consistent hashing
        param_str = json.dumps(kwargs, sort_keys=True)
        return hashlib.md5(param_str.encode()).hexdigest()

    def clear_cache(self) -> None:
        """Clear validation cache"""
        if hasattr(self, '_cached_validate'):
            self._cached_validate.cache_clear()
        if hasattr(self, '_param_cache'):
            self._param_cache.clear()

    def get_signature_info(self) -> Dict[str, Any]:
        """Get function signature information"""
        if not hasattr(self, 'signature'):
            return {"name": self.name, "description": self.description, "parameters": {}, "return_type": None, "is_async": False}

        result: Dict[str, Any] = {
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

            param_info: Dict[str, Any] = {
                "required": param.default is inspect.Parameter.empty,
                "type": str(self.type_hints.get(param_name, Any)),
                "default": None if param.default is inspect.Parameter.empty else param.default
            }

            result["parameters"][param_name] = param_info

        return result

    @classmethod
    def register(cls, plugin_name: str, function: Callable[..., Any],
                 name: Optional[str] = None, description: Optional[str] = None,
                 schema: Optional[Dict[str, Any]] = None) -> Callable[..., Any]:
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
        params: Dict[str, Dict[str, Any]] = {}

        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue

            param_info: Dict[str, Any] = {
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
        return_type_str: Optional[str] = None
        if sig.return_annotation != inspect.Signature.empty:
            return_type_str = str(sig.return_annotation)

        # Store function metadata
        cls._registered_functions[qualified_name] = {
            "plugin": plugin_name,
            "name": func_name,
            "qualified_name": qualified_name,
            "description": description or function.__doc__ or f"Function {func_name}",
            "params": params,
            "return_type": return_type_str,
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
            to_remove: List[str] = []
            for name, info in cls._registered_functions.items():
                if info["plugin"] == plugin_name:
                    to_remove.append(name)

            for name_to_remove in to_remove:
                if name_to_remove in cls._registered_functions:
                    del cls._registered_functions[name_to_remove]

    @classmethod
    def get_function(cls, plugin_name: str, function_name: str) -> Optional[Callable[..., Any]]:
        """
        Get a registered function by name

        Args:
            plugin_name: Name of the plugin
            function_name: Name of the function

        Returns:
            The function if found, None otherwise
        """
        qualified_name = f"{plugin_name}.{function_name}"
        func_data = cls._registered_functions.get(qualified_name)
        if func_data:
            return func_data.get("function")
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
        result: List[Dict[str, Any]] = []
        for _, info in cls._registered_functions.items():
            if info["plugin"] == plugin_name:
                info_copy = dict(info)
                # Don't expose the actual function object
                if "function" in info_copy:
                    del info_copy["function"]
                result.append(info_copy)
        return result

    @classmethod
    def async_safe_call(cls, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
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
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                return loop.run_until_complete(func(*args, **kwargs))
            else:
                # Call synchronous function directly
                return func(*args, **kwargs)

        except Exception as e:
            logger.error(
                f"Error calling function {getattr(func, '__name__', 'unknown_function')}: {e}")
            logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the wrapped function with metrics tracking"""
        start_time = time.time()
        self.total_calls += 1

        try:
            if not hasattr(self, 'is_coroutine'):  # Ensure is_coroutine is set
                if self.func:
                    self.is_coroutine = asyncio.iscoroutinefunction(self.func)
                else:
                    raise ValueError(
                        "Function is not defined for PluginFunction call")

            if self.is_coroutine:
                # Handle async functions
                async def wrapped() -> Any:
                    try:
                        if self.func is not None:
                            return await self.func(*args, **kwargs)
                        else:
                            raise ValueError("Function is not defined")
                    except Exception as e:
                        self.total_errors += 1
                        logger.error(
                            f"Error in plugin function {self.name}: {e}")
                        logger.debug(
                            f"Exception details: {traceback.format_exc()}")
                        raise

                return wrapped()
            else:
                # Handle sync functions
                try:
                    if self.func is not None:
                        return self.func(*args, **kwargs)
                    else:
                        raise ValueError("Function is not defined")
                except Exception as e:
                    self.total_errors += 1
                    logger.error(f"Error in plugin function {self.name}: {e}")
                    logger.debug(
                        f"Exception details: {traceback.format_exc()}")
                    raise
        finally:
            # Record execution time
            exec_time = time.time() - start_time
            if len(self.execution_times) >= self.max_execution_samples:
                self.execution_times.pop(0)
            self.execution_times.append(exec_time)

    def get_metrics(self) -> Dict[str, Any]:
        """Get function execution metrics"""
        avg_time = sum(self.execution_times) / \
            len(self.execution_times) if self.execution_times else 0.0
        max_time = max(self.execution_times) if self.execution_times else 0.0
        min_time = min(self.execution_times) if self.execution_times else 0.0

        return {
            "total_calls": self.total_calls,
            "total_errors": self.total_errors,
            "error_rate": (self.total_errors / self.total_calls) if self.total_calls > 0 else 0.0,
            "avg_execution_time": avg_time,
            "max_execution_time": max_time,
            "min_execution_time": min_time,
            "sample_count": len(self.execution_times),
            "cache_size": self.cache_size if hasattr(self, 'cache_size') else 0,
        }


def export_function(name: Optional[str] = None, description: Optional[str] = None,
                    schema: Optional[Dict[str, Any]] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to export a plugin function, making it available through the API

    Args:
        name: Custom name for the function (defaults to function name)
        description: Description of what the function does
        schema: JSON Schema for function parameters

    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # The actual registration happens when the plugin loads and
        # scans for decorated functions
        export_info_data: Dict[str, Any] = {
            "name": name or func.__name__,
            "description": description or func.__doc__ or f"Function {func.__name__}",
            "schema": schema or {}
        }
        setattr(func, '_export_info', export_info_data)
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
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.success: bool = False
        # Changed from Exception to BaseException
        self.error: Optional[BaseException] = None

    def __enter__(self) -> 'FunctionExecutionContext':
        """Start tracking execution time"""
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 _exc_tb: Optional[TracebackType]) -> bool:
        """End tracking and record metrics"""
        self.end_time = time.time()
        self.success = exc_type is None
        self.error = exc_val

        # Record metrics in a central location if needed
        if self.start_time is not None:  # This check is fine
            duration = self.end_time - self.start_time
            logger.debug(
                f"Function {self.plugin_name}.{self.function_name} executed in {duration:.4f}s")

        # Don't suppress exceptions
        return False

    @property
    def duration(self) -> float:
        """Get execution duration in seconds"""
        if self.start_time is None:
            return 0.0
        current_end_time = self.end_time if self.end_time is not None else time.time()
        return current_end_time - self.start_time


class FunctionRegistry:
    """Central registry for plugin functions"""

    def __init__(self):
        """Initialize registry with empty collections"""
        self._functions: Dict[str, Dict[str, Any]] = {}
        self._metrics: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()  # For thread-safe operations

    async def register_function(self, plugin_name: str, func: Callable[..., Any],
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
                "description": description or func.__doc__ or "",
                "schema": schema or {},
                "function": func,
                "is_async": asyncio.iscoroutinefunction(func)
            }

            # Initialize metrics
            self._metrics[qualified_name] = {
                "calls": 0,
                "errors": 0,
                "total_time": 0.0,
                "avg_time": 0.0,
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
        to_remove: List[str] = []

        async with self._lock:
            # Iterate over a copy of items for safe removal
            for name_key, info in list(self._functions.items()):
                if info["plugin"] == plugin_name:
                    to_remove.append(name_key)

            for name_to_remove in to_remove:
                if name_to_remove in self._functions:
                    del self._functions[name_to_remove]
                if name_to_remove in self._metrics:
                    del self._metrics[name_to_remove]

        return to_remove

    async def call_function(self, plugin_name: str, function_name: str,
                            *args: Any, **kwargs: Any) -> Any:
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
        function_info: Optional[Dict[str, Any]] = None
        func_to_call: Optional[Callable[..., Any]] = None

        async with self._lock:
            if qualified_name not in self._functions:
                raise ValueError(f"Function {qualified_name} not found")
            function_info = self._functions[qualified_name]
            func_to_call = function_info["function"]

        if func_to_call is None:  # Should not happen if logic above is correct
            raise ValueError(
                f"Function {qualified_name} data is inconsistent.")

        # Track execution
        with FunctionExecutionContext(plugin_name, function_name) as context:
            try:
                # Update metrics before call
                async with self._lock:
                    self._metrics[qualified_name]["calls"] += 1
                    self._metrics[qualified_name]["last_call"] = time.time()

                # Call function based on type
                if function_info["is_async"]:
                    result = await func_to_call(*args, **kwargs)
                else:
                    result = func_to_call(*args, **kwargs)

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
                    # Ensure calls is at least 1 for avg_time
                    calls = self._metrics[qualified_name]["calls"]
                    if calls > 0:
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
        result: List[Dict[str, Any]] = []
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

    def get_function(self, plugin_name: str, function_name: str) -> Optional[Callable[..., Any]]:
        """
        Get the actual function object

        Args:
            plugin_name: Name of the plugin
            function_name: Name of the function

        Returns:
            Function object or None if not found
        """
        qualified_name = f"{plugin_name}.{function_name}"
        # No lock needed for read if self._functions structure is stable after init
        # However, if plugins can be registered/unregistered dynamically, a lock might be safer
        # For simplicity and assuming potential dynamic changes, keeping it simple:
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
                # Iterate over a copy
                for qualified_name, info in list(self._functions.items()):
                    if info["plugin"] == plugin_name:
                        if qualified_name in self._metrics:
                            self._metrics[qualified_name] = {
                                "calls": 0,
                                "errors": 0,
                                "total_time": 0.0,
                                "avg_time": 0.0,
                                "last_call": None
                            }
                            count += 1
            else:
                # Reset all metrics
                # Iterate over a copy
                for qualified_name in list(self._metrics.keys()):
                    self._metrics[qualified_name] = {
                        "calls": 0,
                        "errors": 0,
                        "total_time": 0.0,
                        "avg_time": 0.0,
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
            return dict(self._metrics)  # Return a copy
