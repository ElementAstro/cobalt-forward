"""
Utility functions for the plugin system.

This module provides various utility functions and helpers
for plugin development, loading, and management.
"""

import os
import sys
import traceback
import uuid
import time
import json
import logging
import inspect
import importlib
import importlib.util
import hashlib
import asyncio
import functools
import shutil
from pathlib import Path
from typing import Dict, List, Any, Callable, Optional, Union, Type, Set, Tuple, TypeVar, Generic

import yaml

logger = logging.getLogger(__name__)

# Type variable for generic functions
T = TypeVar('T')


def generate_plugin_id(name: str, version: str = "1.0.0") -> str:
    """
    Generate a unique plugin ID based on name and version
    
    Args:
        name: Plugin name
        version: Plugin version
        
    Returns:
        Unique plugin ID
    """
    base = f"{name}:{version}:{int(time.time())}"
    return hashlib.md5(base.encode()).hexdigest()


def find_plugins(directory: Union[str, Path]) -> List[str]:
    """
    Find Python files in a directory that might be plugins
    
    Args:
        directory: Directory to search
        
    Returns:
        List of potential plugin file paths
    """
    if isinstance(directory, str):
        directory = Path(directory)
        
    if not directory.exists() or not directory.is_dir():
        logger.warning(f"Plugin directory not found: {directory}")
        return []
        
    return [str(p) for p in directory.glob("*.py")
            if p.is_file() and not p.name.startswith("_")]


def import_module_from_path(path: str) -> Tuple[Any, Optional[Exception]]:
    """
    Import a Python module from a file path
    
    Args:
        path: Path to Python file
        
    Returns:
        Tuple of (module, exception) - if successful, exception is None
    """
    try:
        module_name = os.path.basename(path)
        if module_name.endswith(".py"):
            module_name = module_name[:-3]
            
        # Create a unique module name to avoid conflicts
        module_name = f"_plugin_{module_name}_{uuid.uuid4().hex[:8]}"
        
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            return None, ImportError(f"Could not load spec for {path}")
            
        module = importlib.util.module_from_spec(spec)
        
        # Add module to sys.modules
        sys.modules[module_name] = module
        
        # Execute module
        spec.loader.exec_module(module)
        
        return module, None
        
    except Exception as e:
        logger.error(f"Error importing module from {path}: {e}")
        logger.debug(f"Import error details: {traceback.format_exc()}")
        return None, e


def discover_plugin_classes(module: Any, base_class: Type) -> List[Type]:
    """
    Find all classes in a module that are subclasses of base_class
    
    Args:
        module: Imported module object
        base_class: Base class to find subclasses of
        
    Returns:
        List of plugin classes
    """
    result = []
    for name, obj in inspect.getmembers(module):
        if (inspect.isclass(obj) and 
            issubclass(obj, base_class) and 
            obj is not base_class and 
            not name.startswith("_")):
            result.append(obj)
    return result


def discover_plugin_functions(module: Any, decorator_attribute: str = "_export_info") -> List[Tuple[str, Callable]]:
    """
    Find all functions in a module that have been decorated for export
    
    Args:
        module: Imported module object
        decorator_attribute: Attribute added by decorator to mark functions
        
    Returns:
        List of (name, function) pairs
    """
    result = []
    for name, obj in inspect.getmembers(module):
        if inspect.isfunction(obj) and hasattr(obj, decorator_attribute):
            result.append((name, obj))
    return result


def safe_reload(module: Any) -> Tuple[Any, Optional[Exception]]:
    """
    Safely reload a module
    
    Args:
        module: Module to reload
        
    Returns:
        Tuple of (reloaded_module, exception)
    """
    try:
        return importlib.reload(module), None
    except Exception as e:
        logger.error(f"Error reloading module {module.__name__}: {e}")
        logger.debug(f"Reload error details: {traceback.format_exc()}")
        return None, e


class PluginMetricsCollector:
    """
    Collects and aggregates plugin metrics
    """
    
    def __init__(self, plugin_name: str):
        """
        Initialize metrics collector
        
        Args:
            plugin_name: Name of the plugin
        """
        self.plugin_name = plugin_name
        self.start_time = time.time()
        self.execution_count = 0
        self.error_count = 0
        self.execution_times = []
        self.max_execution_samples = 100
        self._lock = asyncio.Lock()  # Thread-safe lock for metrics
        
    async def record_execution_async(self, execution_time: float = None):
        """
        Record a successful execution asynchronously
        
        Args:
            execution_time: Time taken for execution
        """
        async with self._lock:
            self.execution_count += 1
            if execution_time is not None:
                self.execution_times.append(execution_time)
                # Keep only the most recent samples
                if len(self.execution_times) > self.max_execution_samples:
                    self.execution_times = self.execution_times[-self.max_execution_samples:]
        
    def record_execution(self, execution_time: float = None):
        """
        Record a successful execution
        
        Args:
            execution_time: Time taken for execution
        """
        self.execution_count += 1
        if execution_time is not None:
            self.execution_times.append(execution_time)
            # Keep only the most recent samples
            if len(self.execution_times) > self.max_execution_samples:
                self.execution_times = self.execution_times[-self.max_execution_samples:]
                
    async def record_error_async(self):
        """Record an execution error asynchronously"""
        async with self._lock:
            self.error_count += 1
                
    def record_error(self):
        """Record an execution error"""
        self.error_count += 1
        
    async def get_metrics_summary_async(self) -> Dict[str, Any]:
        """
        Get summary of collected metrics asynchronously
        
        Returns:
            Dictionary with metrics
        """
        async with self._lock:
            return self._generate_metrics_summary()
        
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get summary of collected metrics
        
        Returns:
            Dictionary with metrics
        """
        return self._generate_metrics_summary()
        
    def _generate_metrics_summary(self) -> Dict[str, Any]:
        """
        Generate metrics summary
        
        Returns:
            Dictionary with metrics
        """
        avg_time = sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0
        return {
            "plugin": self.plugin_name,
            "uptime": time.time() - self.start_time,
            "execution_count": self.execution_count,
            "error_count": self.error_count,
            "error_rate": self.error_count / max(1, self.execution_count),
            "avg_execution_time": avg_time,
            "max_execution_time": max(self.execution_times) if self.execution_times else 0,
            "min_execution_time": min(self.execution_times) if self.execution_times else 0,
            "timestamp": time.time()
        }
        
    def reset(self):
        """Reset all metrics"""
        self.execution_count = 0
        self.error_count = 0
        self.execution_times = []


def catch_exceptions(func):
    """
    Decorator to catch and log exceptions
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function
    """
    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Exception in {func.__name__}: {e}")
            logger.debug(f"Exception details: {traceback.format_exc()}")
            return None
            
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Exception in {func.__name__}: {e}")
            logger.debug(f"Exception details: {traceback.format_exc()}")
            return None
            
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return sync_wrapper


def retry(max_attempts: int = 3, delay: float = 1.0, backoff_factor: float = 2.0, exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """
    Decorator to retry function calls that fail
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Factor to increase delay with each attempt
        exceptions: Tuple of exception types to catch and retry
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    logger.warning(f"Retry {attempt+1}/{max_attempts} for {func.__name__} failed: {e}")
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
            
            logger.error(f"All {max_attempts} retries failed for {func.__name__}")
            if last_exception:
                raise last_exception
                
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    logger.warning(f"Retry {attempt+1}/{max_attempts} for {func.__name__} failed: {e}")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff_factor
            
            logger.error(f"All {max_attempts} retries failed for {func.__name__}")
            if last_exception:
                raise last_exception
                
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    return decorator


def measure_time(func):
    """
    Decorator to measure function execution time
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function
    """
    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        logger.debug(f"{func.__name__} executed in {execution_time:.4f}s")
        return result
        
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        execution_time = time.time() - start_time
        logger.debug(f"{func.__name__} executed in {execution_time:.4f}s")
        return result
        
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return sync_wrapper


class AsyncCache(Generic[T]):
    """
    Asynchronous cache decorator with TTL
    
    Example:
        cache = AsyncCache(ttl=60)  # 1 minute TTL
        
        @cache
        async def fetch_data(key):
            # Expensive operation
            return result
    """
    
    def __init__(self, ttl: float = 60.0, max_size: int = 100):
        """
        Initialize cache
        
        Args:
            ttl: Time-to-live in seconds
            max_size: Maximum cache size
        """
        self.ttl = ttl
        self.max_size = max_size
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task = None
        
    def __call__(self, func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = self._generate_key(func.__name__, args, kwargs)
            
            # Check if cached value exists and is valid
            async with self._lock:
                if cache_key in self.cache:
                    entry = self.cache[cache_key]
                    if time.time() < entry["expires_at"]:
                        self.access_times[cache_key] = time.time()
                        return entry["value"]
            
            # If cache miss, call the function
            result = await func(*args, **kwargs)
            
            # Store result in cache
            async with self._lock:
                self.cache[cache_key] = {
                    "value": result,
                    "expires_at": time.time() + self.ttl
                }
                self.access_times[cache_key] = time.time()
                
                # If cache is too large, remove oldest entries
                if len(self.cache) > self.max_size:
                    self._cleanup_oldest()
                
                # Start cleanup task if needed
                if self._cleanup_task is None or self._cleanup_task.done():
                    self._cleanup_task = asyncio.create_task(self._cleanup_loop())
                    
            return result
            
        return wrapper
        
    def _generate_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """
        Generate a unique cache key
        
        Args:
            func_name: Function name
            args: Positional arguments
            kwargs: Keyword arguments
            
        Returns:
            Cache key string
        """
        key_parts = [func_name]
        
        # Handle positional arguments
        for arg in args:
            key_parts.append(str(arg))
            
        # Handle keyword arguments
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
            
        # Hash the key
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
        
    def _cleanup_oldest(self):
        """Remove the oldest cache entry"""
        if not self.access_times:
            return
            
        oldest_key = min(self.access_times.items(), key=lambda x: x[1])[0]
        if oldest_key in self.cache:
            del self.cache[oldest_key]
        if oldest_key in self.access_times:
            del self.access_times[oldest_key]
            
    async def _cleanup_loop(self):
        """Periodically clean up expired cache entries"""
        try:
            while True:
                # Wait before checking
                await asyncio.sleep(min(self.ttl / 2, 30))
                
                # Clean up expired entries
                async with self._lock:
                    now = time.time()
                    expired_keys = []
                    
                    for key, entry in self.cache.items():
                        if now > entry["expires_at"]:
                            expired_keys.append(key)
                    
                    for key in expired_keys:
                        if key in self.cache:
                            del self.cache[key]
                        if key in self.access_times:
                            del self.access_times[key]
                            
                    if expired_keys:
                        logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
                        
        except asyncio.CancelledError:
            logger.debug("Cache cleanup task cancelled")
        except Exception as e:
            logger.error(f"Error in cache cleanup: {e}")
            
    async def clear(self):
        """Clear all cache entries"""
        async with self._lock:
            self.cache.clear()
            self.access_times.clear()
            logger.debug("Cache cleared")
            
    async def invalidate(self, func_name: str, *args, **kwargs):
        """
        Invalidate a specific cache entry
        
        Args:
            func_name: Function name
            *args: Positional arguments
            **kwargs: Keyword arguments
        """
        async with self._lock:
            cache_key = self._generate_key(func_name, args, kwargs)
            if cache_key in self.cache:
                del self.cache[cache_key]
            if cache_key in self.access_times:
                del self.access_times[cache_key]
                
            logger.debug(f"Cache entry invalidated for {func_name}")


def plugin_enabled(plugin_name: str, enabled_plugins: Set[str]) -> bool:
    """
    Check if a plugin is enabled
    
    Args:
        plugin_name: Name of the plugin to check
        enabled_plugins: Set of enabled plugin names
        
    Returns:
        True if plugin is enabled
    """
    return plugin_name in enabled_plugins or '*' in enabled_plugins


def load_plugin_config(config_file: Union[str, Path]) -> Dict[str, Any]:
    """
    Load plugin configuration from a file
    
    Args:
        config_file: Path to config file
        
    Returns:
        Configuration dictionary
    """
    if isinstance(config_file, str):
        config_file = Path(config_file)
        
    if not config_file.exists():
        logger.warning(f"Plugin config file not found: {config_file}")
        return {}
        
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            if config_file.suffix.lower() == '.json':
                return json.load(f)
            elif config_file.suffix.lower() in ('.yaml', '.yml'):
                return yaml.safe_load(f) or {}
            else:
                logger.warning(f"Unknown config file format: {config_file}")
                return {}
    except Exception as e:
        logger.error(f"Error loading plugin config from {config_file}: {e}")
        logger.debug(f"Config load error details: {traceback.format_exc()}")
        return {}


def save_plugin_config(config: Dict[str, Any], config_file: Union[str, Path]) -> bool:
    """
    Save plugin configuration to a file
    
    Args:
        config: Configuration dictionary
        config_file: Path to config file
        
    Returns:
        True if successful
    """
    if isinstance(config_file, str):
        config_file = Path(config_file)
        
    try:
        # Create parent directory if it doesn't exist
        if not config_file.parent.exists():
            config_file.parent.mkdir(parents=True)
            
        with open(config_file, 'w', encoding='utf-8') as f:
            if config_file.suffix.lower() == '.json':
                json.dump(config, f, indent=2, ensure_ascii=False)
            elif config_file.suffix.lower() in ('.yaml', '.yml'):
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
            else:
                logger.warning(f"Unknown config file format: {config_file}")
                return False
        return True
    except Exception as e:
        logger.error(f"Error saving plugin config to {config_file}: {e}")
        logger.debug(f"Config save error details: {traceback.format_exc()}")
        return False


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


def get_file_hash(file_path: str) -> str:
    """
    Calculate MD5 hash of a file
    
    Args:
        file_path: Path to the file
        
    Returns:
        MD5 hash as a hexadecimal string
    """
    try:
        if not os.path.exists(file_path):
            return ""
            
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash for {file_path}: {e}")
        return ""


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """
    Load and parse YAML config file
    
    Args:
        file_path: Path to the YAML file
        
    Returns:
        Dictionary with configuration
    """
    try:
        if not os.path.exists(file_path):
            return {}
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Error loading YAML config from {file_path}: {e}")
        return {}


def safe_import_module(module_name: str, file_path: Optional[str] = None,
                       reload: bool = False) -> Optional[Any]:
    """
    Safely import a module with error handling
    
    Args:
        module_name: Name of the module to import
        file_path: Optional file path for direct imports
        reload: Whether to reload the module if already loaded
        
    Returns:
        Loaded module or None if import failed
    """
    try:
        if file_path and os.path.exists(file_path):
            # Direct import from file path
            spec = importlib.util.spec_from_file_location(
                module_name, file_path)
            if not spec or not spec.loader:
                logger.error(f"Failed to create module spec for {file_path}")
                return None
                
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        else:
            # Standard import by name
            if reload and module_name in sys.modules:
                module = importlib.reload(sys.modules[module_name])
            else:
                module = importlib.import_module(module_name)
                
        return module
    except Exception as e:
        logger.error(f"Error importing module {module_name}: {e}")
        logger.debug(f"Import error details: {traceback.format_exc()}")
        return None


def ensure_directory(directory: Union[str, Path]) -> bool:
    """
    Ensure a directory exists, create it if necessary
    
    Args:
        directory: Directory path
        
    Returns:
        True if directory exists or was created, False otherwise
    """
    try:
        if isinstance(directory, str):
            directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error creating directory {directory}: {e}")
        return False


def safe_json_dump(data: Any, file_path: str, indent: int = 2) -> bool:
    """
    Safely save data as JSON to a file
    
    Args:
        data: Data to save
        file_path: Target file path
        indent: JSON indentation level
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure directory exists
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
            
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        return True
    except Exception as e:
        logger.error(f"Error saving JSON to {file_path}: {e}")
        return False


def safe_json_load(file_path: str, default: Dict = None) -> Dict:
    """
    Safely load JSON data from a file
    
    Args:
        file_path: Source file path
        default: Default value if loading fails
        
    Returns:
        Loaded data or default value
    """
    try:
        if not os.path.exists(file_path):
            return default or {}
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON from {file_path}: {e}")
        return default or {}


def format_error_trace() -> str:
    """
    Format the current exception traceback as a string
    
    Returns:
        Formatted traceback string
    """
    return traceback.format_exc()


def get_plugin_dir() -> str:
    """
    Get the default plugins directory path
    
    Returns:
        Path to the plugins directory
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'plugins'))


def version_to_tuple(version_str: str) -> tuple:
    """
    Convert version string to comparable tuple
    
    Args:
        version_str: Version string (e.g., "1.2.3")
        
    Returns:
        Tuple of version components
    """
    try:
        return tuple(map(int, version_str.split('.')))
    except Exception:
        # Handle invalid version strings
        logger.warning(f"Invalid version string: {version_str}")
        return (0, 0, 0)


def check_version_compatibility(actual: str, required: str) -> bool:
    """
    Check if actual version meets the required version
    
    Args:
        actual: Actual version string
        required: Required version string
        
    Returns:
        True if compatible, False otherwise
    """
    if not required:
        return True
        
    try:
        actual_tuple = version_to_tuple(actual)
        required_tuple = version_to_tuple(required)
        return actual_tuple >= required_tuple
    except Exception as e:
        logger.error(f"Error comparing versions {actual} and {required}: {e}")
        return False


def get_caller_info() -> Dict[str, Any]:
    """
    Get information about the caller function and module
    
    Returns:
        Dictionary with caller information
    """
    import inspect
    
    frame = inspect.currentframe().f_back.f_back
    try:
        module = inspect.getmodule(frame)
        module_name = module.__name__ if module else "unknown"
        
        function_name = frame.f_code.co_name
        filename = frame.f_code.co_filename
        line_number = frame.f_lineno
        
        return {
            "module": module_name,
            "function": function_name,
            "file": filename,
            "line": line_number,
            "timestamp": time.time()
        }
    finally:
        del frame  # Avoid reference cycles


def save_yaml_config(file_path: str, data: Dict[str, Any]) -> bool:
    """
    Save data to a YAML file
    
    Args:
        file_path: Path where to save the file
        data: Data to save
        
    Returns:
        True if successful, False otherwise
    """
    return save_plugin_config(data, file_path)  # Reuse existing function


def get_plugin_config_path(plugin_name: str, config_dir: str) -> str:
    """
    Get the path to a plugin's configuration file
    
    Args:
        plugin_name: Name of the plugin
        config_dir: Base configuration directory
        
    Returns:
        Full path to the plugin's configuration file
    """
    return os.path.join(config_dir, f"{plugin_name}.yaml")


def get_system_info() -> Dict[str, Any]:
    """
    Get system information
    
    Returns:
        Dictionary with system information
    """
    import platform
    
    return {
        "os": platform.system(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "timestamp": time.time(),
        "hostname": platform.node()
    }


def copy_plugin_template(template_path: str, dest_path: str, replacements: Dict[str, str]) -> bool:
    """
    Copy a plugin template file with replacements
    
    Args:
        template_path: Path to the template file
        dest_path: Destination path
        replacements: Dictionary of replacements (key=placeholder, value=replacement)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        if not os.path.exists(template_path):
            logger.error(f"Template file not found: {template_path}")
            return False
            
        # Create parent directory if needed
        dest_dir = os.path.dirname(dest_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
            
        # Read template
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Apply replacements
        for key, value in replacements.items():
            content = content.replace(f"{{{key}}}", value)
            
        # Write result
        with open(dest_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        logger.info(f"Plugin template copied to {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Error copying plugin template: {e}")
        logger.debug(f"Template copy error details: {traceback.format_exc()}")
        return False


def backup_file(file_path: str) -> str:
    """
    Create a backup of a file
    
    Args:
        file_path: Path to the file to backup
        
    Returns:
        Path to the backup file, or empty string if backup failed
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Cannot backup non-existent file: {file_path}")
            return ""
            
        # Create backup filename with timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        backup_path = f"{file_path}.{timestamp}.bak"
        
        # Copy file
        shutil.copy2(file_path, backup_path)
        logger.debug(f"Created backup: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"Error creating backup of {file_path}: {e}")
        return ""


def is_plugin_compatible(plugin_requirements: Dict[str, str], system_info: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Check if a plugin is compatible with the current system
    
    Args:
        plugin_requirements: Dictionary of requirements (key=requirement, value=version)
        system_info: System information dictionary
        
    Returns:
        Tuple of (is_compatible, reason)
    """
    # Check Python version
    if "python" in plugin_requirements:
        required_python = plugin_requirements["python"]
        actual_python = system_info.get("python_version", "0.0.0")
        if not check_version_compatibility(actual_python, required_python):
            return False, f"Incompatible Python version: required {required_python}, got {actual_python}"
            
    # Check platform
    if "platform" in plugin_requirements:
        required_platform = plugin_requirements["platform"]
        actual_platform = system_info.get("os", "unknown").lower()
        if required_platform.lower() not in actual_platform:
            return False, f"Incompatible platform: required {required_platform}, got {actual_platform}"
            
    return True, "Compatible"


def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two configuration dictionaries
    
    Args:
        base_config: Base configuration
        override_config: Configuration to override base with
        
    Returns:
        Merged configuration dictionary
    """
    result = base_config.copy()
    
    for key, value in override_config.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            # Recursively merge nested dictionaries
            result[key] = merge_configs(result[key], value)
        else:
            # Override or add value
            result[key] = value
            
    return result


def scan_directory_for_plugins(directory: str) -> List[Dict[str, Any]]:
    """
    Scan a directory for potential plugin files and return basic info
    
    Args:
        directory: Directory to scan
        
    Returns:
        List of dictionaries with plugin information
    """
    results = []
    plugin_files = find_plugins(directory)
    
    for file_path in plugin_files:
        try:
            # Get basic info about the plugin file
            file_name = os.path.basename(file_path)
            plugin_name = os.path.splitext(file_name)[0]
            file_size = os.path.getsize(file_path)
            file_modified = os.path.getmtime(file_path)
            file_hash = get_file_hash(file_path)
            
            results.append({
                "name": plugin_name,
                "path": file_path,
                "size": file_size,
                "modified": file_modified,
                "hash": file_hash
            })
        except Exception as e:
            logger.warning(f"Error scanning potential plugin file {file_path}: {e}")
            
    return results


@asyncio.coroutine
def run_in_executor(func, *args, **kwargs):
    """
    Run a synchronous function in a thread pool executor
    
    Args:
        func: Function to run
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Function result
    """
    loop = asyncio.get_event_loop()
    return (yield from loop.run_in_executor(None, lambda: func(*args, **kwargs)))
