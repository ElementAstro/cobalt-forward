"""
Plugin sandbox for secure plugin execution.

This module provides sandbox functionality to isolate plugin code execution
and limit access to system resources.
"""
import sys
import os
import tempfile
import logging
import inspect
import threading
import contextlib
import importlib
import time
from typing import Dict, Any, Set, List, Optional, Tuple, Callable
import builtins

logger = logging.getLogger(__name__)

# List of safe builtins that plugins are allowed to use by default
SAFE_BUILTINS = {
    # Basic types
    'int', 'float', 'str', 'bool', 'list', 'dict', 'set', 'tuple', 'frozenset',
    # Type functions
    'len', 'isinstance', 'type', 'hasattr', 'getattr', 'setattr', 'id',
    # Iterators and generators
    'map', 'filter', 'zip', 'reversed', 'enumerate', 'iter', 'next',
    # Math
    'abs', 'min', 'max', 'sum', 'round',
    # String handling
    'repr', 'format', 'chr', 'ord',
    # Collection creation
    'range',
    # Context managers
    'contextlib',
    # Help
    'help', 'dir',
    # Exceptions
    'Exception', 'ValueError', 'TypeError', 'KeyError', 'RuntimeError'
}

# Modules that should be blocked by default
BLOCKED_MODULES = {
    'subprocess', 'os.system', 'os.popen', 'os.spawn', 'pty',
    'socket', 'requests', 'urllib', 'ftplib',
    'importlib', 'imp', 'sys.modules', '__import__',
    'pickle', 'marshal', 'shelve',
    'multiprocessing',
}


class RestrictedImporter:
    """
    Controls imports within a sandbox to prevent access to dangerous modules.
    """
    
    def __init__(self, 
                 allowed_modules: Optional[Set[str]] = None,
                 blocked_modules: Optional[Set[str]] = None):
        """
        Initialize restricted importer.
        
        Args:
            allowed_modules: Set of module names that are allowed to be imported.
                If None, all modules not in blocked_modules are allowed.
            blocked_modules: Set of module names that are blocked from being imported.
                Takes precedence over allowed_modules.
        """
        self.allowed_modules = allowed_modules or set()
        self.blocked_modules = blocked_modules or set(BLOCKED_MODULES)
        self._original_import = builtins.__import__
        self._import_cache = {}
    
    def install(self) -> None:
        """Install the restricted importer"""
        builtins.__import__ = self._restricted_import
    
    def uninstall(self) -> None:
        """Restore the original importer"""
        builtins.__import__ = self._original_import
    
    def _restricted_import(self, name, *args, **kwargs):
        """
        Restricted import function that checks if the module is allowed.
        
        Args:
            name: Name of module to import
            
        Returns:
            Imported module if allowed
            
        Raises:
            ImportError: If module is not allowed
        """
        # Check cache first
        if name in self._import_cache:
            return self._import_cache[name]
        
        # Check if module is allowed
        if self._is_blocked_module(name):
            logger.warning(f"Sandbox: Blocked import of module '{name}'")
            raise ImportError(f"Module '{name}' is not allowed in the sandbox")
            
        # Proceed with import
        try:
            module = self._original_import(name, *args, **kwargs)
            self._import_cache[name] = module
            return module
        except ImportError as e:
            # Log the import error
            logger.debug(f"Sandbox: Failed to import module '{name}': {e}")
            raise
    
    def _is_blocked_module(self, name: str) -> bool:
        """
        Check if module is blocked from importing.
        
        Args:
            name: Module name
            
        Returns:
            True if module is blocked, False otherwise
        """
        # Direct match in blocked modules
        if name in self.blocked_modules:
            return True
            
        # Check if parent module is blocked 
        # (e.g. 'os.path' is blocked if 'os' is blocked)
        parts = name.split('.')
        for i in range(1, len(parts)):
            parent = '.'.join(parts[:i])
            if parent in self.blocked_modules:
                return True
                
        # Check if specific function is blocked
        # (e.g. 'os.system' is blocked if 'os.system' is in blocked_modules)
        if self.allowed_modules:
            # If we have an allowed list, everything not allowed is blocked
            if name not in self.allowed_modules:
                parent_allowed = False
                for allowed in self.allowed_modules:
                    if name.startswith(allowed + '.'):
                        parent_allowed = True
                        break
                if not parent_allowed:
                    return True
                    
        return False
    
    def add_allowed_module(self, name: str) -> None:
        """
        Add a module to the allowed list.
        
        Args:
            name: Module name
        """
        self.allowed_modules.add(name)
    
    def add_blocked_module(self, name: str) -> None:
        """
        Add a module to the blocked list.
        
        Args:
            name: Module name
        """
        self.blocked_modules.add(name)
        
        # Remove from cache if present
        if name in self._import_cache:
            del self._import_cache[name]


class RestrictedDict(dict):
    """
    Dictionary with controlled access to keys.
    """
    
    def __init__(self, allowed_keys: Optional[Set[str]] = None, *args, **kwargs):
        """
        Initialize restricted dictionary.
        
        Args:
            allowed_keys: Set of keys that are allowed to be accessed, modified, or deleted.
                If None, all keys are allowed.
            *args: Additional arguments to pass to dict constructor.
            **kwargs: Additional keyword arguments to pass to dict constructor.
        """
        super().__init__(*args, **kwargs)
        self.allowed_keys = allowed_keys or set()
    
    def __getitem__(self, key):
        """
        Get item with access control.
        
        Args:
            key: Dictionary key
            
        Returns:
            Value for key
            
        Raises:
            KeyError: If key is not allowed
        """
        if self.allowed_keys and key not in self.allowed_keys:
            raise KeyError(f"Key '{key}' is not allowed in sandbox")
        return super().__getitem__(key)
    
    def __setitem__(self, key, value):
        """
        Set item with access control.
        
        Args:
            key: Dictionary key
            value: Value to set
            
        Raises:
            KeyError: If key is not allowed
        """
        if self.allowed_keys and key not in self.allowed_keys:
            raise KeyError(f"Key '{key}' is not allowed to be set in sandbox")
        return super().__setitem__(key, value)
    
    def __delitem__(self, key):
        """
        Delete item with access control.
        
        Args:
            key: Dictionary key
            
        Raises:
            KeyError: If key is not allowed
        """
        if self.allowed_keys and key not in self.allowed_keys:
            raise KeyError(f"Key '{key}' is not allowed to be deleted in sandbox")
        return super().__delitem__(key)


class PluginSandbox:
    """
    Sandbox for secure plugin execution.
    """
    
    def __init__(self):
        """Initialize plugin sandbox."""
        self.importer = RestrictedImporter()
        self.allowed_builtins = SAFE_BUILTINS.copy()
        self._original_builtins = dict(builtins.__dict__)
        self._restricted_globals = {}
        self._temp_dir = None
        self._resource_limits = {
            'cpu_time': 10.0,  # CPU time in seconds
            'memory': 100 * 1024 * 1024,  # Memory limit in bytes (100MB)
            'file_size': 10 * 1024 * 1024,  # File size limit in bytes (10MB)
            'open_files': 10,  # Maximum number of open files
        }
        self._cache = {}
        
    def _setup_restricted_environment(self):
        """
        Set up restricted execution environment.
        
        Returns:
            Dictionary of globals for the restricted environment
        """
        # Create restricted builtins
        restricted_builtins = {}
        for name in self.allowed_builtins:
            if name in builtins.__dict__:
                restricted_builtins[name] = builtins.__dict__[name]
        
        # Set up globals dictionary
        globals_dict = {
            '__builtins__': restricted_builtins,
            '__name__': '__plugin__',
            '__doc__': None,
            '__package__': None,
        }
        
        # Add safe modules to globals
        for module_name in ('math', 'datetime', 'random', 'json', 're', 'functools', 'itertools'):
            try:
                globals_dict[module_name] = importlib.import_module(module_name)
            except ImportError:
                pass
        
        return globals_dict
    
    def _create_temp_dir(self):
        """
        Create temporary directory for plugin files.
        
        Returns:
            Path to temporary directory
        """
        if not self._temp_dir:
            self._temp_dir = tempfile.mkdtemp(prefix='plugin_sandbox_')
        return self._temp_dir
    
    def _cleanup_temp_dir(self):
        """Clean up temporary directory."""
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                import shutil
                shutil.rmtree(self._temp_dir)
                self._temp_dir = None
            except Exception as e:
                logger.error(f"Error cleaning up sandbox temp dir: {e}")
    
    @contextlib.contextmanager
    def apply(self):
        """
        Apply sandbox restrictions and restore original state afterward.
        
        Yields:
            Dictionary of globals for the restricted environment
        """
        # Store original state
        original_import = builtins.__import__
        original_globals = dict(globals())
        original_sys_path = list(sys.path)
        
        try:
            # Set up restricted environment
            globals_dict = self._setup_restricted_environment()
            
            # Install restricted importer
            self.importer.install()
            
            # Modify sys.path to include temp directory first
            temp_dir = self._create_temp_dir()
            sys.path.insert(0, temp_dir)
            
            # Set resource limits (implementation varies by platform)
            self._set_resource_limits()
            
            yield globals_dict
            
        finally:
            # Restore original state
            builtins.__import__ = original_import
            sys.path = original_sys_path
    
    def _set_resource_limits(self):
        """Set resource usage limits for sandbox."""
        try:
            import resource
            
            # Set CPU time limit
            resource.setrlimit(resource.RLIMIT_CPU, 
                               (self._resource_limits['cpu_time'], 
                                self._resource_limits['cpu_time']))
            
            # Set memory limit
            resource.setrlimit(resource.RLIMIT_AS, 
                               (self._resource_limits['memory'], 
                                self._resource_limits['memory']))
            
            # Set file size limit
            resource.setrlimit(resource.RLIMIT_FSIZE, 
                               (self._resource_limits['file_size'], 
                                self._resource_limits['file_size']))
            
            # Set open files limit
            resource.setrlimit(resource.RLIMIT_NOFILE, 
                               (self._resource_limits['open_files'], 
                                self._resource_limits['open_files']))
                                
        except (ImportError, AttributeError):
            # Resource module not available on this platform
            logger.warning("Resource limits couldn't be set (not supported on this platform)")
    
    def execute(self, code: str, globals_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute code in the sandbox.
        
        Args:
            code: Python code to execute
            globals_dict: Dictionary of globals to use
            
        Returns:
            Dictionary containing the execution result and any new global variables
            
        Raises:
            Exception: If code execution fails
        """
        with self.apply() as sandbox_globals:
            # Use provided globals or the sandbox defaults
            execution_globals = globals_dict or sandbox_globals
            
            # Add safe executions helpers to globals
            execution_globals['print'] = self._safe_print
            
            # Track execution time
            start_time = time.time()
            
            # Execute the code
            try:
                exec(code, execution_globals)
                execution_time = time.time() - start_time
                
                # Return result and updated globals
                result = {
                    'success': True,
                    'execution_time': execution_time,
                    'globals': {
                        k: v for k, v in execution_globals.items()
                        if k not in sandbox_globals and not k.startswith('__')
                    }
                }
            except Exception as e:
                execution_time = time.time() - start_time
                result = {
                    'success': False,
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'execution_time': execution_time
                }
            
            return result
    
    def _safe_print(self, *args, **kwargs):
        """Safe version of print that logs output instead of printing."""
        output = " ".join(str(arg) for arg in args)
        logger.info(f"[Plugin Output] {output}")
    
    def reset_cache(self):
        """Clear internal caches."""
        self._cache.clear()
        self.importer._import_cache.clear()
    
    def allow_module(self, module_name: str):
        """
        Allow a module to be imported in the sandbox.
        
        Args:
            module_name: Name of module to allow
        """
        self.importer.add_allowed_module(module_name)
    
    def block_module(self, module_name: str):
        """
        Block a module from being imported in the sandbox.
        
        Args:
            module_name: Name of module to block
        """
        self.importer.add_blocked_module(module_name)
    
    def allow_builtin(self, builtin_name: str):
        """
        Allow a builtin function in the sandbox.
        
        Args:
            builtin_name: Name of builtin to allow
        """
        if builtin_name in builtins.__dict__:
            self.allowed_builtins.add(builtin_name)
    
    def block_builtin(self, builtin_name: str):
        """
        Block a builtin function in the sandbox.
        
        Args:
            builtin_name: Name of builtin to block
        """
        if builtin_name in self.allowed_builtins:
            self.allowed_builtins.remove(builtin_name)
    
    def set_resource_limit(self, resource: str, value: int):
        """
        Set a resource limit for the sandbox.
        
        Args:
            resource: Resource name ('cpu_time', 'memory', 'file_size', 'open_files')
            value: Limit value
        """
        if resource in self._resource_limits:
            self._resource_limits[resource] = value
    
    def get_resource_limits(self) -> Dict[str, int]:
        """
        Get current resource limits.
        
        Returns:
            Dictionary of resource limits
        """
        return dict(self._resource_limits)
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get sandbox status information.
        
        Returns:
            Dictionary of sandbox status
        """
        return {
            'allowed_builtins': list(self.allowed_builtins),
            'blocked_modules': list(self.importer.blocked_modules),
            'allowed_modules': list(self.importer.allowed_modules),
            'resource_limits': self.get_resource_limits(),
            'temp_dir': self._temp_dir,
        }
    
    def execute_function(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function in the sandbox.
        
        Args:
            func: Function to execute
            *args: Arguments to pass to function
            **kwargs: Keyword arguments to pass to function
            
        Returns:
            Function result
            
        Raises:
            Exception: If function execution fails
        """
        with self.apply():
            return func(*args, **kwargs)
    
    def __del__(self):
        """Cleanup when object is deleted."""
        self._cleanup_temp_dir()
