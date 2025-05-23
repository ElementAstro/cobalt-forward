"""
Plugin sandbox for secure plugin execution.

This module provides sandbox functionality to isolate plugin code execution
and limit access to system resources.
"""
import sys
import os
import tempfile
import logging
# import inspect # Unused
# import threading # Unused
import contextlib
import importlib
import time
from typing import Dict, Any, Set, Optional, Callable, Union, Generator, TypeVar
import builtins
import types # For types.ModuleType

logger = logging.getLogger(__name__)

# List of safe builtins that plugins are allowed to use by default
SAFE_BUILTINS: Set[str] = {
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
    # 'contextlib', # This is a module, not a builtin function usually exposed directly
    # Help
    'help', 'dir',
    # Exceptions
    'Exception', 'ValueError', 'TypeError', 'KeyError', 'RuntimeError'
}

# Modules that should be blocked by default
BLOCKED_MODULES: Set[str] = {
    'subprocess', 'os.system', 'os.popen', 'os.spawn', 'pty',
    'socket', 'requests', 'urllib', 'ftplib',
    'importlib', 'imp', 'sys.modules', # '__import__' is handled by overriding
    'pickle', 'marshal', 'shelve',
    'multiprocessing',
}

# Type variables for RestrictedDict
_K = TypeVar('_K')
_V = TypeVar('_V')

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
        self.allowed_modules: Set[str] = allowed_modules or set()
        self.blocked_modules: Set[str] = blocked_modules or set(BLOCKED_MODULES)
        self._original_import: Callable[..., types.ModuleType] = builtins.__import__
        self._import_cache: Dict[str, types.ModuleType] = {}
    
    def install(self) -> None:
        """Install the restricted importer"""
        builtins.__import__ = self._restricted_import # type: ignore[assignment] # Overriding builtin
    
    def uninstall(self) -> None:
        """Restore the original importer"""
        builtins.__import__ = self._original_import
        
    def clear_import_cache(self) -> None:
        """Clear the import cache"""
        self._import_cache.clear()
    
    def _restricted_import(self, name: str, globals: Optional[Dict[str, Any]] = None, locals: Optional[Dict[str, Any]] = None, fromlist: Optional[tuple[str, ...]] = (), level: int = 0) -> types.ModuleType:
        """
        Restricted import function that checks if the module is allowed.
        
        Args:
            name: Name of module to import
            globals: Global namespace
            locals: Local namespace
            fromlist: List of names to import from module
            level: Relative import level
            
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
            # Call original import with all standard arguments
            module = self._original_import(name, globals, locals, fromlist, level)
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
        # This logic seems to be for when allowed_modules is used as an allow-list
        if self.allowed_modules: 
            if name not in self.allowed_modules:
                # Check if a parent of 'name' is allowed (e.g. 'os' is allowed, so 'os.path' should be too unless explicitly blocked)
                is_submodule_of_allowed = any(name.startswith(allowed + '.') for allowed in self.allowed_modules)
                if not is_submodule_of_allowed:
                    return True # Not in allowed list and not a submodule of an allowed one
                    
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


class RestrictedDict(Dict[_K, _V]):
    """
    Dictionary with controlled access to keys.
    Assumes keys are strings for permission checking.
    """
    
    def __init__(self, allowed_keys: Optional[Set[str]] = None, *args: Any, **kwargs: Any):
        """
        Initialize restricted dictionary.
        
        Args:
            allowed_keys: Set of keys that are allowed to be accessed, modified, or deleted.
                If None, all keys are allowed.
            *args: Additional arguments to pass to dict constructor.
            **kwargs: Additional keyword arguments to pass to dict constructor.
        """
        super().__init__(*args, **kwargs)
        self.allowed_keys: Optional[Set[str]] = allowed_keys # Store it as Optional
    
    def __getitem__(self, key: _K) -> _V:
        """
        Get item with access control.
        
        Args:
            key: Dictionary key
            
        Returns:
            Value for key
            
        Raises:
            KeyError: If key is not allowed
        """
        if self.allowed_keys is not None and isinstance(key, str) and key not in self.allowed_keys:
            raise KeyError(f"Key '{key}' is not allowed in sandbox")
        return super().__getitem__(key)
    
    def __setitem__(self, key: _K, value: _V) -> None:
        """
        Set item with access control.
        
        Args:
            key: Dictionary key
            value: Value to set
            
        Raises:
            KeyError: If key is not allowed
        """
        if self.allowed_keys is not None and isinstance(key, str) and key not in self.allowed_keys:
            raise KeyError(f"Key '{key}' is not allowed to be set in sandbox")
        super().__setitem__(key, value)
    
    def __delitem__(self, key: _K) -> None:
        """
        Delete item with access control.
        
        Args:
            key: Dictionary key
            
        Raises:
            KeyError: If key is not allowed
        """
        if self.allowed_keys is not None and isinstance(key, str) and key not in self.allowed_keys:
            raise KeyError(f"Key '{key}' is not allowed to be deleted in sandbox")
        super().__delitem__(key)


class PluginSandbox:
    """
    Sandbox for secure plugin execution.
    """
    
    def __init__(self):
        """Initialize plugin sandbox."""
        self.importer: RestrictedImporter = RestrictedImporter()
        self.allowed_builtins: Set[str] = SAFE_BUILTINS.copy()
        self._original_builtins: Dict[str, Any] = dict(builtins.__dict__)
        # self._restricted_globals: Dict[str, Any] = {} # This seems unused, consider removing
        self._temp_dir: Optional[str] = None
        self._resource_limits: Dict[str, Union[float, int]] = {
            'cpu_time': 10.0,  # CPU time in seconds
            'memory': 100 * 1024 * 1024,  # Memory limit in bytes (100MB)
            'file_size': 10 * 1024 * 1024,  # File size limit in bytes (10MB)
            'open_files': 10,  # Maximum number of open files
        }
        self._cache: Dict[str, Any] = {} # Generic cache, e.g. for execution results if needed
        
    def _setup_restricted_environment(self) -> Dict[str, Any]:
        """
        Set up restricted execution environment.
        
        Returns:
            Dictionary of globals for the restricted environment
        """
        # Create restricted builtins
        restricted_builtins_dict: Dict[str, Any] = {}
        for name in self.allowed_builtins:
            if hasattr(builtins, name): # Check if name exists in builtins module
                restricted_builtins_dict[name] = getattr(builtins, name)
        
        # Set up globals dictionary
        globals_dict: Dict[str, Any] = {
            '__builtins__': restricted_builtins_dict,
            '__name__': '__plugin__',
            '__doc__': None,
            '__package__': None,
        }
        
        # Add safe modules to globals
        for module_name in ('math', 'datetime', 'random', 'json', 're', 'functools', 'itertools', 'time'):
            try:
                # Use the restricted importer to load these modules for consistency
                # Or assume these are "system-level" safe modules loaded outside restriction for the sandbox setup
                globals_dict[module_name] = importlib.import_module(module_name)
            except ImportError:
                logger.warning(f"Could not import standard module {module_name} for sandbox globals.")
        
        return globals_dict
    
    def _create_temp_dir(self) -> Optional[str]:
        """
        Create temporary directory for plugin files.
        
        Returns:
            Path to temporary directory or None if error
        """
        if not self._temp_dir:
            try:
                self._temp_dir = tempfile.mkdtemp(prefix='plugin_sandbox_')
            except Exception as e:
                logger.error(f"Failed to create temp dir for sandbox: {e}")
                return None
        return self._temp_dir
    
    def _cleanup_temp_dir(self) -> None:
        """Clean up temporary directory."""
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                import shutil
                shutil.rmtree(self._temp_dir)
                self._temp_dir = None
            except Exception as e:
                logger.error(f"Error cleaning up sandbox temp dir: {e}")
    
    @contextlib.contextmanager
    def apply(self) -> Generator[Dict[str, Any], None, None]:
        """
        Apply sandbox restrictions and restore original state afterward.
        
        Yields:
            Dictionary of globals for the restricted environment
        """
        # Store original state
        original_import_func = builtins.__import__
        # original_globals = dict(globals()) # Unused variable
        original_sys_path = list(sys.path)
        
        current_globals: Dict[str, Any] = {}
        try:
            # Set up restricted environment
            current_globals = self._setup_restricted_environment()
            
            # Install restricted importer
            self.importer.install()
            
            # Modify sys.path to include temp directory first
            temp_dir_path = self._create_temp_dir()
            if (temp_dir_path):
                sys.path.insert(0, temp_dir_path)
            
            # Set resource limits (implementation varies by platform)
            self._set_resource_limits()
            
            yield current_globals
            
        finally:
            # Restore original state
            builtins.__import__ = original_import_func
            sys.path = original_sys_path
            # No need to restore globals() as it's a function call, not a modified object
    
    def _set_resource_limits(self) -> None:
        """Set resource usage limits for sandbox."""
        try:
            import resource # Platform-specific
            
            # Set CPU time limit
            cpu_limit = int(self._resource_limits['cpu_time'])
            resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit, cpu_limit)) # type: ignore[attr-defined]
            
            # Set memory limit
            mem_limit = int(self._resource_limits['memory'])
            resource.setrlimit(resource.RLIMIT_AS, (mem_limit, mem_limit)) # type: ignore[attr-defined]
            
            # Set file size limit
            fsize_limit = int(self._resource_limits['file_size'])
            resource.setrlimit(resource.RLIMIT_FSIZE, (fsize_limit, fsize_limit)) # type: ignore[attr-defined]
            
            # Set open files limit
            nofile_limit = int(self._resource_limits['open_files'])
            resource.setrlimit(resource.RLIMIT_NOFILE, (nofile_limit, nofile_limit)) # type: ignore[attr-defined]
                                
        except (ImportError, AttributeError):
            # Resource module not available on this platform
            logger.warning("Resource limits couldn't be set (module 'resource' not available or lacks 'setrlimit' on this platform)")
    
    def execute(self, code: str, globals_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute code in the sandbox.
        
        Args:
            code: Python code to execute
            globals_dict: Dictionary of globals to use
            
        Returns:
            Dictionary containing the execution result and any new global variables
            
        Raises:
            Exception: If code execution fails (caught and reported in result dict)
        """
        result: Dict[str, Any]
        with self.apply() as sandbox_globals_from_context:
            # Use provided globals or the sandbox defaults
            execution_globals: Dict[str, Any] = globals_dict if globals_dict is not None else sandbox_globals_from_context
            
            # Add safe executions helpers to globals
            execution_globals['print'] = self._safe_print
            
            # Track execution time
            exec_start_time = time.time()
            
            # Execute the code
            try:
                exec(code, execution_globals)
                exec_execution_time = time.time() - exec_start_time
                
                # Return result and updated globals
                # Filter out sandbox_globals_from_context to only get user-defined vars
                user_globals = {
                    k: v for k, v in execution_globals.items()
                    if k not in sandbox_globals_from_context or execution_globals[k] is not sandbox_globals_from_context.get(k)
                }
                # Further filter out builtins that might have been re-added if not careful
                final_user_globals = {
                     k: v for k,v in user_globals.items() if not (k.startswith("__") and k.endswith("__")) and k != "print"
                }


                result = {
                    'success': True,
                    'execution_time': exec_execution_time,
                    'globals': final_user_globals
                }
            except Exception as e:
                exec_execution_time = time.time() - exec_start_time
                result = {
                    'success': False,
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'execution_time': exec_execution_time
                }
            
            return result
    
    def _safe_print(self, *args: Any, **_kwargs: Any) -> None: # kwargs usually for sep, end, file, flush
        """Safe version of print that logs output instead of printing."""
    def reset_cache(self) -> None:
        """Clear internal caches."""
        self._cache.clear()
        # Access importer's cache through a public method
        if hasattr(self.importer, 'clear_import_cache'):
            self.importer.clear_import_cache()
        """Clear internal caches."""
        self._cache.clear()
        if hasattr(self.importer, '_import_cache'): # Check attribute existence
            self.importer.clear_import_cache() # Clear the import cache in the importer
    
    def allow_module(self, module_name: str) -> None:
        """
        Allow a module to be imported in the sandbox.
        
        Args:
            module_name: Name of module to allow
        """
        self.importer.add_allowed_module(module_name)
    
    def block_module(self, module_name: str) -> None:
        """
        Block a module from being imported in the sandbox.
        
        Args:
            module_name: Name of module to block
        """
        self.importer.add_blocked_module(module_name)
    
    def allow_builtin(self, builtin_name: str) -> None:
        """
        Allow a builtin function in the sandbox.
        
        Args:
            builtin_name: Name of builtin to allow
        """
        if hasattr(builtins, builtin_name): # Check if it's a valid builtin
            self.allowed_builtins.add(builtin_name)
    
    def block_builtin(self, builtin_name: str) -> None:
        """
        Block a builtin function in the sandbox.
        
        Args:
            builtin_name: Name of builtin to block
        """
        if builtin_name in self.allowed_builtins:
            self.allowed_builtins.remove(builtin_name)
    
    def set_resource_limit(self, resource_key: str, value: Union[int, float]) -> None:
        """
        Set a resource limit for the sandbox.
        
        Args:
            resource_key: Resource name ('cpu_time', 'memory', 'file_size', 'open_files')
            value: Limit value
        """
        if resource_key in self._resource_limits:
            self._resource_limits[resource_key] = value
        else:
            logger.warning(f"Attempted to set unknown resource limit: {resource_key}")

    def get_resource_limits(self) -> Dict[str, Union[float, int]]:
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
    
    def execute_function(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Execute a function in the sandbox.
        
        Args:
            func: Function to execute
            *args: Arguments to pass to function
            **kwargs: Keyword arguments to pass to function
            
        Returns:
            Function result
            
        Raises:
            Exception: If function execution fails (propagated from the function)
        """
        # Note: Applying the full sandbox (import restrictions, resource limits)
        # for every function call might be heavy.
        # This also means the function 'func' itself must be accessible
        # within the sandbox's restricted global/builtin environment if it relies on them.
        # If 'func' is defined outside and passed in, it runs with its closure's scope
        # but its *new* imports or resource usage would be sandboxed.
        with self.apply(): # This sets up the environment for the duration of func's execution
            return func(*args, **kwargs)
    
    def __del__(self) -> None:
        """Cleanup when object is deleted."""
        self._cleanup_temp_dir()
        # Ensure importer is uninstalled if it was installed by this instance
        # This is tricky if multiple sandboxes exist. A ref count or explicit cleanup might be better.
        # For now, assuming one active sandbox or careful management.
        # self.importer.uninstall() # This might be too aggressive if importer is shared or managed globally
