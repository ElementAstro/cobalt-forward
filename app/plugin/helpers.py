from typing import Dict, List, Any, Callable, Optional, TypeVar, Generic, Union
from functools import wraps
from datetime import datetime, timedelta
import logging
import asyncio
import time
import threading
import hashlib
import json
import os
import uuid
import traceback

logger = logging.getLogger(__name__)

T = TypeVar('T')


class AsyncCache(Generic[T]):
    """Asynchronous cache decorator for caching asynchronous function results"""
    
    def __init__(self, ttl: float = 60.0, max_size: int = 100):
        self.ttl = ttl
        self.max_size = max_size
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.access_times: Dict[str, float] = {}
        self._cleanup_task = None
    
    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = self._generate_key(func.__name__, args, kwargs)
            
            # Check if cache is valid
            if cache_key in self.cache:
                entry = self.cache[cache_key]
                if time.time() < entry["expires_at"]:
                    self.access_times[cache_key] = time.time()
                    return entry["value"]
            
            # If no cache or cache expired, execute function
            result = await func(*args, **kwargs)
            
            # Store result
            self.cache[cache_key] = {
                "value": result,
                "expires_at": time.time() + self.ttl
            }
            self.access_times[cache_key] = time.time()
            
            # If cache is too large, clean up oldest entries
            if len(self.cache) > self.max_size:
                self._cleanup_oldest()
                
            # Start cleanup task (if not already started)
            if self._cleanup_task is None or self._cleanup_task.done():
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
                
            return result
            
        return wrapper
    
    def _generate_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Generate cache key"""
        key_parts = [func_name]
        
        # Handle positional arguments
        for arg in args:
            key_parts.append(str(arg))
            
        # Handle keyword arguments
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
            
        # Use hash as key
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _cleanup_oldest(self):
        """Clean up oldest cache entries"""
        if not self.access_times:
            return
            
        oldest_key = min(self.access_times.items(), key=lambda x: x[1])[0]
        if oldest_key in self.cache:
            del self.cache[oldest_key]
        if oldest_key in self.access_times:
            del self.access_times[oldest_key]
    
    async def _cleanup_loop(self):
        """Periodically clean up expired cache"""
        try:
            while True:
                # Clean up expired entries
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
                
                # Clean up every 10 seconds
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
    
    def clear(self):
        """Clear all cache"""
        self.cache.clear()
        self.access_times.clear()
    
    def invalidate(self, func_name: str, *args, **kwargs):
        """Invalidate specific cache entry"""
        cache_key = self._generate_key(func_name, args, kwargs)
        if cache_key in self.cache:
            del self.cache[cache_key]
        if cache_key in self.access_times:
            del self.access_times[cache_key]


class RetryDecorator:
    """Decorator with retry functionality"""
    
    def __init__(self, 
                 max_retries: int = 3, 
                 retry_delay: float = 1.0,
                 backoff_factor: float = 2.0,
                 exceptions: tuple = (Exception,)):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
        self.exceptions = exceptions
        
    def __call__(self, func):
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                retries = 0
                delay = self.retry_delay
                
                while True:
                    try:
                        return await func(*args, **kwargs)
                    except self.exceptions as e:
                        retries += 1
                        if retries > self.max_retries:
                            logger.error(f"Maximum retries ({self.max_retries}) exceeded for {func.__name__}: {e}")
                            raise
                        
                        logger.warning(f"Retry {retries}/{self.max_retries} for {func.__name__} after error: {e}")
                        await asyncio.sleep(delay)
                        delay *= self.backoff_factor
            
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                retries = 0
                delay = self.retry_delay
                
                while True:
                    try:
                        return func(*args, **kwargs)
                    except self.exceptions as e:
                        retries += 1
                        if retries > self.max_retries:
                            logger.error(f"Maximum retries ({self.max_retries}) exceeded for {func.__name__}: {e}")
                            raise
                        
                        logger.warning(f"Retry {retries}/{self.max_retries} for {func.__name__} after error: {e}")
                        time.sleep(delay)
                        delay *= self.backoff_factor
            
            return sync_wrapper


retry = RetryDecorator


def rate_limit(calls: int, period: float = 60.0):
    """Rate limiting decorator to limit function call frequency"""
    def decorator(func):
        # Use a list to store recent call times
        call_history = []
        lock = asyncio.Lock() if asyncio.iscoroutinefunction(func) else threading.RLock()
        
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                async with lock:
                    # Remove expired call records
                    now = time.time()
                    while call_history and call_history[0] < now - period:
                        call_history.pop(0)
                    
                    # Check if rate limit exceeded
                    if len(call_history) >= calls:
                        wait_time = call_history[0] + period - now
                        if wait_time > 0:
                            logger.warning(f"Rate limit exceeded for {func.__name__}, waiting {wait_time:.2f} seconds")
                            await asyncio.sleep(wait_time)
                    
                    # Record current call
                    call_history.append(time.time())
                    
                    # Call original function
                    return await func(*args, **kwargs)
            
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                with lock:
                    # Remove expired call records
                    now = time.time()
                    while call_history and call_history[0] < now - period:
                        call_history.pop(0)
                    
                    # Check if rate limit exceeded
                    if len(call_history) >= calls:
                        wait_time = call_history[0] + period - now
                        if wait_time > 0:
                            logger.warning(f"Rate limit exceeded for {func.__name__}, waiting {wait_time:.2f} seconds")
                            time.sleep(wait_time)
                    
                    # Record current call
                    call_history.append(time.time())
                    
                    # Call original function
                    return func(*args, **kwargs)
                    
            return sync_wrapper
    
    return decorator


def timing_decorator(func):
    """Decorator to measure function execution time"""
    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.debug(f"{func.__name__} executed in {execution_time:.4f} seconds")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{func.__name__} failed after {execution_time:.4f} seconds: {e}")
                raise
        return async_wrapper
    else:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.debug(f"{func.__name__} executed in {execution_time:.4f} seconds")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{func.__name__} failed after {execution_time:.4f} seconds: {e}")
                raise
        return sync_wrapper


def memoize(ttl: float = None):
    """Memoization decorator to cache function results"""
    cache = {}
    expire_times = {}
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            key = str((args, frozenset(kwargs.items())))
            
            # Check if cache expired
            if ttl is not None and key in expire_times:
                if time.time() > expire_times[key]:
                    if key in cache:
                        del cache[key]
            
            # If cache miss, compute result and store
            if key not in cache:
                result = func(*args, **kwargs)
                cache[key] = result
                if ttl is not None:
                    expire_times[key] = time.time() + ttl
                return result
            
            # Return cached result
            return cache[key]
        
        # Add method to clear cache
        wrapper.clear_cache = lambda: cache.clear()
        
        return wrapper
    
    return decorator


def safe_json_dumps(obj: Any, default: Any = None, **kwargs) -> str:
    """Safe JSON serialization, handling non-serializable objects"""
    def json_serializer(o):
        if isinstance(o, (datetime, )):
            return o.isoformat()
        if isinstance(o, set):
            return list(o)
        if hasattr(o, '__dict__'):
            return o.__dict__
        if default is not None:
            return default
        return str(o)
    
    try:
        return json.dumps(obj, default=json_serializer, **kwargs)
    except Exception as e:
        logger.error(f"JSON serialization error: {e}")
        return json.dumps({"error": "Serialization failed", "type": str(type(obj))})


def generate_id(prefix: str = "") -> str:
    """Generate unique identifier"""
    random_id = uuid.uuid4().hex[:12]
    timestamp = int(time.time())
    return f"{prefix}{timestamp:x}_{random_id}"


async def run_with_timeout(coro, timeout: float = 5.0, default=None):
    """Async function executor with timeout"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Operation timed out ({timeout}s)")
        return default
    except Exception as e:
        logger.error(f"Error running async function: {e}")
        return default


def load_json_file(file_path: str, default: Dict = None) -> Dict:
    """Safely load JSON file"""
    try:
        if not os.path.exists(file_path):
            return default or {}
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON file {file_path}: {e}")
        return default or {}


def save_json_file(file_path: str, data: Any, indent: int = 2) -> bool:
    """Safely save JSON file"""
    try:
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        return True
    except Exception as e:
        logger.error(f"Failed to save JSON file {file_path}: {e}")
        return False


class LimitedSizeDict(dict):
    """Fixed size dictionary that automatically removes oldest entries when size limit is exceeded"""
    
    def __init__(self, max_size=1000, *args, **kwargs):
        self.max_size = max_size
        self.insertion_order = []
        super().__init__(*args, **kwargs)
        
    def __setitem__(self, key, value):
        if key not in self:
            if len(self.insertion_order) >= self.max_size:
                oldest_key = self.insertion_order.pop(0)
                if oldest_key in self:
                    del self[oldest_key]
            self.insertion_order.append(key)
        super().__setitem__(key, value)
    
    def __delitem__(self, key):
        if key in self.insertion_order:
            self.insertion_order.remove(key)
        super().__delitem__(key)
        
    def clear(self):
        self.insertion_order = []
        super().clear()


class SafeDict:
    """Thread-safe dictionary"""
    
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)
        self._lock = threading.RLock()
        
    def __getitem__(self, key):
        with self._lock:
            return self._dict[key]
    
    def __setitem__(self, key, value):
        with self._lock:
            self._dict[key] = value
    
    def __delitem__(self, key):
        with self._lock:
            del self._dict[key]
    
    def __contains__(self, key):
        with self._lock:
            return key in self._dict
    
    def get(self, key, default=None):
        with self._lock:
            return self._dict.get(key, default)
    
    def pop(self, key, default=None):
        with self._lock:
            return self._dict.pop(key, default)
    
    def items(self):
        with self._lock:
            return list(self._dict.items())
            
    def keys(self):
        with self._lock:
            return list(self._dict.keys())
            
    def values(self):
        with self._lock:
            return list(self._dict.values())
            
    def clear(self):
        with self._lock:
            self._dict.clear()
            
    def update(self, other=None, **kwargs):
        with self._lock:
            self._dict.update(other, **kwargs)
            
    def setdefault(self, key, default=None):
        with self._lock:
            return self._dict.setdefault(key, default)
    
    def __len__(self):
        with self._lock:
            return len(self._dict)


class AsyncTaskManager:
    """Asynchronous task manager for tracking and controlling async tasks"""
    
    def __init__(self):
        self.tasks = {}
        self.results = {}
        self._lock = asyncio.Lock()
        
    async def create_task(self, name: str, coro, timeout: float = None) -> str:
        """Create and register a task"""
        async with self._lock:
            task_id = str(uuid.uuid4())
            
            # Create wrapper coroutine
            async def task_wrapper():
                try:
                    if timeout:
                        result = await asyncio.wait_for(coro, timeout=timeout)
                    else:
                        result = await coro
                        
                    # Store result
                    async with self._lock:
                        self.results[task_id] = {
                            "success": True,
                            "result": result,
                            "time": time.time()
                        }
                        
                except asyncio.CancelledError:
                    # Task cancelled
                    async with self._lock:
                        self.results[task_id] = {
                            "success": False,
                            "error": "Task cancelled",
                            "time": time.time()
                        }
                    raise
                    
                except Exception as e:
                    # Task error
                    async with self._lock:
                        self.results[task_id] = {
                            "success": False,
                            "error": str(e),
                            "traceback": traceback.format_exc(),
                            "time": time.time()
                        }
                finally:
                    # Remove from active tasks when done
                    async with self._lock:
                        if task_id in self.tasks:
                            del self.tasks[task_id]
            
            # Create and start task
            task = asyncio.create_task(task_wrapper(), name=name)
            self.tasks[task_id] = {
                "task": task,
                "name": name,
                "start_time": time.time()
            }
            
            return task_id
            
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel task"""
        async with self._lock:
            if task_id in self.tasks:
                task_info = self.tasks[task_id]
                task = task_info["task"]
                task.cancel()
                return True
            return False
    
    async def get_task_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task information"""
        async with self._lock:
            task_info = self.tasks.get(task_id)
            if not task_info:
                # Check if there are results
                result = self.results.get(task_id)
                if result:
                    return {
                        "id": task_id,
                        "status": "completed" if result["success"] else "failed",
                        "result": result,
                        "active": False
                    }
                return None
                
            task = task_info["task"]
            return {
                "id": task_id,
                "name": task_info["name"],
                "status": "running" if not task.done() else
                          "completed" if not task.cancelled() else "cancelled",
                "start_time": task_info["start_time"],
                "duration": time.time() - task_info["start_time"],
                "active": True
            }
    
    async def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """Get information about all tasks"""
        async with self._lock:
            all_tasks = {}
            # Active tasks
            for task_id, task_info in self.tasks.items():
                task = task_info["task"]
                all_tasks[task_id] = {
                    "id": task_id,
                    "name": task_info["name"],
                    "status": "running" if not task.done() else
                              "completed" if not task.cancelled() else "cancelled",
                    "start_time": task_info["start_time"],
                    "duration": time.time() - task_info["start_time"],
                    "active": True
                }
            
            # Completed tasks
            for task_id, result in self.results.items():
                if task_id not in all_tasks:
                    all_tasks[task_id] = {
                        "id": task_id,
                        "status": "completed" if result["success"] else "failed",
                        "result": result,
                        "active": False,
                        "time": result["time"]
                    }
            
            return all_tasks
            
    async def cleanup_old_results(self, max_age: float = 3600) -> int:
        """Clean up old task results"""
        async with self._lock:
            now = time.time()
            old_tasks = [
                task_id for task_id, result in self.results.items()
                if now - result["time"] > max_age
            ]
            
            for task_id in old_tasks:
                del self.results[task_id]
                
            return len(old_tasks)
            

class Signal:
    """Simple signal/slot implementation for decoupling components"""
    
    def __init__(self, name=None):
        self.name = name
        self._handlers = []
        self._lock = threading.RLock()
    
    def connect(self, handler):
        """Connect handler"""
        with self._lock:
            if handler not in self._handlers:
                self._handlers.append(handler)
        return self
    
    def disconnect(self, handler):
        """Disconnect handler"""
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)
        return self
    
    def emit(self, *args, **kwargs):
        """Emit signal synchronously"""
        with self._lock:
            handlers = list(self._handlers)
            
        results = []
        for handler in handlers:
            try:
                results.append(handler(*args, **kwargs))
            except Exception as e:
                logger.error(f"Signal handler error: {e}")
                
        return results
    
    async def emit_async(self, *args, **kwargs):
        """Emit signal asynchronously"""
        with self._lock:
            handlers = list(self._handlers)
            
        tasks = []
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                tasks.append(handler(*args, **kwargs))
            else:
                # Wrap synchronous handlers as async
                def make_wrapper(h):
                    async def wrapper():
                        return h(*args, **kwargs)
                    return wrapper
                tasks.append(make_wrapper(handler)())
                
        if tasks:
            return await asyncio.gather(*tasks, return_exceptions=True)
        return []
    
    def clear(self):
        """Clear all handlers"""
        with self._lock:
            self._handlers.clear()


def singleton(cls):
    """Singleton decorator"""
    instances = {}
    
    @wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
        
    return get_instance


def debounce(wait_time):
    """Debounce decorator to limit function call frequency"""
    def decorator(func):
        last_called = [0]
        tasks = {}
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            now = time.time()
            call_id = str(uuid.uuid4())
            tasks[call_id] = True
            
            should_call = now - last_called[0] > wait_time
            last_called[0] = now
            
            if should_call:
                try:
                    return await func(*args, **kwargs)
                finally:
                    if call_id in tasks:
                        del tasks[call_id]
            else:
                await asyncio.sleep(wait_time)
                if call_id in tasks:
                    del tasks[call_id]
                    return await func(*args, **kwargs)
                return None
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            now = time.time()
            if now - last_called[0] > wait_time:
                last_called[0] = now
                return func(*args, **kwargs)
            return None
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
        
    return decorator


def profile_memory(func):
    """Memory profiling decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_before = process.memory_info().rss / (1024 * 1024)  # MB
            
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            mem_after = process.memory_info().rss / (1024 * 1024)  # MB
            logger.info(f"Memory profile - {func.__name__}: before={mem_before:.2f}MB, "
                        f"after={mem_after:.2f}MB, diff={mem_after-mem_before:.2f}MB, "
                        f"time={execution_time:.4f}s")
            return result
            
        except ImportError:
            logger.warning("psutil not available, memory profiling disabled")
            return func(*args, **kwargs)
            
    return wrapper


def auto_retry(max_retries=3, exceptions=(Exception,), retry_delay=1.0, backoff_factor=2.0):
    """Auto retry decorator"""
    return RetryDecorator(
        max_retries=max_retries, 
        retry_delay=retry_delay, 
        backoff_factor=backoff_factor, 
        exceptions=exceptions
    )