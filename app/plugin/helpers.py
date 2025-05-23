from typing import Dict, Any, Callable, Optional, TypeVar, Generic, Union, Coroutine, Awaitable, Tuple, Type, List
from functools import wraps
from datetime import datetime
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
KT = TypeVar('KT')
VT = TypeVar('VT')


class AsyncCache(Generic[T]):
    """Asynchronous cache decorator for caching asynchronous function results"""

    def __init__(self, ttl: float = 60.0, max_size: int = 100):
        self.ttl = ttl
        self.max_size = max_size
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.access_times: Dict[str, float] = {}
        self._cleanup_task: Optional[asyncio.Task[None]] = None

    def __call__(self, func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            # Generate cache key
            cache_key = self._generate_key(func.__name__, args, kwargs)

            # Check if cache is valid
            if cache_key in self.cache:
                entry = self.cache[cache_key]
                if time.time() < entry["expires_at"]:
                    self.access_times[cache_key] = time.time()
                    return entry["value"]

            # If no cache or cache expired, execute function
            result: T = await func(*args, **kwargs)

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

    def _generate_key(self, func_name: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> str:
        """Generate cache key"""
        key_parts: List[str] = [func_name]

        # Handle positional arguments
        for arg in args:
            key_parts.append(str(arg))

        # Handle keyword arguments
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")

        # Use hash as key
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _cleanup_oldest(self) -> None:
        """Clean up oldest cache entries"""
        if not self.access_times:
            return

        oldest_key: str = min(self.access_times.items(), key=lambda x: x[1])[0]
        if oldest_key in self.cache:
            del self.cache[oldest_key]
        if oldest_key in self.access_times:
            del self.access_times[oldest_key]

    async def _cleanup_loop(self) -> None:
        """Periodically clean up expired cache"""
        try:
            while True:
                # Clean up expired entries
                now = time.time()
                expired_keys: List[str] = []

                for key, entry in list(self.cache.items()): # Iterate over a copy
                    if now > entry["expires_at"]:
                        expired_keys.append(key)

                for key_to_remove in expired_keys:
                    if key_to_remove in self.cache:
                        del self.cache[key_to_remove]
                    if key_to_remove in self.access_times:
                        del self.access_times[key_to_remove]

                # Clean up every 10 seconds
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    def clear(self) -> None:
        """Clear all cache"""
        self.cache.clear()
        self.access_times.clear()

    def invalidate(self, func_name: str, *args: Any, **kwargs: Any) -> None:
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
                 exceptions: Tuple[Type[BaseException], ...] = (Exception,)):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
        self.exceptions = exceptions

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                retries = 0
                delay = self.retry_delay
                last_exception: Optional[BaseException] = None
                while True:
                    try:
                        return await func(*args, **kwargs)
                    except self.exceptions as e:
                        last_exception = e
                        retries += 1
                        if retries > self.max_retries:
                            logger.error(f"Maximum retries ({self.max_retries}) exceeded for {func.__name__}: {e}")
                            raise
                        
                        logger.warning(f"Retry {retries}/{self.max_retries} for {func.__name__} after error: {e}")
                        await asyncio.sleep(delay)
                        delay *= self.backoff_factor
                # This part is unreachable due to `raise` or `return` in the loop
                # but to satisfy type checkers that might not see it:
                if last_exception: raise last_exception 
                return None # Should not be reached

            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                retries = 0
                delay = self.retry_delay
                last_exception: Optional[BaseException] = None
                while True:
                    try:
                        return func(*args, **kwargs)
                    except self.exceptions as e:
                        last_exception = e
                        retries += 1
                        if retries > self.max_retries:
                            logger.error(f"Maximum retries ({self.max_retries}) exceeded for {func.__name__}: {e}")
                            raise

                        logger.warning(f"Retry {retries}/{self.max_retries} for {func.__name__} after error: {e}")
                        time.sleep(delay)
                        delay *= self.backoff_factor
                # This part is unreachable
                if last_exception: raise last_exception
                return None # Should not be reached
            return sync_wrapper


retry = RetryDecorator


def rate_limit(calls: int, period: float = 60.0) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Rate limiting decorator to limit function call frequency"""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        call_history: List[float] = []
        lock: Union[asyncio.Lock, threading.RLock]
        if asyncio.iscoroutinefunction(func):
            lock = asyncio.Lock()
        else:
            lock = threading.RLock()

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async with cast(asyncio.Lock, lock): # Cast for type checker
                    now = time.time()
                    while call_history and call_history[0] < now - period:
                        call_history.pop(0)

                    if len(call_history) >= calls:
                        wait_time = (call_history[0] + period) - now
                        if wait_time > 0:
                            logger.warning(f"Rate limit exceeded for {func.__name__}, waiting {wait_time:.2f} seconds")
                            await asyncio.sleep(wait_time)

                    call_history.append(time.time())
                    return await func(*args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                with cast(threading.RLock, lock): # Cast for type checker
                    now = time.time()
                    while call_history and call_history[0] < now - period:
                        call_history.pop(0)

                    if len(call_history) >= calls:
                        wait_time = (call_history[0] + period) - now
                        if wait_time > 0:
                            logger.warning(f"Rate limit exceeded for {func.__name__}, waiting {wait_time:.2f} seconds")
                            time.sleep(wait_time)
                            
                    call_history.append(time.time())
                    return func(*args, **kwargs)
            return sync_wrapper
    return decorator


def timing_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to measure function execution time"""
    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            try:
                result: Any = await func(*args, **kwargs)
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
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            try:
                result: Any = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.debug(f"{func.__name__} executed in {execution_time:.4f} seconds")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{func.__name__} failed after {execution_time:.4f} seconds: {e}")
                raise
        return sync_wrapper

_FuncT = TypeVar('_FuncT', bound=Callable[..., Any])

def memoize(ttl: Optional[float] = None) -> Callable[[_FuncT], _FuncT]:
    """Memoization decorator to cache function results"""
    cache: Dict[str, Any] = {}
    expire_times: Dict[str, float] = {}

    def decorator(func: _FuncT) -> _FuncT:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = str((args, frozenset(kwargs.items())))

            if ttl is not None and key in expire_times:
                if time.time() > expire_times[key]:
                    if key in cache:
                        del cache[key]
                    if key in expire_times: # check again as it might have been deleted if cache was cleared
                        del expire_times[key]


            if key not in cache:
                result: Any = func(*args, **kwargs)
                cache[key] = result
                if ttl is not None:
                    expire_times[key] = time.time() + ttl
                return result
            
            return cache[key]

        setattr(wrapper, 'clear_cache', lambda: (cache.clear(), expire_times.clear())) # type: ignore
        return cast(_FuncT, wrapper)

    return decorator


def safe_json_dumps(obj: Any, default: Optional[Callable[[Any], Any]] = None, **kwargs: Any) -> str:
    """Safe JSON serialization, handling non-serializable objects"""
    def json_serializer(o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, set):
            # Convert set to list with explicit casting to satisfy type checker
            return list(cast(Iterable[Any], o))
        if hasattr(o, '__dict__'):
            return o.__dict__
        if default is not None:
            return default(o) # Call the provided default
        return str(o)

    try:
        return json.dumps(obj, default=json_serializer, **kwargs)
    except Exception as e:
        logger.error(f"JSON serialization error: {e}")
        return json.dumps({"error": "Serialization failed", "type": obj.__class__.__name__})


def generate_id(prefix: str = "") -> str:
    """Generate unique identifier"""
    random_id = uuid.uuid4().hex[:12]
    timestamp = int(time.time())
    return f"{prefix}{timestamp:x}_{random_id}"


async def run_with_timeout(coro: Coroutine[Any, Any, T], timeout: float = 5.0, default: Optional[T] = None) -> Optional[T]:
    """Async function executor with timeout"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Operation timed out ({timeout}s)")
        return default
    except Exception as e:
        logger.error(f"Error running async function: {e}")
        return default


def load_json_file(file_path: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Safely load JSON file"""
    try:
        if not os.path.exists(file_path):
            return default if default is not None else {}

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if not isinstance(data, dict): # Ensure it's a dict
                 logger.warning(f"JSON file {file_path} does not contain a dictionary.")
                 return default if default is not None else {}
            return cast(Dict[str, Any], data)
    except Exception as e:
        logger.error(f"Failed to load JSON file {file_path}: {e}")
        return default if default is not None else {}


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


class LimitedSizeDict(Dict[KT, VT], Generic[KT, VT]):
    """Fixed size dictionary that automatically removes oldest entries when size limit is exceeded"""

    def __init__(self, max_size: int = 1000, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.max_size = max_size
        self.insertion_order: List[KT] = list(super().keys()) # Initialize with existing keys if any
        self._trim() # Trim if initial data exceeds max_size

    def _trim(self) -> None:
        while len(self.insertion_order) > self.max_size:
            oldest_key = self.insertion_order.pop(0)
            super().__delitem__(oldest_key)

    def __setitem__(self, key: KT, value: VT) -> None:
        if key not in self:
            if len(self.insertion_order) >= self.max_size and self.max_size > 0 : # Check max_size > 0
                oldest_key = self.insertion_order.pop(0)
                super().__delitem__(oldest_key)
            self.insertion_order.append(key)
        else: # Key exists, move to end to mark as recently used
            self.insertion_order.remove(key)
            self.insertion_order.append(key)
        super().__setitem__(key, value)

    def __delitem__(self, key: KT) -> None:
        if key in self.insertion_order:
            self.insertion_order.remove(key)
        super().__delitem__(key)

    def clear(self) -> None:
        self.insertion_order.clear()
        super().clear()

    # If you are inheriting from dict, popitem might need overriding if LIFO behavior is critical
    # For now, we assume standard dict popitem is fine or not used in a way that conflicts.

class SafeDict(Generic[KT, VT]):
    """Thread-safe dictionary"""

    def __init__(self, *args: Any, **kwargs: Any):
        self._dict: Dict[KT, VT] = {}
        if args or kwargs:
            dict_obj = dict(*args, **kwargs)
            self._dict.update(cast(Dict[KT, VT], dict_obj))
        self._lock = threading.RLock()

    def __getitem__(self, key: KT) -> VT:
        with self._lock:
            return self._dict[key]

    def __setitem__(self, key: KT, value: VT) -> None:
        with self._lock:
            self._dict[key] = value

    def __delitem__(self, key: KT) -> None:
        with self._lock:
            del self._dict[key]

    def __contains__(self, key: Any) -> bool: # `key` can be any hashable for `in`
        with self._lock:
            return key in self._dict

    def get(self, key: KT, default: Optional[VT] = None) -> Optional[VT]:
        with self._lock:
            return self._dict.get(key, default)

    def pop(self, key: KT, default: Optional[VT] = None) -> Optional[VT]: # Should match dict.pop signature more closely
        with self._lock:
            if default is _sentinel: # type: ignore
                 return self._dict.pop(key)
            return self._dict.pop(key, default) # type: ignore

    def items(self) -> List[Tuple[KT, VT]]:
        with self._lock:
            return list(self._dict.items())

    def keys(self) -> List[KT]:
        with self._lock:
            return list(self._dict.keys())

    def values(self) -> List[VT]:
        with self._lock:
            return list(self._dict.values())

    def clear(self) -> None:
        with self._lock:
            self._dict.clear()

    def update(self, other: Union[Dict[KT, VT], Iterable[Tuple[KT, VT]], None] = None, **kwargs: VT) -> None:
        with self._lock:
            if other is not None:
                self._dict.update(other) # type: ignore
            self._dict.update(**kwargs)


    def setdefault(self, key: KT, default: Optional[VT] = None) -> Optional[VT]:
        with self._lock:
            return self._dict.setdefault(key, default) # type: ignore

    def __len__(self) -> int:
        with self._lock:
            return len(self._dict)

_sentinel = object()


TaskResult = TypedDict('TaskResult', {
    "success": bool,
    "result": Optional[Any],
    "error": Optional[str],
    "traceback": Optional[str],
    "time": float
})

TaskInfo = TypedDict('TaskInfo', {
    "task": asyncio.Task[Any],
    "name": str,
    "start_time": float
})

FullTaskInfo = TypedDict('FullTaskInfo', {
    "id": str,
    "name": Optional[str], # Name might not be present for completed tasks if only result is stored
    "status": str,
    "start_time": Optional[float],
    "duration": Optional[float],
    "active": bool,
    "result": Optional[TaskResult], # For completed/failed tasks from results dict
    "time": Optional[float] # From results dict
})


class AsyncTaskManager:
    """Asynchronous task manager for tracking and controlling async tasks"""

    def __init__(self) -> None:
        self.tasks: Dict[str, TaskInfo] = {}
        self.results: Dict[str, TaskResult] = {}
        self._lock = asyncio.Lock()

    async def create_task(self, name: str, coro: Coroutine[Any, Any, Any], timeout: Optional[float] = None) -> str:
        """Create and register a task"""
        async with self._lock:
            task_id = str(uuid.uuid4())

            async def task_wrapper() -> None:
                try:
                    current_result: Any
                    if timeout:
                        current_result = await asyncio.wait_for(coro, timeout=timeout)
                    else:
                        current_result = await coro

                    async with self._lock:
                        self.results[task_id] = {
                            "success": True,
                            "result": current_result,
                            "error": None,
                            "traceback": None,
                            "time": time.time()
                        }

                except asyncio.CancelledError:
                    async with self._lock:
                        self.results[task_id] = {
                            "success": False,
                            "result": None,
                            "error": "Task cancelled",
                            "traceback": None,
                            "time": time.time()
                        }
                    # Do not raise here, let the cancellation propagate if needed by caller of task.cancel()
                except Exception as e:
                    async with self._lock:
                        self.results[task_id] = {
                            "success": False,
                            "result": None,
                            "error": str(e),
                            "traceback": traceback.format_exc(),
                            "time": time.time()
                        }
                finally:
                    async with self._lock:
                        if task_id in self.tasks:
                            del self.tasks[task_id]

            task: asyncio.Task[Any] = asyncio.create_task(task_wrapper(), name=name)
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
                if not task.done(): # Only cancel if not done
                    task.cancel()
                    return True
            return False

    async def get_task_info(self, task_id: str) -> Optional[FullTaskInfo]:
        """Get task information"""
        async with self._lock:
            task_info_entry = self.tasks.get(task_id)
            if not task_info_entry:
                result_entry = self.results.get(task_id)
                if result_entry:
                    return {
                        "id": task_id,
                        "name": None, # Name might not be known if only result is present
                        "status": "completed" if result_entry["success"] else "failed",
                        "start_time": None,
                        "duration": None,
                        "active": False,
                        "result": result_entry,
                        "time": result_entry["time"]
                    }
                return None

            task = task_info_entry["task"]
            status: str
            if task.done():
                if task.cancelled():
                    status = "cancelled"
                elif task.exception() is not None:
                    status = "failed"
                else:
                    status = "completed"
            else:
                status = "running"

            return {
                "id": task_id,
                "name": task_info_entry["name"],
                "status": status,
                "start_time": task_info_entry["start_time"],
                "duration": time.time() - task_info_entry["start_time"],
                "active": not task.done(),
                "result": self.results.get(task_id),
                "time": self.results.get(task_id, {}).get("time")
            }

    async def get_all_tasks(self) -> Dict[str, FullTaskInfo]:
        """Get information about all tasks"""
        async with self._lock:
            all_tasks_info: Dict[str, FullTaskInfo] = {}
            current_time = time.time()

            for task_id, task_info_entry in self.tasks.items():
                task = task_info_entry["task"]
                status: str
                if task.done():
                    if task.cancelled():
                        status = "cancelled"
                    elif task.exception() is not None:
                        status = "failed"
                    else:
                        status = "completed"
                else:
                    status = "running"
                
                all_tasks_info[task_id] = {
                    "id": task_id,
                    "name": task_info_entry["name"],
                    "status": status,
                    "start_time": task_info_entry["start_time"],
                    "duration": current_time - task_info_entry["start_time"],
                    "active": not task.done(),
                    "result": self.results.get(task_id),
                    "time": self.results.get(task_id, {}).get("time")
                }

            for task_id, result_entry in self.results.items():
                if task_id not in all_tasks_info: # Only add if not already processed as an active task
                    all_tasks_info[task_id] = {
                        "id": task_id,
                        "name": None, # Name might not be available
                        "status": "completed" if result_entry["success"] else "failed",
                        "start_time": None,
                        "duration": None,
                        "active": False,
                        "result": result_entry,
                        "time": result_entry["time"]
                    }
            return all_tasks_info

    async def cleanup_old_results(self, max_age: float = 3600) -> int:
        """Clean up old task results"""
        async with self._lock:
            now = time.time()
            old_task_ids: List[str] = [
                task_id for task_id, result in self.results.items()
                if now - result["time"] > max_age
            ]

            for task_id_to_remove in old_task_ids:
                if task_id_to_remove in self.results:
                    del self.results[task_id_to_remove]
            return len(old_task_ids)


HandlerType = Callable[..., Any]

class Signal:
    """Simple signal/slot implementation for decoupling components"""

    def __init__(self, name: Optional[str] = None):
        self.name: Optional[str] = name
        self._handlers: List[HandlerType] = []
        self._lock = threading.RLock()

    def connect(self, handler: HandlerType) -> 'Signal':
        """Connect handler"""
        with self._lock:
            if handler not in self._handlers:
                self._handlers.append(handler)
        return self

    def disconnect(self, handler: HandlerType) -> 'Signal':
        """Disconnect handler"""
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)
        return self

    def emit(self, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit signal synchronously"""
        with self._lock:
            # Iterate over a copy in case a handler modifies the list
            handlers_copy = list(self._handlers)

        results: List[Any] = []
        for handler in handlers_copy:
            try:
                results.append(handler(*args, **kwargs))
            except Exception as e:
                logger.error(f"Signal handler error in '{self.name or 'Unnamed'}': {e}", exc_info=True)
        return results

    async def emit_async(self, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit signal asynchronously"""
        with self._lock:
            handlers_copy = list(self._handlers)

        tasks: List[Awaitable[Any]] = []
        for handler in handlers_copy:
            if asyncio.iscoroutinefunction(handler):
                tasks.append(handler(*args, **kwargs))
            else:
                # Wrap synchronous handlers to be awaitable if needed,
                # but asyncio.gather can handle non-awaitable results too.
                # For consistency and to catch errors properly:
                async def sync_to_async_wrapper(sync_handler: HandlerType, *a: Any, **kwa: Any) -> Any:
                    try:
                        return sync_handler(*a, **kwa)
                    except Exception as e:
                        logger.error(f"Sync signal handler error during async emit in '{self.name or 'Unnamed'}': {e}", exc_info=True)
                        return e # Or raise, depending on desired behavior for gather
                tasks.append(sync_to_async_wrapper(handler, *args, **kwargs))
        
        if tasks:
            # return_exceptions=True allows all tasks to complete
            gathered_results = await asyncio.gather(*tasks, return_exceptions=True)
            return list(gathered_results) # Ensure it's a list
        return []

    def clear(self) -> None:
        """Clear all handlers"""
        with self._lock:
            self._handlers.clear()

ClsT = TypeVar('ClsT')

def singleton(cls: Type[ClsT]) -> Callable[..., ClsT]:
    """Singleton decorator"""
    instances: Dict[Type[ClsT], ClsT] = {}
    _lock = threading.Lock()

    @wraps(cls)
    def get_instance(*args: Any, **kwargs: Any) -> ClsT:
        with _lock:
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
            return instances[cls]
    return get_instance


def debounce(wait_time: float) -> Callable[[_FuncT], _FuncT]:
    """Debounce decorator to limit function call frequency"""
    def decorator(func: _FuncT) -> _FuncT:
        last_called: List[float] = [0.0] # Store as a list to modify in inner scope
        # For async, we need to manage pending tasks to ensure only the last one executes
        pending_task: Optional[asyncio.TimerHandle] = None
        _lock = asyncio.Lock() # For async version to protect pending_task and last_args/kwargs

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Optional[Any]:
            nonlocal pending_task
            async with _lock:
                if pending_task:
                    pending_task.cancel()

                async def debounced_call() -> Any:
                    return await func(*args, **kwargs)

                # Schedule the call
                loop = asyncio.get_running_loop()
                pending_task = loop.call_later(wait_time, lambda: asyncio.create_task(debounced_call()))
            # Debounced functions often don't return the immediate result,
            # or they might return a future/promise. This one will execute later.
            # If a return value is expected, this design needs adjustment.
            # For now, returning None as it's a delayed execution.
            return None


        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Optional[Any]:
            # Simplified sync debounce: calls if enough time has passed since last actual call.
            # More complex sync debounce might use threading.Timer.
            current_time = time.time()
            if (current_time - last_called[0]) >= wait_time:
                last_called[0] = current_time
                return func(*args, **kwargs)
            return None # Not called

        if asyncio.iscoroutinefunction(func):
            return cast(_FuncT, async_wrapper)
        return cast(_FuncT, sync_wrapper)
    return decorator


def profile_memory(func: _FuncT) -> _FuncT:
    """Memory profiling decorator"""
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_before = process.memory_info().rss / (1024 * 1024)  # MB

            start_time = time.time()
            result: Any = func(*args, **kwargs)
            execution_time = time.time() - start_time

            mem_after = process.memory_info().rss / (1024 * 1024)  # MB
            logger.info(f"Memory profile - {func.__name__}: before={mem_before:.2f}MB, "
                        f"after={mem_after:.2f}MB, diff={mem_after-mem_before:.2f}MB, "
                        f"time={execution_time:.4f}s")
            return result

        except ImportError:
            logger.warning("psutil not available, memory profiling disabled for %s", func.__name__)
            return func(*args, **kwargs)
    return cast(_FuncT, wrapper)


def auto_retry(max_retries: int = 3,
               exceptions: Tuple[Type[BaseException], ...] = (Exception,),
               retry_delay: float = 1.0,
               backoff_factor: float = 2.0) -> Callable[[_FuncT], _FuncT]:
    """Auto retry decorator"""
    return RetryDecorator( # type: ignore # RetryDecorator expects a func, but here it's a factory
        max_retries=max_retries,
        retry_delay=retry_delay,
        backoff_factor=backoff_factor,
        exceptions=exceptions
    )

from typing import cast, TypedDict, Iterable # Ensure cast and TypedDict are imported