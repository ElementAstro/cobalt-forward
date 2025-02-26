import asyncio
import logging
import time
import inspect
import functools
import traceback
from typing import Dict, List, Any, Callable, Optional, TypeVar, Generic, Union
from functools import wraps
import os
import json
from datetime import datetime, timedelta
import hashlib
import uuid
import threading

logger = logging.getLogger(__name__)

T = TypeVar('T')


class AsyncCache(Generic[T]):
    """异步缓存装饰器，用于缓存异步函数的结果"""
    
    def __init__(self, ttl: float = 60.0, max_size: int = 100):
        self.ttl = ttl
        self.max_size = max_size
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.access_times: Dict[str, float] = {}
        self._cleanup_task = None
    
    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = self._generate_key(func.__name__, args, kwargs)
            
            # 检查缓存是否有效
            if cache_key in self.cache:
                entry = self.cache[cache_key]
                if time.time() < entry["expires_at"]:
                    self.access_times[cache_key] = time.time()
                    return entry["value"]
            
            # 如果没有缓存或缓存已过期，执行函数
            result = await func(*args, **kwargs)
            
            # 存储结果
            self.cache[cache_key] = {
                "value": result,
                "expires_at": time.time() + self.ttl
            }
            self.access_times[cache_key] = time.time()
            
            # 如果缓存太大，清理最旧的条目
            if len(self.cache) > self.max_size:
                self._cleanup_oldest()
                
            # 启动清理任务(如果尚未启动)
            if self._cleanup_task is None or self._cleanup_task.done():
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
                
            return result
            
        return wrapper
    
    def _generate_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """生成缓存键"""
        key_parts = [func_name]
        
        # 处理位置参数
        for arg in args:
            key_parts.append(str(arg))
            
        # 处理关键字参数
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
            
        # 使用哈希值作为键
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _cleanup_oldest(self):
        """清理最旧的缓存条目"""
        if not self.access_times:
            return
            
        oldest_key = min(self.access_times.items(), key=lambda x: x[1])[0]
        if oldest_key in self.cache:
            del self.cache[oldest_key]
        if oldest_key in self.access_times:
            del self.access_times[oldest_key]
    
    async def _cleanup_loop(self):
        """定期清理过期缓存"""
        try:
            while True:
                # 清理过期条目
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
                
                # 每10秒清理一次
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
    
    def clear(self):
        """清除所有缓存"""
        self.cache.clear()
        self.access_times.clear()
    
    def invalidate(self, func_name: str, *args, **kwargs):
        """使特定的缓存条目失效"""
        cache_key = self._generate_key(func_name, args, kwargs)
        if cache_key in self.cache:
            del self.cache[cache_key]
        if cache_key in self.access_times:
            del self.access_times[cache_key]


class RetryDecorator:
    """带重试功能的装饰器"""
    
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
    """速率限制装饰器，限制函数调用频率"""
    def decorator(func):
        # 使用列表存储最近的调用时间
        call_history = []
        lock = asyncio.Lock() if asyncio.iscoroutinefunction(func) else threading.RLock()
        
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                async with lock:
                    # 移除过期的调用记录
                    now = time.time()
                    while call_history and call_history[0] < now - period:
                        call_history.pop(0)
                    
                    # 检查是否超过速率限制
                    if len(call_history) >= calls:
                        wait_time = call_history[0] + period - now
                        if wait_time > 0:
                            logger.warning(f"Rate limit exceeded for {func.__name__}, waiting {wait_time:.2f} seconds")
                            await asyncio.sleep(wait_time)
                            
                            # 重新检查(因为可能有其他调用)
                            now = time.time()
                            while call_history and call_history[0] < now - period:
                                call_history.pop(0)
                    
                    # 记录当前调用
                    call_history.append(time.time())
                    
                    # 调用原函数
                    return await func(*args, **kwargs)
            
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                with lock:
                    # 移除过期的调用记录
                    now = time.time()
                    while call_history and call_history[0] < now - period:
                        call_history.pop(0)
                    
                    # 检查是否超过速率限制
                    if len(call_history) >= calls:
                        wait_time = call_history[0] + period - now
                        if wait_time > 0:
                            logger.warning(f"Rate limit exceeded for {func.__name__}, waiting {wait_time:.2f} seconds")
                            time.sleep(wait_time)
                            
                            # 重新检查
                            now = time.time()
                            while call_history and call_history[0] < now - period:
                                call_history.pop(0)
                    
                    # 记录当前调用
                    call_history.append(time.time())
                    
                    # 调用原函数
                    return func(*args, **kwargs)
                    
            return sync_wrapper
    
    return decorator


def timing_decorator(func):
    """测量函数执行时间的装饰器"""
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
    """记忆化装饰器，缓存函数结果"""
    cache = {}
    expire_times = {}
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            key = str((args, frozenset(kwargs.items())))
            
            # 检查缓存是否过期
            if ttl is not None and key in expire_times:
                if time.time() > expire_times[key]:
                    if key in cache:
                        del cache[key]
            
            # 如果未命中缓存，计算结果并存储
            if key not in cache:
                result = func(*args, **kwargs)
                cache[key] = result
                if ttl is not None:
                    expire_times[key] = time.time() + ttl
                return result
            
            # 返回缓存结果
            return cache[key]
        
        # 添加清除缓存的方法
        wrapper.clear_cache = lambda: cache.clear()
        
        return wrapper
    
    return decorator


def safe_json_dumps(obj: Any, default: Any = None, **kwargs) -> str:
    """安全的JSON序列化，处理不可序列化的对象"""
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
        return json.dumps({"error": "序列化失败", "type": str(type(obj))})


def generate_id(prefix: str = "") -> str:
    """生成唯一标识符"""
    random_id = uuid.uuid4().hex[:12]
    timestamp = int(time.time())
    return f"{prefix}{timestamp:x}_{random_id}"


async def run_with_timeout(coro, timeout: float = 5.0, default=None):
    """带超时的异步函数执行器"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"操作超时 ({timeout}s)")
        return default
    except Exception as e:
        logger.error(f"运行异步函数时出错: {e}")
        return default


def load_json_file(file_path: str, default: Dict = None) -> Dict:
    """安全加载JSON文件"""
    try:
        if not os.path.exists(file_path):
            return default or {}
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载 JSON 文件 {file_path} 失败: {e}")
        return default or {}


def save_json_file(file_path: str, data: Any, indent: int = 2) -> bool:
    """安全保存JSON文件"""
    try:
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        return True
    except Exception as e:
        logger.error(f"保存 JSON 文件 {file_path} 失败: {e}")
        return False


class LimitedSizeDict(dict):
    """固定大小的字典，当超过大小限制时自动删除最早添加的条目"""
    
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
    """线程安全的字典"""
    
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
    """异步任务管理器，用于跟踪和控制异步任务"""
    
    def __init__(self):
        self.tasks = {}
        self.results = {}
        self._lock = asyncio.Lock()
        
    async def create_task(self, name: str, coro, timeout: float = None) -> str:
        """创建并注册一个任务"""
        async with self._lock:
            task_id = str(uuid.uuid4())
            
            # 创建包装器协程
            async def task_wrapper():
                try:
                    if timeout:
                        result = await asyncio.wait_for(coro, timeout=timeout)
                    else:
                        result = await coro
                        
                    # 存储结果
                    async with self._lock:
                        self.results[task_id] = {
                            "success": True,
                            "result": result,
                            "time": time.time()
                        }
                        
                except asyncio.CancelledError:
                    # 任务被取消
                    async with self._lock:
                        self.results[task_id] = {
                            "success": False,
                            "error": "Task cancelled",
                            "time": time.time()
                        }
                    raise
                    
                except Exception as e:
                    # 任务出错
                    async with self._lock:
                        self.results[task_id] = {
                            "success": False,
                            "error": str(e),
                            "traceback": traceback.format_exc(),
                            "time": time.time()
                        }
                finally:
                    # 完成时从活动任务中移除
                    async with self._lock:
                        if task_id in self.tasks:
                            del self.tasks[task_id]
            
            # 创建并启动任务
            task = asyncio.create_task(task_wrapper(), name=name)
            self.tasks[task_id] = {
                "task": task,
                "name": name,
                "start_time": time.time()
            }
            
            return task_id
            
    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        async with self._lock:
            if task_id in self.tasks:
                task_info = self.tasks[task_id]
                task = task_info["task"]
                task.cancel()
                return True
            return False
    
    async def get_task_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务信息"""
        async with self._lock:
            task_info = self.tasks.get(task_id)
            if not task_info:
                # 检查是否有结果
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
        """获取所有任务信息"""
        async with self._lock:
            all_tasks = {}
            # 活动任务
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
            
            # 已完成任务
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
        """清理旧的任务结果"""
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
    """简单的信号/槽实现，用于解耦组件"""
    
    def __init__(self, name=None):
        self.name = name
        self._handlers = []
        self._lock = threading.RLock()
    
    def connect(self, handler):
        """连接处理器"""
        with self._lock:
            if handler not in self._handlers:
                self._handlers.append(handler)
        return self
    
    def disconnect(self, handler):
        """断开处理器"""
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)
        return self
    
    def emit(self, *args, **kwargs):
        """同步发射信号"""
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
        """异步发射信号"""
        with self._lock:
            handlers = list(self._handlers)
            
        tasks = []
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                tasks.append(handler(*args, **kwargs))
            else:
                # 将同步处理器包装为异步
                def make_wrapper(h):
                    async def wrapper():
                        return h(*args, **kwargs)
                    return wrapper
                tasks.append(make_wrapper(handler)())
                
        if tasks:
            return await asyncio.gather(*tasks, return_exceptions=True)
        return []
    
    def clear(self):
        """清除所有处理器"""
        with self._lock:
            self._handlers.clear()


def singleton(cls):
    """单例装饰器"""
    instances = {}
    
    @wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
        
    return get_instance


def debounce(wait_time):
    """防抖装饰器，限制函数调用频率"""
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
    """内存分析装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            import psutil
            import os
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
    """自动重试装饰器"""
    return RetryDecorator(
        max_retries=max_retries, 
        retry_delay=retry_delay, 
        backoff_factor=backoff_factor, 
        exceptions=exceptions
    )