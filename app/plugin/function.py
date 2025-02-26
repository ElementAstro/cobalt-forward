import inspect
import asyncio
import time
import traceback
from typing import Any, Callable, Dict, Optional, Tuple
from pydantic import create_model, ValidationError
from typing import get_type_hints
from functools import lru_cache, wraps
import hashlib
import json


class PluginFunction:
    """插件函数包装器，提供参数验证和性能测量"""

    def __init__(self, 
                 func: Callable, 
                 name: str = None, 
                 description: str = None,
                 cache_size: int = 128):
        self.func = func
        self.name = name or func.__name__
        self.description = description or func.__doc__ or ""
        self.type_hints = get_type_hints(func)
        self.signature = inspect.signature(func)
        self.is_coroutine = asyncio.iscoroutinefunction(func)
        self.param_model = self._create_param_model()
        self.cache_size = cache_size
        
        # 性能监控相关
        self.total_calls = 0
        self.total_errors = 0
        self.execution_times = []
        self.max_execution_samples = 100
        
        # 初始化缓存
        self._init_cache()

    def _create_param_model(self):
        """创建参数验证模型"""
        fields = {}
        for param_name, param in self.signature.parameters.items():
            if param.name == 'self':
                continue
            annotation = self.type_hints.get(param_name, Any)
            default = ... if param.default == inspect.Parameter.empty else param.default
            fields[param_name] = (annotation, default)
        return create_model(f'{self.name}_params', **fields)
    
    def _init_cache(self):
        """初始化缓存装饰器"""
        @lru_cache(maxsize=self.cache_size)
        def cached_validate(param_hash):
            """缓存参数验证结果"""
            return True
        
        self._cached_validate = cached_validate
        
        # 参数字典的缓存
        self._param_cache = {}
    
    def _get_param_hash(self, kwargs: Dict) -> str:
        """获取参数哈希值，用于缓存键"""
        # 将字典转换为排序后的字符串，以确保相同内容的字典产生相同的哈希
        param_str = json.dumps(kwargs, sort_keys=True)
        return hashlib.md5(param_str.encode()).hexdigest()
    
    def clear_cache(self):
        """清除缓存"""
        self._cached_validate.cache_clear()
        self._param_cache.clear()
    
    async def __call__(self, *args, **kwargs):
        """优化的函数调用"""
        start_time = time.time()
        self.total_calls += 1
        
        try:
            # 优化参数验证
            param_hash = self._get_param_hash(kwargs)
            
            # 检查缓存
            if param_hash in self._param_cache:
                params = self._param_cache[param_hash]
            else:
                # 验证参数
                try:
                    params = self.param_model(**kwargs).dict()
                    # 缓存结果
                    self._param_cache[param_hash] = params
                    # 如果缓存太大，删除一些条目
                    if len(self._param_cache) > self.cache_size:
                        # 简单策略：删除第一个键
                        self._param_cache.pop(next(iter(self._param_cache)))
                except ValidationError as e:
                    raise ValueError(f"参数验证失败: {e}")

            # 执行原函数
            if self.is_coroutine:
                result = await self.func(*args, **params)
            else:
                result = self.func(*args, **params)
                
            # 记录执行时间
            execution_time = time.time() - start_time
            self._record_execution(execution_time)
            
            return result
            
        except Exception as e:
            self.total_errors += 1
            execution_time = time.time() - start_time
            self._record_execution(execution_time)
            # 增强异常信息
            raise type(e)(f"{self.name} 执行失败: {str(e)}\n{traceback.format_exc()}")
    
    def _record_execution(self, execution_time: float):
        """记录执行时间"""
        self.execution_times.append(execution_time)
        if len(self.execution_times) > self.max_execution_samples:
            self.execution_times = self.execution_times[-self.max_execution_samples:]
    
    def get_stats(self) -> Dict[str, Any]:
        """获取函数调用统计信息"""
        avg_time = sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0
        return {
            "name": self.name,
            "total_calls": self.total_calls,
            "total_errors": self.total_errors,
            "error_rate": self.total_errors / self.total_calls if self.total_calls > 0 else 0,
            "avg_execution_time": avg_time,
            "max_execution_time": max(self.execution_times) if self.execution_times else 0,
            "min_execution_time": min(self.execution_times) if self.execution_times else 0
        }
    
    def get_signature_info(self) -> Dict[str, Any]:
        """获取函数签名信息"""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": {
                name: {
                    "type": str(param.annotation if param.annotation != inspect.Parameter.empty else Any),
                    "default": None if param.default == inspect.Parameter.empty else param.default,
                    "required": param.default == inspect.Parameter.empty
                }
                for name, param in self.signature.parameters.items()
                if name != 'self'
            },
            "return_type": str(self.type_hints.get("return", Any)),
            "is_async": self.is_coroutine
        }
