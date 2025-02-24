import inspect
import asyncio
from typing import Any, Callable, Dict
from pydantic import create_model, ValidationError
from typing import get_type_hints
from functools import lru_cache


class PluginFunction:
    """插件函数包装器"""

    def __init__(self, func: Callable, name: str = None, description: str = None):
        self.func = func
        self.name = name or func.__name__
        self.description = description or func.__doc__
        self.type_hints = get_type_hints(func)
        self.signature = inspect.signature(func)
        self.is_coroutine = asyncio.iscoroutinefunction(func)
        self.param_model = self._create_param_model()

        # 缓存验证模型
        self._param_validation_cache = {}

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

    @lru_cache(maxsize=128)
    def _validate_params(self, param_key: str) -> Dict:
        """缓存参数验证结果"""
        return self.param_model.parse_obj(eval(param_key)).__dict__

    async def __call__(self, *args, **kwargs):
        """优化的函数调用"""
        try:
            # 使用缓存验证参数
            param_key = str(kwargs)
            params = self._validate_params(param_key)

            if self.is_coroutine:
                return await self.func(*args, **params)
            return self.func(*args, **params)
        except Exception as e:
            raise ValueError(f"Invalid parameters for {self.name}: {e}")
