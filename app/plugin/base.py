from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type, Optional, Callable, Set, Union
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from datetime import datetime
import logging
import traceback
import json

from .models import PluginMetadata, PluginState, PluginMetrics
from .sandbox import PluginSandbox
from .function import PluginFunction
from .permissions import PluginPermission, require_permission

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """插件基类，提供插件生命周期管理和各种功能"""
    
    def __init__(self):
        self.metadata: PluginMetadata = None
        self.state: str = PluginState.UNLOADED
        self.config: Dict[str, Any] = {}
        self.start_time: float = None
        self._hooks: Dict[str, List[Callable]] = {}
        self._event_handlers: Dict[str, List[Callable]] = {}
        self.health_status: Dict[str, Any] = {"status": "unknown"}
        self.last_health_check: float = None
        self._lifecycle_hooks = {
            "pre_initialize": [],
            "post_initialize": [],
            "pre_shutdown": [],
            "post_shutdown": []
        }
        self._functions: Dict[str, PluginFunction] = {}
        self._exported_types: Dict[str, Type] = {}
        self._permissions: Set[str] = set()
        self._sandbox = PluginSandbox()
        self._metrics = PluginMetrics()
        self._message_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)  # 限制队列大小
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        self._local_storage = threading.local()
        self._plugin_manager = None  # 引用插件管理器
        self._event_bus = None       # 引用事件总线
        self._lock = asyncio.Lock()  # 保护关键操作的锁
        self._initialized = False    # 初始化标志
        self._shutdown_complete = False  # 关闭完成标志
        self._config_version = 0     # 配置版本号
        self._error_count = 0        # 错误计数
        self._last_error = None      # 最后一次错误

    def _init_thread_pool(self, max_workers: int = 3):
        """初始化线程池"""
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(
                max_workers=max_workers, 
                thread_name_prefix=f"{self.metadata.name if self.metadata else 'plugin'}_worker"
            )
    
    def _shutdown_thread_pool(self):
        """关闭线程池"""
        if self._thread_pool:
            self._thread_pool.shutdown(wait=False)
            self._thread_pool = None

    @abstractmethod
    async def initialize(self) -> None:
        """插件初始化，必须由子类实现"""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """插件关闭，必须由子类实现"""
        pass

    async def safe_initialize(self) -> bool:
        """安全的初始化包装器"""
        if self._initialized:
            return True
            
        try:
            # 执行前置初始化钩子
            for hook in self._lifecycle_hooks.get("pre_initialize", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
                    
            # 初始化线程池
            self._init_thread_pool()
            
            # 执行子类的初始化方法
            await self.initialize()
            
            # 执行后置初始化钩子
            for hook in self._lifecycle_hooks.get("post_initialize", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
                    
            self._initialized = True
            self.state = PluginState.ACTIVE
            self.start_time = time.time()
            return True
            
        except Exception as e:
            self._error_count += 1
            self._last_error = {
                "time": datetime.now().isoformat(),
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.state = PluginState.ERROR
            logger.error(f"插件 {self.metadata.name if self.metadata else 'unknown'} 初始化失败: {e}")
            return False

    async def safe_shutdown(self) -> bool:
        """安全的关闭包装器"""
        if self._shutdown_complete:
            return True
            
        try:
            # 执行前置关闭钩子
            for hook in self._lifecycle_hooks.get("pre_shutdown", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
                    
            # 执行子类的关闭方法
            await self.shutdown()
            
            # 执行后置关闭钩子
            for hook in self._lifecycle_hooks.get("post_shutdown", []):
                if asyncio.iscoroutinefunction(hook):
                    await hook()
                else:
                    hook()
            
            # 清理线程池
            self._shutdown_thread_pool()
            
            # 清空队列
            while not self._message_queue.empty():
                try:
                    self._message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                    
            self._shutdown_complete = True
            self.state = PluginState.UNLOADED
            return True
            
        except Exception as e:
            self._error_count += 1
            self._last_error = {
                "time": datetime.now().isoformat(),
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            logger.error(f"插件 {self.metadata.name if self.metadata else 'unknown'} 关闭失败: {e}")
            return False

    async def on_config_change(self, new_config: Dict[str, Any]) -> bool:
        """配置变更处理，支持验证配置"""
        try:
            # 验证新配置
            if hasattr(self, 'validate_config'):
                valid = await self.validate_config(new_config)
                if not valid:
                    logger.warning(f"插件 {self.metadata.name if self.metadata else 'unknown'} 配置验证失败")
                    return False
            
            # 更新配置
            old_config = self.config.copy()
            self.config = new_config
            self._config_version += 1
            
            # 记录配置变更
            logger.info(f"插件 {self.metadata.name if self.metadata else 'unknown'} 配置已更新")
            
            # 刷新沙箱缓存
            self._sandbox.reset_cache()
            
            return True
        except Exception as e:
            logger.error(f"处理配置变更失败: {e}")
            return False

    def register_hook(self, hook_name: str, callback: Callable) -> None:
        """注册钩子函数，线程安全"""
        if hook_name not in self._hooks:
            self._hooks[hook_name] = []
        if callback not in self._hooks[hook_name]:
            self._hooks[hook_name].append(callback)

    def unregister_hook(self, hook_name: str, callback: Callable) -> bool:
        """取消钩子函数注册，线程安全"""
        if hook_name in self._hooks and callback in self._hooks[hook_name]:
            self._hooks[hook_name].remove(callback)
            return True
        return False

    def register_event_handler(self, event_name: str, handler: Callable) -> None:
        """注册事件处理器，线程安全"""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        if handler not in self._event_handlers[event_name]:
            self._event_handlers[event_name].append(handler)

    def unregister_event_handler(self, event_name: str, handler: Callable) -> bool:
        """取消事件处理器注册，线程安全"""
        if event_name in self._event_handlers and handler in self._event_handlers[event_name]:
            self._event_handlers[event_name].remove(handler)
            return True
        return False

    async def handle_event(self, event_name: str, event_data: Any) -> List[Any]:
        """处理事件，返回处理结果列表"""
        results = []
        start_time = time.time()
        
        if event_name in self._event_handlers:
            for handler in self._event_handlers[event_name]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(event_data)
                    else:
                        result = handler(event_data)
                    results.append(result)
                except Exception as e:
                    logger.error(f"事件处理器 {handler.__name__} 执行失败: {e}")
                    self._metrics.record_error()
        
        # 记录执行指标
        execution_time = time.time() - start_time
        self._metrics.record_execution(execution_time)
        
        return results

    def add_lifecycle_hook(self, stage: str, callback: Callable) -> bool:
        """添加生命周期钩子，线程安全"""
        if stage in self._lifecycle_hooks:
            if callback not in self._lifecycle_hooks[stage]:
                self._lifecycle_hooks[stage].append(callback)
                return True
        return False

    async def check_health(self) -> Dict[str, Any]:
        """检查插件健康状态，添加超时处理"""
        try:
            # 使用超时限制健康检查执行时间
            health_result = await asyncio.wait_for(
                self._health_check(), 
                timeout=5.0  # 5秒超时
            )
            self.health_status = health_result
            self.last_health_check = time.time()
            return health_result
        except asyncio.TimeoutError:
            self.health_status = {
                "status": "error", 
                "error": "Health check timeout"
            }
            return self.health_status
        except Exception as e:
            self.health_status = {
                "status": "error", 
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            return self.health_status

    async def _health_check(self) -> Dict[str, Any]:
        """默认健康检查实现"""
        return {
            "status": "healthy",
            "initialized": self._initialized,
            "error_count": self._error_count,
            "uptime": time.time() - self.start_time if self.start_time else 0,
            "metrics": self._metrics.get_metrics_summary() if hasattr(self._metrics, 'get_metrics_summary') else {}
        }

    async def validate_config(self, config: Dict[str, Any]) -> bool:
        """验证配置，支持JSON Schema验证"""
        if not hasattr(self.metadata, 'config_schema') or not self.metadata.config_schema:
            return True
            
        try:
            # 如果有jsonschema模块，使用它进行验证
            try:
                import jsonschema
                jsonschema.validate(instance=config, schema=self.metadata.config_schema)
            except ImportError:
                # 简单验证：检查所有必需字段是否存在
                required_fields = self.metadata.config_schema.get('required', [])
                for field in required_fields:
                    if field not in config:
                        logger.error(f"缺少必需的配置字段: {field}")
                        return False
            return True
        except Exception as e:
            logger.error(f"配置验证失败: {e}")
            return False

    def export_function(self, name: str = None, description: str = None):
        """导出函数装饰器，优化缓存管理"""
        def decorator(func):
            func_name = name or func.__name__
            wrapped = PluginFunction(func, func_name, description)
            self._functions[func_name] = wrapped
            return func
        return decorator

    def export_type(self, type_class: Type, name: str = None):
        """导出类型，支持类型检查"""
        type_name = name or type_class.__name__
        self._exported_types[type_name] = type_class
        return type_class

    def get_function(self, name: str) -> Optional[PluginFunction]:
        """获取导出的函数"""
        return self._functions.get(name)

    def get_type(self, name: str) -> Optional[Type]:
        """获取导出的类型"""
        return self._exported_types.get(name)

    def get_api_schema(self) -> Dict[str, Any]:
        """获取插件API模式，包含更多信息"""
        schema = {
            "functions": {},
            "types": {},
            "metadata": self.metadata.to_dict() if hasattr(self.metadata, 'to_dict') else (self.metadata.__dict__ if self.metadata else {}),
            "permissions": list(self._permissions),
            "version": self.metadata.version if self.metadata else "unknown"
        }

        # 收集函数信息
        for name, func in self._functions.items():
            if hasattr(func, 'get_signature_info'):
                schema["functions"][name] = func.get_signature_info()
            else:
                schema["functions"][name] = {
                    "name": name,
                    "description": func.description if hasattr(func, 'description') else "",
                    "is_async": asyncio.iscoroutinefunction(func.func if hasattr(func, 'func') else func)
                }

        # 收集类型信息
        for name, type_class in self._exported_types.items():
            type_info = {"fields": {}}
            
            # 处理数据类
            if hasattr(type_class, "__dataclass_fields__"):
                for field_name, field_obj in type_class.__dataclass_fields__.items():
                    type_info["fields"][field_name] = {
                        "type": str(field_obj.type),
                        "default": field_obj.default if field_obj.default is not field_obj.default_factory else "has_factory"
                    }
            # 处理普通类
            elif hasattr(type_class, "__annotations__"):
                for field_name, field_type in type_class.__annotations__.items():
                    type_info["fields"][field_name] = {
                        "type": str(field_type),
                        "default": getattr(type_class, field_name, None) if hasattr(type_class, field_name) else None
                    }
                
            schema["types"][name] = type_info

        return schema

    async def send_message(self, target_plugin: str, message: Any) -> bool:
        """发送消息到其他插件，支持超时"""
        if not self._plugin_manager:
            logger.warning(f"插件 {self.metadata.name if self.metadata else 'unknown'} 尝试发送消息，但插件管理器未连接")
            return False
            
        try:
            # 限制消息大小
            message_str = json.dumps(message) if not isinstance(message, str) else message
            if len(message_str) > 1024 * 1024:  # 1MB限制
                logger.warning(f"消息过大 ({len(message_str)} bytes)，已被拒绝")
                return False
                
            # 发送消息，带5秒超时
            return await asyncio.wait_for(
                self._plugin_manager.send_plugin_message(
                    self.metadata.name if self.metadata else "unknown", 
                    target_plugin, 
                    message
                ),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.error(f"发送消息到插件 {target_plugin} 超时")
            return False
        except Exception as e:
            logger.error(f"发送消息失败: {e}")
            return False

    def run_in_sandbox(self, func: Callable, *args, **kwargs):
        """在沙箱中运行函数，带异常处理"""
        start_time = time.time()
        try:
            with self._sandbox.apply():
                result = func(*args, **kwargs)
                
                # 记录执行指标
                execution_time = time.time() - start_time
                self._metrics.record_execution(execution_time)
                
                return result
        except Exception as e:
            # 记录错误
            execution_time = time.time() - start_time
            self._metrics.record_execution(execution_time)
            self._metrics.record_error()
            
            # 重新抛出带上下文的异常
            raise type(e)(f"沙箱执行失败: {str(e)}")

    async def run_in_thread(self, func: Callable, *args, **kwargs):
        """在线程池中运行函数，异步等待结果"""
        if self._thread_pool is None:
            self._init_thread_pool()
            
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._thread_pool, 
            lambda: func(*args, **kwargs)
        )

    async def get_messages(self, timeout: float = 0.1) -> List[Any]:
        """获取待处理消息，支持超时参数"""
        messages = []
        try:
            # 尝试在指定时间内获取消息
            while True:
                try:
                    message = await asyncio.wait_for(
                        self._message_queue.get(), 
                        timeout=timeout
                    )
                    messages.append(message)
                    self._message_queue.task_done()
                except asyncio.TimeoutError:
                    # 超时，返回已获取的消息
                    break
        except Exception as e:
            logger.error(f"获取消息时出错: {e}")
            
        return messages
        
    def get_status(self) -> Dict[str, Any]:
        """获取插件状态"""
        return {
            "name": self.metadata.name if self.metadata else "unknown",
            "version": self.metadata.version if self.metadata else "unknown",
            "state": self.state,
            "initialized": self._initialized,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "uptime": time.time() - self.start_time if self.start_time else 0,
            "config_version": self._config_version,
            "health": self.health_status,
            "metrics": self._metrics.get_metrics_summary() if hasattr(self._metrics, 'get_metrics_summary') else {},
            "sandbox_status": self._sandbox.get_status() if hasattr(self._sandbox, 'get_status') else {},
        }
        
    def __del__(self):
        """确保资源被正确清理"""
        try:
            self._shutdown_thread_pool()
        except:
            pass
