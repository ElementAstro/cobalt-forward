import importlib
import inspect
from typing import Any, Dict, Generator, List, Type, Optional, Callable, Set, ContextManager
from abc import ABC, abstractmethod
import os
import asyncio
import yaml
from datetime import datetime
from loguru import logger
from dataclasses import dataclass
import jsonschema
import typing
from functools import wraps
from typing import get_type_hints, Any, Dict, List, Type, Optional, Callable, Union
from pydantic import create_model, ValidationError
import contextlib
import time
import resource
from concurrent.futures import ThreadPoolExecutor
import threading
from packaging import version


@dataclass
class PluginMetadata:
    """插件元数据"""
    name: str
    version: str
    author: str
    description: str
    dependencies: List[str] = None
    required_version: str = None
    load_priority: int = 100
    config_schema: Dict = None


class PluginState:
    """插件状态"""
    UNLOADED = "UNLOADED"
    LOADING = "LOADING"
    ACTIVE = "ACTIVE"
    ERROR = "ERROR"
    DISABLED = "DISABLED"


class PluginPermission:
    """插件权限定义"""
    ADMIN = "admin"
    FILE_IO = "file_io"
    NETWORK = "network"
    SYSTEM = "system"
    DATABASE = "database"
    ALL = {ADMIN, FILE_IO, NETWORK, SYSTEM, DATABASE}


class PluginSandbox:
    """插件沙箱环境"""
    def __init__(self):
        self.memory_limit = 1024 * 1024 * 100  # 100MB
        self.cpu_time_limit = 30  # 30秒
        self.file_access_paths = set()
        self.allowed_modules = set()

    @contextlib.contextmanager
    def apply(self) -> Generator[None, None, None]:
        """应用沙箱限制"""
        old_limits = resource.getrlimit(resource.RLIMIT_AS)
        try:
            # 设置内存限制
            resource.setrlimit(resource.RLIMIT_AS, (self.memory_limit, self.memory_limit))
            yield
        finally:
            # 恢复原始限制
            resource.setrlimit(resource.RLIMIT_AS, old_limits)


class PluginMetrics:
    """插件性能指标"""
    def __init__(self):
        self.execution_times: List[float] = []
        self.memory_usage: List[float] = []
        self.error_count: int = 0
        self.last_execution: float = 0
        self.total_calls: int = 0

    def record_execution(self, execution_time: float, memory_used: float):
        """记录执行数据"""
        self.execution_times.append(execution_time)
        self.memory_usage.append(memory_used)
        self.last_execution = time.time()
        self.total_calls += 1

    def get_average_execution_time(self) -> float:
        """获取平均执行时间"""
        return sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0


class PluginFunction:
    """插件函数包装器"""
    def __init__(self, func: Callable, name: str = None, description: str = None):
        self.func = func
        self.name = name or func.__name__
        self.description = description or func.__doc__
        self.type_hints = get_type_hints(func)
        self.signature = inspect.signature(func)
        self.is_coroutine = asyncio.iscoroutinefunction(func)
        
        # 创建参数验证模型
        self.param_model = self._create_param_model()

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

    async def __call__(self, *args, **kwargs):
        """调用函数并进行参数验证"""
        try:
            # 验证参数
            params = self.param_model(**kwargs)
            if self.is_coroutine:
                return await self.func(*args, **params.dict())
            return self.func(*args, **params.dict())
        except ValidationError as e:
            raise ValueError(f"Invalid parameters for {self.name}: {e}")


class Plugin(ABC):
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
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._thread_pool = ThreadPoolExecutor(max_workers=3)
        self._local_storage = threading.local()

    @abstractmethod
    async def initialize(self) -> None:
        """插件初始化"""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """插件关闭"""
        pass

    async def on_config_change(self, new_config: Dict[str, Any]) -> None:
        """配置变更处理"""
        self.config = new_config

    def register_hook(self, hook_name: str, callback: Callable) -> None:
        """注册钩子函数"""
        if hook_name not in self._hooks:
            self._hooks[hook_name] = []
        self._hooks[hook_name].append(callback)

    def register_event_handler(self, event_name: str, handler: Callable) -> None:
        """注册事件处理器"""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        self._event_handlers[event_name].append(handler)

    async def handle_event(self, event_name: str, event_data: Any) -> None:
        """处理事件"""
        if event_name in self._event_handlers:
            for handler in self._event_handlers[event_name]:
                await handler(event_data)

    def add_lifecycle_hook(self, stage: str, callback: Callable) -> None:
        """添加生命周期钩子"""
        if stage in self._lifecycle_hooks:
            self._lifecycle_hooks[stage].append(callback)

    async def check_health(self) -> Dict[str, Any]:
        """检查插件健康状态"""
        try:
            health_result = await self._health_check()
            self.health_status = health_result
            self.last_health_check = datetime.now().timestamp()
            return health_result
        except Exception as e:
            self.health_status = {"status": "error", "error": str(e)}
            return self.health_status

    async def _health_check(self) -> Dict[str, Any]:
        """实现具体的健康检查逻辑"""
        return {"status": "healthy"}

    async def validate_config(self, config: Dict[str, Any]) -> bool:
        """验证配置"""
        if not hasattr(self.metadata, 'config_schema'):
            return True
        try:
            jsonschema.validate(instance=config, schema=self.metadata.config_schema)
            return True
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Config validation failed: {e}")
            return False

    def export_function(self, name: str = None, description: str = None):
        """导出函数装饰器"""
        def decorator(func):
            func_name = name or func.__name__
            wrapped = PluginFunction(func, func_name, description)
            self._functions[func_name] = wrapped
            return func
        return decorator

    def export_type(self, type_class: Type, name: str = None):
        """导出类型"""
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
        """获取插件API模式"""
        schema = {
            "functions": {},
            "types": {}
        }
        
        # 收集函数信息
        for name, func in self._functions.items():
            schema["functions"][name] = {
                "description": func.description,
                "parameters": {
                    name: {
                        "type": str(param.annotation),
                        "default": None if param.default == inspect.Parameter.empty else param.default,
                        "required": param.default == inspect.Parameter.empty
                    }
                    for name, param in func.signature.parameters.items()
                    if name != 'self'
                },
                "return_type": str(func.type_hints.get("return", Any)),
                "is_async": func.is_coroutine
            }
        
        # 收集类型信息
        for name, type_class in self._exported_types.items():
            if hasattr(type_class, "__annotations__"):
                schema["types"][name] = {
                    "fields": {
                        field_name: str(field_type)
                        for field_name, field_type in type_class.__annotations__.items()
                    }
                }
        
        return schema

    def require_permission(self, permission: str):
        """权限检查装饰器"""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                if permission not in self._permissions:
                    raise PermissionError(f"Plugin lacks permission: {permission}")
                return await func(*args, **kwargs)
            return wrapper
        return decorator

    async def send_message(self, target_plugin: str, message: Any):
        """发送消息到其他插件"""
        if self.plugin_manager:
            await self.plugin_manager.send_plugin_message(
                self.metadata.name, target_plugin, message
            )

    def run_in_sandbox(self, func: Callable):
        """在沙箱中运行函数"""
        with self._sandbox.apply():
            return func()

    def run_in_thread(self, func: Callable, *args, **kwargs):
        """在线程池中运行函数"""
        return self._thread_pool.submit(func, *args, **kwargs)

    async def get_messages(self) -> List[Any]:
        """获取待处理消息"""
        messages = []
        while not self._message_queue.empty():
            messages.append(await self._message_queue.get())
        return messages


from typing import Dict, List, Any, Optional, Type
import importlib
import os
import asyncio
import yaml
from datetime import datetime
from loguru import logger
from .models import PluginMetadata, PluginState
from .base import Plugin
from .permissions import PluginPermission

class PluginManager:
    def __init__(self, plugin_dir: str = "plugins", config_dir: str = "config/plugins"):
        self.plugin_dir = plugin_dir
        self.config_dir = config_dir
        self.plugins: Dict[str, Plugin] = {}
        self.plugin_configs: Dict[str, Dict[str, Any]] = {}
        self.hooks: Dict[str, List[Callable]] = {}
        self._load_order: List[str] = []
        self._dependency_graph: Dict[str, List[str]] = {}
        self._watch_task: Optional[asyncio.Task] = None
        self._file_hashes: Dict[str, str] = {}
        self._health_check_task: Optional[asyncio.Task] = None
        self.plugin_stats: Dict[str, Dict[str, Any]] = {}
        self._message_brokers: Dict[str, asyncio.Queue] = {}
        self._version_requirements: Dict[str, str] = {}

        # 确保目录存在
        os.makedirs(plugin_dir, exist_ok=True)
        os.makedirs(config_dir, exist_ok=True)

    async def load_plugins(self):
        """加载所有插件"""
        # 扫描插件目录
        plugin_files = [f for f in os.listdir(self.plugin_dir)
                        if f.endswith('.py') and not f.startswith('_')]

        # 首先收集所有插件的元数据和依赖信息
        for file in plugin_files:
            plugin_name = file[:-3]
            try:
                await self._collect_plugin_metadata(plugin_name)
            except Exception as e:
                logger.error(
                    f"Error collecting metadata for plugin {plugin_name}: {e}")

        # 根据依赖关系和优先级排序
        self._load_order = self._resolve_load_order()

        # 按顺序加载插件
        for plugin_name in self._load_order:
            await self._load_plugin(plugin_name)

    async def _collect_plugin_metadata(self, plugin_name: str):
        """收集插件元数据"""
        try:
            module = importlib.import_module(
                f"{self.plugin_dir}.{plugin_name}")
            metadata = getattr(module, 'PLUGIN_METADATA', None)
            if metadata:
                self._dependency_graph[plugin_name] = metadata.dependencies or [
                ]
                logger.info(f"Collected metadata for plugin: {plugin_name}")
        except Exception as e:
            logger.error(f"Error collecting metadata for {plugin_name}: {e}")

    def _resolve_load_order(self) -> List[str]:
        """解析插件加载顺序"""
        visited = set()
        temp_visited = set()
        order = []

        def visit(plugin: str):
            if plugin in temp_visited:
                raise ValueError(f"Circular dependency detected: {plugin}")
            if plugin in visited:
                return

            temp_visited.add(plugin)
            for dep in self._dependency_graph.get(plugin, []):
                visit(dep)
            temp_visited.remove(plugin)
            visited.add(plugin)
            order.append(plugin)

        for plugin in self._dependency_graph:
            if plugin not in visited:
                visit(plugin)

        return order

    async def _load_plugin(self, plugin_name: str):
        """加载单个插件"""
        try:
            # 执行预初始化钩子
            await self._execute_lifecycle_hooks(plugin_name, "pre_initialize")
            
            # 加载插件模块
            module = importlib.import_module(
                f"{self.plugin_dir}.{plugin_name}")

            # 查找插件类
            plugin_classes = [obj for name, obj in inspect.getmembers(module)
                              if inspect.isclass(obj) and issubclass(obj, Plugin) and obj != Plugin]

            if not plugin_classes:
                raise ValueError(f"No plugin class found in {plugin_name}")

            plugin_class = plugin_classes[0]
            plugin = plugin_class()
            plugin.state = PluginState.LOADING

            # 加载配置
            config_file = os.path.join(self.config_dir, f"{plugin_name}.yaml")
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    plugin.config = yaml.safe_load(f)

            # 初始化插件
            await plugin.initialize()
            plugin.state = PluginState.ACTIVE
            plugin.start_time = datetime.now().timestamp()

            # 注册插件
            self.plugins[plugin_name] = plugin

            # 注册插件的事件处理器和钩子
            for event_name, handlers in plugin._event_handlers.items():
                for handler in handlers:
                    self.register_event_handler(event_name, handler)

            for hook_name, callbacks in plugin._hooks.items():
                for callback in callbacks:
                    self.register_hook(hook_name, callback)

            # 添加文件哈希记录
            plugin_file = os.path.join(self.plugin_dir, f"{plugin_name}.py")
            self._file_hashes[plugin_name] = self._get_file_hash(plugin_file)
            
            # 执行后初始化钩子
            await self._execute_lifecycle_hooks(plugin_name, "post_initialize")
            
            # 初始化插件统计信息
            self.plugin_stats[plugin_name] = {
                "load_time": datetime.now().timestamp(),
                "error_count": 0,
                "last_error": None
            }

            # 初始化插件沙箱
            plugin._sandbox.allowed_modules = {'os.path', 'json', 'yaml'}
            plugin._sandbox.file_access_paths = {self.config_dir}
            
            # 设置默认权限
            plugin._permissions = {PluginPermission.FILE_IO}

            logger.success(f"Successfully loaded plugin: {plugin_name}")

        except Exception as e:
            logger.error(f"Error loading plugin {plugin_name}: {e}")
            self.plugin_stats[plugin_name] = {
                "error_count": 1,
                "last_error": str(e)
            }
            if plugin_name in self.plugins:
                self.plugins[plugin_name].state = PluginState.ERROR

    def register_hook(self, hook_name: str, callback: Callable):
        """注册钩子"""
        if hook_name not in self.hooks:
            self.hooks[hook_name] = []
        self.hooks[hook_name].append(callback)

    def register_event_handler(self, event_name: str, handler: Callable):
        """注册事件处理器"""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        self._event_handlers[event_name].append(handler)

    async def execute_hook(self, hook_name: str, *args, **kwargs):
        """执行钩子"""
        results = []
        if hook_name in self.hooks:
            for callback in self.hooks[hook_name]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        result = await callback(*args, **kwargs)
                    else:
                        result = callback(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error executing hook {hook_name}: {e}")
        return results

    async def reload_plugin(self, plugin_name: str):
        """重新加载插件"""
        if plugin_name in self.plugins:
            try:
                await self.plugins[plugin_name].shutdown()
                del self.plugins[plugin_name]
                importlib.reload(importlib.import_module(
                    f"{self.plugin_dir}.{plugin_name}"))
                await self._load_plugin(plugin_name)
                return True
            except Exception as e:
                logger.error(f"Error reloading plugin {plugin_name}: {e}")
                return False
        return False

    async def shutdown_plugins(self):
        """关闭所有插件"""
        # 按照加载顺序的反序关闭插件
        for plugin_name in reversed(self._load_order):
            if plugin_name in self.plugins:
                try:
                    plugin = self.plugins[plugin_name]
                    await plugin.shutdown()
                    plugin.state = PluginState.UNLOADED
                    logger.info(f"Shutdown plugin: {plugin_name}")
                except Exception as e:
                    logger.error(
                        f"Error shutting down plugin {plugin_name}: {e}")

    async def start_plugin_watcher(self):
        """启动插件文件监视器"""
        async def watch_plugins():
            while True:
                for plugin_name, plugin in self.plugins.items():
                    config_file = os.path.join(
                        self.config_dir, f"{plugin_name}.yaml")
                    if os.path.exists(config_file):
                        mtime = os.path.getmtime(config_file)
                        if mtime > plugin.start_time:
                            # 配置文件已更新，重新加载配置
                            try:
                                with open(config_file, 'r') as f:
                                    new_config = yaml.safe_load(f)
                                await plugin.on_config_change(new_config)
                                logger.info(
                                    f"Reloaded config for plugin: {plugin_name}")
                            except Exception as e:
                                logger.error(
                                    f"Error reloading config for {plugin_name}: {e}")
                await asyncio.sleep(5)  # 每5秒检查一次

        self._watch_task = asyncio.create_task(watch_plugins())

    def stop_plugin_watcher(self):
        """停止插件文件监视器"""
        if self._watch_task:
            self._watch_task.cancel()

    async def start_health_monitor(self, interval: int = 300):
        """启动健康监控"""
        async def health_check_loop():
            while True:
                for plugin_name, plugin in self.plugins.items():
                    try:
                        health_status = await plugin.check_health()
                        if health_status["status"] != "healthy":
                            logger.warning(f"Plugin {plugin_name} health check failed: {health_status}")
                    except Exception as e:
                        logger.error(f"Health check failed for {plugin_name}: {e}")
                await asyncio.sleep(interval)

        self._health_check_task = asyncio.create_task(health_check_loop())

    def stop_health_monitor(self):
        """停止健康监控"""
        if self._health_check_task:
            self._health_check_task.cancel()

    async def hot_reload_plugin(self, plugin_name: str):
        """热重载插件"""
        try:
            plugin_file = os.path.join(self.plugin_dir, f"{plugin_name}.py")
            current_hash = self._get_file_hash(plugin_file)
            
            if current_hash != self._file_hashes.get(plugin_name):
                logger.info(f"Detected changes in plugin {plugin_name}, hot reloading...")
                await self.reload_plugin(plugin_name)
                self._file_hashes[plugin_name] = current_hash
                return True
            return False
        except Exception as e:
            logger.error(f"Hot reload failed for {plugin_name}: {e}")
            return False

    def _get_file_hash(self, file_path: str) -> str:
        """获取文件哈希值"""
        import hashlib
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    async def _execute_lifecycle_hooks(self, plugin_name: str, stage: str):
        """执行生命周期钩子"""
        if plugin_name in self.plugins:
            plugin = self.plugins[plugin_name]
            for hook in plugin._lifecycle_hooks.get(stage, []):
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook()
                    else:
                        hook()
                except Exception as e:
                    logger.error(f"Error executing {stage} hook for {plugin_name}: {e}")

    def get_plugin_info(self) -> Dict[str, Any]:
        """获取所有插件信息"""
        return {
            name: {
                "metadata": plugin.metadata.__dict__,
                "state": plugin.state,
                "start_time": plugin.start_time,
                "config": plugin.config
            }
            for name, plugin in self.plugins.items()
        }

    def get_plugin_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取插件统计信息"""
        return self.plugin_stats

    async def get_plugin_health(self, plugin_name: str) -> Dict[str, Any]:
        """获取插件健康状态"""
        if plugin_name in self.plugins:
            return self.plugins[plugin_name].health_status
        return {"status": "not_found"}

    async def call_plugin_function(self, plugin_name: str, function_name: str, *args, **kwargs) -> Any:
        """调用插件函数"""
        if plugin_name not in self.plugins:
            raise ValueError(f"Plugin {plugin_name} not found")
        
        plugin = self.plugins[plugin_name]
        func = plugin.get_function(function_name)
        if not func:
            raise ValueError(f"Function {function_name} not found in plugin {plugin_name}")
        
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error calling {plugin_name}.{function_name}: {e}")
            raise

    def get_plugin_api_schema(self, plugin_name: str) -> Dict[str, Any]:
        """获取插件API模式"""
        if plugin_name not in self.plugins:
            raise ValueError(f"Plugin {plugin_name} not found")
        return self.plugins[plugin_name].get_api_schema()

    def get_plugin_type(self, plugin_name: str, type_name: str) -> Optional[Type]:
        """获取插件类型"""
        if plugin_name not in self.plugins:
            raise ValueError(f"Plugin {plugin_name} not found")
        return self.plugins[plugin_name].get_type(type_name)

    async def send_plugin_message(self, sender: str, target: str, message: Any):
        """发送插件间消息"""
        if target in self.plugins:
            await self.plugins[target]._message_queue.put({
                "sender": sender,
                "content": message,
                "timestamp": time.time()
            })

    def check_plugin_compatibility(self, plugin_name: str, required_version: str) -> bool:
        """检查插件版本兼容性"""
        if plugin_name not in self.plugins:
            return False
        plugin_version = self.plugins[plugin_name].metadata.version
        return version.parse(plugin_version) >= version.parse(required_version)

    async def grant_permission(self, plugin_name: str, permission: str):
        """授予插件权限"""
        if plugin_name in self.plugins:
            self.plugins[plugin_name]._permissions.add(permission)
            logger.info(f"Granted {permission} permission to {plugin_name}")

    async def revoke_permission(self, plugin_name: str, permission: str):
        """撤销插件权限"""
        if plugin_name in self.plugins:
            self.plugins[plugin_name]._permissions.discard(permission)
            logger.info(f"Revoked {permission} permission from {plugin_name}")

    def get_plugin_metrics(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """获取插件性能指标"""
        if plugin_name in self.plugins:
            metrics = self.plugins[plugin_name]._metrics
            return {
                "avg_execution_time": metrics.get_average_execution_time(),
                "total_calls": metrics.total_calls,
                "error_rate": metrics.error_count / metrics.total_calls if metrics.total_calls > 0 else 0,
                "last_execution": metrics.last_execution,
                "memory_usage": sum(metrics.memory_usage) / len(metrics.memory_usage) if metrics.memory_usage else 0
            }
        return None

    async def broadcast_message(self, message: Any, exclude: List[str] = None):
        """广播消息到所有插件"""
        exclude = exclude or []
        for plugin_name, plugin in self.plugins.items():
            if plugin_name not in exclude:
                await plugin._message_queue.put({
                    "sender": "system",
                    "content": message,
                    "timestamp": time.time()
                })
