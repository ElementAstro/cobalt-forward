import importlib
import inspect
from typing import Any, Dict, List, Type, Optional, Callable
from abc import ABC, abstractmethod
import os
import asyncio
import yaml
from datetime import datetime
from loguru import logger
from dataclasses import dataclass


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


class Plugin(ABC):
    def __init__(self):
        self.metadata: PluginMetadata = None
        self.state: str = PluginState.UNLOADED
        self.config: Dict[str, Any] = {}
        self.start_time: float = None
        self._hooks: Dict[str, List[Callable]] = {}
        self._event_handlers: Dict[str, List[Callable]] = {}

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

            logger.success(f"Successfully loaded plugin: {plugin_name}")

        except Exception as e:
            logger.error(f"Error loading plugin {plugin_name}: {e}")
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
