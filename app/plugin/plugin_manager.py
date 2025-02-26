import os
import sys
import time
import asyncio
import importlib
import importlib.util
import inspect
import logging
import threading
import traceback
import yaml
import hashlib
import json
from typing import Dict, List, Set, Any, Optional, Type, Callable, Tuple, Union
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import pkg_resources
import signal

from app.core.event_bus import EventPriority

from .base import Plugin
from .models import PluginMetadata, PluginState, PluginMetrics
from .utils import get_file_hash, load_yaml_config, safe_import_module
from .permissions import PluginPermission

logger = logging.getLogger(__name__)


class PluginManager:
    """插件管理器，负责加载、初始化和管理插件"""

    def __init__(self, 
                 plugin_dir: str = "plugins", 
                 config_dir: str = "config/plugins",
                 cache_dir: str = "cache/plugins"):
        """初始化插件管理器"""
        # 插件目录
        self.plugin_dir = plugin_dir
        self.config_dir = config_dir
        self.cache_dir = cache_dir
        
        # 插件容器
        self.plugins: Dict[str, Plugin] = {}
        self.plugin_modules: Dict[str, Any] = {}
        self.plugin_states: Dict[str, str] = {}
        self.plugin_configs: Dict[str, Dict[str, Any]] = {}
        self.disabled_plugins: Set[str] = set()
        
        # 钩子和事件系统
        self.hooks: Dict[str, List[Callable]] = {}
        self.event_handlers: Dict[str, List[Tuple[str, Callable]]] = {}
        
        # 依赖管理
        self._load_order: List[str] = []
        self._dependency_graph: Dict[str, List[str]] = {}
        self._dependency_count: Dict[str, int] = {}
        self._reverse_deps: Dict[str, Set[str]] = {}
        
        # 文件监控
        self._watch_task: Optional[asyncio.Task] = None
        self._file_hashes: Dict[str, str] = {}
        self._config_mtimes: Dict[str, float] = {}
        
        # 健康监控
        self._health_check_task: Optional[asyncio.Task] = None
        self.plugin_health: Dict[str, Dict[str, Any]] = {}
        
        # 插件性能指标
        self.plugin_stats: Dict[str, Dict[str, Any]] = {}
        
        # 权限管理
        self.plugin_permissions: Dict[str, Set[str]] = {}
        self._permission_log: List[Dict[str, Any]] = []
        
        # 消息系统
        self._message_queues: Dict[str, asyncio.Queue] = {}
        
        # 版本和兼容性
        self._version_requirements: Dict[str, str] = {}
        self._system_info = self._get_system_info()
        
        # 多线程资源
        self._thread_pool = ThreadPoolExecutor(
            max_workers=min(32, (os.cpu_count() or 1) + 4),
            thread_name_prefix="plugin_worker"
        )
        self._lock = asyncio.Lock()
        
        # 缓存
        self._plugin_cache: Dict[str, Dict[str, Any]] = {}
        self._function_cache: Dict[str, Dict[str, Any]] = {}
        self._reload_count: Dict[str, int] = {}
        
        # 错误追踪
        self._error_log: List[Dict[str, Any]] = []
        self._max_errors = 1000
        
        # 确保目录存在
        os.makedirs(plugin_dir, exist_ok=True)
        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(cache_dir, exist_ok=True)
        
        # 初始化标志
        self.initialized = False
        self.shutting_down = False
        
        # 注册信号处理器
        self._setup_signal_handlers()
        
        # 添加核心系统集成点
        self._event_bus = None
        self._message_bus = None
        self._command_dispatcher = None
        self._integration_manager = None

    def _setup_signal_handlers(self):
        """设置信号处理器，用于优雅关闭"""
        def signal_handler(sig, frame):
            logger.info(f"接收到信号 {sig}，准备关闭插件系统")
            if not self.shutting_down:
                self.shutting_down = True
                asyncio.create_task(self.shutdown_plugins())
        
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except (ValueError, AttributeError):
            # 某些环境如Windows中可能不支持特定信号
            logger.warning("无法设置信号处理器")

    def _get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        import platform
        
        return {
            "os": platform.system(),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "time": datetime.now().isoformat()
        }

    async def set_event_bus(self, event_bus):
        """设置事件总线引用"""
        self._event_bus = event_bus
        logger.info("事件总线已与插件管理器集成")

    async def set_message_bus(self, message_bus):
        """设置消息总线引用"""
        self._message_bus = message_bus
        logger.info("消息总线已与插件管理器集成")
        
    async def set_command_dispatcher(self, command_dispatcher):
        """设置命令分发器引用"""
        self._command_dispatcher = command_dispatcher
        logger.info("命令分发器已与插件管理器集成")
        
    async def set_integration_manager(self, integration_manager):
        """设置集成管理器引用"""
        self._integration_manager = integration_manager
        logger.info("集成管理器已与插件管理器集成")

    async def initialize(self):
        """初始化插件管理器"""
        if self.initialized:
            return
            
        logger.info("正在初始化插件管理器...")
        
        # 发布插件系统启动事件
        if self._event_bus:
            await self._event_bus.publish(
                "plugin.system.startup", 
                {
                    "timestamp": time.time(),
                    "plugin_dir": self.plugin_dir,
                    "config_dir": self.config_dir
                }
            )
        
        # 扫描插件目录
        await self._scan_plugins()
        
        # 解析依赖关系和加载顺序
        self._resolve_dependencies()
        
        # 加载插件
        await self.load_plugins()
        
        # 启动文件监控和健康检查
        await self.start_plugin_watcher()
        await self.start_health_monitor()
        
        self.initialized = True
        logger.info(f"插件管理器初始化完成，已加载 {len(self.plugins)} 个插件")
        
        # 发布插件系统就绪事件
        if self._event_bus:
            await self._event_bus.publish(
                "plugin.system.ready", 
                {
                    "timestamp": time.time(),
                    "loaded_plugins": list(self.plugins.keys())
                }
            )

    async def _scan_plugins(self):
        """扫描插件目录，收集所有可用的插件"""
        plugin_files = []
        
        # 获取插件目录中的所有Python文件
        for root, dirs, files in os.walk(self.plugin_dir):
            for file in files:
                if file.endswith('.py') and not file.startswith('__'):
                    rel_path = os.path.relpath(os.path.join(root, file), self.plugin_dir)
                    module_path = os.path.splitext(rel_path)[0].replace(os.sep, '.')
                    plugin_files.append((module_path, os.path.join(root, file)))
        
        # 并行收集插件元数据
        tasks = []
        for module_path, file_path in plugin_files:
            tasks.append(self._collect_plugin_metadata(module_path, file_path))
        
        await asyncio.gather(*tasks)
        logger.info(f"扫描到 {len(self._dependency_graph)} 个插件")

    async def _collect_plugin_metadata(self, module_name: str, file_path: str):
        """收集插件元数据"""
        try:
            # 记录文件哈希值
            self._file_hashes[module_name] = get_file_hash(file_path)
            
            # 尝试导入模块
            module = await self.run_in_thread(safe_import_module, module_name, file_path)
            self.plugin_modules[module_name] = module
            
            # 提取元数据
            metadata = None
            
            # 方法1：查找PLUGIN_METADATA属性
            if hasattr(module, 'PLUGIN_METADATA'):
                metadata = module.PLUGIN_METADATA
            else:
                # 方法2：查找插件类并从类中获取元数据
                plugin_classes = [
                    obj for name, obj in inspect.getmembers(module)
                    if inspect.isclass(obj) and issubclass(obj, Plugin) and obj != Plugin
                ]
                
                if plugin_classes and hasattr(plugin_classes[0], 'metadata'):
                    # 创建类的实例来获取元数据
                    metadata = plugin_classes[0].metadata
            
            if metadata:
                # 存储依赖信息
                self._dependency_graph[module_name] = getattr(metadata, 'dependencies', []) or []
                
                # 记录版本要求
                if hasattr(metadata, 'required_version'):
                    self._version_requirements[module_name] = metadata.required_version
                
                # 记录初始状态
                self.plugin_states[module_name] = PluginState.UNLOADED
                
                logger.info(f"收集到插件元数据: {module_name}")
            else:
                logger.warning(f"未找到插件元数据: {module_name}")
                
        except Exception as e:
            logger.error(f"收集插件 {module_name} 元数据失败: {e}")
            self._log_error(module_name, f"元数据收集失败: {str(e)}")

    def _resolve_dependencies(self):
        """解析插件依赖关系，确定加载顺序"""
        # 初始化依赖计数和反向依赖
        for plugin in self._dependency_graph:
            self._dependency_count[plugin] = len(self._dependency_graph[plugin])
            for dep in self._dependency_graph[plugin]:
                if dep not in self._reverse_deps:
                    self._reverse_deps[dep] = set()
                self._reverse_deps[dep].add(plugin)
        
        # 使用拓扑排序确定加载顺序
        load_order = []
        no_deps = [p for p in self._dependency_graph if self._dependency_count[p] == 0]
        
        while no_deps:
            plugin = no_deps.pop(0)
            load_order.append(plugin)
            
            # 更新依赖此插件的插件
            for dependent in self._reverse_deps.get(plugin, set()):
                self._dependency_count[dependent] -= 1
                if self._dependency_count[dependent] == 0:
                    no_deps.append(dependent)
        
        # 检查是否有循环依赖
        if len(load_order) < len(self._dependency_graph):
            # 找出循环依赖的插件
            cyclic_plugins = [p for p in self._dependency_graph if self._dependency_count[p] > 0]
            logger.error(f"检测到循环依赖: {cyclic_plugins}")
            
            # 将剩余插件按名称排序添加到加载顺序
            remaining = sorted([p for p in self._dependency_graph if p not in load_order])
            load_order.extend(remaining)
        
        self._load_order = load_order
        logger.info(f"插件加载顺序: {load_order}")

    async def load_plugins(self):
        """加载所有插件"""
        logger.info("开始加载插件...")
        
        async with self._lock:
            # 按加载顺序加载插件
            for plugin_name in self._load_order:
                try:
                    # 跳过禁用的插件
                    if plugin_name in self.disabled_plugins:
                        logger.info(f"跳过禁用的插件: {plugin_name}")
                        continue
                    
                    # 检查依赖是否都已加载
                    deps = self._dependency_graph.get(plugin_name, [])
                    missing_deps = [dep for dep in deps if dep not in self.plugins]
                    if missing_deps:
                        logger.error(f"插件 {plugin_name} 缺少依赖: {missing_deps}")
                        self._log_error(plugin_name, f"缺少依赖: {missing_deps}")
                        continue
                    
                    # 加载插件
                    await self._load_plugin(plugin_name)
                    
                except Exception as e:
                    logger.error(f"加载插件 {plugin_name} 失败: {e}\n{traceback.format_exc()}")
                    self._log_error(plugin_name, f"加载失败: {str(e)}")
                    self.plugin_states[plugin_name] = PluginState.ERROR
        
        logger.info(f"插件加载完成，成功加载 {len(self.plugins)} 个插件")

    async def _load_plugin(self, plugin_name: str):
        """加载单个插件"""
        try:
            logger.debug(f"正在加载插件: {plugin_name}")
            
            # 更新状态
            self.plugin_states[plugin_name] = PluginState.LOADING
            
            # 获取模块
            module = self.plugin_modules.get(plugin_name)
            if not module:
                raise ValueError(f"插件模块 {plugin_name} 未找到")
            
            # 查找插件类
            plugin_classes = [
                obj for name, obj in inspect.getmembers(module)
                if inspect.isclass(obj) and issubclass(obj, Plugin) and obj != Plugin
            ]
            
            if not plugin_classes:
                raise ValueError(f"在 {plugin_name} 中未找到插件类")
            
            # 创建插件实例
            plugin_class = plugin_classes[0]
            plugin = plugin_class()
            
            # 加载配置
            config = await self._load_plugin_config(plugin_name)
            if config:
                plugin.config = config
            
            # 设置基本权限
            self.plugin_permissions[plugin_name] = {PluginPermission.FILE_IO}
            plugin._permissions = self.plugin_permissions[plugin_name]
            
            # 设置插件管理器引用
            plugin._plugin_manager = self
            
            # 设置核心系统引用
            plugin._event_bus = self._event_bus
            plugin._message_bus = self._message_bus
            plugin._command_dispatcher = self._command_dispatcher
            
            # 初始化插件
            await plugin.safe_initialize()
            
            # 检查初始化状态
            if plugin.state == PluginState.ERROR:
                raise RuntimeError(f"插件初始化失败: {plugin._last_error}")
            
            # 注册插件
            self.plugins[plugin_name] = plugin
            self.plugin_states[plugin_name] = PluginState.ACTIVE
            
            # 创建消息队列
            self._message_queues[plugin_name] = plugin._message_queue
            
            # 记录统计信息
            self.plugin_stats[plugin_name] = {
                "load_time": time.time(),
                "initialize_time": time.time() - (plugin.start_time or time.time()),
                "error_count": 0,
                "config_version": 1
            }
            
            # 发布插件加载事件
            if self._event_bus:
                await self._event_bus.publish(
                    "plugin.loaded", 
                    {
                        "name": plugin_name,
                        "timestamp": time.time(),
                        "metadata": plugin.metadata.__dict__ if plugin.metadata else {}
                    }
                )
            
            logger.info(f"成功加载插件: {plugin_name}")
            return plugin
            
        except Exception as e:
            logger.error(f"加载插件 {plugin_name} 失败: {e}\n{traceback.format_exc()}")
            self._log_error(plugin_name, f"加载失败: {str(e)}")
            self.plugin_states[plugin_name] = PluginState.ERROR
            
            # 发布插件错误事件
            if self._event_bus:
                await self._event_bus.publish(
                    "plugin.error", 
                    {
                        "name": plugin_name,
                        "timestamp": time.time(),
                        "error": str(e),
                        "traceback": traceback.format_exc()
                    },
                    EventPriority.HIGH
                )
            
            return None
    
    async def _load_plugin_config(self, plugin_name: str) -> Dict[str, Any]:
        """加载插件配置"""
        config_file = os.path.join(self.config_dir, f"{plugin_name}.yaml")
        try:
            if os.path.exists(config_file):
                config = load_yaml_config(config_file)
                self._config_mtimes[plugin_name] = os.path.getmtime(config_file)
                self.plugin_configs[plugin_name] = config
                return config
        except Exception as e:
            logger.error(f"加载插件 {plugin_name} 配置失败: {e}")
            self._log_error(plugin_name, f"配置加载失败: {str(e)}")
        
        # 返回空配置
        return {}
    
    async def _save_plugin_config(self, plugin_name: str, config: Dict[str, Any]) -> bool:
        """保存插件配置"""
        config_file = os.path.join(self.config_dir, f"{plugin_name}.yaml")
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
            self._config_mtimes[plugin_name] = os.path.getmtime(config_file)
            self.plugin_configs[plugin_name] = config
            return True
        except Exception as e:
            logger.error(f"保存插件 {plugin_name} 配置失败: {e}")
            self._log_error(plugin_name, f"配置保存失败: {str(e)}")
            return False
    
    async def reload_plugin(self, plugin_name: str) -> bool:
        """重新加载插件"""
        logger.info(f"正在重新加载插件: {plugin_name}")
        
        async with self._lock:
            try:
                # 检查插件是否存在
                if plugin_name not in self.plugin_modules:
                    logger.error(f"插件 {plugin_name} 不存在，无法重新加载")
                    return False
                
                # 关闭插件
                if plugin_name in self.plugins:
                    old_plugin = self.plugins[plugin_name]
                    await old_plugin.safe_shutdown()
                    del self.plugins[plugin_name]
                
                # 清理缓存
                if plugin_name in self._plugin_cache:
                    del self._plugin_cache[plugin_name]
                if plugin_name in self._function_cache:
                    del self._function_cache[plugin_name]
                
                # 重新加载模块
                module_path = os.path.join(self.plugin_dir, f"{plugin_name.replace('.', os.sep)}.py")
                self.plugin_modules[plugin_name] = await self.run_in_thread(
                    safe_import_module, plugin_name, module_path, True)
                
                # 重新加载插件
                await self._load_plugin(plugin_name)
                
                # 更新重载计数
                self._reload_count[plugin_name] = self._reload_count.get(plugin_name, 0) + 1
                
                logger.info(f"插件 {plugin_name} 重新加载成功")
                return True
                
            except Exception as e:
                logger.error(f"重新加载插件 {plugin_name} 失败: {e}\n{traceback.format_exc()}")
                self._log_error(plugin_name, f"重新加载失败: {str(e)}")
                self.plugin_states[plugin_name] = PluginState.ERROR
                return False
    
    async def enable_plugin(self, plugin_name: str) -> bool:
        """启用插件"""
        if (plugin_name in self.disabled_plugins):
            self.disabled_plugins.remove(plugin_name)
            # 如果插件已加载但被禁用，则重新加载
            if plugin_name in self.plugin_modules and plugin_name not in self.plugins:
                return await self.reload_plugin(plugin_name)
            return True
        return False
    
    async def disable_plugin(self, plugin_name: str) -> bool:
        """禁用插件"""
        if plugin_name not in self.disabled_plugins:
            self.disabled_plugins.add(plugin_name)
            # 如果插件已加载，则关闭它
            if plugin_name in self.plugins:
                await self.plugins[plugin_name].safe_shutdown()
                del self.plugins[plugin_name]
                self.plugin_states[plugin_name] = PluginState.DISABLED
            return True
        return False
    
    async def shutdown_plugins(self):
        """关闭所有插件"""
        logger.info("正在关闭所有插件...")
        self.shutting_down = True
        
        # 发布插件系统关闭事件
        if self._event_bus:
            await self._event_bus.publish(
                "plugin.system.shutdown", 
                {"timestamp": time.time()}
            )
        
        # 按依赖顺序的反向关闭插件
        for plugin_name in reversed(self._load_order):
            if plugin_name in self.plugins:
                try:
                    logger.debug(f"正在关闭插件: {plugin_name}")
                    plugin = self.plugins[plugin_name]
                    await plugin.safe_shutdown()
                    self.plugin_states[plugin_name] = PluginState.UNLOADED
                    logger.debug(f"插件 {plugin_name} 已关闭")
                except Exception as e:
                    logger.error(f"关闭插件 {plugin_name} 失败: {e}")
                    self._log_error(plugin_name, f"关闭失败: {str(e)}")
        
        # 停止任务
        if self._watch_task:
            self._watch_task.cancel()
        if self._health_check_task:
            self._health_check_task.cancel()
        
        # 关闭线程池
        self._thread_pool.shutdown(wait=False)
        
        logger.info("所有插件已关闭")
    
    async def run_in_thread(self, func: Callable, *args, **kwargs):
        """在线程池中运行函数"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._thread_pool,
            lambda: func(*args, **kwargs)
        )
    
    async def start_plugin_watcher(self):
        """启动插件文件监视器"""
        async def watch_plugins():
            while not self.shutting_down:
                try:
                    # 检查配置文件变更
                    for plugin_name in list(self.plugins.keys()):
                        config_file = os.path.join(self.config_dir, f"{plugin_name}.yaml")
                        if os.path.exists(config_file):
                            mtime = os.path.getmtime(config_file)
                            if plugin_name in self._config_mtimes and mtime > self._config_mtimes[plugin_name]:
                                # 配置文件已更新
                                try:
                                    logger.info(f"检测到插件 {plugin_name} 配置变更")
                                    config = load_yaml_config(config_file)
                                    plugin = self.plugins[plugin_name]
                                    result = await plugin.on_config_change(config)
                                    if result:
                                        self._config_mtimes[plugin_name] = mtime
                                        self.plugin_configs[plugin_name] = config
                                        self.plugin_stats[plugin_name]["config_version"] += 1
                                except Exception as e:
                                    logger.error(f"更新插件 {plugin_name} 配置失败: {e}")
                    
                    # 检查插件文件变更
                    for plugin_name, old_hash in self._file_hashes.items():
                        if plugin_name in self.disabled_plugins:
                            continue
                            
                        module_path = os.path.join(self.plugin_dir, f"{plugin_name.replace('.', os.sep)}.py")
                        if os.path.exists(module_path):
                            current_hash = get_file_hash(module_path)
                            if current_hash != old_hash:
                                # 插件文件已更新
                                logger.info(f"检测到插件 {plugin_name} 文件变更，自动重新加载")
                                success = await self.reload_plugin(plugin_name)
                                if success:
                                    self._file_hashes[plugin_name] = current_hash
                
                except Exception as e:
                    logger.error(f"插件监视器错误: {e}")
                
                # 每5秒检查一次
                await asyncio.sleep(5)
        
        self._watch_task = asyncio.create_task(watch_plugins())
        logger.info("插件文件监视器已启动")
    
    async def start_health_monitor(self):
        """启动健康监控"""
        async def health_check_loop():
            while not self.shutting_down:
                try:
                    for plugin_name, plugin in list(self.plugins.items()):
                        try:
                            # 每个插件有5秒超时
                            health_status = await asyncio.wait_for(
                                plugin.check_health(),
                                timeout=5.0
                            )
                            self.plugin_health[plugin_name] = health_status
                            
                            if health_status.get("status") != "healthy":
                                logger.warning(f"插件 {plugin_name} 健康检查异常: {health_status}")
                                
                                # 如果插件状态严重异常，可以考虑重启插件
                                if health_status.get("status") == "critical":
                                    logger.error(f"插件 {plugin_name} 状态严重，尝试重启")
                                    await self.reload_plugin(plugin_name)
                            
                        except asyncio.TimeoutError:
                            logger.warning(f"插件 {plugin_name} 健康检查超时")
                            self.plugin_health[plugin_name] = {"status": "timeout"}
                        except Exception as e:
                            logger.error(f"插件 {plugin_name} 健康检查失败: {e}")
                            self.plugin_health[plugin_name] = {"status": "error", "error": str(e)}
                
                except Exception as e:
                    logger.error(f"健康监控错误: {e}")
                
                # 每60秒进行一次健康检查
                await asyncio.sleep(60)
        
        self._health_check_task = asyncio.create_task(health_check_loop())
        logger.info("插件健康监控已启动")
    
    def _log_error(self, plugin_name: str, error_message: str):
        """记录插件错误"""
        error_entry = {
            "plugin": plugin_name,
            "time": datetime.now().isoformat(),
            "message": error_message,
            "traceback": traceback.format_exc()
        }
        self._error_log.append(error_entry)
        
        # 更新插件统计信息
        if plugin_name in self.plugin_stats:
            self.plugin_stats[plugin_name]["error_count"] = self.plugin_stats[plugin_name].get("error_count", 0) + 1
            self.plugin_stats[plugin_name]["last_error"] = error_entry
        
        # 保持错误日志大小在限制内
        if len(self._error_log) > self._max_errors:
            self._error_log = self._error_log[-self._max_errors:]
    
    async def send_plugin_message(self, sender: str, target: str, message: Any) -> bool:
        """发送插件间消息"""
        try:
            if target in self._message_queues:
                # 构造消息
                msg_obj = {
                    "sender": sender,
                    "content": message,
                    "timestamp": time.time()
                }
                
                # 使用超时避免阻塞
                try:
                    await asyncio.wait_for(
                        self._message_queues[target].put(msg_obj),
                        timeout=1.0
                    )
                    return True
                except asyncio.TimeoutError:
                    logger.warning(f"向插件 {target} 发送消息超时")
                    return False
            else:
                logger.warning(f"目标插件 {target} 不存在或未加载")
                return False
        except Exception as e:
            logger.error(f"发送消息失败: {e}")
            return False
    
    async def broadcast_message(self, sender: str, message: Any, exclude: List[str] = None) -> Dict[str, bool]:
        """广播消息到所有插件"""
        results = {}
        exclude = exclude or []
        
        for plugin_name in self.plugins:
            if plugin_name not in exclude and plugin_name != sender:
                results[plugin_name] = await self.send_plugin_message(sender, plugin_name, message)
        
        return results
    
    def get_plugin_status(self) -> Dict[str, Dict[str, Any]]:
        """获取所有插件的状态"""
        status = {}
        for plugin_name, state in self.plugin_states.items():
            plugin_status = {
                "state": state,
                "enabled": plugin_name not in self.disabled_plugins
            }
            
            if plugin_name in self.plugins:
                plugin = self.plugins[plugin_name]
                plugin_status.update({
                    "metadata": plugin.metadata.__dict__ if plugin.metadata else {},
                    "initialized": plugin._initialized,
                    "error_count": plugin._error_count,
                    "config_version": plugin._config_version,
                    "uptime": time.time() - plugin.start_time if plugin.start_time else 0,
                    "health": self.plugin_health.get(plugin_name, {"status": "unknown"})
                })
            
            if plugin_name in self.plugin_stats:
                plugin_status["stats"] = self.plugin_stats[plugin_name]
            
            status[plugin_name] = plugin_status
        
        return status
    
    def get_plugin_errors(self, plugin_name: str = None) -> List[Dict[str, Any]]:
        """获取插件错误日志"""
        if plugin_name:
            return [error for error in self._error_log if error["plugin"] == plugin_name]
        return self._error_log
    
    def get_plugin_api_schema(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """获取插件API模式"""
        if plugin_name in self.plugins:
            return self.plugins[plugin_name].get_api_schema()
        return None
    
    async def call_plugin_function(self, plugin_name: str, function_name: str, *args, **kwargs) -> Any:
        """调用插件函数"""
        if plugin_name not in self.plugins:
            raise ValueError(f"插件 {plugin_name} 不存在或未加载")
            
        plugin = self.plugins[plugin_name]
        func = plugin.get_function(function_name)
        
        if not func:
            raise ValueError(f"插件 {plugin_name} 中不存在函数 {function_name}")
            
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"调用插件函数 {plugin_name}.{function_name} 失败: {e}")
            self._log_error(plugin_name, f"函数调用失败: {function_name} - {str(e)}")
            raise
    
    async def grant_permission(self, plugin_name: str, permission: str) -> bool:
        """授予插件权限"""
        if plugin_name not in self.plugin_permissions:
            self.plugin_permissions[plugin_name] = set()
        
        self.plugin_permissions[plugin_name].add(permission)
        
        # 如果插件已加载，更新其权限
        if plugin_name in self.plugins:
            self.plugins[plugin_name]._permissions = self.plugin_permissions[plugin_name]
        
        logger.info(f"授予插件 {plugin_name} 权限: {permission}")
        return True
    
    async def revoke_permission(self, plugin_name: str, permission: str) -> bool:
        """撤销插件权限"""
        if plugin_name in self.plugin_permissions:
            self.plugin_permissions[plugin_name].discard(permission)
            
            # 如果插件已加载，更新其权限
            if plugin_name in self.plugins:
                self.plugins[plugin_name]._permissions = self.plugin_permissions[plugin_name]
            
            logger.info(f"撤销插件 {plugin_name} 权限: {permission}")
            return True
        return False
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取插件系统状态"""
        return {
            "initialized": self.initialized,
            "plugin_count": len(self.plugins),
            "total_plugins_found": len(self.plugin_modules),
            "disabled_plugins": len(self.disabled_plugins),
            "error_count": len(self._error_log),
            "system_info": self._system_info,
            "uptime": time.time() - self._system_info.get("start_time", time.time()),
        }
    
    async def handle_topic(self, topic: str, data: Any) -> bool:
        """处理来自消息总线的主题，并转发给相关插件"""
        handled = False
        
        # 查找是否有插件注册了对此主题的处理器
        for plugin_name, plugin in self.plugins.items():
            if hasattr(plugin, "handle_topic"):
                try:
                    plugin_handled = await plugin.handle_topic(topic, data)
                    if plugin_handled:
                        handled = True
                        logger.debug(f"插件 {plugin_name} 处理了主题: {topic}")
                except Exception as e:
                    logger.error(f"插件 {plugin_name} 处理主题 {topic} 时出错: {e}")
                    self._log_error(plugin_name, f"主题处理失败: {topic} - {str(e)}")
        
        return handled