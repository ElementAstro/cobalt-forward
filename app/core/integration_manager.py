import asyncio
from typing import Dict, List, Any, Optional, Callable, Union, Set
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
import time

from app.core.event_bus import EventBus, Event, EventPriority
from app.core.message_bus import MessageBus, Message, MessageType
from app.core.message_transformer import MessageTransformer, MessageTransform
from app.core.message_processor import MessageProcessor, MessageFilter
from app.core.base import BaseComponent
from app.models.config_model import RuntimeConfig
from app.config.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class ComponentType(Enum):
    """组件类型枚举"""
    EVENT_BUS = auto()
    MESSAGE_BUS = auto()
    TRANSFORMER = auto()
    PROCESSOR = auto()
    CUSTOM = auto()
    CONFIG_MANAGER = auto()  # 添加配置管理器类型


@dataclass
class ComponentRegistration:
    """组件注册信息"""
    component: Any
    component_type: ComponentType
    name: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    dependencies: Set[str] = field(default_factory=set)
    initialized: bool = False


class IntegrationStatus(Enum):
    INACTIVE = auto()     # 未激活
    STARTING = auto()     # 启动中
    RUNNING = auto()      # 运行中
    STOPPING = auto()     # 停止中
    ERROR = auto()        # 错误状态
    RESTARTING = auto()   # 重启中


class IntegrationManager:
    """
    集成管理器，用于协调不同组件的交互

    这个管理器负责:
    1. 组件的注册和生命周期管理
    2. 组件间的通信协调
    3. 消息/事件的路由和转换链管理
    4. 提供统一的监控和指标收集
    5. 集中管理系统配置
    """

    def __init__(self, config_path: Optional[str] = None):
        """初始化集成管理器"""
        self._components: Dict[str, ComponentRegistration] = {}
        self._startup_order: List[str] = []
        self._shutdown_order: List[str] = []
        self._running = False
        self._metrics: Dict[str, Any] = {
            'component_count': 0,
            'messages_routed': 0,
            'events_routed': 0,
            'startup_time': 0,
            'uptime': 0,
            'start_timestamp': 0,
            'errors': 0
        }
        self._message_routes: Dict[str, List[str]] = {}
        self._event_routes: Dict[str, List[str]] = {}
        self._config_path = config_path
        self._config_manager = None
        self._config: Optional[RuntimeConfig] = None

        # 添加标准组件接口
        self._standard_interfaces = {
            'event_bus': ['publish', 'subscribe', 'unsubscribe'],
            'message_bus': ['publish', 'request', 'subscribe', 'unsubscribe'],
            'plugin_manager': ['register_plugin', 'unregister_plugin', 'get_plugin'],
            'command_transmitter': ['send', 'register_handler']
        }

    async def register_component(self, name: str, component: Any,
                                 component_type: ComponentType,
                                 metadata: Dict[str, Any] = None,
                                 dependencies: List[str] = None) -> None:
        """
        注册组件到管理器

        Args:
            name: 组件名称（唯一标识符）
            component: 组件实例
            component_type: 组件类型
            metadata: 组件元数据
            dependencies: 组件依赖项（组件名称列表）
        """
        if name in self._components:
            raise ValueError(f"组件 '{name}' 已经存在")

        self._components[name] = ComponentRegistration(
            component=component,
            component_type=component_type,
            name=name,
            metadata=metadata or {},
            dependencies=set(dependencies or [])
        )

        self._metrics['component_count'] += 1
        logger.info(f"组件 '{name}' ({component_type.name}) 注册成功")

        # 更新启动和关闭顺序
        self._update_startup_shutdown_order()

        # 如果是配置管理器，设置为当前配置管理器
        if component_type == ComponentType.CONFIG_MANAGER:
            self._config_manager = component
            # 注册配置变更监听器
            self._config_manager.register_observer(self._on_config_change)

        # 动态连接组件
        await self._connect_component(name, component, component_type)

    async def _connect_component(self, name: str, component: Any, component_type: ComponentType):
        """自动连接组件与其他已注册组件"""
        if component_type == ComponentType.EVENT_BUS:
            # 事件总线需要注册系统事件
            if hasattr(component, 'register_system_events'):
                await component.register_system_events()
            
            # 连接到消息总线
            message_bus = self._find_component_by_type(ComponentType.MESSAGE_BUS)
            if message_bus:
                # 创建事件-消息桥接
                for event_name in ['system.error', 'system.startup.complete', 'system.shutdown']:
                    component.add_hook('after_publish', 
                                      lambda e: self._forward_event_to_message_bus(e, message_bus)
                                      if e.name in [event_name] else None)
            
        elif component_type == ComponentType.MESSAGE_BUS:
            # 连接到事件总线
            event_bus = self._find_component_by_type(ComponentType.EVENT_BUS)
            if event_bus and hasattr(component, 'set_event_bus'):
                await component.set_event_bus(event_bus)
                
        elif component_type == ComponentType.CUSTOM:
            # 为自定义组件提供核心服务访问
            for service_type, service_component in self._get_core_services().items():
                setter_method = f"set_{service_type}"
                if hasattr(component, setter_method):
                    getattr(component, setter_method)(service_component)

    async def _on_config_change(self, new_config: RuntimeConfig) -> None:
        """
        配置变更回调

        Args:
            new_config: 新的配置对象
        """
        logger.info("配置已更新，正在应用新配置")
        self._config = new_config

        # 发布配置变更事件，让其他组件能够响应
        await self.route_event("config.updated", new_config)

        # 可以在这里添加对特定配置变更的处理逻辑
        # 例如，如果TCP端口变更，可能需要重启TCP服务器组件

    def _update_startup_shutdown_order(self) -> None:
        """更新组件的启动和关闭顺序"""
        # 拓扑排序计算启动顺序（考虑依赖关系）
        visited = set()
        temp_visited = set()
        order = []

        def visit(name):
            if name in temp_visited:
                raise ValueError(f"检测到组件依赖循环，涉及组件: '{name}'")
            if name in visited:
                return

            temp_visited.add(name)

            # 访问所有依赖项
            for dep in self._components[name].dependencies:
                if dep not in self._components:
                    raise ValueError(f"组件 '{name}' 依赖未注册的组件 '{dep}'")
                visit(dep)

            temp_visited.remove(name)
            visited.add(name)
            order.append(name)

        # 对所有组件执行拓扑排序
        for name in self._components:
            if name not in visited:
                visit(name)

        self._startup_order = order
        # 关闭顺序与启动顺序相反
        self._shutdown_order = order[::-1]

    async def start(self) -> None:
        """启动所有组件，按依赖顺序"""
        if self._running:
            logger.warning("集成管理器已经在运行中")
            return

        logger.info("开始启动集成管理器中的组件...")
        start_time = time.time()
        self._metrics['start_timestamp'] = start_time

        # 确保配置管理器首先初始化
        if self._config_path and not self._config_manager:
            await self._init_config_manager()

        # 按启动顺序启动组件
        for name in self._startup_order:
            component_reg = self._components[name]
            component = component_reg.component

            try:
                logger.info(f"正在启动组件: '{name}'")

                # 根据组件类型调用适当的启动方法
                if hasattr(component, 'start'):
                    if asyncio.iscoroutinefunction(component.start):
                        await component.start()
                    else:
                        component.start()

                # 特殊组件的初始化逻辑
                if component_reg.component_type == ComponentType.EVENT_BUS:
                    self._setup_event_bus(component)
                elif component_reg.component_type == ComponentType.MESSAGE_BUS:
                    await self._setup_message_bus(component)
                elif component_reg.component_type == ComponentType.TRANSFORMER:
                    await self._setup_transformer(component)
                elif component_reg.component_type == ComponentType.PROCESSOR:
                    await self._setup_processor(component)
                elif component_reg.component_type == ComponentType.CONFIG_MANAGER:
                    await self._setup_config_manager(component)

                component_reg.initialized = True
                logger.info(f"组件 '{name}' 启动成功")

            except Exception as e:
                logger.error(f"启动组件 '{name}' 失败: {e}")
                self._metrics['errors'] += 1
                # 在生产环境中可能需要决定是继续还是中断启动过程
                raise

        self._running = True
        self._metrics['startup_time'] = time.time() - start_time
        logger.info(f"集成管理器启动完成，用时 {self._metrics['startup_time']:.2f} 秒")

    async def _init_config_manager(self) -> None:
        """初始化配置管理器"""
        logger.info(f"初始化配置管理器，配置路径: {self._config_path}")
        config_manager = ConfigManager(self._config_path)

        # 加载配置
        await config_manager.load_config()

        # 注册配置管理器
        await self.register_component(
            name="config_manager",
            component=config_manager,
            component_type=ComponentType.CONFIG_MANAGER
        )

        # 启动配置热重载
        config_manager.start_hot_reload()

        # 缓存当前配置
        self._config = config_manager.runtime_config

    async def _setup_config_manager(self, config_manager: ConfigManager) -> None:
        """设置配置管理器的集成点"""
        # 确保配置已加载
        try:
            await config_manager.wait_for_config(timeout=5.0)
            self._config = config_manager.runtime_config
            logger.info("配置已成功加载")
        except asyncio.TimeoutError:
            logger.warning("等待配置加载超时，使用默认配置")

    def _setup_event_bus(self, event_bus: EventBus) -> None:
        """设置事件总线的集成点"""
        # 添加事件总线钩子以收集指标
        event_bus.add_hook('after_publish', self._on_event_published)
        event_bus.add_hook('after_process', self._on_event_processed)
        event_bus.add_hook('on_error', self._on_event_error)

    async def _setup_message_bus(self, message_bus: MessageBus) -> None:
        """设置消息总线的集成点"""
        # 将其他组件注册到消息总线
        for name, reg in self._components.items():
            if reg.component_type == ComponentType.TRANSFORMER:
                # 注册转换器到消息总线
                await message_bus.register_transformer(reg.component)

    async def _setup_transformer(self, transformer: MessageTransformer) -> None:
        """设置消息转换器的集成点"""
        # 检查是否有消息总线可以关联
        message_buses = [
            reg.component for reg in self._components.values()
            if reg.component_type == ComponentType.MESSAGE_BUS
        ]

        if message_buses:
            # 将转换器关联到第一个可用的消息总线
            await transformer.set_message_bus(message_buses[0])

    async def _setup_processor(self, processor: MessageProcessor) -> None:
        """设置消息处理器的集成点"""
        # 可以添加自定义的消息处理器集成逻辑
        pass

    async def stop(self) -> None:
        """停止所有组件，按依赖的逆序"""
        if not self._running:
            logger.warning("集成管理器已经停止")
            return

        logger.info("开始停止集成管理器中的组件...")

        # 按关闭顺序停止组件
        for name in self._shutdown_order:
            component_reg = self._components[name]
            component = component_reg.component

            try:
                logger.info(f"正在停止组件: '{name}'")

                # 根据组件类型调用适当的停止方法
                if hasattr(component, 'stop'):
                    if asyncio.iscoroutinefunction(component.stop):
                        await component.stop()
                    else:
                        component.stop()

                component_reg.initialized = False
                logger.info(f"组件 '{name}' 停止成功")

            except Exception as e:
                logger.error(f"停止组件 '{name}' 失败: {e}")
                self._metrics['errors'] += 1
                # 继续停止其他组件

        self._running = False
        self._metrics['uptime'] = time.time(
        ) - self._metrics['start_timestamp']
        logger.info(f"集成管理器停止完成，总运行时间 {self._metrics['uptime']:.2f} 秒")

    def _on_event_published(self, event: Event) -> None:
        """事件发布钩子"""
        self._metrics['events_routed'] += 1

    def _on_event_processed(self, event: Event) -> None:
        """事件处理完成钩子"""
        pass

    def _on_event_error(self, event: Event, error: Exception) -> None:
        """事件错误钩子"""
        self._metrics['errors'] += 1

    async def route_message(self, topic: str, data: Any) -> None:
        """
        路由消息到注册的处理组件

        Args:
            topic: 消息主题
            data: 消息数据
        """
        if not self._running:
            logger.warning("集成管理器未运行，消息未路由")
            return

        self._metrics['messages_routed'] += 1

        # 如果有消息总线，通过它发送
        message_buses = [
            reg.component for reg in self._components.values()
            if reg.component_type == ComponentType.MESSAGE_BUS
        ]

        if message_buses:
            message_bus = message_buses[0]
            await message_bus.publish(topic, data)

        # 检查是否有匹配的消息路由
        if topic in self._message_routes:
            for component_name in self._message_routes[topic]:
                if component_name in self._components:
                    component_reg = self._components[component_name]
                    component = component_reg.component

                    if hasattr(component, 'handle_message'):
                        if asyncio.iscoroutinefunction(component.handle_message):
                            await component.handle_message(topic, data)
                        else:
                            component.handle_message(topic, data)

    async def route_event(self, event_name: str, event_data: Any = None,
                          priority: EventPriority = EventPriority.NORMAL) -> None:
        """
        路由事件到事件总线

        Args:
            event_name: 事件名称
            event_data: 事件数据
            priority: 事件优先级
        """
        if not self._running:
            logger.warning("集成管理器未运行，事件未路由")
            return

        self._metrics['events_routed'] += 1

        # 查找事件总线
        event_buses = [
            reg.component for reg in self._components.values()
            if reg.component_type == ComponentType.EVENT_BUS
        ]

        if event_buses:
            event_bus = event_buses[0]
            await event_bus.publish(event_name, event_data, priority)

    def add_message_route(self, topic_pattern: str, component_name: str) -> None:
        """
        添加消息路由规则

        Args:
            topic_pattern: 主题模式
            component_name: 目标组件名称
        """
        if component_name not in self._components:
            raise ValueError(f"未找到组件: '{component_name}'")

        if topic_pattern not in self._message_routes:
            self._message_routes[topic_pattern] = []

        self._message_routes[topic_pattern].append(component_name)
        logger.debug(f"添加消息路由: {topic_pattern} -> {component_name}")

    def add_event_route(self, event_pattern: str, component_name: str) -> None:
        """
        添加事件路由规则

        Args:
            event_pattern: 事件模式
            component_name: 目标组件名称
        """
        if component_name not in self._components:
            raise ValueError(f"未找到组件: '{component_name}'")

        if event_pattern not in self._event_routes:
            self._event_routes[event_pattern] = []

        self._event_routes[event_pattern].append(component_name)
        logger.debug(f"添加事件路由: {event_pattern} -> {component_name}")

    def get_component(self, name: str) -> Optional[Any]:
        """
        获取注册的组件

        Args:
            name: 组件名称

        Returns:
            组件实例，如果不存在则返回None
        """
        if name not in self._components:
            return None
        return self._components[name].component

    def get_component_by_type(self, component_type: ComponentType) -> List[Any]:
        """
        获取指定类型的所有组件

        Args:
            component_type: 组件类型

        Returns:
            符合类型的组件列表
        """
        return [
            reg.component for reg in self._components.values()
            if reg.component_type == component_type
        ]

    def get_config(self) -> Optional[RuntimeConfig]:
        """
        获取当前系统配置

        Returns:
            当前系统配置对象
        """
        return self._config

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        获取配置值

        Args:
            key: 配置键名
            default: 默认值（如果键不存在）

        Returns:
            配置值
        """
        if not self._config_manager:
            return default

        return self._config_manager.get_config_value(key, default)

    async def update_config(self, config_updates: Dict[str, Any]) -> None:
        """
        更新配置值

        Args:
            config_updates: 要更新的配置键值对字典
        """
        if not self._config_manager:
            logger.warning("没有注册配置管理器，无法更新配置")
            return

        await self._config_manager.update_config(config_updates)

    def get_metrics(self) -> Dict[str, Any]:
        """
        获取集成管理器和所有组件的指标

        Returns:
            指标字典
        """
        metrics = self._metrics.copy()

        # 计算当前运行时间
        if self._running and self._metrics['start_timestamp'] > 0:
            metrics['uptime'] = time.time() - self._metrics['start_timestamp']

        # 收集各组件的指标
        component_metrics = {}
        for name, reg in self._components.items():
            if hasattr(reg.component, 'metrics') or hasattr(reg.component, 'get_metrics'):
                try:
                    if hasattr(reg.component, 'get_metrics'):
                        component_metrics[name] = reg.component.get_metrics()
                    else:
                        component_metrics[name] = reg.component.metrics
                except Exception as e:
                    logger.error(f"从组件 '{name}' 收集指标失败: {e}")
                    component_metrics[name] = {"error": str(e)}

        metrics['components'] = component_metrics
        return metrics

    def register(self, integration: 'Integration') -> None:
        """
        注册集成服务
        
        Args:
            integration: 集成服务实例
        """
        name = integration.name
        
        if name in self._integrations:
            logger.warning(f"集成服务 {name} 已存在，将被替换")
        
        self._integrations[name] = integration
        self._dependencies[name] = set()
        
        if name not in self._dependents:
            self._dependents[name] = set()
        
        # 添加依赖关系
        for dep in integration.dependencies:
            self._dependencies[name].add(dep)
            
            if dep not in self._dependents:
                self._dependents[dep] = set()
            
            self._dependents[dep].add(name)
        
        # 注册到服务注册表
        self._service_registry[name] = {
            'name': name,
            'type': integration.__class__.__name__,
            'status': IntegrationStatus.INACTIVE.name,
            'metadata': integration.metadata,
            'dependencies': list(integration.dependencies),
            'last_updated': time.time()
        }
        
        logger.info(f"注册集成服务: {name}, 类型: {integration.__class__.__name__}")
    
    def unregister(self, name: str) -> bool:
        """
        注销集成服务
        
        Args:
            name: 服务名称
            
        Returns:
            是否成功注销
        """
        if name not in self._integrations:
            logger.warning(f"集成服务 {name} 不存在，无法注销")
            return False
        
        integration = self._integrations[name]
        
        # 检查是否有依赖于此服务的其他服务
        if name in self._dependents and self._dependents[name]:
            dependent_services = ", ".join(self._dependents[name])
            logger.error(f"无法注销服务 {name}，以下服务依赖于它: {dependent_services}")
            return False
        
        # 如果服务正在运行，先停止
        if integration.status == IntegrationStatus.RUNNING:
            # 异步方法不能在同步方法中直接调用，因此记录警告
            logger.warning(f"集成服务 {name} 仍在运行，建议先停止再注销")
        
        # 移除依赖关系
        for dep in self._dependencies[name]:
            self._dependents[dep].remove(name)
        
        # 移除健康检查任务
        if name in self._health_check_tasks and not self._health_check_tasks[name].done():
            self._health_check_tasks[name].cancel()
        
        # 从集合中移除
        del self._integrations[name]
        del self._dependencies[name]
        if name in self._dependents:
            del self._dependents[name]
        if name in self._service_registry:
            del self._service_registry[name]
        if name in self._status_change_callbacks:
            del self._status_change_callbacks[name]
        
        logger.info(f"注销集成服务: {name}")
        return True
    
    def get(self, name: str) -> Optional['Integration']:
        """
        获取集成服务实例
        
        Args:
            name: 服务名称
            
        Returns:
            集成服务实例
        """
        return self._integrations.get(name)
    
    def get_all(self) -> Dict[str, 'Integration']:
        """获取所有集成服务"""
        return self._integrations.copy()
    
    def get_services(self) -> Dict[str, Dict[str, Any]]:
        """获取服务注册表"""
        return self._service_registry.copy()
    
    def get_service_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        获取服务信息
        
        Args:
            name: 服务名称
            
        Returns:
            服务信息字典
        """
        return self._service_registry.get(name)
    
    async def start_service(self, name: str) -> bool:
        """
        启动指定服务
        
        Args:
            name: 服务名称
            
        Returns:
            是否成功启动
        
        Raises:
            ServiceNotFoundError: 服务不存在
            IntegrationError: 启动失败
        """
        if name not in self._integrations:
            logger.error(f"集成服务 {name} 不存在，无法启动")
            raise ServiceNotFoundError(f"服务 {name} 不存在")
        
        integration = self._integrations[name]
        
        # 已经在运行，直接返回成功
        if integration.status == IntegrationStatus.RUNNING:
            logger.debug(f"集成服务 {name} 已经在运行中")
            return True
        
        # 如果正在启动或停止中，等待完成
        if integration.status in (IntegrationStatus.STARTING, IntegrationStatus.STOPPING):
            logger.debug(f"集成服务 {name} 正在 {integration.status.name}，等待完成")
            await self._wait_for_status_change(name)
        
        # 如果在重启中，等待完成
        if integration.status == IntegrationStatus.RESTARTING:
            logger.debug(f"集成服务 {name} 正在重启中，等待完成")
            await self._wait_for_status_change(name)
            return integration.status == IntegrationStatus.RUNNING
        
        # 更新状态
        await self._update_service_status(name, IntegrationStatus.STARTING)
        
        try:
            # 检查依赖
            for dep in integration.dependencies:
                if dep not in self._integrations:
                    logger.error(f"集成服务 {name} 依赖的服务 {dep} 不存在")
                    await self._update_service_status(name, IntegrationStatus.ERROR)
                    raise IntegrationError(f"依赖服务 {dep} 不存在")
                
                # 如果依赖服务未运行，先启动依赖
                dep_service = self._integrations[dep]
                if dep_service.status != IntegrationStatus.RUNNING:
                    logger.info(f"启动依赖服务 {dep}")
                    success = await self.start_service(dep)
                    if not success:
                        logger.error(f"启动依赖服务 {dep} 失败")
                        await self._update_service_status(name, IntegrationStatus.ERROR)
                        raise IntegrationError(f"启动依赖服务 {dep} 失败")
            
            # 启动服务
            logger.info(f"正在启动集成服务 {name}")
            await integration.start()
            
            # 更新状态
            await self._update_service_status(name, IntegrationStatus.RUNNING)
            
            # 启动健康检查
            if integration.health_check_interval > 0:
                self._start_health_check(name)
            
            logger.info(f"集成服务 {name} 启动成功")
            return True
            
        except Exception as e:
            logger.error(f"启动集成服务 {name} 失败: {e}")
            await self._update_service_status(name, IntegrationStatus.ERROR)
            raise IntegrationError(f"启动服务 {name} 失败: {e}") from e
    
    async def stop_service(self, name: str, force: bool = False) -> bool:
        """
        停止指定服务
        
        Args:
            name: 服务名称
            force: 是否强制停止，如果为True，会同时停止依赖于此服务的其他服务
            
        Returns:
            是否成功停止
            
        Raises:
            ServiceNotFoundError: 服务不存在
            IntegrationError: 停止失败
        """
        if name not in self._integrations:
            logger.error(f"集成服务 {name} 不存在，无法停止")
            raise ServiceNotFoundError(f"服务 {name} 不存在")
        
        integration = self._integrations[name]
        
        # 已经停止，直接返回成功
        if integration.status == IntegrationStatus.INACTIVE:
            logger.debug(f"集成服务 {name} 已经处于停止状态")
            return True
        
        # 如果正在启动或停止中，等待完成
        if integration.status in (IntegrationStatus.STARTING, IntegrationStatus.STOPPING):
            logger.debug(f"集成服务 {name} 正在 {integration.status.name}，等待完成")
            await self._wait_for_status_change(name)
        
        # 如果在重启中，等待完成
        if integration.status == IntegrationStatus.RESTARTING:
            logger.debug(f"集成服务 {name} 正在重启中，等待完成")
            await self._wait_for_status_change(name)
        
        # 检查是否有依赖于此服务的其他服务
        dependents = self._dependents.get(name, set())
        if dependents and not force:
            dependent_services = ", ".join(dependents)
            logger.error(f"无法停止服务 {name}，以下服务依赖于它: {dependent_services}")
            raise IntegrationError(f"无法停止服务，存在依赖: {dependent_services}")
        
        # 如果强制停止，先停止所有依赖于此服务的服务
        if force and dependents:
            for dep_service in dependents:
                logger.info(f"停止依赖于 {name} 的服务: {dep_service}")
                try:
                    await self.stop_service(dep_service, force=True)
                except Exception as e:
                    logger.error(f"停止依赖服务 {dep_service} 失败: {e}")
                    # 继续尝试停止其他服务
        
        # 更新状态
        await self._update_service_status(name, IntegrationStatus.STOPPING)
        
        # 停止健康检查
        if name in self._health_check_tasks and not self._health_check_tasks[name].done():
            self._health_check_tasks[name].cancel()
            try:
                await self._health_check_tasks[name]
            except asyncio.CancelledError:
                pass
        
        try:
            # 停止服务
            logger.info(f"正在停止集成服务 {name}")
            await integration.stop()
            
            # 更新状态
            await self._update_service_status(name, IntegrationStatus.INACTIVE)
            
            logger.info(f"集成服务 {name} 停止成功")
            return True
            
        except Exception as e:
            logger.error(f"停止集成服务 {name} 失败: {e}")
            await self._update_service_status(name, IntegrationStatus.ERROR)
            raise IntegrationError(f"停止服务 {name} 失败: {e}") from e
    
    async def restart_service(self, name: str) -> bool:
        """
        重启指定服务
        
        Args:
            name: 服务名称
            
        Returns:
            是否成功重启
            
        Raises:
            ServiceNotFoundError: 服务不存在
            IntegrationError: 重启失败
        """
        if name not in self._integrations:
            logger.error(f"集成服务 {name} 不存在，无法重启")
            raise ServiceNotFoundError(f"服务 {name} 不存在")
        
        integration = self._integrations[name]
        
        # 如果正在启动或停止中，等待完成
        if integration.status in (IntegrationStatus.STARTING, IntegrationStatus.STOPPING):
            logger.debug(f"集成服务 {name} 正在 {integration.status.name}，等待完成")
            await self._wait_for_status_change(name)
        
        # 如果已经在重启中，等待完成
        if integration.status == IntegrationStatus.RESTARTING:
            logger.debug(f"集成服务 {name} 已经在重启中，等待完成")
            await self._wait_for_status_change(name)
            return integration.status == IntegrationStatus.RUNNING
        
        # 更新状态
        await self._update_service_status(name, IntegrationStatus.RESTARTING)
        
        try:
            logger.info(f"正在重启集成服务 {name}")
            
            # 如果服务正在运行，先停止
            if integration.status == IntegrationStatus.RUNNING:
                await integration.stop()
            
            # 启动服务
            await integration.start()
            
            # 更新状态
            await self._update_service_status(name, IntegrationStatus.RUNNING)
            
            # 重启健康检查
            if integration.health_check_interval > 0:
                if name in self._health_check_tasks and not self._health_check_tasks[name].done():
                    self._health_check_tasks[name].cancel()
                    try:
                        await self._health_check_tasks[name]
                    except asyncio.CancelledError:
                        pass
                
                self._start_health_check(name)
            
            logger.info(f"集成服务 {name} 重启成功")
            return True
            
        except Exception as e:
            logger.error(f"重启集成服务 {name} 失败: {e}")
            await self._update_service_status(name, IntegrationStatus.ERROR)
            raise IntegrationError(f"重启服务 {name} 失败: {e}") from e
    
    async def start_all(self, ignore_errors: bool = False) -> Dict[str, bool]:
        """
        启动所有服务
        
        Args:
            ignore_errors: 是否忽略单个服务启动失败的错误
            
        Returns:
            服务名称 -> 是否成功启动 的字典
        """
        # 按依赖关系排序，确保依赖项先启动
        ordered_services = self._order_services_by_dependencies()
        
        results = {}
        for service_name in ordered_services:
            try:
                success = await self.start_service(service_name)
                results[service_name] = success
                
                if not success and not ignore_errors:
                    logger.error(f"启动服务 {service_name} 失败，中止启动过程")
                    break
                    
            except Exception as e:
                logger.error(f"启动服务 {service_name} 异常: {e}")
                results[service_name] = False
                
                if not ignore_errors:
                    break
        
        return results
    
    async def stop_all(self, ignore_errors: bool = False) -> Dict[str, bool]:
        """
        停止所有服务
        
        Args:
            ignore_errors: 是否忽略单个服务停止失败的错误
            
        Returns:
            服务名称 -> 是否成功停止 的字典
        """
        # 按依赖关系逆序，确保依赖服务最后停止
        ordered_services = self._order_services_by_dependencies()
        ordered_services.reverse()
        
        results = {}
        for service_name in ordered_services:
            try:
                success = await self.stop_service(service_name, force=True)
                results[service_name] = success
                
                if not success and not ignore_errors:
                    logger.error(f"停止服务 {service_name} 失败，中止停止过程")
                    break
                    
            except Exception as e:
                logger.error(f"停止服务 {service_name} 异常: {e}")
                results[service_name] = False
                
                if not ignore_errors:
                    break
        
        return results
    
    async def check_health(self, name: str) -> bool:
        """
        检查服务健康状态
        
        Args:
            name: 服务名称
            
        Returns:
            是否健康
            
        Raises:
            ServiceNotFoundError: 服务不存在
        """
        if name not in self._integrations:
            logger.error(f"集成服务 {name} 不存在，无法检查健康状态")
            raise ServiceNotFoundError(f"服务 {name} 不存在")
        
        integration = self._integrations[name]
        
        try:
            # 如果服务不在运行状态，直接返回False
            if integration.status != IntegrationStatus.RUNNING:
                return False
            
            # 执行健康检查
            healthy = await integration.health_check()
            
            # 更新服务注册表
            self._service_registry[name]['last_health_check'] = time.time()
            self._service_registry[name]['healthy'] = healthy
            
            return healthy
            
        except Exception as e:
            logger.error(f"服务 {name} 健康检查异常: {e}")
            
            # 更新服务注册表
            self._service_registry[name]['last_health_check'] = time.time()
            self._service_registry[name]['healthy'] = False
            self._service_registry[name]['last_error'] = str(e)
            
            return False
    
    async def check_all_health(self) -> Dict[str, bool]:
        """
        检查所有服务的健康状态
        
        Returns:
            服务名称 -> 是否健康 的字典
        """
        results = {}
        
        for name in self._integrations:
            try:
                healthy = await self.check_health(name)
                results[name] = healthy
            except Exception as e:
                logger.error(f"检查服务 {name} 健康状态异常: {e}")
                results[name] = False
        
        return results
    
    def register_status_change_callback(self, service_name: str, callback: Callable) -> None:
        """
        注册状态变化回调函数
        
        Args:
            service_name: 服务名称
            callback: 回调函数，接收服务名称和新状态作为参数
        """
        if service_name not in self._status_change_callbacks:
            self._status_change_callbacks[service_name] = []
        
        self._status_change_callbacks[service_name].append(callback)
        logger.debug(f"为服务 {service_name} 注册状态变化回调函数")
    
    async def _update_service_status(self, name: str, status: IntegrationStatus) -> None:
        """
        更新服务状态并触发回调
        
        Args:
            name: 服务名称
            status: 新状态
        """
        if name not in self._integrations:
            logger.error(f"集成服务 {name} 不存在，无法更新状态")
            return
        
        integration = self._integrations[name]
        old_status = integration.status
        
        # 更新状态
        integration.status = status
        
        # 更新服务注册表
        self._service_registry[name]['status'] = status.name
        self._service_registry[name]['last_updated'] = time.time()
        
        logger.debug(f"更新服务 {name} 状态: {old_status.name} -> {status.name}")
        
        # 触发回调
        if name in self._status_change_callbacks:
            for callback in self._status_change_callbacks[name]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(name, status)
                    else:
                        callback(name, status)
                except Exception as e:
                    logger.error(f"执行服务 {name} 状态变化回调函数异常: {e}")
    
    async def _wait_for_status_change(self, name: str, timeout: float = 30.0) -> None:
        """
        等待服务状态变化
        
        Args:
            name: 服务名称
            timeout: 超时时间（秒）
            
        Raises:
            asyncio.TimeoutError: 等待超时
        """
        if name not in self._integrations:
            logger.error(f"集成服务 {name} 不存在，无法等待状态变化")
            return
        
        integration = self._integrations[name]
        current_status = integration.status
        
        # 创建一个事件来等待状态变化
        status_changed = asyncio.Event()
        
        # 定义回调函数
        async def on_status_change(service_name: str, new_status: IntegrationStatus) -> None:
            if service_name == name and new_status != current_status:
                status_changed.set()
        
        # 注册回调
        self.register_status_change_callback(name, on_status_change)
        
        try:
            # 等待状态变化或超时
            await asyncio.wait_for(status_changed.wait(), timeout)
        except asyncio.TimeoutError:
            logger.warning(f"等待服务 {name} 状态变化超时")
            raise
        finally:
            # 移除回调
            if name in self._status_change_callbacks:
                self._status_change_callbacks[name].remove(on_status_change)
    
    def _start_health_check(self, name: str) -> None:
        """
        启动服务健康检查任务
        
        Args:
            name: 服务名称
        """
        if name not in self._integrations:
            logger.error(f"集成服务 {name} 不存在，无法启动健康检查")
            return
        
        integration = self._integrations[name]
        
        # 如果已经有健康检查任务在运行，先取消
        if name in self._health_check_tasks and not self._health_check_tasks[name].done():
            self._health_check_tasks[name].cancel()
        
        # 创建健康检查任务
        self._health_check_tasks[name] = asyncio.create_task(
            self._health_check_loop(name, integration.health_check_interval)
        )
        logger.debug(f"启动服务 {name} 健康检查任务，间隔: {integration.health_check_interval}秒")
    
    async def _health_check_loop(self, name: str, interval: float) -> None:
        """
        健康检查循环
        
        Args:
            name: 服务名称
            interval: 检查间隔（秒）
        """
        logger.debug(f"服务 {name} 健康检查循环开始，间隔: {interval}秒")
        
        try:
            while True:
                # 执行健康检查
                try:
                    healthy = await self.check_health(name)
                    
                    # 如果服务不健康且状态为RUNNING，将状态设为ERROR
                    if not healthy and self._integrations[name].status == IntegrationStatus.RUNNING:
                        logger.warning(f"服务 {name} 健康检查失败，设置状态为ERROR")
                        await self._update_service_status(name, IntegrationStatus.ERROR)
                    
                    # 如果服务健康且状态为ERROR，将状态恢复为RUNNING
                    elif healthy and self._integrations[name].status == IntegrationStatus.ERROR:
                        logger.info(f"服务 {name} 恢复健康，设置状态为RUNNING")
                        await self._update_service_status(name, IntegrationStatus.RUNNING)
                    
                except Exception as e:
                    logger.error(f"执行服务 {name} 健康检查异常: {e}")
                
                # 等待下一次检查
                await asyncio.sleep(interval)
                
        except asyncio.CancelledError:
            logger.debug(f"服务 {name} 健康检查任务被取消")
            raise
        except Exception as e:
            logger.error(f"服务 {name} 健康检查循环异常: {e}")
    
    def _order_services_by_dependencies(self) -> List[str]:
        """
        按依赖关系对服务进行排序
        
        Returns:
            排序后的服务名称列表，依赖项在前
        """
        # 使用拓扑排序
        result = []
        visited = set()
        temp_mark = set()
        
        def visit(node: str) -> None:
            # 检测循环依赖
            if node in temp_mark:
                # 检测到循环依赖，但不中断排序
                logger.warning(f"检测到循环依赖: {node}")
                return
            
            if node not in visited:
                temp_mark.add(node)
                
                # 访问所有依赖
                for dep in self._dependencies.get(node, set()):
                    if dep in self._integrations:  # 只考虑已注册的服务
                        visit(dep)
                
                temp_mark.remove(node)
                visited.add(node)
                result.append(node)
        
        # 对所有服务进行排序
        for service in list(self._integrations.keys()):
            if service not in visited:
                visit(service)
        
        return result
    
    def import_service(self, module_path: str, class_name: str, **kwargs) -> 'Integration':
        """
        从模块导入并实例化服务
        
        Args:
            module_path: 模块路径
            class_name: 类名
            **kwargs: 传递给构造函数的参数
            
        Returns:
            实例化的服务
            
        Raises:
            ImportError: 导入失败
            AttributeError: 类不存在
            Exception: 实例化失败
        """
        try:
            # 导入模块
            module = importlib.import_module(module_path)
            
            # 获取类
            service_class = getattr(module, class_name)
            
            # 实例化
            service = service_class(**kwargs)
            
            # 注册
            self.register(service)
            
            return service
            
        except ImportError as e:
            logger.error(f"导入模块 {module_path} 失败: {e}")
            raise
        except AttributeError as e:
            logger.error(f"类 {class_name} 在模块 {module_path} 中不存在: {e}")
            raise
        except Exception as e:
            logger.error(f"实例化服务 {class_name} 失败: {e}")
            raise


class Integration:
    """
    集成服务基类
    
    为各种集成服务提供统一的接口和生命周期管理
    子类必须实现start和stop方法
    """
    
    def __init__(self, name: str, dependencies: List[str] = None,
                 health_check_interval: float = 60.0, metadata: Dict[str, Any] = None):
        """
        初始化集成服务
        
        Args:
            name: 服务名称
            dependencies: 依赖的其他服务名称列表
            health_check_interval: 健康检查间隔（秒），0表示不进行健康检查
            metadata: 服务元数据
        """
        self.name = name
        self.dependencies = dependencies or []
        self.health_check_interval = health_check_interval
        self.metadata = metadata or {}
        self.status = IntegrationStatus.INACTIVE
        logger.debug(f"初始化集成服务: {name}")
    
    async def start(self) -> None:
        """
        启动服务
        
        Raises:
            NotImplementedError: 子类必须实现此方法
        """
        raise NotImplementedError("子类必须实现start方法")
    
    async def stop(self) -> None:
        """
        停止服务
        
        Raises:
            NotImplementedError: 子类必须实现此方法
        """
        raise NotImplementedError("子类必须实现stop方法")
    
    async def health_check(self) -> bool:
        """
        健康检查
        
        Returns:
            服务是否健康
        """
        # 默认实现，子类可以重写此方法提供实际的健康检查逻辑
        return self.status == IntegrationStatus.RUNNING
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        获取服务指标
        
        Returns:
            服务指标字典
        """
        # 默认实现返回基本信息，子类可以重写此方法提供更详细的指标
        return {
            'name': self.name,
            'status': self.status.name,
            'uptime': self.get_uptime(),
        }
    
    def get_uptime(self) -> float:
        """
        获取服务运行时间（秒）
        
        Returns:
            运行时间，如果服务未运行则返回0
        """
        # 默认实现，子类可以重写此方法提供实际的运行时间计算
        return 0.0  # 默认实现不跟踪运行时间
