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
