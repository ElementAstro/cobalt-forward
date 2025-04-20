from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable, Union, Type
from enum import Enum, auto
import asyncio
from loguru import logger


class PluginPriority(Enum):
    """插件执行优先级级别"""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()


class MQTTPlugin(ABC):
    """MQTT 插件基类接口
    
    为 MQTT 客户端插件定义接口的抽象类。
    所有插件必须继承此类并实现必需的方法。
    
    插件可以：
    - 处理消息的发布和订阅
    - 实现消息转换和处理
    - 添加自定义主题处理
    - 扩展连接和断开连接行为
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """插件唯一标识符
        
        Returns:
            str: 插件名称
        """
        pass

    @property
    def description(self) -> str:
        """插件描述
        
        Returns:
            str: 插件功能描述
        """
        return "MQTT 插件"

    @property
    def version(self) -> str:
        """插件版本
        
        Returns:
            str: 版本字符串（推荐使用语义化版本）
        """
        return "1.0.0"

    @property
    def priority(self) -> PluginPriority:
        """插件执行优先级
        
        Returns:
            PluginPriority: 优先级级别
        """
        return PluginPriority.MEDIUM

    def dependencies(self) -> List[str]:
        """插件依赖列表
        
        Returns:
            List[str]: 所需插件的名称
        """
        return []

    async def initialize(self, client: Any) -> None:
        """初始化插件
        
        在插件加载时调用。用于注册事件处理程序
        和设置所需资源。
        
        Args:
            client: MQTTClient 实例
        """
        self.client = client
        logger.info(f"已初始化插件: {self.name}")

    async def shutdown(self) -> None:
        """清理插件资源
        
        插件卸载时调用。清理事件处理程序
        并释放任何资源。
        """
        logger.info(f"正在关闭插件: {self.name}")

    async def on_connect(self) -> None:
        """处理连接建立事件"""
        pass

    async def on_disconnect(self) -> None:
        """处理连接关闭事件"""
        pass

    async def on_subscribe(self, topic: str, qos: int) -> None:
        """处理订阅主题事件
        
        Args:
            topic: 已订阅的主题
            qos: 服务质量级别
        """
        pass

    async def on_unsubscribe(self, topic: str) -> None:
        """处理取消订阅主题事件
        
        Args:
            topic: 已取消订阅的主题
        """
        pass

    async def pre_publish(self, topic: str, payload: Any, qos: int, retain: bool) -> tuple[str, Any, int, bool]:
        """发布消息前预处理
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
            retain: 是否保留消息
            
        Returns:
            tuple[str, Any, int, bool]: 处理后的主题、内容、QoS 和保留标志
        """
        return topic, payload, qos, retain

    async def post_publish(self, topic: str, payload: Any, qos: int, retain: bool, success: bool) -> None:
        """发布消息后处理
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
            retain: 是否保留消息
            success: 发布是否成功
        """
        pass

    async def pre_message_received(self, topic: str, payload: Any, qos: int) -> tuple[str, Any, int]:
        """消息接收前预处理
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
            
        Returns:
            tuple[str, Any, int]: 处理后的主题、内容和 QoS
            若返回 None，则阻止进一步处理该消息
        """
        return topic, payload, qos

    async def post_message_received(self, topic: str, payload: Any, qos: int) -> None:
        """消息接收后处理
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
        """
        pass

    async def on_error(self, error: Exception) -> None:
        """处理错误事件
        
        Args:
            error: 发生的异常
        """
        pass


class MQTTPluginManager:
    """MQTT 插件管理器
    
    管理 MQTT 客户端的插件生命周期和执行链。
    """

    def __init__(self):
        self.plugins: Dict[str, MQTTPlugin] = {}
        self._sorted_plugins: List[MQTTPlugin] = []
        self._initialized = False
        self._client = None

    def register(self, plugin_class: Type[MQTTPlugin]) -> bool:
        """注册插件类
        
        Args:
            plugin_class: 要注册的 MQTTPlugin 类
            
        Returns:
            bool: 注册是否成功
        """
        try:
            plugin_instance = plugin_class()
            plugin_name = plugin_instance.name
            
            if plugin_name in self.plugins:
                logger.warning(f"插件 '{plugin_name}' 已注册")
                return False
                
            self.plugins[plugin_name] = plugin_instance
            self._sort_plugins()
            
            logger.info(f"已注册插件: {plugin_name}")
            return True
            
        except Exception as e:
            logger.error(f"注册插件失败: {e}")
            return False

    def unregister(self, plugin_name: str) -> bool:
        """注销指定名称的插件
        
        Args:
            plugin_name: 要注销的插件名称
            
        Returns:
            bool: 是否成功注销
        """
        if plugin_name not in self.plugins:
            logger.warning(f"未找到插件 '{plugin_name}'")
            return False
            
        asyncio.create_task(self.plugins[plugin_name].shutdown())
        del self.plugins[plugin_name]
        self._sort_plugins()
        
        logger.info(f"已注销插件: {plugin_name}")
        return True

    def _sort_plugins(self) -> None:
        """按优先级排序插件
        
        确保插件按正确的顺序执行。
        """
        self._sorted_plugins = sorted(
            self.plugins.values(),
            key=lambda p: p.priority.value,
            reverse=True  # 高优先级先执行
        )

    async def initialize(self, client: Any) -> None:
        """初始化所有插件
        
        Args:
            client: MQTTClient 实例
        """
        if self._initialized:
            return
            
        self._client = client
        
        # 检查依赖
        for plugin in self._sorted_plugins:
            for dep in plugin.dependencies():
                if dep not in self.plugins:
                    logger.warning(f"插件 '{plugin.name}' 缺少依赖 '{dep}'")

        # 初始化已验证依赖的插件
        for plugin in self._sorted_plugins:
            try:
                await plugin.initialize(client)
            except Exception as e:
                logger.error(f"初始化插件 '{plugin.name}' 失败: {e}")
                
        self._initialized = True

    async def shutdown(self) -> None:
        """关闭所有插件"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.shutdown()
            except Exception as e:
                logger.error(f"关闭插件 '{plugin.name}' 时出错: {e}")
                
        self._initialized = False
        self._client = None

    async def run_on_connect(self) -> None:
        """运行连接事件处理链"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_connect()
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 on_connect 中出错: {e}")

    async def run_on_disconnect(self) -> None:
        """运行断开连接事件处理链"""
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_disconnect()
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 on_disconnect 中出错: {e}")

    async def run_on_subscribe(self, topic: str, qos: int) -> None:
        """运行订阅事件处理链
        
        Args:
            topic: 已订阅的主题
            qos: 服务质量级别
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_subscribe(topic, qos)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 on_subscribe 中出错: {e}")

    async def run_on_unsubscribe(self, topic: str) -> None:
        """运行取消订阅事件处理链
        
        Args:
            topic: 已取消订阅的主题
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_unsubscribe(topic)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 on_unsubscribe 中出错: {e}")

    async def run_pre_publish(self, topic: str, payload: Any, qos: int, retain: bool) -> tuple[str, Any, int, bool]:
        """运行发布前处理链
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
            retain: 是否保留消息
            
        Returns:
            tuple[str, Any, int, bool]: 处理后的主题、内容、QoS 和保留标志
        """
        current_topic = topic
        current_payload = payload
        current_qos = qos
        current_retain = retain
        
        for plugin in self._sorted_plugins:
            try:
                result = await plugin.pre_publish(current_topic, current_payload, current_qos, current_retain)
                if result is None:
                    logger.debug(f"消息发布被插件取消: {plugin.name}")
                    return None
                current_topic, current_payload, current_qos, current_retain = result
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_publish 中出错: {e}")
                
        return current_topic, current_payload, current_qos, current_retain

    async def run_post_publish(self, topic: str, payload: Any, qos: int, retain: bool, success: bool) -> None:
        """运行发布后处理链
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
            retain: 是否保留消息
            success: 发布是否成功
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_publish(topic, payload, qos, retain, success)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_publish 中出错: {e}")

    async def run_pre_message_received(self, topic: str, payload: Any, qos: int) -> tuple[str, Any, int]:
        """运行消息接收前处理链
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
            
        Returns:
            tuple[str, Any, int]: 处理后的主题、内容和 QoS
            若返回 None，则阻止进一步处理该消息
        """
        current_topic = topic
        current_payload = payload
        current_qos = qos
        
        for plugin in self._sorted_plugins:
            try:
                result = await plugin.pre_message_received(current_topic, current_payload, current_qos)
                if result is None:
                    logger.debug(f"消息接收处理被插件取消: {plugin.name}")
                    return None
                current_topic, current_payload, current_qos = result
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_message_received 中出错: {e}")
                
        return current_topic, current_payload, current_qos

    async def run_post_message_received(self, topic: str, payload: Any, qos: int) -> None:
        """运行消息接收后处理链
        
        Args:
            topic: 主题
            payload: 消息内容
            qos: 服务质量级别
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_message_received(topic, payload, qos)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_message_received 中出错: {e}")

    async def run_on_error(self, error: Exception) -> None:
        """运行错误处理链
        
        Args:
            error: 发生的异常
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_error(error)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 on_error 中出错: {e}")