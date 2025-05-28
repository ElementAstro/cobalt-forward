from abc import ABC, abstractmethod
from typing import Dict, Any, List, Type, Optional  # Removed Callable, Union
from enum import Enum, auto
import asyncio
from loguru import logger


class PluginPriority(Enum):
    """插件执行优先级级别"""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()


class TCPPlugin(ABC):
    """TCP 插件基类接口

    为 TCP 客户端插件定义接口的抽象类。
    所有插件必须继承此类并实现必需的方法。

    插件可以：
    - 处理数据的发送和接收
    - 实现数据包转换和解析
    - 处理连接和断开连接事件
    - 添加自定义协议实现
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
        return "TCP 插件"

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
            client: TCPClient 实例
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

    async def pre_send(self, data: bytes) -> Optional[bytes]:
        """发送数据前预处理

        Args:
            data: 要发送的数据

        Returns:
            bytes: 处理后的数据
            若返回 None，则阻止发送
        """
        return data

    async def post_send(self, _data: bytes, _success: bool) -> None:
        """发送数据后处理

        Args:
            _data: 已发送的数据
            _success: 发送是否成功
        """
        pass

    async def pre_receive(self, data: bytes) -> Optional[bytes]:
        """接收数据前预处理

        Args:
            data: 收到的原始数据

        Returns:
            bytes: 处理后的数据
            若返回 None，则阻止进一步处理
        """
        return data

    async def post_receive(self, _data: bytes) -> None:
        """接收数据后处理

        Args:
            _data: 处理后的接收数据
        """
        pass

    async def pre_heartbeat(self) -> bool:
        """发送心跳前处理

        Returns:
            bool: 是否允许发送心跳
        """
        return True

    async def post_heartbeat(self, _success: bool) -> None:
        """发送心跳后处理

        Args:
            _success: 心跳发送是否成功
        """
        pass

    async def on_error(self, _error: Exception) -> None:
        """处理错误事件

        Args:
            _error: 发生的异常
        """
        pass

    async def on_idle(self, _idle_time: float) -> None:
        """处理连接空闲事件

        Args:
            _idle_time: 连接空闲时间（秒）
        """
        pass


class TCPPluginManager:
    """TCP 插件管理器

    管理 TCP 客户端的插件生命周期和执行链。
    """

    def __init__(self):
        self.plugins: Dict[str, TCPPlugin] = {}
        self._sorted_plugins: List[TCPPlugin] = []
        self._initialized = False
        self._client = None

    def register(self, plugin_class: Type[TCPPlugin]) -> bool:
        """注册插件类

        Args:
            plugin_class: 要注册的 TCPPlugin 类

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
            client: TCPClient 实例
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

    async def run_pre_send(self, data: bytes) -> Optional[bytes]:
        """运行发送前处理链

        Args:
            data: 要发送的数据

        Returns:
            bytes: 处理后的数据
            若返回 None，则阻止发送
        """
        current_data: Optional[bytes] = data

        for plugin in self._sorted_plugins:
            try:
                # type: ignore
                current_data = await plugin.pre_send(current_data)
                if current_data is None:
                    logger.debug(f"数据发送被插件取消: {plugin.name}")
                    return None
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_send 中出错: {e}")

        return current_data

    async def run_post_send(self, data: bytes, success: bool) -> None:
        """运行发送后处理链

        Args:
            data: 已发送的数据
            success: 发送是否成功
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_send(data, success)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_send 中出错: {e}")

    async def run_pre_receive(self, data: bytes) -> Optional[bytes]:
        """运行接收前处理链

        Args:
            data: 收到的原始数据

        Returns:
            bytes: 处理后的数据
            若返回 None，则阻止进一步处理
        """
        current_data: Optional[bytes] = data

        for plugin in self._sorted_plugins:
            try:
                # type: ignore
                current_data = await plugin.pre_receive(current_data)
                if current_data is None:
                    logger.debug(f"数据接收处理被插件取消: {plugin.name}")
                    return None
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_receive 中出错: {e}")

        return current_data

    async def run_post_receive(self, data: bytes) -> None:
        """运行接收后处理链

        Args:
            data: 处理后的接收数据
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_receive(data)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_receive 中出错: {e}")

    async def run_pre_heartbeat(self) -> bool:
        """运行心跳前处理链

        Returns:
            bool: 是否允许发送心跳
        """
        allow_heartbeat = True

        for plugin in self._sorted_plugins:
            try:
                result = await plugin.pre_heartbeat()
                if not result:
                    logger.debug(f"心跳发送被插件取消: {plugin.name}")
                    allow_heartbeat = False
                    break
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_heartbeat 中出错: {e}")

        return allow_heartbeat

    async def run_post_heartbeat(self, success: bool) -> None:
        """运行心跳后处理链

        Args:
            success: 心跳发送是否成功
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_heartbeat(success)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_heartbeat 中出错: {e}")

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

    async def run_on_idle(self, idle_time: float) -> None:
        """运行连接空闲事件处理链

        Args:
            idle_time: 连接空闲时间（秒）
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_idle(idle_time)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 on_idle 中出错: {e}")
