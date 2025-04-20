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


class SSHPlugin(ABC):
    """SSH 插件基类接口
    
    为 SSH 客户端插件定义接口的抽象类。
    所有插件必须继承此类并实现必需的方法。
    
    插件可以：
    - 处理命令执行前后的操作
    - 处理文件传输操作
    - 处理连接和断开连接事件
    - 添加自定义 SSH 操作
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
        return "SSH 插件"

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
            client: SSHClient 实例
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

    async def pre_execute_command(self, command: str, timeout: Optional[int] = None) -> tuple[str, Optional[int]]:
        """执行命令前预处理
        
        Args:
            command: 要执行的命令
            timeout: 命令超时时间（秒）
            
        Returns:
            tuple[str, Optional[int]]: 处理后的命令和超时时间
            若返回 None，则阻止执行
        """
        return command, timeout

    async def post_execute_command(self, command: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """执行命令后处理
        
        Args:
            command: 已执行的命令
            result: 命令执行结果
            
        Returns:
            Dict[str, Any]: 处理后的结果
        """
        return result

    async def pre_upload_file(self, local_path: str, remote_path: str) -> tuple[str, str]:
        """上传文件前预处理
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程文件路径
            
        Returns:
            tuple[str, str]: 处理后的本地路径和远程路径
            若返回 None，则阻止上传
        """
        return local_path, remote_path

    async def post_upload_file(self, local_path: str, remote_path: str, success: bool) -> None:
        """上传文件后处理
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程文件路径
            success: 上传是否成功
        """
        pass

    async def pre_download_file(self, remote_path: str, local_path: str) -> tuple[str, str]:
        """下载文件前预处理
        
        Args:
            remote_path: 远程文件路径
            local_path: 本地文件路径
            
        Returns:
            tuple[str, str]: 处理后的远程路径和本地路径
            若返回 None，则阻止下载
        """
        return remote_path, local_path

    async def post_download_file(self, remote_path: str, local_path: str, success: bool) -> None:
        """下载文件后处理
        
        Args:
            remote_path: 远程文件路径
            local_path: 本地文件路径
            success: 下载是否成功
        """
        pass

    async def on_error(self, operation: str, error: Exception) -> None:
        """处理错误事件
        
        Args:
            operation: 发生错误的操作名称
            error: 发生的异常
        """
        pass


class SSHPluginManager:
    """SSH 插件管理器
    
    管理 SSH 客户端的插件生命周期和执行链。
    """

    def __init__(self):
        self.plugins: Dict[str, SSHPlugin] = {}
        self._sorted_plugins: List[SSHPlugin] = []
        self._initialized = False
        self._client = None

    def register(self, plugin_class: Type[SSHPlugin]) -> bool:
        """注册插件类
        
        Args:
            plugin_class: 要注册的 SSHPlugin 类
            
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
            client: SSHClient 实例
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

    async def run_pre_execute_command(self, command: str, timeout: Optional[int] = None) -> tuple[Optional[str], Optional[int]]:
        """运行命令执行前处理链
        
        Args:
            command: 要执行的命令
            timeout: 命令超时时间（秒）
            
        Returns:
            tuple[Optional[str], Optional[int]]: 处理后的命令和超时时间
            若返回 None，则阻止执行
        """
        current_command = command
        current_timeout = timeout
        
        for plugin in self._sorted_plugins:
            try:
                result = await plugin.pre_execute_command(current_command, current_timeout)
                if result is None:
                    logger.debug(f"命令执行被插件取消: {plugin.name}")
                    return None, None
                current_command, current_timeout = result
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_execute_command 中出错: {e}")
                
        return current_command, current_timeout

    async def run_post_execute_command(self, command: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """运行命令执行后处理链
        
        Args:
            command: 已执行的命令
            result: 命令执行结果
            
        Returns:
            Dict[str, Any]: 处理后的结果
        """
        current_result = result
        
        for plugin in self._sorted_plugins:
            try:
                current_result = await plugin.post_execute_command(command, current_result)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_execute_command 中出错: {e}")
                
        return current_result

    async def run_pre_upload_file(self, local_path: str, remote_path: str) -> tuple[Optional[str], Optional[str]]:
        """运行文件上传前处理链
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程文件路径
            
        Returns:
            tuple[Optional[str], Optional[str]]: 处理后的本地路径和远程路径
            若返回 None，则阻止上传
        """
        current_local = local_path
        current_remote = remote_path
        
        for plugin in self._sorted_plugins:
            try:
                result = await plugin.pre_upload_file(current_local, current_remote)
                if result is None:
                    logger.debug(f"文件上传被插件取消: {plugin.name}")
                    return None, None
                current_local, current_remote = result
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_upload_file 中出错: {e}")
                
        return current_local, current_remote

    async def run_post_upload_file(self, local_path: str, remote_path: str, success: bool) -> None:
        """运行文件上传后处理链
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程文件路径
            success: 上传是否成功
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_upload_file(local_path, remote_path, success)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_upload_file 中出错: {e}")

    async def run_pre_download_file(self, remote_path: str, local_path: str) -> tuple[Optional[str], Optional[str]]:
        """运行文件下载前处理链
        
        Args:
            remote_path: 远程文件路径
            local_path: 本地文件路径
            
        Returns:
            tuple[Optional[str], Optional[str]]: 处理后的远程路径和本地路径
            若返回 None，则阻止下载
        """
        current_remote = remote_path
        current_local = local_path
        
        for plugin in self._sorted_plugins:
            try:
                result = await plugin.pre_download_file(current_remote, current_local)
                if result is None:
                    logger.debug(f"文件下载被插件取消: {plugin.name}")
                    return None, None
                current_remote, current_local = result
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 pre_download_file 中出错: {e}")
                
        return current_remote, current_local

    async def run_post_download_file(self, remote_path: str, local_path: str, success: bool) -> None:
        """运行文件下载后处理链
        
        Args:
            remote_path: 远程文件路径
            local_path: 本地文件路径
            success: 下载是否成功
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.post_download_file(remote_path, local_path, success)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 post_download_file 中出错: {e}")

    async def run_on_error(self, operation: str, error: Exception) -> None:
        """运行错误处理链
        
        Args:
            operation: 发生错误的操作名称
            error: 发生的异常
        """
        for plugin in self._sorted_plugins:
            try:
                await plugin.on_error(operation, error)
            except Exception as e:
                logger.error(f"插件 '{plugin.name}' 的 on_error 中出错: {e}")