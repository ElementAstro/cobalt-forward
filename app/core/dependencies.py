from typing import Optional, Callable, Any
from fastapi import Depends, HTTPException, Request, status

from app.core.event_bus import EventBus
from app.core.message_bus import MessageBus
from app.core.command_dispatcher import CommandDispatcher, CommandTransmitter
from app.plugin.plugin_manager import PluginManager
from app.core.integration_manager import IntegrationManager
from app.config.config_manager import ConfigManager


# FastAPI依赖函数，用于获取各种核心组件

async def get_config_manager(request: Request) -> ConfigManager:
    """获取配置管理器实例"""
    return request.app.state.config_manager


async def get_integration_manager(request: Request) -> IntegrationManager:
    """获取集成管理器"""
    if not hasattr(request.app.state, "integration_manager"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="系统未初始化，集成管理器不可用"
        )
    return request.app.state.integration_manager


async def get_event_bus(
    integration_manager: IntegrationManager = Depends(get_integration_manager)
) -> EventBus:
    """获取事件总线"""
    event_bus = integration_manager.get_component("event_bus")
    if not event_bus:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="事件总线不可用"
        )
    return event_bus


async def get_message_bus(
    integration_manager: IntegrationManager = Depends(get_integration_manager)
) -> MessageBus:
    """获取消息总线"""
    message_bus = integration_manager.get_component("message_bus")
    if not message_bus:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="消息总线不可用"
        )
    return message_bus


async def get_command_dispatcher(
    integration_manager: IntegrationManager = Depends(get_integration_manager)
) -> CommandDispatcher:
    """获取命令分发器"""
    dispatcher = integration_manager.get_component("command_dispatcher")
    if not dispatcher:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="命令分发器不可用"
        )
    return dispatcher


async def get_command_transmitter(
    integration_manager: IntegrationManager = Depends(get_integration_manager)
) -> CommandTransmitter:
    """获取命令发送器"""
    transmitter = integration_manager.get_component("command_transmitter")
    if not transmitter:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="命令发送器不可用"
        )
    return transmitter


async def get_plugin_manager(
    integration_manager: IntegrationManager = Depends(get_integration_manager)
) -> PluginManager:
    """获取插件管理器"""
    plugin_manager = integration_manager.get_component("plugin_manager")
    if not plugin_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="插件管理器不可用"
        )
    return plugin_manager


class RequiresPlugin:
    """需要特定插件的依赖"""
    def __init__(self, plugin_name: str):
        self.plugin_name = plugin_name
        
    async def __call__(
        self, 
        plugin_manager: PluginManager = Depends(get_plugin_manager)
    ) -> Any:
        """检查插件是否可用"""
        if self.plugin_name not in plugin_manager.plugins:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"所需插件 {self.plugin_name} 不可用"
            )
        return plugin_manager.plugins[self.plugin_name]


def requires_permission(permission: str) -> Callable:
    """需要特定权限的依赖装饰器"""
    async def dependency(request: Request):
        # 这里可以实现权限检查逻辑
        # 例如从请求头部获取token，验证用户权限等
        if not hasattr(request, "user") or permission not in getattr(request, "user").permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"需要权限: {permission}"
            )
        return True
    
    return dependency


def optional_component(dependency_func):
    """将组件依赖转换为可选依赖"""
    async def optional_dependency(request: Request):
        try:
            return await dependency_func(request)
        except HTTPException:
            return None
    
    return optional_dependency


# 创建可选组件依赖
optional_event_bus = optional_component(get_event_bus)
optional_message_bus = optional_component(get_message_bus)
optional_plugin_manager = optional_component(get_plugin_manager)
