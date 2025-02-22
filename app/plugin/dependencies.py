from fastapi import Depends, Request
from typing import Callable
from app.exceptions import AuthenticationError
from plugin_system import PluginManager
from app.core.event_bus import EventBus

def get_plugin_manager() -> PluginManager:
    """获取插件管理器实例"""
    from app.server import app
    return app.state.plugin_manager

def get_event_bus() -> EventBus:
    """获取事件总线实例"""
    from app.server import app
    return app.state.event_bus

async def get_current_user(request: Request):
    """获取当前用户"""
    token = request.headers.get("Authorization")
    if not token:
        raise AuthenticationError("No authentication token provided")
    # 实现用户认证逻辑
    return {"id": "user_id"}

async def verify_admin(user = Depends(get_current_user)):
    """验证管理员权限"""
    if user.get("role") != "admin":
        raise AuthenticationError("Admin privileges required")
    return user

async def get_cache():
    """获取缓存实例"""
    from app.server import app
    return app.forwarder.cache

async def get_performance_monitor():
    """获取性能监控实例"""
    from app.server import app
    return app.forwarder.performance_monitor
