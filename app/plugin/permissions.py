from typing import Set, Callable
from functools import wraps

class PluginPermission:
    """插件权限定义"""
    ADMIN = "admin"
    FILE_IO = "file_io"
    NETWORK = "network"
    SYSTEM = "system"
    DATABASE = "database"
    ALL = {ADMIN, FILE_IO, NETWORK, SYSTEM, DATABASE}

def require_permission(permission: str):
    """权限检查装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            if not hasattr(self, '_permissions') or permission not in self._permissions:
                raise PermissionError(f"Plugin lacks permission: {permission}")
            return await func(self, *args, **kwargs)
        return wrapper
    return decorator
