from typing import Set, Callable, Dict, List, Any
from functools import wraps
import logging
import inspect
import asyncio

logger = logging.getLogger(__name__)


class PluginPermission:
    """插件权限定义"""
    ADMIN = "admin"
    FILE_IO = "file_io"
    NETWORK = "network"
    SYSTEM = "system"
    DATABASE = "database"
    UI = "ui"
    EVENT = "event"
    
    # 所有权限
    ALL = {ADMIN, FILE_IO, NETWORK, SYSTEM, DATABASE, UI, EVENT}
    
    # 权限分级
    PERMISSION_LEVELS = {
        ADMIN: 100,
        SYSTEM: 80,
        DATABASE: 60,
        NETWORK: 40,
        FILE_IO: 30,
        EVENT: 20,
        UI: 10
    }
    
    # 依赖关系，拥有键权限会自动获得值中的权限
    DEPENDENCIES = {
        ADMIN: {SYSTEM, DATABASE, NETWORK, FILE_IO, EVENT, UI},
        SYSTEM: {DATABASE, NETWORK, FILE_IO},
        DATABASE: {FILE_IO}
    }
    
    @classmethod
    def get_level(cls, permission: str) -> int:
        """获取权限级别"""
        return cls.PERMISSION_LEVELS.get(permission, 0)
    
    @classmethod
    def get_implied_permissions(cls, permission: str) -> Set[str]:
        """获取某权限隐含的所有权限"""
        result = {permission}
        for implied in cls.DEPENDENCIES.get(permission, set()):
            result.add(implied)
            result.update(cls.get_implied_permissions(implied))
        return result


def require_permission(permission: str):
    """权限检查装饰器"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            if not hasattr(self, '_permissions'):
                raise AttributeError(f"Object {self.__class__.__name__} has no _permissions attribute")
                
            # 检查直接权限或隐含权限
            allowed_permissions = set()
            for perm in self._permissions:
                allowed_permissions.add(perm)
                allowed_permissions.update(PluginPermission.get_implied_permissions(perm))
                
            if permission not in allowed_permissions:
                error_msg = f"Plugin {getattr(self, 'name', self.__class__.__name__)} lacks permission: {permission}"
                logger.warning(error_msg)
                raise PermissionError(error_msg)
                
            return await func(self, *args, **kwargs)
            
        @wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            if not hasattr(self, '_permissions'):
                raise AttributeError(f"Object {self.__class__.__name__} has no _permissions attribute")
                
            # 检查直接权限或隐含权限
            allowed_permissions = set()
            for perm in self._permissions:
                allowed_permissions.add(perm)
                allowed_permissions.update(PluginPermission.get_implied_permissions(perm))
                
            if permission not in allowed_permissions:
                error_msg = f"Plugin {getattr(self, 'name', self.__class__.__name__)} lacks permission: {permission}"
                logger.warning(error_msg)
                raise PermissionError(error_msg)
                
            return func(self, *args, **kwargs)
        
        # 根据函数类型返回合适的包装器
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


class PermissionManager:
    """权限管理器"""
    
    def __init__(self):
        self._plugin_permissions: Dict[str, Set[str]] = {}
        self._permission_log: List[Dict[str, Any]] = []
        self._max_log_size = 1000
    
    def grant_permission(self, plugin_name: str, permission: str) -> bool:
        """授予插件权限"""
        if plugin_name not in self._plugin_permissions:
            self._plugin_permissions[plugin_name] = set()
            
        self._plugin_permissions[plugin_name].add(permission)
        self._log_permission_change(plugin_name, permission, "grant")
        return True
    
    def revoke_permission(self, plugin_name: str, permission: str) -> bool:
        """撤销插件权限"""
        if plugin_name in self._plugin_permissions:
            self._plugin_permissions[plugin_name].discard(permission)
            self._log_permission_change(plugin_name, permission, "revoke")
            return True
        return False
    
    def has_permission(self, plugin_name: str, permission: str) -> bool:
        """检查插件是否有特定权限"""
        if plugin_name not in self._plugin_permissions:
            return False
            
        direct_permissions = self._plugin_permissions[plugin_name]
        
        # 检查直接权限
        if permission in direct_permissions:
            return True
            
        # 检查隐含权限
        for perm in direct_permissions:
            implied = PluginPermission.get_implied_permissions(perm)
            if permission in implied:
                return True
                
        return False
    
    def get_plugin_permissions(self, plugin_name: str) -> Set[str]:
        """获取插件的所有权限（包括隐含权限）"""
        direct_permissions = self._plugin_permissions.get(plugin_name, set())
        
        # 计算所有隐含权限
        all_permissions = set(direct_permissions)
        for perm in direct_permissions:
            all_permissions.update(PluginPermission.get_implied_permissions(perm))
            
        return all_permissions
    
    def _log_permission_change(self, plugin_name: str, permission: str, action: str):
        """记录权限变更"""
        import time
        
        log_entry = {
            "timestamp": time.time(),
            "plugin": plugin_name,
            "permission": permission,
            "action": action
        }
        
        self._permission_log.append(log_entry)
        
        # 保持日志大小
        if len(self._permission_log) > self._max_log_size:
            self._permission_log = self._permission_log[-self._max_log_size:]
    
    def get_permission_log(self) -> List[Dict[str, Any]]:
        """获取权限变更日志"""
        return self._permission_log
