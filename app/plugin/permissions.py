"""
Plugin permission management system.

This module provides functionality for managing plugin permissions and
implementing permission-based access control.
"""
from functools import wraps
from typing import Dict, List, Set, Callable, Any, TypeVar, cast
import logging
# import inspect # Not used in this snippet, remove if not used elsewhere
import asyncio

logger = logging.getLogger(__name__)

_F = TypeVar('_F', bound=Callable[..., Any])


class PluginPermission:
    """
    Permission definition and checking class for plugin system
    """

    # Standard permission definitions
    READ_CONFIG = "config.read"
    WRITE_CONFIG = "config.write"
    READ_FILES = "files.read"
    WRITE_FILES = "files.write"
    EXECUTE_COMMANDS = "commands.execute"
    NETWORK_ACCESS = "network.access"
    PLUGIN_MANAGEMENT = "plugins.manage"
    USER_MANAGEMENT = "users.manage"
    SYSTEM_INFO = "system.info"

    # Hierarchical permission structure
    PERMISSION_HIERARCHY: Dict[str, List[str]] = {
        "config": ["read", "write"],
        "files": ["read", "write", "delete"],
        "commands": ["execute"],
        "network": ["access", "listen"],
        "plugins": ["manage", "execute"],
        "users": ["manage", "read"],
        "system": ["info", "manage"],
        "events": ["subscribe", "publish"],
    }

    @classmethod
    def get_all_permissions(cls) -> List[str]:
        """Get list of all defined permissions"""
        result: List[str] = []
        for category, actions in cls.PERMISSION_HIERARCHY.items():
            for action in actions:
                result.append(f"{category}.{action}")
        return result

    @classmethod
    def get_permission_categories(cls) -> List[str]:
        """Get list of permission categories"""
        return list(cls.PERMISSION_HIERARCHY.keys())

    @classmethod
    def get_category_permissions(cls, category: str) -> List[str]:
        """Get list of permissions in a category"""
        if category in cls.PERMISSION_HIERARCHY:
            return [f"{category}.{action}" for action in cls.PERMISSION_HIERARCHY[category]]
        return []

    @staticmethod
    def check_permission(required: str, granted: Set[str]) -> bool:
        """
        Check if a required permission is granted

        Args:
            required: Required permission (can use wildcards)
            granted: Set of granted permissions

        Returns:
            True if permission is granted, False otherwise
        """
        # Direct match
        if required in granted:
            return True

        # Check for wildcard matches
        if "*" in granted or "*.*" in granted:
            return True

        # Check category wildcard (e.g., "files.*")
        category_part = required.split(".")[0]
        if f"{category_part}.*" in granted:
            return True

        return False


def require_permission(permission: str) -> Callable[[_F], _F]:
    """
    Decorator to require permission for a function

    Args:
        permission: Required permission string

    Returns:
        Decorated function
    """
    def decorator(func: _F) -> _F:
        current_permissions: Set[str]
        # Ensure _required_permissions attribute exists and is Set[str]
        if hasattr(func, '_required_permissions'):
            perms_attr = getattr(func, '_required_permissions')
            # type: ignore[var-annotated]
            if not (isinstance(perms_attr, set) and all(isinstance(p, str) for p in perms_attr)):
                # If attribute exists but is not Set[str], overwrite it
                current_permissions = set()
                setattr(func, '_required_permissions', current_permissions)
            else:
                current_permissions = cast(
                    Set[str], perms_attr)  # It's already Set[str]
        else:
            current_permissions = set()
            setattr(func, '_required_permissions', current_permissions)

        current_permissions.add(permission)

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                # Permission checking happens at the plugin system level,
                # this decorator just marks the function
                # type: ignore[no-any-return]
                return await func(*args, **kwargs)
            return cast(_F, async_wrapper)
        else:
            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                # Permission checking happens at the plugin system level,
                # this decorator just marks the function
                return func(*args, **kwargs)  # type: ignore[no-any-return]
            return cast(_F, sync_wrapper)

    return decorator


class PermissionManager:
    """
    Manager for plugin permissions
    """

    def __init__(self):
        """Initialize permission manager"""
        self._plugin_permissions: Dict[str, Set[str]] = {}
        self._role_permissions: Dict[str, Set[str]] = {}
        self._plugin_roles: Dict[str, Set[str]] = {}
        self._default_permissions: Set[str] = set()

    def grant_permission(self, plugin_id: str, permission: str) -> None:
        """
        Grant permission to a plugin

        Args:
            plugin_id: Plugin identifier
            permission: Permission to grant
        """
        if plugin_id not in self._plugin_permissions:
            self._plugin_permissions[plugin_id] = set()
        self._plugin_permissions[plugin_id].add(permission)
        logger.debug(
            f"Granted permission '{permission}' to plugin '{plugin_id}'")

    def revoke_permission(self, plugin_id: str, permission: str) -> bool:
        """
        Revoke permission from a plugin

        Args:
            plugin_id: Plugin identifier
            permission: Permission to revoke

        Returns:
            True if permission was revoked, False if it wasn't granted
        """
        if plugin_id in self._plugin_permissions and permission in self._plugin_permissions[plugin_id]:
            self._plugin_permissions[plugin_id].remove(permission)
            logger.debug(
                f"Revoked permission '{permission}' from plugin '{plugin_id}'")
            return True
        return False

    def has_permission(self, plugin_id: str, permission: str) -> bool:
        """
        Check if a plugin has a specific permission

        Args:
            plugin_id: Plugin identifier
            permission: Permission to check

        Returns:
            True if plugin has permission, False otherwise
        """
        # Check default permissions first
        if PluginPermission.check_permission(permission, self._default_permissions):
            return True

        # Check explicit plugin permissions
        if plugin_id in self._plugin_permissions:
            if PluginPermission.check_permission(permission, self._plugin_permissions[plugin_id]):
                return True

        # Check role-based permissions
        if plugin_id in self._plugin_roles:
            for role in self._plugin_roles[plugin_id]:
                if role in self._role_permissions:
                    if PluginPermission.check_permission(permission, self._role_permissions[role]):
                        return True

        return False

    def set_plugin_roles(self, plugin_id: str, roles: List[str]) -> None:
        """
        Set roles for a plugin

        Args:
            plugin_id: Plugin identifier
            roles: List of role names
        """
        self._plugin_roles[plugin_id] = set(roles)

    def add_plugin_role(self, plugin_id: str, role: str) -> None:
        """
        Add a role to a plugin

        Args:
            plugin_id: Plugin identifier
            role: Role to add
        """
        if plugin_id not in self._plugin_roles:
            self._plugin_roles[plugin_id] = set()
        self._plugin_roles[plugin_id].add(role)

    def remove_plugin_role(self, plugin_id: str, role: str) -> bool:
        """
        Remove a role from a plugin

        Args:
            plugin_id: Plugin identifier
            role: Role to remove

        Returns:
            True if role was removed, False if plugin didn't have the role
        """
        if plugin_id in self._plugin_roles and role in self._plugin_roles[plugin_id]:
            self._plugin_roles[plugin_id].remove(role)
            return True
        return False

    def set_role_permissions(self, role: str, permissions: List[str]) -> None:
        """
        Set permissions for a role

        Args:
            role: Role name
            permissions: List of permissions to grant
        """
        self._role_permissions[role] = set(permissions)

    def add_role_permission(self, role: str, permission: str) -> None:
        """
        Add permission to a role

        Args:
            role: Role name
            permission: Permission to grant
        """
        if role not in self._role_permissions:
            self._role_permissions[role] = set()
        self._role_permissions[role].add(permission)

    def remove_role_permission(self, role: str, permission: str) -> bool:
        """
        Remove permission from a role

        Args:
            role: Role name
            permission: Permission to revoke

        Returns:
            True if permission was removed, False if role didn't have the permission
        """
        if role in self._role_permissions and permission in self._role_permissions[role]:
            self._role_permissions[role].remove(permission)
            return True
        return False

    def get_plugin_permissions(self, plugin_id: str) -> Set[str]:
        """
        Get all permissions granted to a plugin

        Args:
            plugin_id: Plugin identifier

        Returns:
            Set of granted permissions
        """
        permissions: Set[str] = set(self._default_permissions)

        # Add explicit plugin permissions
        if plugin_id in self._plugin_permissions:
            permissions.update(self._plugin_permissions[plugin_id])

        # Add permissions from roles
        if plugin_id in self._plugin_roles:
            for role in self._plugin_roles[plugin_id]:
                if role in self._role_permissions:
                    permissions.update(self._role_permissions[role])

        return permissions

    def set_default_permissions(self, permissions: List[str]) -> None:
        """
        Set default permissions granted to all plugins

        Args:
            permissions: List of default permissions
        """
        self._default_permissions = set(permissions)

    def add_default_permission(self, permission: str) -> None:
        """
        Add a default permission

        Args:
            permission: Permission to add as default
        """
        self._default_permissions.add(permission)

    def remove_default_permission(self, permission: str) -> bool:
        """
        Remove a default permission

        Args:
            permission: Permission to remove

        Returns:
            True if permission was removed, False if it wasn't a default permission
        """
        if permission in self._default_permissions:
            self._default_permissions.remove(permission)
            return True
        return False

    def clear_plugin_permissions(self, plugin_id: str) -> None:
        """
        Clear all permissions for a plugin

        Args:
            plugin_id: Plugin identifier
        """
        if plugin_id in self._plugin_permissions:
            del self._plugin_permissions[plugin_id]

        if plugin_id in self._plugin_roles:
            del self._plugin_roles[plugin_id]

    def check_function_permissions(self, plugin_id: str, func: Callable[..., Any]) -> bool:
        """
        Check if plugin has all permissions required by a function

        Args:
            plugin_id: Plugin identifier
            func: Function to check

        Returns:
            True if all required permissions are granted, False otherwise
        """
        # The decorator `require_permission` ensures `_required_permissions` is a Set[str].
        # If the attribute is not present, it means no permissions were added by the decorator.
        default_empty_str_set: Set[str] = set()
        required_permissions: Set[str] = getattr(
            func, '_required_permissions', default_empty_str_set)

        if not required_permissions:  # Handles case where attribute doesn't exist or is empty
            return True

        for permission_str in required_permissions:
            if not self.has_permission(plugin_id, permission_str):
                logger.warning(
                    f"Plugin '{plugin_id}' lacks required permission '{permission_str}' "
                    f"for function '{func.__name__}'"
                )
                return False

        return True

    def get_missing_permissions(self, plugin_id: str, func: Callable[..., Any]) -> List[str]:
        """
        Get list of missing permissions a plugin needs for a function

        Args:
            plugin_id: Plugin identifier
            func: Function to check

        Returns:
            List of missing permissions
        """
        default_empty_str_set: Set[str] = set()
        required_permissions: Set[str] = getattr(
            func, '_required_permissions', default_empty_str_set)

        missing: List[str] = []
        if not required_permissions:
            return missing  # Return empty list if no permissions are required

        for permission_str in required_permissions:
            if not self.has_permission(plugin_id, permission_str):
                missing.append(permission_str)

        return missing

    def get_roles(self) -> List[str]:
        """
        Get all defined roles

        Returns:
            List of role names
        """
        return list(self._role_permissions.keys())

    def get_role_permissions_dict(self) -> Dict[str, List[str]]:
        """
        Get dictionary mapping roles to their permissions

        Returns:
            Dictionary mapping role names to lists of permissions
        """
        return {role: list(perms) for role, perms in self._role_permissions.items()}

    def get_plugin_roles_dict(self) -> Dict[str, List[str]]:
        """
        Get dictionary mapping plugins to their roles

        Returns:
            Dictionary mapping plugin IDs to lists of roles
        """
        return {plugin_id: list(roles) for plugin_id, roles in self._plugin_roles.items()}
