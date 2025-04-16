from fastapi import Depends, Request
from typing import Callable, Optional, Dict, Any, Type
from app.exceptions import AuthenticationError
from app.core.event_bus import EventBus
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


def get_plugin_manager():
    """Get the plugin manager instance"""
    from app.server import app
    return app.state.plugin_manager


def get_event_bus() -> EventBus:
    """Get the event bus instance"""
    from app.server import app
    return app.state.event_bus


async def get_current_user(request: Request):
    """Get the current authenticated user from the request"""
    token = request.headers.get("Authorization")
    if not token:
        raise AuthenticationError("No authentication token provided")
    # Implement user authentication logic here
    return {"id": "user_id"}


async def verify_admin(user=Depends(get_current_user)):
    """Verify administrator privileges"""
    if user.get("role") != "admin":
        raise AuthenticationError("Admin privileges required")
    return user


async def get_cache():
    """Get the cache instance"""
    from app.server import app
    return app.forwarder.cache


async def get_performance_monitor():
    """Get the performance monitor instance"""
    from app.server import app
    return app.forwarder.performance_monitor


# Plugin-specific dependencies
def get_plugin_context(plugin_name: str):
    """Create a dependency for accessing a specific plugin by name"""
    async def _get_plugin():
        plugin_manager = get_plugin_manager()
        plugin = plugin_manager.get_plugin(plugin_name)
        if plugin is None:
            raise ValueError(f"Plugin '{plugin_name}' not found")
        return plugin
    return _get_plugin


def get_plugin_config(plugin_name: str):
    """Create a dependency for accessing a specific plugin's configuration"""
    async def _get_plugin_config():
        plugin_manager = get_plugin_manager()
        plugin = plugin_manager.get_plugin(plugin_name)
        if plugin is None:
            raise ValueError(f"Plugin '{plugin_name}' not found")
        return plugin.config
    return _get_plugin_config


def require_feature(feature_name: str):
    """Check if a required feature is available"""
    def _require_feature(request: Request):
        feature_manager = getattr(request.app.state, "feature_manager", None)
        if not feature_manager:
            logger.warning("Feature manager not available")
            return False
            
        if not feature_manager.is_feature_enabled(feature_name):
            logger.warning(f"Required feature '{feature_name}' is not enabled")
            return False
            
        return True
    return _require_feature


@lru_cache()
def get_plugin_registry():
    """Get the plugin function registry"""
    from app.server import app
    return app.state.plugin_registry


def get_plugin_function(plugin_name: str, function_name: str):
    """Create a dependency for accessing a plugin function"""
    async def _get_function():
        registry = get_plugin_registry()
        function = registry.get_function(plugin_name, function_name)
        if function is None:
            raise ValueError(f"Function '{function_name}' not found in plugin '{plugin_name}'")
        return function
    return _get_function


async def get_shared_resources():
    """Get shared resources for plugins"""
    from app.server import app
    return getattr(app.state, "shared_resources", {})


def plugin_api_dependency(requires_auth: bool = True):
    """Create a dependency for plugin API access control"""
    if requires_auth:
        return Depends(get_current_user)
    return None


class PluginDependencyProvider:
    """Centralized provider for plugin dependencies"""
    
    def __init__(self):
        self._dependencies = {}
        
    def register(self, name: str, dependency: Callable, overwrite: bool = False):
        """Register a new dependency"""
        if name in self._dependencies and not overwrite:
            logger.warning(f"Dependency '{name}' already registered")
            return False
        
        self._dependencies[name] = dependency
        return True
        
    def get(self, name: str) -> Optional[Callable]:
        """Get a dependency by name"""
        return self._dependencies.get(name)
        
    def list_dependencies(self) -> Dict[str, Callable]:
        """List all registered dependencies"""
        return dict(self._dependencies)
