from fastapi import Depends, Request
from typing import Awaitable, Callable, Optional, Dict, Any, cast
from app.exceptions import AuthenticationError
from app.core.event_bus import EventBus
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


def get_plugin_manager() -> Any:  # Assuming PluginManager type, using Any for now
    """Get the plugin manager instance"""
    from app.server import app
    return app.state.plugin_manager


def get_event_bus() -> EventBus:
    """Get the event bus instance"""
    from app.server import app
    return app.state.event_bus


async def get_current_user(request: Request) -> Dict[str, Any]:
    """Get the current authenticated user from the request"""
    token = request.headers.get("Authorization")
    if not token:
        raise AuthenticationError("No authentication token provided")
    # Implement user authentication logic here
    # For now, returning a placeholder user object
    # Added role for verify_admin example
    return {"id": "user_id", "role": "user"}


async def verify_admin(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """Verify administrator privileges"""
    if user.get("role") != "admin":
        raise AuthenticationError("Admin privileges required")
    return user


async def get_cache() -> Any:
    """Get the cache instance"""
    from app.server import app
    # Assuming forwarder and cache are on app.state or app itself
    # If app.forwarder is intended, FastAPI app needs a 'forwarder' attribute.
    # Using app.state.forwarder as a more likely pattern.
    if hasattr(app.state, 'forwarder') and hasattr(app.state.forwarder, 'cache'):
        return app.state.forwarder.cache
    logger.warning("Cache not found in app.state.forwarder")
    return None


async def get_performance_monitor() -> Any:
    """Get the performance monitor instance"""
    from app.server import app
    # Similar assumption as get_cache
    if hasattr(app.state, 'forwarder') and hasattr(app.state.forwarder, 'performance_monitor'):
        return app.state.forwarder.performance_monitor
    logger.warning("Performance monitor not found in app.state.forwarder")
    return None


# Plugin-specific dependencies
# Assuming Plugin type
def get_plugin_context(plugin_name: str) -> Callable[[], Awaitable[Any]]:
    """Create a dependency for accessing a specific plugin by name"""
    async def _get_plugin() -> Any:  # Assuming Plugin type
        plugin_manager: Any = get_plugin_manager()
        plugin: Any = plugin_manager.get_plugin(plugin_name)
        if plugin is None:
            raise ValueError(f"Plugin '{plugin_name}' not found")
        return plugin
    return _get_plugin


def get_plugin_config(plugin_name: str) -> Callable[[], Awaitable[Dict[str, Any]]]:
    """Create a dependency for accessing a specific plugin's configuration"""
    async def _get_plugin_config() -> Dict[str, Any]:
        # plugin_manager = get_plugin_manager() # This line was unused and removed.
        plugin: Any = await get_plugin_context(plugin_name)()  # plugin is Any

        if hasattr(plugin, 'config') and isinstance(plugin.config, dict):
            # Cast to Dict[str, Any] to satisfy the type checker,
            # aligning with the function's return type annotation.
            return cast(Dict[str, Any], plugin.config)  # type: ignore
        raise ValueError(
            f"Configuration not found or not a dict for plugin '{plugin_name}'")
    return _get_plugin_config


def require_feature(feature_name: str) -> Callable[[Request], bool]:
    """Check if a required feature is available"""
    def _require_feature(request: Request) -> bool:
        # Assuming feature_manager has is_feature_enabled method
        feature_manager: Any = getattr(
            request.app.state, "feature_manager", None)
        if not feature_manager:
            logger.warning("Feature manager not available")
            return False  # Or raise HTTPException

        if not feature_manager.is_feature_enabled(feature_name):
            logger.warning(f"Required feature '{feature_name}' is not enabled")
            return False  # Or raise HTTPException

        return True
    return _require_feature


@lru_cache()
def get_plugin_registry() -> Any:  # Assuming PluginRegistry type
    """Get the plugin function registry"""
    from app.server import app
    return app.state.plugin_registry


def get_plugin_function(plugin_name: str, function_name: str) -> Callable[[], Awaitable[Callable[..., Any]]]:
    """Create a dependency for accessing a plugin function"""
    async def _get_function() -> Callable[..., Any]:
        registry = get_plugin_registry()
        # Assuming registry has get_function method
        function: Optional[Callable[..., Any]] = registry.get_function(
            plugin_name, function_name)
        if function is None:
            raise ValueError(
                f"Function '{function_name}' not found in plugin '{plugin_name}'")
        return function
    return _get_function


async def get_shared_resources() -> Dict[str, Any]:
    """Get shared resources for plugins"""
    from app.server import app
    return getattr(app.state, "shared_resources", {})


def plugin_api_dependency(requires_auth: bool = True) -> Optional[Callable[..., Any]]:
    """Create a dependency for plugin API access control"""
    if requires_auth:
        return Depends(get_current_user)
    return None


class PluginDependencyProvider:
    """Centralized provider for plugin dependencies"""

    def __init__(self):
        self._dependencies: Dict[str, Callable[..., Any]] = {}

    def register(self, name: str, dependency: Callable[..., Any], overwrite: bool = False) -> bool:
        """Register a new dependency"""
        if name in self._dependencies and not overwrite:
            logger.warning(f"Dependency '{name}' already registered")
            return False

        self._dependencies[name] = dependency
        return True

    def get(self, name: str) -> Optional[Callable[..., Any]]:
        """Get a dependency by name"""
        return self._dependencies.get(name)

    def list_dependencies(self) -> Dict[str, Callable[..., Any]]:
        """List all registered dependencies"""
        return dict(self._dependencies)

# Example usage (assuming you have a PluginManager and other necessary components)
# async def example_route(user: Dict[str, Any] = Depends(verify_admin), cache: Any = Depends(get_cache)):
#     return {"message": "Admin access granted", "user": user, "cache_status": "available" if cache else "unavailable"}
