import time
from fastapi import APIRouter, Depends, HTTPException, Query, Body, Path, status
from typing import Dict, List, Any, Optional
import logging
import asyncio
from datetime import datetime

from .plugin_manager import PluginManager
from .event import EventBus

logger = logging.getLogger(__name__)

# Create API router
router = APIRouter(prefix="/api/plugins", tags=["plugins"])


def get_plugin_manager() -> PluginManager:
    """
    Get the plugin manager instance
    
    Returns:
        The plugin manager instance from the app state
    
    Raises:
        HTTPException: If the plugin manager is not initialized
    """
    from app.server import app
    if not hasattr(app.state, "plugin_manager"):
        logger.error("Plugin manager not initialized in app state")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Plugin manager not initialized"
        )
    return app.state.plugin_manager


def get_event_bus() -> EventBus:
    """
    Get the event bus instance
    
    Returns:
        The event bus instance from the app state
    
    Raises:
        HTTPException: If the event bus is not initialized
    """
    from app.server import app
    if not hasattr(app.state, "event_bus"):
        logger.error("Event bus not initialized in app state")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Event bus not initialized"
        )
    return app.state.event_bus


@router.get("/")
async def get_all_plugins(
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Get information about all plugins
    
    Args:
        manager: Plugin manager instance
        
    Returns:
        Dictionary with plugin information and system status
    """
    logger.debug("API request: Get all plugins info")
    return {
        "plugins": manager.get_plugin_status(),
        "system": manager.get_system_status()
    }


@router.get("/{plugin_name}")
async def get_plugin_info(
    plugin_name: str = Path(..., description="Name of the plugin"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Get information about a specific plugin
    
    Args:
        plugin_name: Name of the plugin
        manager: Plugin manager instance
    
    Returns:
        Plugin information
        
    Raises:
        HTTPException: If the plugin is not found
    """
    logger.debug(f"API request: Get info for plugin '{plugin_name}'")
    
    plugin_status = manager.get_plugin_status().get(plugin_name)
    if not plugin_status:
        logger.warning(f"API request failed: Plugin '{plugin_name}' not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Plugin '{plugin_name}' not found"
        )
    
    # Get API schema if available
    api_schema = manager.get_plugin_api_schema(plugin_name)
    if api_schema:
        plugin_status["api"] = api_schema
    
    # Get plugin errors
    plugin_status["errors"] = manager.get_plugin_errors(plugin_name)
    
    return plugin_status


@router.post("/{plugin_name}/reload")
async def reload_plugin(
    plugin_name: str = Path(..., description="Name of the plugin to reload"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Reload a specific plugin
    
    Args:
        plugin_name: Name of the plugin to reload
        manager: Plugin manager instance
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If the plugin is not found or reload fails
    """
    logger.info(f"API request: Reload plugin '{plugin_name}'")
    
    if plugin_name not in manager.plugin_modules:
        logger.warning(f"API request failed: Plugin '{plugin_name}' not found for reload")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Plugin '{plugin_name}' not found"
        )
    
    success = await manager.reload_plugin(plugin_name)
    
    if not success:
        logger.error(f"Failed to reload plugin '{plugin_name}'")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reload plugin '{plugin_name}'"
        )
    
    logger.info(f"Successfully reloaded plugin '{plugin_name}'")
    return {
        "status": "success",
        "message": f"Plugin '{plugin_name}' reloaded successfully",
        "timestamp": datetime.now().isoformat()
    }


@router.post("/{plugin_name}/enable")
async def enable_plugin(
    plugin_name: str = Path(..., description="Name of the plugin to enable"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Enable a specific plugin
    
    Args:
        plugin_name: Name of the plugin to enable
        manager: Plugin manager instance
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If the plugin is not found or enable fails
    """
    logger.info(f"API request: Enable plugin '{plugin_name}'")
    
    if plugin_name not in manager.plugin_modules:
        logger.warning(f"API request failed: Plugin '{plugin_name}' not found for enable")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Plugin '{plugin_name}' not found"
        )
    
    success = await manager.enable_plugin(plugin_name)
    
    if not success:
        logger.warning(f"Plugin '{plugin_name}' already enabled or could not be enabled")
        return {
            "status": "info",
            "message": f"Plugin '{plugin_name}' was already enabled",
            "timestamp": datetime.now().isoformat()
        }
    
    logger.info(f"Successfully enabled plugin '{plugin_name}'")
    return {
        "status": "success",
        "message": f"Plugin '{plugin_name}' enabled successfully",
        "timestamp": datetime.now().isoformat()
    }


@router.post("/{plugin_name}/disable")
async def disable_plugin(
    plugin_name: str = Path(..., description="Name of the plugin to disable"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Disable a specific plugin
    
    Args:
        plugin_name: Name of the plugin to disable
        manager: Plugin manager instance
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If the plugin is not found or disable fails
    """
    logger.info(f"API request: Disable plugin '{plugin_name}'")
    
    if plugin_name not in manager.plugin_modules:
        logger.warning(f"API request failed: Plugin '{plugin_name}' not found for disable")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Plugin '{plugin_name}' not found"
        )
    
    success = await manager.disable_plugin(plugin_name)
    
    if not success:
        logger.warning(f"Plugin '{plugin_name}' already disabled or could not be disabled")
        return {
            "status": "info",
            "message": f"Plugin '{plugin_name}' was already disabled",
            "timestamp": datetime.now().isoformat()
        }
    
    logger.info(f"Successfully disabled plugin '{plugin_name}'")
    return {
        "status": "success",
        "message": f"Plugin '{plugin_name}' disabled successfully",
        "timestamp": datetime.now().isoformat()
    }


@router.post("/{plugin_name}/function/{function_name}")
async def call_plugin_function(
    plugin_name: str = Path(..., description="Plugin name"),
    function_name: str = Path(..., description="Function name"),
    params: Dict[str, Any] = Body({}, description="Function parameters"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Call a function exported by a specific plugin
    
    Args:
        plugin_name: Name of the plugin
        function_name: Name of the function to call
        params: Parameters to pass to the function
        manager: Plugin manager instance
        
    Returns:
        Function result
        
    Raises:
        HTTPException: If the plugin or function is not found, or function call fails
    """
    logger.info(f"API request: Call function '{function_name}' for plugin '{plugin_name}'")
    
    if plugin_name not in manager.plugins:
        logger.warning(f"API request failed: Plugin '{plugin_name}' not found for function call")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Plugin '{plugin_name}' not found or not loaded"
        )
    
    try:
        result = await manager.call_plugin_function(plugin_name, function_name, **params)
        logger.info(f"Successfully called function '{function_name}' for plugin '{plugin_name}'")
        
        # Wrap the result in a response
        return {
            "status": "success",
            "plugin": plugin_name,
            "function": function_name,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
    except ValueError as e:
        # Function not found
        logger.warning(f"API request failed: Function '{function_name}' not found in plugin '{plugin_name}'")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except PermissionError as e:
        # Permission error
        logger.warning(f"API request failed: Permission denied for function '{function_name}' in plugin '{plugin_name}'")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )
    except Exception as e:
        # Other errors
        logger.error(f"API request failed: Error calling function '{function_name}' for plugin '{plugin_name}': {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calling function: {str(e)}"
        )


@router.get("/{plugin_name}/config")
async def get_plugin_config(
    plugin_name: str = Path(..., description="Plugin name"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Get the configuration of a plugin"""
    if plugin_name not in manager.plugin_configs:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} has no configuration")
            
    return manager.plugin_configs.get(plugin_name, {})


@router.put("/{plugin_name}/config")
async def update_plugin_config(
    plugin_name: str = Path(..., description="Plugin name"),
    config: Dict[str, Any] = Body(..., description="New configuration"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Update the configuration of a plugin"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")
            
    # Save the configuration
    success = await manager._save_plugin_config(plugin_name, config)
    if not success:
        raise HTTPException(
            status_code=500, detail=f"Failed to save configuration for plugin {plugin_name}")
            
    # If plugin is loaded, update its configuration
    if plugin_name in manager.plugins:
        result = await manager.plugins[plugin_name].on_config_change(config)
        if not result:
            raise HTTPException(
                status_code=400, 
                detail=f"Plugin {plugin_name} rejected the configuration"
            )
            
    return {
        "status": "success",
        "message": f"Configuration for plugin {plugin_name} updated successfully"
    }


@router.get("/{plugin_name}/permissions")
async def get_plugin_permissions(
    plugin_name: str = Path(..., description="Name of the plugin"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Get permissions for a specific plugin
    
    Args:
        plugin_name: Name of the plugin
        manager: Plugin manager instance
        
    Returns:
        Plugin permissions information
        
    Raises:
        HTTPException: If the plugin is not found
    """
    logger.debug(f"API request: Get permissions for plugin '{plugin_name}'")
    
    if plugin_name not in manager.plugin_modules:
        logger.warning(f"API request failed: Plugin '{plugin_name}' not found for permissions")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Plugin '{plugin_name}' not found"
        )
    
    # Get direct permissions
    direct_permissions = manager.plugin_permissions.get(plugin_name, set())
    
    # Get all permissions (including implied)
    all_permissions = set()
    for perm in direct_permissions:
        from .permissions import PluginPermission
        all_permissions.add(perm)
        all_permissions.update(PluginPermission.get_implied_permissions(perm))
    
    return {
        "plugin": plugin_name,
        "direct_permissions": list(direct_permissions),
        "all_permissions": list(all_permissions),
        "timestamp": datetime.now().isoformat()
    }


@router.post("/{plugin_name}/permissions/{permission}")
async def manage_plugin_permission(
    plugin_name: str = Path(..., description="Name of the plugin"),
    permission: str = Path(..., description="Permission to manage"),
    action: str = Query(..., description="Action to take", regex="^(grant|revoke)$"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """
    Manage permissions for a specific plugin
    
    Args:
        plugin_name: Name of the plugin
        permission: Permission to manage
        action: Action to take ('grant' or 'revoke')
        manager: Plugin manager instance
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If the plugin is not found or permission management fails
    """
    logger.info(f"API request: {action.capitalize()} '{permission}' permission for plugin '{plugin_name}'")
    
    if plugin_name not in manager.plugin_modules:
        logger.warning(f"API request failed: Plugin '{plugin_name}' not found for permission management")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Plugin '{plugin_name}' not found"
        )
    
    # Validate permission
    from .permissions import PluginPermission
    if permission not in PluginPermission.ALL:
        logger.warning(f"API request failed: Unknown permission '{permission}'")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Unknown permission: {permission}"
        )
    
    # Apply action
    success = False
    if action == "grant":
        success = await manager.grant_permission(plugin_name, permission)
    else:  # action == "revoke"
        success = await manager.revoke_permission(plugin_name, permission)
    
    if not success:
        logger.error(f"Failed to {action} '{permission}' for plugin '{plugin_name}'")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to {action} permission"
        )
    
    logger.info(f"Successfully {action}ed '{permission}' permission for plugin '{plugin_name}'")
    return {
        "status": "success",
        "message": f"Permission '{permission}' {action}ed for plugin '{plugin_name}'",
        "timestamp": datetime.now().isoformat()
    }


@router.delete("/{plugin_name}/permissions/{permission}")
async def revoke_plugin_permission(
    plugin_name: str = Path(..., description="Plugin name"),
    permission: str = Path(..., description="Permission name"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Revoke a permission from a plugin"""
    if plugin_name not in manager.plugin_permissions:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")
            
    # Revoke permission
    success = await manager.revoke_permission(plugin_name, permission)
    if success:
        return {
            "status": "success",
            "message": f"Permission {permission} revoked from plugin {plugin_name}"
        }
    else:
        return {
            "status": "info",
            "message": f"Plugin {plugin_name} does not have permission {permission}"
        }


@router.get("/events")
async def get_events_info(
    event_bus: EventBus = Depends(get_event_bus)
) -> Dict[str, Any]:
    """
    Get information about the event system
    
    Args:
        event_bus: Event bus instance
        
    Returns:
        Event system information
    """
    logger.debug("API request: Get events info")
    
    return {
        "stats": event_bus.get_stats(),
        "subscribers": event_bus.get_subscribers(),
        "recent_events": event_bus.get_event_history(limit=20)
    }


@router.post("/events/{event_name}")
async def emit_event(
    event_name: str = Path(..., description="Event name"),
    data: Any = Body(None, description="Event data"),
    source: str = Query("api", description="Event source"),
    event_bus: EventBus = Depends(get_event_bus)
) -> Dict[str, Any]:
    """
    Emit a new event
    
    Args:
        event_name: Name of the event to emit
        data: Event data
        source: Source of the event
        event_bus: Event bus instance
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If event emission fails
    """
    logger.info(f"API request: Emit event '{event_name}'")
    
    success = await event_bus.emit(event_name, data, source)
    
    if success:
        logger.info(f"Successfully emitted event '{event_name}'")
        return {
            "status": "success",
            "message": f"Event '{event_name}' emitted successfully",
            "timestamp": datetime.now().isoformat()
        }
    else:
        logger.error(f"Failed to emit event '{event_name}'")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Failed to emit event '{event_name}'"
        )


@router.get("/errors")
async def get_plugin_errors(
    plugin_name: Optional[str] = Query(None, description="Plugin name"),
    limit: int = Query(50, description="Error limit"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> List[Dict[str, Any]]:
    """Get error logs for plugins"""
    errors = manager.get_plugin_errors(plugin_name)
    return errors[:limit]


@router.get("/stats")
async def get_plugin_system_stats(
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Get statistics about the plugin system"""
    system_status = manager.get_system_status()
    plugin_statuses = manager.get_plugin_status()
    
    # Collect plugin metrics
    metrics = {}
    for plugin_name, plugin in manager.plugins.items():
        if hasattr(plugin, '_metrics') and hasattr(plugin._metrics, 'get_metrics_summary'):
            metrics[plugin_name] = plugin._metrics.get_metrics_summary()
    
    return {
        "system": system_status,
        "plugin_count": len(plugin_statuses),
        "active_plugins": len([p for p, status in plugin_statuses.items() 
                              if status.get("state") == "ACTIVE"]),
        "error_plugins": len([p for p, status in plugin_statuses.items() 
                             if status.get("state") == "ERROR"]),
        "disabled_plugins": len(manager.disabled_plugins),
        "metrics": metrics
    }

"""
Public API for plugin developers.

This module provides the public API interface for plugin developers to interact
with the core application. It serves as the main integration point between
plugins and the application.
"""
import logging
import inspect
import asyncio
import functools
import time
from typing import Dict, List, Any, Optional, Callable, Union, Type, TypeVar, Generic, cast

from .models import PluginMetadata, PluginState, PluginCategory
from .base import Plugin
from .event import EventCallback

logger = logging.getLogger(__name__)

# Type variables for better type hinting
T = TypeVar('T')
R = TypeVar('R')


class PluginAPI:
    """
    Public API for plugins to interact with the core application.
    
    This class provides methods for accessing core functionality,
    interacting with other plugins, and accessing shared resources.
    """
    
    def __init__(self, plugin: Plugin):
        """
        Initialize plugin API.
        
        Args:
            plugin: The plugin instance this API is associated with
        """
        self._plugin = plugin
        self._plugin_name = plugin.metadata.name if plugin.metadata else "unknown"
        
    async def register_function(self, 
                               func: Callable[..., Any], 
                               name: Optional[str] = None,
                               description: Optional[str] = None) -> bool:
        """
        Register a function that can be called by other plugins or the core.
        
        Args:
            func: Function to register
            name: Name of the function (defaults to function name)
            description: Description of what the function does
            
        Returns:
            True if registration was successful, False otherwise
        """
        try:
            if not self._plugin._plugin_manager or not self._plugin._plugin_manager.function_registry:
                logger.warning(f"Function registry not available for plugin {self._plugin_name}")
                return False
                
            # Default to function name if not provided
            if name is None:
                name = func.__name__
                
            # Default description from function docstring
            if description is None and func.__doc__:
                description = inspect.getdoc(func)
                
            # Register with the function registry
            result = await self._plugin._plugin_manager.function_registry.register_function(
                self._plugin_name,
                func,
                name,
                description or f"Function {name} from plugin {self._plugin_name}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error registering function {name} for plugin {self._plugin_name}: {e}")
            return False
            
    async def call_function(self, 
                           plugin_name: str, 
                           function_name: str, 
                           *args, 
                           **kwargs) -> Any:
        """
        Call a function from another plugin.
        
        Args:
            plugin_name: Name of the target plugin
            function_name: Name of the function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result
            
        Raises:
            ValueError: If plugin or function is not found
            RuntimeError: If function call fails
        """
        if not self._plugin._plugin_manager or not self._plugin._plugin_manager.function_registry:
            raise RuntimeError(f"Function registry not available for plugin {self._plugin_name}")
            
        # Check if plugin exists
        if not self._plugin._plugin_manager.get_plugin(plugin_name):
            raise ValueError(f"Plugin {plugin_name} not found or not loaded")
            
        # Call function through registry
        result = await self._plugin._plugin_manager.function_registry.call_function(
            plugin_name,
            function_name,
            *args,
            **kwargs
        )
        
        return result
    
    async def get_all_functions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get a list of all available functions from all plugins.
        
        Returns:
            Dictionary mapping plugin names to their available functions
        """
        if not self._plugin._plugin_manager or not self._plugin._plugin_manager.function_registry:
            logger.warning(f"Function registry not available for plugin {self._plugin_name}")
            return {}
            
        return await self._plugin._plugin_manager.function_registry.get_all_functions()
    
    async def get_plugin_functions(self, plugin_name: str) -> Dict[str, Any]:
        """
        Get all functions from a specific plugin.
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            Dictionary of function information
            
        Raises:
            ValueError: If plugin is not found
        """
        if not self._plugin._plugin_manager or not self._plugin._plugin_manager.function_registry:
            raise RuntimeError(f"Function registry not available for plugin {self._plugin_name}")
            
        # Check if plugin exists
        if not self._plugin._plugin_manager.get_plugin(plugin_name):
            raise ValueError(f"Plugin {plugin_name} not found or not loaded")
            
        functions = await self._plugin._plugin_manager.function_registry.get_plugin_functions(plugin_name)
        return functions
    
    async def subscribe_to_event(self, 
                                event_name: str, 
                                callback: EventCallback) -> bool:
        """
        Subscribe to an event.
        
        Args:
            event_name: Name of the event to subscribe to
            callback: Function to call when event is emitted
            
        Returns:
            True if subscription was successful, False otherwise
        """
        if not self._plugin._event_bus:
            logger.warning(f"Event bus not available for plugin {self._plugin_name}")
            return False
            
        # Add to plugin's event handlers dict for tracking
        if not hasattr(self._plugin, '_event_handlers'):
            self._plugin._event_handlers = {}
            
        if event_name not in self._plugin._event_handlers:
            self._plugin._event_handlers[event_name] = []
            
        self._plugin._event_handlers[event_name].append(callback)
        
        # Subscribe to event bus
        self._plugin._event_bus.subscribe(
            event_name,
            callback,
            self._plugin_name
        )
        
        logger.debug(f"Plugin {self._plugin_name} subscribed to event: {event_name}")
        return True
    
    async def unsubscribe_from_event(self, 
                                   event_name: str, 
                                   callback: Optional[EventCallback] = None) -> bool:
        """
        Unsubscribe from an event.
        
        Args:
            event_name: Name of the event to unsubscribe from
            callback: Specific callback to unsubscribe, or None to unsubscribe all
            
        Returns:
            True if unsubscription was successful, False otherwise
        """
        if not self._plugin._event_bus:
            logger.warning(f"Event bus not available for plugin {self._plugin_name}")
            return False
            
        if not hasattr(self._plugin, '_event_handlers') or event_name not in self._plugin._event_handlers:
            logger.warning(f"Plugin {self._plugin_name} is not subscribed to event: {event_name}")
            return False
            
        # Unsubscribe specific callback or all callbacks
        if callback is not None:
            # Remove specific callback
            if callback in self._plugin._event_handlers[event_name]:
                self._plugin._event_handlers[event_name].remove(callback)
                self._plugin._event_bus.unsubscribe(event_name, callback)
                return True
            return False
        else:
            # Remove all callbacks for this event
            for cb in self._plugin._event_handlers[event_name]:
                self._plugin._event_bus.unsubscribe(event_name, cb)
                
            # Clear event handlers list
            self._plugin._event_handlers[event_name] = []
            
            return True
    
    async def emit_event(self, 
                       event_name: str, 
                       data: Any = None) -> bool:
        """
        Emit an event to all subscribers.
        
        Args:
            event_name: Name of the event to emit
            data: Data to include with the event
            
        Returns:
            True if event was emitted, False otherwise
        """
        if not self._plugin._event_bus:
            logger.warning(f"Event bus not available for plugin {self._plugin_name}")
            return False
            
        # Add source plugin to data
        if isinstance(data, dict):
            event_data = dict(data)  # Make a copy
            event_data["source_plugin"] = self._plugin_name
        else:
            event_data = {
                "data": data,
                "source_plugin": self._plugin_name
            }
            
        # Emit event
        await self._plugin._event_bus.emit(event_name, event_data)
        return True
    
    async def get_plugin_info(self, plugin_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get information about a plugin.
        
        Args:
            plugin_name: Name of the plugin, or None for current plugin
            
        Returns:
            Dictionary of plugin information, or None if not found
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return None
            
        # Default to current plugin
        target_name = plugin_name or self._plugin_name
        
        # Get plugin metadata
        metadata = self._plugin._plugin_manager.get_plugin_metadata(target_name)
        if not metadata:
            return None
            
        # Get plugin status
        status = self._plugin._plugin_manager.get_plugin_status(target_name)
        
        # Combine info
        info = {
            "name": target_name,
            "metadata": metadata.to_dict() if metadata else {},
            "status": status,
            "dependencies": self._plugin._plugin_manager.get_plugin_dependencies(target_name),
            "dependents": self._plugin._plugin_manager.get_plugin_dependents(target_name)
        }
        
        return info
    
    async def get_all_plugins(self) -> List[str]:
        """
        Get list of all discovered plugins.
        
        Returns:
            List of plugin names
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return []
            
        return self._plugin._plugin_manager.get_all_plugins()
    
    async def get_loaded_plugins(self) -> List[str]:
        """
        Get list of all loaded plugins.
        
        Returns:
            List of loaded plugin names
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return []
            
        return self._plugin._plugin_manager.get_loaded_plugins()
    
    async def send_message(self, 
                         target_plugin: str, 
                         message: Any) -> bool:
        """
        Send a message to another plugin.
        
        Args:
            target_plugin: Name of the target plugin
            message: Message data
            
        Returns:
            True if message was sent, False otherwise
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return False
            
        return await self._plugin._plugin_manager.send_plugin_message(
            self._plugin_name,
            target_plugin,
            message
        )
    
    def log_debug(self, message: str) -> None:
        """
        Log a debug message.
        
        Args:
            message: Message to log
        """
        logger.debug(f"[{self._plugin_name}] {message}")
    
    def log_info(self, message: str) -> None:
        """
        Log an info message.
        
        Args:
            message: Message to log
        """
        logger.info(f"[{self._plugin_name}] {message}")
    
    def log_warning(self, message: str) -> None:
        """
        Log a warning message.
        
        Args:
            message: Message to log
        """
        logger.warning(f"[{self._plugin_name}] {message}")
    
    def log_error(self, message: str) -> None:
        """
        Log an error message.
        
        Args:
            message: Message to log
        """
        logger.error(f"[{self._plugin_name}] {message}")
    
    async def get_shared_resource(self, name: str) -> Optional[Any]:
        """
        Get a shared resource.
        
        Args:
            name: Resource name
            
        Returns:
            Resource object or None if not found
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return None
            
        return self._plugin._plugin_manager.get_shared_resource(name)
    
    async def register_shared_resource(self, name: str, resource: Any) -> bool:
        """
        Register a shared resource accessible to all plugins.
        
        Args:
            name: Resource name
            resource: Resource object
            
        Returns:
            True if resource was registered, False otherwise
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return False
            
        # Prefix resource name with plugin name to avoid conflicts
        resource_name = f"{self._plugin_name}.{name}"
        
        self._plugin._plugin_manager.register_shared_resource(resource_name, resource)
        return True
    
    async def unregister_shared_resource(self, name: str) -> bool:
        """
        Unregister a shared resource.
        
        Args:
            name: Resource name
            
        Returns:
            True if resource was unregistered, False if not found
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return False
            
        # Prefix resource name with plugin name to avoid conflicts
        resource_name = f"{self._plugin_name}.{name}"
        
        return self._plugin._plugin_manager.unregister_shared_resource(resource_name)
    
    async def get_all_shared_resources(self) -> Dict[str, Any]:
        """
        Get all shared resources.
        
        Returns:
            Dictionary of resource names to objects
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return {}
            
        return self._plugin._plugin_manager.get_all_shared_resources()
    
    async def check_permission(self, permission: str) -> bool:
        """
        Check if plugin has a specific permission.
        
        Args:
            permission: Permission to check
            
        Returns:
            True if plugin has permission, False otherwise
        """
        if (not self._plugin._plugin_manager or 
            not self._plugin._plugin_manager.permission_manager):
            logger.warning(f"Permission manager not available for plugin {self._plugin_name}")
            return False
            
        return self._plugin._plugin_manager.permission_manager.check_permission(
            self._plugin_name,
            permission
        )
    
    async def get_permissions(self) -> List[str]:
        """
        Get all permissions granted to this plugin.
        
        Returns:
            List of permission names
        """
        if (not self._plugin._plugin_manager or 
            not self._plugin._plugin_manager.permission_manager):
            logger.warning(f"Permission manager not available for plugin {self._plugin_name}")
            return []
            
        return self._plugin._plugin_manager.permission_manager.get_permissions(
            self._plugin_name
        )
    
    async def get_config(self) -> Dict[str, Any]:
        """
        Get plugin configuration.
        
        Returns:
            Configuration dictionary
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return {}
            
        return self._plugin._plugin_manager.get_plugin_config(self._plugin_name)
    
    async def update_config(self, config: Dict[str, Any]) -> bool:
        """
        Update plugin configuration.
        
        Args:
            config: New configuration values
            
        Returns:
            True if configuration was updated, False otherwise
        """
        if not self._plugin._plugin_manager:
            logger.warning(f"Plugin manager not available for plugin {self._plugin_name}")
            return False
            
        # Merge with existing config
        current_config = self._plugin._plugin_manager.get_plugin_config(self._plugin_name)
        merged_config = {**current_config, **config}
        
        return await self._plugin._plugin_manager.update_plugin_config(
            self._plugin_name,
            merged_config
        )
    
    def measure_time(self, func: Callable[..., T]) -> Callable[..., T]:
        """
        Decorator to measure execution time of a function.
        
        Args:
            func: Function to measure
            
        Returns:
            Decorated function
        """
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            self.log_debug(f"Function {func.__name__} executed in {execution_time:.4f}s")
            return result
            
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            self.log_debug(f"Function {func.__name__} executed in {execution_time:.4f}s")
            return result
            
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    def register_function_decorator(self, name: Optional[str] = None, description: Optional[str] = None):
        """
        Decorator to register a function with the plugin system.
        
        Args:
            name: Function name (defaults to function name)
            description: Function description
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            # Register function
            asyncio.create_task(
                self.register_function(
                    func,
                    name or func.__name__,
                    description
                )
            )
            return func
            
        return decorator
    
    def event_handler(self, event_name: str):
        """
        Decorator to register an event handler.
        
        Args:
            event_name: Name of the event to handle
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            # Register event handler
            asyncio.create_task(
                self.subscribe_to_event(event_name, func)
            )
            return func
            
        return decorator
    
    def permission_required(self, permission: str):
        """
        Decorator to check if plugin has required permission before executing a function.
        
        Args:
            permission: Required permission
            
        Returns:
            Decorator function
        """
        def decorator(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                if not await self.check_permission(permission):
                    raise PermissionError(
                        f"Plugin {self._plugin_name} does not have permission: {permission}"
                    )
                return await func(*args, **kwargs)
                
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                # Create a temporary event loop if needed
                if not asyncio.get_event_loop().is_running():
                    loop = asyncio.new_event_loop()
                    has_permission = loop.run_until_complete(self.check_permission(permission))
                    loop.close()
                else:
                    future = asyncio.create_task(self.check_permission(permission))
                    has_permission = asyncio.get_event_loop().run_until_complete(future)
                    
                if not has_permission:
                    raise PermissionError(
                        f"Plugin {self._plugin_name} does not have permission: {permission}"
                    )
                return func(*args, **kwargs)
                
            # Return appropriate wrapper based on function type
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
                
        return decorator


# API errors
class PluginAPIError(Exception):
    """Base class for plugin API errors."""
    pass


class PluginNotFoundError(PluginAPIError):
    """Exception raised when a plugin is not found."""
    pass


class FunctionNotFoundError(PluginAPIError):
    """Exception raised when a function is not found."""
    pass


class PermissionDeniedError(PluginAPIError):
    """Exception raised when a plugin does not have permission for an operation."""
    pass
