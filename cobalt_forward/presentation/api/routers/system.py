"""
System management API endpoints.

This module provides endpoints for system administration,
configuration management, and component control.
"""

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, status

from ....application.container import IContainer
from ....infrastructure.config.models import ApplicationConfig
from ....infrastructure.config.manager import ConfigManager
from ....core.interfaces.plugins import IPluginManager
from ..dependencies import get_container, get_config, require_api_key

router = APIRouter()


@router.get("/info")
async def system_info(
    config: ApplicationConfig = Depends(get_config)
) -> Dict[str, Any]:
    """
    Get system information.
    
    Returns basic system and application information.
    """
    return {
        "application": {
            "name": config.name,
            "version": config.version,
            "environment": config.environment,
            "debug": config.debug
        },
        "configuration": {
            "tcp_enabled": True,  # Always enabled
            "websocket_enabled": True,  # Always enabled
            "ssh_enabled": config.ssh.enabled,
            "ftp_enabled": config.ftp.enabled,
            "mqtt_enabled": config.mqtt.enabled,
            "plugins_enabled": config.plugins.enabled
        }
    }


@router.get("/config")
async def get_configuration(
    container: IContainer = Depends(get_container),
    _: bool = Depends(require_api_key)
) -> Dict[str, Any]:
    """
    Get current configuration.
    
    Returns the current application configuration (sensitive data masked).
    """
    try:
        config_manager = container.resolve(ConfigManager)
        config = config_manager.config
        
        # Return configuration with sensitive data masked
        config_dict = config.to_dict()
        
        # Mask sensitive fields
        if "ssh" in config_dict and "password" in config_dict["ssh"]:
            config_dict["ssh"]["password"] = "***"
        if "ftp" in config_dict and "password" in config_dict["ftp"]:
            config_dict["ftp"]["password"] = "***"
        if "mqtt" in config_dict and "password" in config_dict["mqtt"]:
            config_dict["mqtt"]["password"] = "***"
        
        return config_dict
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get configuration: {str(e)}"
        )


@router.post("/config/reload")
async def reload_configuration(
    container: IContainer = Depends(get_container),
    _: bool = Depends(require_api_key)
) -> Dict[str, Any]:
    """
    Reload configuration from file.
    
    Triggers a configuration reload if hot reload is enabled.
    """
    try:
        config_manager = container.resolve(ConfigManager)
        success = await config_manager.reload_config()
        
        return {
            "success": success,
            "message": "Configuration reloaded successfully" if success else "Configuration reload failed"
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reload configuration: {str(e)}"
        )


@router.get("/services")
async def list_services(
    container: IContainer = Depends(get_container)
) -> Dict[str, Any]:
    """
    List all registered services.
    
    Returns information about all services in the DI container.
    """
    registrations = container.get_registrations()
    
    services = []
    for service_type, registration in registrations.items():
        services.append({
            "name": service_type.__name__,
            "type": str(service_type),
            "lifetime": registration.lifetime.name,
            "has_instance": registration.instance is not None,
            "dependencies": [dep.__name__ for dep in registration.dependencies]
        })
    
    return {
        "services": services,
        "total_count": len(services)
    }


@router.get("/plugins")
async def list_plugins(
    container: IContainer = Depends(get_container)
) -> Dict[str, Any]:
    """
    List all loaded plugins.
    
    Returns information about all loaded plugins.
    """
    try:
        plugin_manager = container.resolve(IPluginManager)
        plugins = plugin_manager.get_all_plugins()
        
        plugin_list = []
        for name, plugin in plugins.items():
            plugin_list.append({
                "name": name,
                "metadata": plugin.metadata,
                "dependencies": plugin.dependencies,
                "permissions": list(plugin.permissions)
            })
        
        return {
            "plugins": plugin_list,
            "total_count": len(plugin_list)
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list plugins: {str(e)}"
        )


@router.post("/plugins/{plugin_name}/reload")
async def reload_plugin(
    plugin_name: str,
    container: IContainer = Depends(get_container),
    _: bool = Depends(require_api_key)
) -> Dict[str, Any]:
    """
    Reload a specific plugin.
    
    Args:
        plugin_name: Name of the plugin to reload
    """
    try:
        plugin_manager = container.resolve(IPluginManager)
        success = await plugin_manager.reload_plugin(plugin_name)
        
        return {
            "success": success,
            "message": f"Plugin {plugin_name} reloaded successfully" if success else f"Failed to reload plugin {plugin_name}"
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reload plugin {plugin_name}: {str(e)}"
        )


@router.get("/metrics")
async def get_system_metrics(
    container: IContainer = Depends(get_container)
) -> Dict[str, Any]:
    """
    Get system performance metrics.
    
    Returns performance metrics from various components.
    """
    metrics = {
        "timestamp": "2024-01-01T00:00:00Z",  # Placeholder
        "system": {
            "uptime": 0,  # Placeholder
            "memory_usage": 0,  # Placeholder
            "cpu_usage": 0  # Placeholder
        },
        "application": {
            "registered_services": len(container.get_registrations()),
            "active_connections": 0,  # Placeholder
            "processed_messages": 0,  # Placeholder
            "processed_commands": 0  # Placeholder
        }
    }
    
    # Try to get metrics from components
    try:
        # Add component-specific metrics here
        pass
    except Exception:
        pass  # Don't fail if metrics collection fails
    
    return metrics
