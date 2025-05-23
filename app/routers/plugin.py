from fastapi import APIRouter, HTTPException, Depends, Body, Query, Path, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, field_validator
from app.plugin.plugin_manager import PluginManager
from app.plugin.models import PluginMetadata, PluginState
from app.plugin.dependencies import get_plugin_manager
from app.core.exceptions import PluginError
import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class PluginConfig(BaseModel):
    """Plugin configuration model"""
    enabled: bool = Field(default=True, description="Plugin enabled status")
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Plugin configuration")

    @field_validator('config')
    def validate_config(cls, v):
        if not isinstance(v, dict):
            raise ValueError("Config must be a dictionary")
        return v


class PluginResponse(BaseModel):
    """Plugin response model"""
    name: str = Field(..., description="Plugin name")
    metadata: Optional[PluginMetadata] = Field(
        None, description="Plugin metadata")
    state: str = Field(..., description="Plugin state")
    config: Dict[str, Any] = Field(..., description="Plugin configuration")
    last_updated: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc))


class PluginFunctionParams(BaseModel):
    """Plugin function parameters model"""
    params: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=30.0, ge=0.1, le=300.0)


router = APIRouter(
    prefix="/api/plugins",
    tags=["plugins"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Plugin not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.get("/", response_model=List[Dict[str, Any]])
async def list_plugins(
    plugin_manager: PluginManager = Depends(get_plugin_manager),
    include_details: bool = Query(
        False, description="Include detailed plugin information"),
    state: Optional[str] = Query(None, description="Filter by plugin state")
):
    """Get all plugins list"""
    try:
        plugins = plugin_manager.get_all_plugins()

        result = []
        for plugin_name in plugins:
            # Get plugin status information
            plugin_status = plugin_manager.get_plugin_status(plugin_name)

            # Check if matches the status filter, if specified
            if state and plugin_status.get("state") != state:
                continue

            # Basic information
            plugin_info = {
                "name": plugin_name,
                "state": plugin_status.get("state", PluginState.UNLOADED),
                "version": plugin_status.get("version", "unknown")
            }

            # If detailed information is needed
            if include_details:
                metadata = plugin_manager.get_plugin_metadata(plugin_name)
                if metadata:
                    plugin_info["description"] = metadata.description
                    plugin_info["author"] = metadata.author
                    plugin_info["dependencies"] = plugin_manager.get_plugin_dependencies(
                        plugin_name)

                if plugin_name in plugin_manager.plugins:
                    plugin_info["uptime"] = plugin_status.get("uptime", 0)
                    plugin_info["error_count"] = plugin_status.get(
                        "error_count", 0)

            result.append(plugin_info)

        return result
    except Exception as e:
        logger.error(f"Failed to get plugin list: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get plugin list: {str(e)}"
        )


@router.get("/{plugin_name}", response_model=Dict[str, Any])
async def get_plugin_details(
    plugin_name: str = Path(..., min_length=1),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Get plugin detailed information"""
    try:
        # Check if the plugin exists
        if plugin_name not in plugin_manager.plugin_metadata:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin '{plugin_name}' does not exist"
            )

        # Get plugin status information
        plugin_status = plugin_manager.get_plugin_status(plugin_name)

        # Get metadata
        metadata = plugin_manager.get_plugin_metadata(plugin_name)

        # Get configuration
        config = plugin_manager.get_plugin_config(plugin_name)

        # Get dependencies and dependents
        dependencies = plugin_manager.get_plugin_dependencies(plugin_name)
        dependents = plugin_manager.get_plugin_dependents(plugin_name)

        # Get API schema (if available)
        api_schema = None
        if plugin_name in plugin_manager.plugins and hasattr(plugin_manager.plugins[plugin_name], 'get_api_schema'):
            plugin = plugin_manager.plugins[plugin_name]
            api_schema = plugin.get_api_schema()

        # Build response
        result = {
            "name": plugin_name,
            "state": plugin_status.get("state", PluginState.UNLOADED),
            "version": plugin_status.get("version", "unknown"),
            "description": metadata.description if metadata else "",
            "author": metadata.author if metadata else "Unknown",
            "config": config,
            "dependencies": dependencies,
            "dependents": dependents,
            "api_schema": api_schema,
            "metrics": plugin_status.get("metrics", {}),
            "health": plugin_status.get("health", {"status": "unknown"}),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # If plugin is loaded, add more information
        if plugin_name in plugin_manager.plugins:
            result["uptime"] = plugin_status.get("uptime", 0)
            result["error_count"] = plugin_status.get("error_count", 0)
            result["last_error"] = plugin_status.get("last_error")

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get plugin details: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get plugin details: {str(e)}"
        )


@router.post("/{plugin_name}/reload", status_code=status.HTTP_200_OK)
async def reload_plugin(
    plugin_name: str = Path(..., min_length=1),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Reload plugin"""
    try:
        async with asyncio.timeout(30):
            success = await plugin_manager.reload_plugin(plugin_name)

        if not success:
            raise PluginError("Plugin reload failed")

        logger.info(f"Successfully reloaded plugin: {plugin_name}")
        return {
            "status": "success",
            "message": f"Plugin '{plugin_name}' reloaded successfully",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except asyncio.TimeoutError:
        logger.error(f"Plugin reload timeout: {plugin_name}")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Plugin reload operation timed out"
        )
    except Exception as e:
        logger.error(f"Failed to reload plugin: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reload plugin: {str(e)}"
        )


@router.get("/{plugin_name}/health")
async def get_plugin_health(
    plugin_name: str = Path(...),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Get plugin health status"""
    health = await plugin_manager.get_plugin_health(plugin_name)
    return health


@router.post("/{plugin_name}/config")
async def update_plugin_config(
    plugin_name: str = Path(...),
    config: Dict[str, Any] = Body(...),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Update plugin configuration"""
    try:
        plugin = plugin_manager.plugins.get(plugin_name)
        if not plugin:
            raise HTTPException(status_code=404, detail="Plugin not found")
        await plugin.on_config_change(config)
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error updating plugin config: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{plugin_name}/permission")
async def manage_plugin_permission(
    plugin_name: str = Path(...),
    permission: str = Body(...),
    action: str = Query(..., regex="^(grant|revoke)$"),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Manage plugin permissions"""
    try:
        if action == "grant":
            await plugin_manager.grant_permission(plugin_name, permission)
        else:
            await plugin_manager.revoke_permission(plugin_name, permission)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{plugin_name}/metrics")
async def get_plugin_metrics(
    plugin_name: str = Path(...),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Get plugin performance metrics"""
    metrics = plugin_manager.get_plugin_metrics(plugin_name)
    if not metrics:
        raise HTTPException(status_code=404, detail="Plugin metrics not found")
    return metrics


@router.post("/{plugin_name}/function/{function_name}")
async def call_plugin_function(
    plugin_name: str = Path(...),
    function_name: str = Path(...),
    params: Dict[str, Any] = Body(default={}),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Call plugin function"""
    try:
        result = await plugin_manager.call_plugin_function(plugin_name, function_name, **params)
        return {"status": "success", "result": result}
    except Exception as e:
        logger.error(f"Error calling plugin function: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{plugin_name}/enable")
async def enable_plugin(
    plugin_name: str = Path(..., description="Plugin name"),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Enable plugin"""
    try:
        if plugin_name not in plugin_manager.plugin_metadata:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin '{plugin_name}' does not exist"
            )

        success = await plugin_manager.enable_plugin(plugin_name)

        if not success:
            # If plugin is already enabled
            if plugin_name in plugin_manager.plugins:
                return {
                    "status": "info",
                    "message": f"Plugin '{plugin_name}' is already enabled",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            # If enabling failed
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unable to enable plugin '{plugin_name}'"
            )

        return {
            "status": "success",
            "message": f"Plugin '{plugin_name}' successfully enabled",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to enable plugin: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enable plugin: {str(e)}"
        )


@router.post("/{plugin_name}/disable")
async def disable_plugin(
    plugin_name: str = Path(..., description="Plugin name"),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Disable plugin"""
    try:
        if plugin_name not in plugin_manager.plugin_metadata:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin '{plugin_name}' does not exist"
            )

        success = await plugin_manager.disable_plugin(plugin_name)

        if not success:
            # If plugin is already disabled
            if plugin_name not in plugin_manager.plugins:
                return {
                    "status": "info",
                    "message": f"Plugin '{plugin_name}' is already disabled",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            # If disabling failed
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unable to disable plugin '{plugin_name}'"
            )

        return {
            "status": "success",
            "message": f"Plugin '{plugin_name}' successfully disabled",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to disable plugin: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to disable plugin: {str(e)}"
        )


@router.get("/system/status")
async def get_system_status(
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Get plugin system status"""
    try:
        if not hasattr(plugin_manager, 'get_system_status'):
            # If method doesn't exist, try to manually build status info
            system_status = {
                "plugin_count": len(plugin_manager.plugins),
                "loaded_plugins": len(plugin_manager.plugins),
                "discovered_plugins": len(plugin_manager.plugin_metadata),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            system_status = plugin_manager.get_system_status()

        return system_status
    except Exception as e:
        logger.error(f"Failed to get system status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system status: {str(e)}"
        )


@router.get("/health")
async def check_all_plugins_health(
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Check health status of all plugins"""
    try:
        if hasattr(plugin_manager, 'check_all_plugins_health'):
            health_results = await plugin_manager.check_all_plugins_health()
        else:
            # If method doesn't exist, manually build health status
            health_results = {}
            for plugin_name in plugin_manager.plugins:
                try:
                    plugin = plugin_manager.plugins[plugin_name]
                    if hasattr(plugin, 'check_health'):
                        health_results[plugin_name] = await plugin.check_health()
                    else:
                        health_results[plugin_name] = {
                            "status": plugin.state if hasattr(plugin, 'state') else "unknown",
                            "plugin": plugin_name
                        }
                except Exception as e:
                    health_results[plugin_name] = {
                        "status": "error",
                        "plugin": plugin_name,
                        "error": str(e)
                    }

        return health_results
    except Exception as e:
        logger.error(
            f"Failed to check plugins health: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check plugins health: {str(e)}"
        )
