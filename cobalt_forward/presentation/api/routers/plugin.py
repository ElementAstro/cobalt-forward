"""
Plugin management API router for the Cobalt Forward application.

This module provides REST API endpoints for managing plugins,
including loading, unloading, and configuration.
"""

from fastapi import APIRouter, HTTPException, Depends, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import logging
from pathlib import Path

from ....core.interfaces.plugins import IPluginManager
from ....application.container import IContainer

logger = logging.getLogger(__name__)


class PluginInfo(BaseModel):
    """Plugin information model."""
    plugin_id: str = Field(..., description="Plugin identifier")
    name: str = Field(..., description="Plugin name")
    version: str = Field(..., description="Plugin version")
    description: Optional[str] = Field(None, description="Plugin description")
    author: Optional[str] = Field(None, description="Plugin author")
    status: str = Field(..., description="Plugin status")
    enabled: bool = Field(..., description="Whether plugin is enabled")
    config: Optional[Dict[str, Any]] = Field(
        None, description="Plugin configuration")


class PluginLoadRequest(BaseModel):
    """Plugin load request model."""
    plugin_path: str = Field(...,
                             description="Path to plugin file or directory")
    auto_enable: bool = Field(
        default=True, description="Auto-enable after loading")


class PluginConfigRequest(BaseModel):
    """Plugin configuration request model."""
    config: Dict[str, Any] = Field(..., description="Plugin configuration")


# Dependency injection
def get_container() -> IContainer:
    """Get the dependency injection container."""
    from fastapi import Request

    def _get_container(request: Request) -> IContainer:
        return request.app.state.container  # type: ignore[no-any-return]

    return Depends(_get_container)  # type: ignore[no-any-return]


def get_plugin_manager(container: IContainer = get_container()) -> IPluginManager:
    """Get plugin manager from container."""
    return container.resolve(IPluginManager)  # type: ignore[type-abstract]


# Router definition
router = APIRouter(
    prefix="/api/plugins",
    tags=["plugins"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Plugin not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.get("/", response_model=List[PluginInfo])
async def list_plugins(
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> List[PluginInfo]:
    """List all plugins."""
    try:
        plugins = list(plugin_manager.get_all_plugins().values())

        return [
            PluginInfo(
                plugin_id=plugin.metadata.get('plugin_id', plugin.name),
                name=plugin.name,
                version=plugin.version,
                description=plugin.metadata.get('description', ''),
                author=plugin.metadata.get('author', ''),
                status=getattr(plugin, 'status', 'unknown'),
                enabled=getattr(plugin, 'enabled', False),
                config=plugin.metadata.get('config', None)
            )
            for plugin in plugins
        ]

    except Exception as e:
        logger.error(f"Failed to list plugins: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list plugins: {str(e)}"
        )


@router.get("/{plugin_id}", response_model=PluginInfo)
async def get_plugin(
    plugin_id: str,
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> PluginInfo:
    """Get plugin information."""
    try:
        plugin = plugin_manager.get_plugin(plugin_id)
        if not plugin:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin not found: {plugin_id}"
            )

        return PluginInfo(
            plugin_id=plugin.metadata.get('plugin_id', plugin.name),
            name=plugin.name,
            version=plugin.version,
            description=plugin.metadata.get('description', ''),
            author=plugin.metadata.get('author', ''),
            status=getattr(plugin, 'status', 'loaded'),
            enabled=getattr(plugin, 'enabled', True),
            config=plugin.metadata.get('config', None)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get plugin {plugin_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get plugin: {str(e)}"
        )


@router.post("/load", response_model=PluginInfo, status_code=status.HTTP_201_CREATED)
async def load_plugin(
    request: PluginLoadRequest,
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> PluginInfo:
    """Load a plugin."""
    try:
        success = await plugin_manager.load_plugin(request.plugin_path)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to load plugin from: {request.plugin_path}"
            )

        # Get the loaded plugin
        plugin_name = Path(request.plugin_path).stem
        plugin = plugin_manager.get_plugin(plugin_name)

        if not plugin:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Plugin loaded but not found: {plugin_name}"
            )

        return PluginInfo(
            plugin_id=plugin.metadata.get('plugin_id', plugin.name),
            name=plugin.name,
            version=plugin.version,
            description=plugin.metadata.get('description', ''),
            author=plugin.metadata.get('author', ''),
            status=getattr(plugin, 'status', 'loaded'),
            enabled=getattr(plugin, 'enabled', True),
            config=plugin.metadata.get('config', None)
        )

    except Exception as e:
        logger.error(f"Failed to load plugin from {request.plugin_path}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to load plugin: {str(e)}"
        )


@router.post("/{plugin_id}/enable")
async def enable_plugin(
    plugin_id: str,
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Enable a plugin."""
    try:
        # Enable plugin functionality not implemented in interface
        # success = await plugin_manager.enable_plugin(plugin_id)
        success = False  # Placeholder
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to enable plugin: {plugin_id}"
            )

        return {"success": True, "message": f"Plugin {plugin_id} enabled"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to enable plugin {plugin_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enable plugin: {str(e)}"
        )


@router.post("/{plugin_id}/disable")
async def disable_plugin(
    plugin_id: str,
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Disable a plugin."""
    try:
        # Disable plugin functionality not implemented in interface
        # success = await plugin_manager.disable_plugin(plugin_id)
        success = False  # Placeholder
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to disable plugin: {plugin_id}"
            )

        return {"success": True, "message": f"Plugin {plugin_id} disabled"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to disable plugin {plugin_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to disable plugin: {str(e)}"
        )


@router.delete("/{plugin_id}")
async def unload_plugin(
    plugin_id: str,
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Unload a plugin."""
    try:
        success = await plugin_manager.unload_plugin(plugin_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin not found: {plugin_id}"
            )

        return {"success": True, "message": f"Plugin {plugin_id} unloaded"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to unload plugin {plugin_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to unload plugin: {str(e)}"
        )


@router.put("/{plugin_id}/config")
async def update_plugin_config(
    plugin_id: str,
    request: PluginConfigRequest,
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Update plugin configuration."""
    try:
        # Update plugin config functionality not implemented in interface
        # success = await plugin_manager.update_plugin_config(plugin_id, request.config)
        success = False  # Placeholder
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin not found: {plugin_id}"
            )

        return {"success": True, "message": f"Plugin {plugin_id} configuration updated"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update plugin config {plugin_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update plugin configuration: {str(e)}"
        )


@router.post("/{plugin_id}/reload")
async def reload_plugin(
    plugin_id: str,
    plugin_manager: IPluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """Reload a plugin."""
    try:
        success = await plugin_manager.reload_plugin(plugin_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin not found: {plugin_id}"
            )

        return {"success": True, "message": f"Plugin {plugin_id} reloaded"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reload plugin {plugin_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reload plugin: {str(e)}"
        )
