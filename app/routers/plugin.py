from fastapi import APIRouter, HTTPException, Depends, Body, Query, Path, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, validator
from app.plugin.plugin_system import PluginManager, PluginMetadata, PluginPermission
from app.plugin.dependencies import get_plugin_manager
from app.core.exceptions import PluginError
import asyncio
from loguru import logger
from datetime import datetime


class PluginConfig(BaseModel):
    """Plugin configuration model"""
    enabled: bool = Field(default=True, description="Plugin enabled status")
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Plugin configuration")

    @validator('config')
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
    last_updated: datetime = Field(default_factory=datetime.utcnow)


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


@router.get("/", response_model=List[PluginResponse])
async def list_plugins(
    plugin_manager: PluginManager = Depends(get_plugin_manager),
    limit: int = Query(default=100, ge=1, le=1000)
):
    """List all available plugins"""
    try:
        plugins = plugin_manager.get_plugin_info()
        return [
            PluginResponse(
                name=name,
                metadata=info.get("metadata"),
                state=info.get("state", "unknown"),
                config=info.get("config", {}),
            )
            for name, info in list(plugins.items())[:limit]
        ]
    except Exception as e:
        logger.error(f"Failed to list plugins: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve plugin list: {str(e)}"
        )


@router.get("/{plugin_name}", response_model=PluginResponse)
async def get_plugin_details(
    plugin_name: str = Path(..., min_length=1),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """Get detailed plugin information"""
    try:
        info = plugin_manager.get_plugin_info().get(plugin_name)
        if not info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Plugin '{plugin_name}' not found"
            )
        return PluginResponse(
            name=plugin_name,
            metadata=info.get("metadata"),
            state=info.get("state", "unknown"),
            config=info.get("config", {})
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get plugin details: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve plugin details: {str(e)}"
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
            "timestamp": datetime.utcnow().isoformat()
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
    """获取插件健康状态"""
    health = await plugin_manager.get_plugin_health(plugin_name)
    return health


@router.post("/{plugin_name}/config")
async def update_plugin_config(
    plugin_name: str = Path(...),
    config: Dict[str, Any] = Body(...),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """更新插件配置"""
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
    """管理插件权限"""
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
    """获取插件性能指标"""
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
    """调用插件函数"""
    try:
        result = await plugin_manager.call_plugin_function(plugin_name, function_name, **params)
        return {"status": "success", "result": result}
    except Exception as e:
        logger.error(f"Error calling plugin function: {e}")
        raise HTTPException(status_code=400, detail=str(e))
