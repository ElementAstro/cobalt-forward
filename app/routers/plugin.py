from fastapi import APIRouter, HTTPException, Depends, Body, Query, Path
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from app.plugin.plugin_system import PluginManager, PluginMetadata, PluginPermission
from app.plugin.dependencies import get_plugin_manager
import yaml
from loguru import logger

router = APIRouter(prefix="/api/plugins", tags=["plugins"])


class PluginConfig(BaseModel):
    enabled: bool = True
    config: Dict[str, Any] = {}


class PluginResponse(BaseModel):
    name: str
    metadata: Optional[PluginMetadata]
    state: str
    config: Dict[str, Any]


@router.get("/", response_model=List[Dict[str, Any]])
async def list_plugins(plugin_manager: PluginManager = Depends(get_plugin_manager)):
    """获取所有插件列表"""
    try:
        return list(plugin_manager.get_plugin_info().values())
    except Exception as e:
        logger.error(f"Error listing plugins: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{plugin_name}")
async def get_plugin_details(
    plugin_name: str = Path(...),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """获取插件详细信息"""
    info = plugin_manager.get_plugin_info().get(plugin_name)
    if not info:
        raise HTTPException(status_code=404, detail="Plugin not found")
    return info


@router.post("/{plugin_name}/reload")
async def reload_plugin(
    plugin_name: str = Path(...),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """重新加载插件"""
    success = await plugin_manager.reload_plugin(plugin_name)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to reload plugin")
    return {"status": "success"}


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
