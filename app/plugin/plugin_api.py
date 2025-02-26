import time
from fastapi import APIRouter, Depends, HTTPException, Query, Body, Path
from typing import Dict, List, Any, Optional
import logging
import asyncio
from datetime import datetime

from .plugin_manager import PluginManager
from .event import EventBus

logger = logging.getLogger(__name__)

# 创建API路由
router = APIRouter(prefix="/api/plugins", tags=["plugins"])


def get_plugin_manager() -> PluginManager:
    """获取插件管理器实例"""
    from app.server import app
    if not hasattr(app.state, "plugin_manager"):
        raise HTTPException(
            status_code=500, detail="Plugin manager not initialized")
    return app.state.plugin_manager


def get_event_bus() -> EventBus:
    """获取事件总线实例"""
    from app.server import app
    if not hasattr(app.state, "event_bus"):
        raise HTTPException(
            status_code=500, detail="Event bus not initialized")
    return app.state.event_bus


@router.get("/")
async def get_all_plugins(
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """获取所有插件信息"""
    return {
        "plugins": manager.get_plugin_status(),
        "system": manager.get_system_status()
    }


@router.get("/{plugin_name}")
async def get_plugin_info(
    plugin_name: str = Path(..., description="插件名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """获取插件详细信息"""
    plugin_status = manager.get_plugin_status().get(plugin_name)
    if not plugin_status:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    # 获取更详细的信息
    api_schema = manager.get_plugin_api_schema(plugin_name)
    errors = manager.get_plugin_errors(plugin_name)

    return {
        "status": plugin_status,
        "api": api_schema,
        "errors": errors[:10]  # 限制返回的错误数量
    }


@router.post("/{plugin_name}/reload")
async def reload_plugin(
    plugin_name: str = Path(..., description="插件名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """重新加载插件"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    success = await manager.reload_plugin(plugin_name)
    if not success:
        raise HTTPException(
            status_code=500, detail=f"Failed to reload plugin {plugin_name}")

    return {"success": True, "message": f"Plugin {plugin_name} reloaded successfully"}


@router.post("/{plugin_name}/enable")
async def enable_plugin(
    plugin_name: str = Path(..., description="插件名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """启用插件"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    success = await manager.enable_plugin(plugin_name)
    return {"success": success, "message": f"Plugin {plugin_name} {'enabled' if success else 'was already enabled'}"}


@router.post("/{plugin_name}/disable")
async def disable_plugin(
    plugin_name: str = Path(..., description="插件名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """禁用插件"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    success = await manager.disable_plugin(plugin_name)
    return {"success": success, "message": f"Plugin {plugin_name} {'disabled' if success else 'was already disabled'}"}


@router.post("/{plugin_name}/function/{function_name}")
async def call_plugin_function(
    plugin_name: str = Path(..., description="插件名称"),
    function_name: str = Path(..., description="函数名称"),
    params: Dict[str, Any] = Body({}, description="函数参数"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """调用插件函数"""
    try:
        if plugin_name not in manager.plugins:
            raise HTTPException(
                status_code=404, detail=f"Plugin {plugin_name} not found or not active")

        plugin = manager.plugins[plugin_name]
        func = plugin.get_function(function_name)

        if not func:
            raise HTTPException(
                status_code=404, detail=f"Function {function_name} not found in plugin {plugin_name}")

        # 调用函数并记录执行时间
        start_time = time.time()
        result = await manager.call_plugin_function(plugin_name, function_name, **params)
        execution_time = time.time() - start_time

        return {
            "success": True,
            "result": result,
            "execution_time": execution_time
        }
    except Exception as e:
        logger.error(
            f"Error calling plugin function {plugin_name}.{function_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{plugin_name}/config")
async def get_plugin_config(
    plugin_name: str = Path(..., description="插件名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """获取插件配置"""
    if plugin_name not in manager.plugin_configs:
        raise HTTPException(
            status_code=404, detail=f"Config for plugin {plugin_name} not found")

    return manager.plugin_configs[plugin_name]


@router.put("/{plugin_name}/config")
async def update_plugin_config(
    plugin_name: str = Path(..., description="插件名称"),
    config: Dict[str, Any] = Body(..., description="新配置"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """更新插件配置"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    # 保存配置
    success = await manager._save_plugin_config(plugin_name, config)
    if not success:
        raise HTTPException(
            status_code=500, detail="Failed to save plugin config")

    # 如果插件已激活，通知它配置已更改
    if plugin_name in manager.plugins:
        plugin = manager.plugins[plugin_name]
        try:
            result = await plugin.on_config_change(config)
            if not result:
                return {"success": False, "message": "Plugin rejected the new config"}
        except Exception as e:
            logger.error(
                f"Plugin {plugin_name} failed to process config change: {e}")
            return {"success": False, "message": f"Error applying config: {str(e)}"}

    return {"success": True, "message": "Config updated successfully"}


@router.get("/{plugin_name}/permissions")
async def get_plugin_permissions(
    plugin_name: str = Path(..., description="插件名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """获取插件权限"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    permissions = manager.plugin_permissions.get(plugin_name, set())
    return {"permissions": list(permissions)}


@router.post("/{plugin_name}/permissions/{permission}")
async def grant_plugin_permission(
    plugin_name: str = Path(..., description="插件名称"),
    permission: str = Path(..., description="权限名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """授予插件权限"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    success = await manager.grant_permission(plugin_name, permission)
    return {"success": success, "message": f"Permission {permission} granted to {plugin_name}"}


@router.delete("/{plugin_name}/permissions/{permission}")
async def revoke_plugin_permission(
    plugin_name: str = Path(..., description="插件名称"),
    permission: str = Path(..., description="权限名称"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """撤销插件权限"""
    if plugin_name not in manager.plugin_modules:
        raise HTTPException(
            status_code=404, detail=f"Plugin {plugin_name} not found")

    success = await manager.revoke_permission(plugin_name, permission)
    return {"success": success, "message": f"Permission {permission} revoked from {plugin_name}"}


@router.get("/events")
async def get_events_info(
    event_bus: EventBus = Depends(get_event_bus)
) -> Dict[str, Any]:
    """获取事件总线信息"""
    stats = event_bus.get_stats()
    subscribers = event_bus.get_subscribers()

    return {
        "stats": stats,
        "subscribers": subscribers
    }


@router.post("/events/{event_name}")
async def emit_event(
    event_name: str = Path(..., description="事件名称"),
    data: Any = Body(None, description="事件数据"),
    source: str = Query("api", description="事件来源"),
    event_bus: EventBus = Depends(get_event_bus)
) -> Dict[str, Any]:
    """发出事件"""
    success = await event_bus.emit(event_name, data, source)
    return {"success": success, "message": f"Event {event_name} emitted successfully"}


@router.get("/errors")
async def get_plugin_errors(
    plugin_name: Optional[str] = Query(None, description="插件名称"),
    limit: int = Query(50, description="错误数量限制"),
    manager: PluginManager = Depends(get_plugin_manager)
) -> List[Dict[str, Any]]:
    """获取插件错误日志"""
    errors = manager.get_plugin_errors(plugin_name)
    return errors[:limit]


@router.get("/stats")
async def get_plugin_system_stats(
    manager: PluginManager = Depends(get_plugin_manager)
) -> Dict[str, Any]:
    """获取插件系统统计信息"""
    return manager.get_system_status()
