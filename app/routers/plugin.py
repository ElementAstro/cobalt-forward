from fastapi import APIRouter, HTTPException, Depends, Body, Query, Path
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from app.plugin_system import PluginManager, PluginMetadata, PluginState
from app.dependencies import get_plugin_manager
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

@router.get("/", response_model=List[PluginResponse])
async def list_plugins(plugin_manager: PluginManager = Depends(get_plugin_manager)):
    """获取所有插件列表"""
    logger.debug("开始获取插件列表")
    plugins = [
        PluginResponse(
            name=name,
            metadata=plugin.metadata,
            state=plugin.state,
            config=plugin.config
        )
        for name, plugin in plugin_manager.plugins.items()
    ]
    logger.info(f"获取插件列表成功 [total_plugins={len(plugins)}]")
    return plugins

@router.get("/{plugin_name}", response_model=PluginResponse)
async def get_plugin(
    plugin_name: str = Path(..., description="插件名称"),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """获取指定插件信息"""
    plugin = plugin_manager.plugins.get(plugin_name)
    if not plugin:
        raise HTTPException(status_code=404, detail="Plugin not found")
    
    return PluginResponse(
        name=plugin_name,
        metadata=plugin.metadata,
        state=plugin.state,
        config=plugin.config
    )

@router.post("/{plugin_name}/config")
async def update_plugin_config(
    plugin_name: str,
    config: Dict[str, Any] = Body(...),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """更新插件配置"""
    logger.info(f"开始更新插件配置 [plugin={plugin_name}]")
    plugin = plugin_manager.plugins.get(plugin_name)
    if not plugin:
        logger.error(f"插件不存在 [plugin={plugin_name}]")
        raise HTTPException(status_code=404, detail="Plugin not found")
    
    try:
        logger.debug(f"应用新配置 [plugin={plugin_name}] [config={config}]")
        await plugin.on_config_change(config)
        # 保存配置到文件
        config_path = f"{plugin_manager.config_dir}/{plugin_name}.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        logger.success(f"插件配置更新成功 [plugin={plugin_name}]")
        return {"status": "success", "message": "Configuration updated"}
    except Exception as e:
        logger.exception(f"更新插件配置失败 [plugin={plugin_name}]: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{plugin_name}/reload")
async def reload_plugin(
    plugin_name: str,
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """重新加载插件"""
    success = await plugin_manager.reload_plugin(plugin_name)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to reload plugin")
    return {"status": "success", "message": "Plugin reloaded"}

@router.post("/{plugin_name}/action/{action_name}")
async def execute_plugin_action(
    plugin_name: str,
    action_name: str,
    params: Dict[str, Any] = Body(default={}),
    plugin_manager: PluginManager = Depends(get_plugin_manager)
):
    """执行插件动作"""
    plugin = plugin_manager.plugins.get(plugin_name)
    if not plugin:
        raise HTTPException(status_code=404, detail="Plugin not found")
    
    try:
        action = getattr(plugin, action_name, None)
        if not action or not callable(action):
            raise HTTPException(status_code=404, detail="Action not found")
        
        result = await action(**params)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
