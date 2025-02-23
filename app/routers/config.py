from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, Any, Optional, List
from app.config.config_manager import ConfigManager
from app.core.dependencies import get_config_manager
from fastapi.responses import JSONResponse
from pathlib import Path
import yaml
from loguru import logger

router = APIRouter(
    prefix="/config",
    tags=["config"]
)

@router.get("/current")
async def get_current_config(
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """获取当前配置"""
    try:
        config = config_manager.runtime_config
        return JSONResponse(content={"config": config.__dict__})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update")
async def update_config(
    config: Dict[str, Any],
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """更新配置"""
    try:
        await config_manager.update_config(config)
        return {"status": "success", "message": "配置已更新"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/reload")
async def reload_config(
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """重新加载配置"""
    try:
        await config_manager._reload_config()
        return {"status": "success", "message": "配置已重新加载"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/backups")
async def list_backups(
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """列出所有配置备份"""
    try:
        backups = config_manager.backup_manager.list_backups()
        return {
            "backups": [
                {
                    "path": str(backup),
                    "timestamp": config_manager.backup_manager.get_backup_metadata(backup).get("timestamp")
                }
                for backup in backups
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/backup/restore/{backup_id}")
async def restore_backup(
    backup_id: str,
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """从备份恢复"""
    try:
        backups = config_manager.backup_manager.list_backups()
        backup_path = next(
            (b for b in backups if backup_id in str(b)),
            None
        )
        if not backup_path:
            raise HTTPException(status_code=404, detail="备份不存在")
            
        await config_manager.restore_from_backup(backup_path)
        return {"status": "success", "message": "配置已从备份恢复"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
