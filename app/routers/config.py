from fastapi import APIRouter, HTTPException, Depends, Query, status, Response, Body
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, field_validator, model_validator
from app.config.config_manager import ConfigManager
from app.core.dependencies import get_config_manager
from fastapi.responses import JSONResponse
from pathlib import Path
import yaml
from loguru import logger
from datetime import datetime
import asyncio
import zipfile
import json

# Input validation models


class ConfigUpdate(BaseModel):
    """Config update validation model"""
    config: Dict[str, Any] = Field(..., description="Configuration data")

    @field_validator('config')
    @classmethod
    def validate_config(cls, v):
        required_fields = {'tcp_host', 'tcp_port',
                           'websocket_host', 'websocket_port'}
        if not all(field in v for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")
        return v


class BackupMetadata(BaseModel):
    """Backup metadata model"""
    path: str
    timestamp: datetime
    size: int
    version: str
    encrypted: bool = False


class PluginConfigUpdate(BaseModel):
    """Plugin configuration update model"""
    plugin_id: str = Field(..., description="Plugin unique identifier")
    config: Dict[str, Any] = Field(...,
                                   description="Plugin configuration data")


class VersionRollback(BaseModel):
    """Version rollback model"""
    version: str = Field(..., description="Version to rollback to")


router = APIRouter(
    prefix="/config",
    tags=["config"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Resource not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.get("/current", response_model=Dict[str, Any])
async def get_current_config(
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Get current configuration"""
    try:
        config = config_manager.runtime_config
        logger.debug("Retrieved current configuration")
        return JSONResponse(
            content={"config": config.__dict__},
            status_code=status.HTTP_200_OK
        )
    except Exception as e:
        logger.error(f"Failed to get current config: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve configuration: {str(e)}"
        )


@router.post("/update", status_code=status.HTTP_200_OK)
async def update_config(
    config_update: ConfigUpdate,
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Update configuration"""
    try:
        # Create backup before update
        await config_manager.create_backup()
        logger.info("Created configuration backup before update")

        # Update with timeout
        async with asyncio.timeout(10):
            success = await config_manager.update_config(config_update.config)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Configuration update failed: invalid data or validation error"
            )

        logger.success("Configuration updated successfully")
        return {
            "status": "success",
            "message": "Configuration updated successfully",
            "timestamp": datetime.utcnow().isoformat()
        }
    except asyncio.TimeoutError:
        logger.error("Configuration update timed out")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Configuration update timed out"
        )
    except ValueError as e:
        logger.error(f"Invalid configuration data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to update configuration: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Configuration update failed: {str(e)}"
        )


@router.post("/reload", status_code=status.HTTP_200_OK)
async def reload_config(
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Reload configuration from disk"""
    try:
        async with asyncio.timeout(5):
            await config_manager._reload_config()
        logger.success("Configuration reloaded successfully")
        return {
            "status": "success",
            "message": "Configuration reloaded successfully",
            "timestamp": datetime.utcnow().isoformat()
        }
    except asyncio.TimeoutError:
        logger.error("Configuration reload timed out")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Configuration reload timed out"
        )
    except Exception as e:
        logger.error(
            f"Failed to reload configuration: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Configuration reload failed: {str(e)}"
        )


@router.get("/backups", response_model=List[BackupMetadata])
async def list_backups(
    config_manager: ConfigManager = Depends(get_config_manager),
    limit: int = Query(default=10, ge=1, le=100)
):
    """List configuration backups"""
    try:
        backups = config_manager.backup_manager.list_backups()
        backup_list = []

        for backup in backups[:limit]:
            # 读取备份文件中的metadata.json
            try:
                with zipfile.ZipFile(backup, 'r') as backup_zip:
                    if 'metadata.json' in backup_zip.namelist():
                        metadata = json.loads(backup_zip.read('metadata.json'))
                    else:
                        metadata = {}
            except Exception:
                metadata = {}

            backup_list.append(BackupMetadata(
                path=str(backup),
                timestamp=datetime.fromisoformat(metadata.get(
                    "backup_time", datetime.now().isoformat())),
                size=backup.stat().st_size,
                version=metadata.get("version", "unknown"),
                encrypted=metadata.get("encrypted", False)
            ))

        logger.debug(f"Retrieved {len(backup_list)} backup(s)")
        return backup_list

    except Exception as e:
        logger.error(f"Failed to list backups: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve backups: {str(e)}"
        )


@router.post("/backup/create", status_code=status.HTTP_200_OK)
async def create_backup(
    config_manager: ConfigManager = Depends(get_config_manager),
    description: str = Body(None, description="Optional backup description")
):
    """Create configuration backup on demand"""
    try:
        metadata = {
            "description": description,
            "created_by": "user",
            "backup_type": "manual"
        }

        backup_path = await config_manager.create_backup()

        logger.info(f"Manual backup created at {backup_path}")
        return {
            "status": "success",
            "message": "Backup created successfully",
            "path": str(backup_path),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to create backup: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create backup: {str(e)}"
        )


@router.post("/backup/restore/{backup_id}", status_code=status.HTTP_200_OK)
async def restore_backup(
    backup_id: str,
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Restore configuration from backup"""
    try:
        backups = config_manager.backup_manager.list_backups()
        backup_path = next(
            (b for b in backups if backup_id in str(b)),
            None
        )

        if not backup_path:
            logger.error(f"Backup not found: {backup_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Backup not found"
            )

        # Create safety backup before restore
        await config_manager.create_backup()

        async with asyncio.timeout(15):
            await config_manager.restore_from_backup(backup_path)

        logger.success(f"Configuration restored from backup: {backup_id}")
        return {
            "status": "success",
            "message": "Configuration restored successfully",
            "backup_id": backup_id,
            "timestamp": datetime.utcnow().isoformat()
        }

    except asyncio.TimeoutError:
        logger.error("Backup restore timed out")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Backup restore operation timed out"
        )
    except Exception as e:
        logger.error(f"Failed to restore backup: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restore from backup: {str(e)}"
        )


@router.post("/backup/verify/{backup_id}", status_code=status.HTTP_200_OK)
async def verify_backup(
    backup_id: str,
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Verify backup integrity"""
    try:
        backups = config_manager.backup_manager.list_backups()
        backup_path = next(
            (b for b in backups if backup_id in str(b)),
            None
        )

        if not backup_path:
            logger.error(f"Backup not found: {backup_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Backup not found"
            )

        # 验证备份完整性
        is_valid = config_manager.backup_manager.verify_backup(backup_path)

        if is_valid:
            logger.info(f"Backup {backup_id} verified successfully")
            return {
                "status": "success",
                "message": "Backup verified successfully",
                "is_valid": True,
                "backup_id": backup_id
            }
        else:
            logger.warning(f"Backup {backup_id} verification failed")
            return {
                "status": "warning",
                "message": "Backup verification failed",
                "is_valid": False,
                "backup_id": backup_id
            }
    except Exception as e:
        logger.error(f"Failed to verify backup: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to verify backup: {str(e)}"
        )


@router.post("/plugin/update", status_code=status.HTTP_200_OK)
async def update_plugin_config(
    update: PluginConfigUpdate,
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Update plugin configuration"""
    try:
        # Create backup before update
        await config_manager.create_backup()

        success = await config_manager.update_plugin_config(
            update.plugin_id,
            update.config
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update plugin configuration: validation error"
            )

        logger.success(
            f"Plugin {update.plugin_id} configuration updated successfully")
        return {
            "status": "success",
            "message": f"Plugin {update.plugin_id} configuration updated successfully",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(
            f"Failed to update plugin configuration: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update plugin configuration: {str(e)}"
        )


@router.get("/plugin/{plugin_id}", response_model=Dict[str, Any])
async def get_plugin_config(
    plugin_id: str,
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Get plugin configuration"""
    try:
        plugin_config = config_manager.get_plugin_config(plugin_id)
        if not plugin_config:
            # 返回空配置而不是404，这样客户端可以得到默认配置
            return {}

        logger.debug(f"Retrieved configuration for plugin {plugin_id}")
        return plugin_config
    except Exception as e:
        logger.error(f"Failed to get plugin config: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve plugin configuration: {str(e)}"
        )


@router.post("/version/rollback", status_code=status.HTTP_200_OK)
async def rollback_version(
    version_data: VersionRollback,
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Rollback to previous configuration version"""
    try:
        # Create backup before rollback
        await config_manager.create_backup()

        success = await config_manager.rollback_to_version(version_data.version)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Version {version_data.version} not found in history"
            )

        logger.success(
            f"Configuration rolled back to version {version_data.version}")
        return {
            "status": "success",
            "message": f"Configuration rolled back to version {version_data.version}",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(
            f"Failed to rollback configuration: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to rollback configuration: {str(e)}"
        )


@router.post("/repair", status_code=status.HTTP_200_OK)
async def repair_config(
    config_manager: ConfigManager = Depends(get_config_manager)
):
    """Repair corrupted configuration"""
    try:
        success = await config_manager.repair_config()

        if success:
            logger.success("Configuration repaired successfully")
            return {
                "status": "success",
                "message": "Configuration repaired successfully",
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to repair configuration"
            )
    except Exception as e:
        logger.error(
            f"Failed to repair configuration: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to repair configuration: {str(e)}"
        )
