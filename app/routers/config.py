from fastapi import APIRouter, HTTPException, Depends, Query, status
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, validator
from app.config.config_manager import ConfigManager
from app.core.dependencies import get_config_manager
from fastapi.responses import JSONResponse
from pathlib import Path
import yaml
from loguru import logger
from datetime import datetime
import asyncio

# Input validation models


class ConfigUpdate(BaseModel):
    """Config update validation model"""
    config: Dict[str, Any] = Field(..., description="Configuration data")

    @validator('config')
    def validate_config(cls, v):
        required_fields = {'host', 'port', 'timeout'}
        if not all(field in v for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")
        return v


class BackupMetadata(BaseModel):
    """Backup metadata model"""
    path: str
    timestamp: datetime
    size: int
    version: str


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
        await config_manager.backup_manager.create_backup()
        logger.info("Created configuration backup before update")

        # Update with timeout
        async with asyncio.timeout(10):
            await config_manager.update_config(config_update.config)

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
            metadata = config_manager.backup_manager.get_backup_metadata(
                backup)
            backup_list.append(BackupMetadata(
                path=str(backup),
                timestamp=metadata.get("timestamp"),
                size=backup.stat().st_size,
                version=metadata.get("version", "unknown")
            ))

        logger.debug(f"Retrieved {len(backup_list)} backup(s)")
        return backup_list

    except Exception as e:
        logger.error(f"Failed to list backups: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve backups: {str(e)}"
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
        await config_manager.backup_manager.create_backup(
            suffix="pre_restore"
        )

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
