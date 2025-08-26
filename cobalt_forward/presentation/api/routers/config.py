"""
Configuration management API router for the Cobalt Forward application.

This module provides REST API endpoints for managing application configuration,
including reading, updating, backup, and restore operations.
"""

from fastapi import APIRouter, HTTPException, Depends, status
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, field_validator
import logging
from datetime import datetime

from ....infrastructure.config.manager import ConfigManager
from ....application.container import IContainer

logger = logging.getLogger(__name__)


class ConfigUpdateRequest(BaseModel):
    """Configuration update request model."""
    config: Dict[str, Any] = Field(..., description="Configuration data")
    validate_only: bool = Field(default=False, description="Only validate, don't apply")

    @field_validator('config')
    @classmethod
    def validate_config(cls, v):
        """Validate configuration structure."""
        if not isinstance(v, dict):
            raise ValueError("Configuration must be a dictionary")
        return v


class ConfigBackupRequest(BaseModel):
    """Configuration backup request model."""
    description: Optional[str] = Field(None, description="Backup description")
    encrypt: bool = Field(default=False, description="Encrypt backup")


class ConfigRestoreRequest(BaseModel):
    """Configuration restore request model."""
    backup_id: str = Field(..., description="Backup identifier")
    force: bool = Field(default=False, description="Force restore without validation")


class ConfigBackupInfo(BaseModel):
    """Configuration backup information model."""
    backup_id: str = Field(..., description="Backup identifier")
    created_at: datetime = Field(..., description="Creation timestamp")
    description: Optional[str] = Field(None, description="Backup description")
    size: int = Field(..., description="Backup size in bytes")
    encrypted: bool = Field(..., description="Whether backup is encrypted")
    version: str = Field(..., description="Configuration version")


class ConfigValidationResult(BaseModel):
    """Configuration validation result model."""
    valid: bool = Field(..., description="Whether configuration is valid")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")


# Dependency injection
def get_container() -> IContainer:
    """Get the dependency injection container."""
    from fastapi import Request
    
    def _get_container(request: Request) -> IContainer:
        return request.app.state.container
    
    return Depends(_get_container)


def get_config_manager(container: IContainer = get_container()) -> ConfigManager:
    """Get configuration manager from container."""
    return container.resolve(ConfigManager)


# Router definition
router = APIRouter(
    prefix="/api/config",
    tags=["configuration"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Configuration not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error"}
    }
)


@router.get("/", response_model=Dict[str, Any])
async def get_configuration(
    config_manager: ConfigManager = Depends(get_config_manager)
) -> Dict[str, Any]:
    """Get current configuration."""
    try:
        config = config_manager.get_config()
        return {
            "success": True,
            "config": config,
            "version": getattr(config_manager, 'version', '1.0.0'),
            "last_modified": getattr(config_manager, 'last_modified', None)
        }
    except Exception as e:
        logger.error(f"Failed to get configuration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get configuration: {str(e)}"
        )


@router.put("/", response_model=Dict[str, Any])
async def update_configuration(
    request: ConfigUpdateRequest,
    config_manager: ConfigManager = Depends(get_config_manager)
) -> Dict[str, Any]:
    """Update configuration."""
    try:
        if request.validate_only:
            # Only validate the configuration
            validation_result = await _validate_configuration(request.config, config_manager)
            return {
                "success": True,
                "validation": validation_result.dict(),
                "message": "Configuration validation completed"
            }
        
        # Update configuration
        await config_manager.update_config(request.config)
        
        return {
            "success": True,
            "message": "Configuration updated successfully",
            "version": getattr(config_manager, 'version', '1.0.0')
        }
        
    except Exception as e:
        logger.error(f"Failed to update configuration: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update configuration: {str(e)}"
        )


@router.post("/validate", response_model=ConfigValidationResult)
async def validate_configuration(
    request: ConfigUpdateRequest,
    config_manager: ConfigManager = Depends(get_config_manager)
) -> ConfigValidationResult:
    """Validate configuration without applying changes."""
    try:
        return await _validate_configuration(request.config, config_manager)
    except Exception as e:
        logger.error(f"Failed to validate configuration: {e}")
        return ConfigValidationResult(
            valid=False,
            errors=[f"Validation failed: {str(e)}"]
        )


@router.post("/backup", response_model=ConfigBackupInfo, status_code=status.HTTP_201_CREATED)
async def create_configuration_backup(
    request: ConfigBackupRequest,
    config_manager: ConfigManager = Depends(get_config_manager)
) -> ConfigBackupInfo:
    """Create a configuration backup."""
    try:
        # Check if config manager has backup functionality
        if not hasattr(config_manager, 'create_backup'):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Configuration backup not supported"
            )
        
        backup_info = await config_manager.create_backup(
            description=request.description,
            encrypt=request.encrypt
        )
        
        return ConfigBackupInfo(
            backup_id=backup_info['backup_id'],
            created_at=backup_info['created_at'],
            description=backup_info.get('description'),
            size=backup_info['size'],
            encrypted=backup_info['encrypted'],
            version=backup_info['version']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create configuration backup: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create backup: {str(e)}"
        )


@router.get("/backups", response_model=List[ConfigBackupInfo])
async def list_configuration_backups(
    config_manager: ConfigManager = Depends(get_config_manager)
) -> List[ConfigBackupInfo]:
    """List all configuration backups."""
    try:
        if not hasattr(config_manager, 'list_backups'):
            return []
        
        backups = await config_manager.list_backups()
        
        return [
            ConfigBackupInfo(
                backup_id=backup['backup_id'],
                created_at=backup['created_at'],
                description=backup.get('description'),
                size=backup['size'],
                encrypted=backup['encrypted'],
                version=backup['version']
            )
            for backup in backups
        ]
        
    except Exception as e:
        logger.error(f"Failed to list configuration backups: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list backups: {str(e)}"
        )


@router.post("/restore")
async def restore_configuration(
    request: ConfigRestoreRequest,
    config_manager: ConfigManager = Depends(get_config_manager)
) -> Dict[str, Any]:
    """Restore configuration from backup."""
    try:
        if not hasattr(config_manager, 'restore_backup'):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Configuration restore not supported"
            )
        
        await config_manager.restore_backup(
            backup_id=request.backup_id,
            force=request.force
        )
        
        return {
            "success": True,
            "message": f"Configuration restored from backup {request.backup_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to restore configuration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restore configuration: {str(e)}"
        )


@router.delete("/backups/{backup_id}")
async def delete_configuration_backup(
    backup_id: str,
    config_manager: ConfigManager = Depends(get_config_manager)
) -> Dict[str, Any]:
    """Delete a configuration backup."""
    try:
        if not hasattr(config_manager, 'delete_backup'):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Configuration backup deletion not supported"
            )
        
        await config_manager.delete_backup(backup_id)
        
        return {
            "success": True,
            "message": f"Configuration backup {backup_id} deleted"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete configuration backup: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete backup: {str(e)}"
        )


async def _validate_configuration(config: Dict[str, Any], config_manager: ConfigManager) -> ConfigValidationResult:
    """Validate configuration data."""
    errors = []
    warnings = []
    
    try:
        # Basic structure validation
        if not isinstance(config, dict):
            errors.append("Configuration must be a dictionary")
            return ConfigValidationResult(valid=False, errors=errors)
        
        # Check for required fields (basic validation)
        required_fields = ['name', 'version']
        for field in required_fields:
            if field not in config:
                warnings.append(f"Recommended field '{field}' is missing")
        
        # Additional validation can be added here
        # For example, validate specific configuration sections
        
        return ConfigValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
        
    except Exception as e:
        errors.append(f"Validation error: {str(e)}")
        return ConfigValidationResult(valid=False, errors=errors)
