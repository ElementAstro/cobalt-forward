from fastapi import APIRouter, HTTPException, Body, Query, status
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, validator
from fastapi.responses import JSONResponse
from dataclasses import asdict
from loguru import logger
import time
import asyncio


class SystemMetrics(BaseModel):
    """System performance metrics model"""
    cpu_usage: float = Field(..., ge=0, le=100)
    memory_usage: float = Field(..., ge=0, le=100)
    disk_usage: float = Field(..., ge=0, le=100)
    network_io: Dict[str, int] = Field(...)
    request_count: int = Field(..., ge=0)


class SystemStatus(BaseModel):
    """System status response model"""
    server: Dict[str, Any]
    clients: Dict[str, Any]
    plugins: Dict[str, int]
    performance: SystemMetrics
    cache: Dict[str, Any]


class MaintenanceOperation(BaseModel):
    """Maintenance operation request model"""
    operation: str = Field(..., description="Maintenance operation type")
    params: Dict[str, Any] = Field(default_factory=dict)

    @validator('operation')
    def validate_operation(cls, v):
        allowed_ops = {'clear_cache', 'reload_plugins', 'reset_metrics'}
        if v not in allowed_ops:
            raise ValueError(
                f"Invalid operation. Must be one of: {allowed_ops}")
        return v


router = APIRouter(
    prefix="/api/system",
    tags=["system"],
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.get("/status", response_model=SystemStatus)
async def get_system_status():
    """Get current system status"""
    logger.debug("Retrieving system status")
    try:
        app = router.app
        status_data = {
            "server": {
                "status": "running",
                "uptime": time.time() - app.state.start_time,
                "version": "1.0.0"
            },
            "clients": {
                "websocket": len(app.forwarder.manager.active_connections),
                "tcp": app.forwarder.tcp_client.state.value if app.forwarder.tcp_client else "Not Connected",
                "mqtt": "Connected" if app.forwarder.mqtt_client and app.forwarder.mqtt_client.connected else "Not Connected"
            },
            "plugins": {
                "loaded": len(app.forwarder.plugin_manager.plugins),
                "active": sum(1 for p in app.forwarder.plugin_manager.plugins.values() if p.state == "ACTIVE")
            },
            "performance": app.forwarder.performance_monitor.get_current_metrics().__dict__,
            "cache": await app.forwarder.cache.raw.stats()
        }
        logger.info(
            f"System status retrieved successfully [uptime={status_data['server']['uptime']:.2f}s]")
        return SystemStatus(**status_data)

    except asyncio.TimeoutError:
        logger.error("System status retrieval timed out")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Operation timed out"
        )
    except Exception as e:
        logger.error(
            f"Failed to retrieve system status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system status"
        )


@router.post("/maintenance", status_code=status.HTTP_200_OK)
async def trigger_maintenance(operation: MaintenanceOperation):
    """Execute system maintenance operation"""
    logger.info(
        f"Starting maintenance operation [operation={operation.operation}]")

    try:
        app = router.app

        async with asyncio.timeout(30):  # Timeout for maintenance operations
            if operation.operation == "clear_cache":
                logger.debug("Starting cache cleanup")
                await app.forwarder.cache.clear()
                logger.info("Cache cleanup completed")
                return {"status": "success", "message": "Cache cleared"}

            elif operation.operation == "reload_plugins":
                logger.debug("Starting plugin reload")
                results = []
                for plugin_name in app.forwarder.plugin_manager.plugins:
                    try:
                        success = await app.forwarder.plugin_manager.reload_plugin(plugin_name)
                        results.append(
                            {"plugin": plugin_name, "success": success})
                    except Exception as e:
                        results.append(
                            {"plugin": plugin_name, "success": False, "error": str(e)})
                logger.info("Plugin reload completed")
                return {"status": "success", "results": results}

            elif operation.operation == "reset_metrics":
                logger.debug("Resetting performance metrics")
                app.forwarder.performance_monitor.reset()
                logger.info("Performance metrics reset completed")
                return {"status": "success", "message": "Metrics reset"}

    except asyncio.TimeoutError:
        logger.error(
            f"Maintenance operation timed out [operation={operation.operation}]")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Operation timed out"
        )
    except Exception as e:
        logger.error(f"Maintenance operation failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Maintenance operation failed: {str(e)}"
        )


@router.post("/config", status_code=status.HTTP_200_OK)
async def update_config(
    config_data: Dict[str, Any] = Body(..., example={"key": "value"})
):
    """Update system configuration"""
    try:
        app = router.app
        async with asyncio.timeout(10):
            command = app.forwarder.SystemConfigCommand(config_data)
            result = await app.forwarder.command_transmitter.send(command)
            logger.info("System configuration updated successfully")
            return result.result

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
        logger.error(f"Configuration update failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update configuration"
        )
