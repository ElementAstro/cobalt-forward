from fastapi import APIRouter, HTTPException, Body, status, Depends
from typing import Dict, Any
from pydantic import BaseModel, Field, field_validator
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

    @field_validator('operation')
    def validate_operation(cls, v):
        allowed_ops = {'clear_cache', 'reload_plugins', 'reset_metrics'}
        if v not in allowed_ops:
            raise ValueError(
                f"Invalid operation. Must be one of: {allowed_ops}")
        return v


# Dependency injection functions
def get_app():
    """Get the FastAPI application instance"""
    from fastapi import Request

    def _get_app(request: Request):
        return request.app
    return Depends(_get_app)


def get_forwarder():
    """Get forwarder from app state"""
    def _get_forwarder(app=get_app()):
        return app.state.forwarder
    return Depends(_get_forwarder)


router = APIRouter(
    prefix="/api/system",
    tags=["system"],
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.get("/status", response_model=SystemStatus)
async def get_system_status(
    app=get_app(),
    forwarder=get_forwarder()
):
    """Get current system status"""
    logger.debug("Retrieving system status")
    try:
        # Get basic server info
        server_info = {
            "status": "running",
            "uptime": time.time() - getattr(app.state, 'start_time', time.time()),
            "version": "1.0.0"
        }

        # Get client connection info
        clients_info = {
            "websocket": len(getattr(forwarder.manager, 'active_connections', [])) if hasattr(forwarder, 'manager') else 0,
            "tcp": getattr(forwarder.tcp_client, 'state', {}).get('value', "Not Connected") if hasattr(forwarder, 'tcp_client') and forwarder.tcp_client else "Not Connected",
            "mqtt": "Connected" if (hasattr(forwarder, 'mqtt_client') and forwarder.mqtt_client and getattr(forwarder.mqtt_client, 'connected', False)) else "Not Connected"
        }

        # Get plugin info
        plugins_info = {
            "loaded": len(getattr(forwarder.plugin_manager, 'plugins', {})) if hasattr(forwarder, 'plugin_manager') else 0,
            "active": sum(1 for p in getattr(forwarder.plugin_manager, 'plugins', {}).values() if getattr(p, 'state', None) == "ACTIVE") if hasattr(forwarder, 'plugin_manager') else 0
        }

        # Get performance metrics
        if hasattr(forwarder, 'performance_monitor') and hasattr(forwarder.performance_monitor, 'get_current_metrics'):
            performance_metrics = forwarder.performance_monitor.get_current_metrics()
            performance_dict = performance_metrics.__dict__ if hasattr(performance_metrics, '__dict__') else {
                "cpu_usage": 0.0,
                "memory_usage": 0.0,
                "disk_usage": 0.0,
                "network_io": {"bytes_sent": 0, "bytes_received": 0},
                "request_count": 0
            }
        else:
            performance_dict = {
                "cpu_usage": 0.0,
                "memory_usage": 0.0,
                "disk_usage": 0.0,
                "network_io": {"bytes_sent": 0, "bytes_received": 0},
                "request_count": 0
            }

        # Get cache info
        cache_info = {}
        if hasattr(forwarder, 'cache') and hasattr(forwarder.cache, 'raw') and hasattr(forwarder.cache.raw, 'stats'):
            try:
                cache_info = await forwarder.cache.raw.stats()
            except Exception:
                cache_info = {"status": "unavailable"}
        else:
            cache_info = {"status": "not_configured"}

        status_data = {
            "server": server_info,
            "clients": clients_info,
            "plugins": plugins_info,
            "performance": performance_dict,
            "cache": cache_info
        }

        logger.info(
            f"System status retrieved successfully [uptime={server_info['uptime']:.2f}s]")
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
async def trigger_maintenance(
    operation: MaintenanceOperation,
    forwarder=get_forwarder()
):
    """Execute system maintenance operation"""
    logger.info(
        f"Starting maintenance operation [operation={operation.operation}]")

    try:
        async with asyncio.timeout(30):  # Timeout for maintenance operations
            if operation.operation == "clear_cache":
                logger.debug("Starting cache cleanup")
                if hasattr(forwarder, 'cache') and hasattr(forwarder.cache, 'clear'):
                    await forwarder.cache.clear()
                    logger.info("Cache cleanup completed")
                    return {"status": "success", "message": "Cache cleared"}
                else:
                    return {"status": "info", "message": "Cache not available"}

            elif operation.operation == "reload_plugins":
                logger.debug("Starting plugin reload")
                results = []
                if hasattr(forwarder, 'plugin_manager') and hasattr(forwarder.plugin_manager, 'plugins'):
                    for plugin_name in forwarder.plugin_manager.plugins:
                        try:
                            if hasattr(forwarder.plugin_manager, 'reload_plugin'):
                                success = await forwarder.plugin_manager.reload_plugin(plugin_name)
                                results.append(
                                    {"plugin": plugin_name, "success": success})
                            else:
                                results.append(
                                    {"plugin": plugin_name, "success": False, "error": "Reload method not available"})
                        except Exception as e:
                            results.append(
                                {"plugin": plugin_name, "success": False, "error": str(e)})
                else:
                    results.append({"error": "Plugin manager not available"})

                logger.info("Plugin reload completed")
                return {"status": "success", "results": results}

            elif operation.operation == "reset_metrics":
                logger.debug("Resetting performance metrics")
                if hasattr(forwarder, 'performance_monitor') and hasattr(forwarder.performance_monitor, 'reset'):
                    forwarder.performance_monitor.reset()
                    logger.info("Performance metrics reset completed")
                    return {"status": "success", "message": "Metrics reset"}
                else:
                    return {"status": "info", "message": "Performance monitor not available"}

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
    config_data: Dict[str, Any] = Body(..., example={"key": "value"}),
    forwarder=get_forwarder()
):
    """Update system configuration"""
    try:
        async with asyncio.timeout(10):
            if hasattr(forwarder, 'SystemConfigCommand') and hasattr(forwarder, 'command_transmitter'):
                command = forwarder.SystemConfigCommand(config_data)
                result = await forwarder.command_transmitter.send(command)
                logger.info("System configuration updated successfully")
                return getattr(result, 'result', {"status": "success", "message": "Configuration updated"})
            else:
                # Fallback if command system is not available
                logger.info(
                    "Configuration received but command system not available")
                return {"status": "info", "message": "Configuration received but command system not available", "config": config_data}

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


@router.get("/health")
async def get_system_health(
    forwarder=get_forwarder()
):
    """Get system health status"""
    try:
        health_status = {
            "status": "healthy",
            "components": {},
            "timestamp": time.time()
        }

        # Check forwarder health
        if forwarder:
            health_status["components"]["forwarder"] = "healthy"

            # Check plugin manager
            if hasattr(forwarder, 'plugin_manager'):
                health_status["components"]["plugin_manager"] = "healthy"
            else:
                health_status["components"]["plugin_manager"] = "not_available"

            # Check cache
            if hasattr(forwarder, 'cache'):
                health_status["components"]["cache"] = "healthy"
            else:
                health_status["components"]["cache"] = "not_available"

            # Check performance monitor
            if hasattr(forwarder, 'performance_monitor'):
                health_status["components"]["performance_monitor"] = "healthy"
            else:
                health_status["components"]["performance_monitor"] = "not_available"
        else:
            health_status["status"] = "unhealthy"
            health_status["components"]["forwarder"] = "not_available"

        return health_status

    except Exception as e:
        logger.error(f"Failed to get system health: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get system health"
        )
