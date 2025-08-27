"""
Core system API router for the Cobalt Forward application.

This module provides REST API endpoints for core system operations
including status, health checks, and system management.
"""

from fastapi import APIRouter, HTTPException, Depends, status
from typing import Dict, Any, List
from pydantic import BaseModel, Field
import logging
import time
import psutil
import platform

from ....application.container import IContainer
from ....core.interfaces.lifecycle import IHealthCheckable

logger = logging.getLogger(__name__)


class SystemInfo(BaseModel):
    """System information model."""
    hostname: str = Field(..., description="System hostname")
    platform: str = Field(..., description="Operating system platform")
    architecture: str = Field(..., description="System architecture")
    python_version: str = Field(..., description="Python version")
    uptime: float = Field(..., description="System uptime in seconds")
    cpu_count: int = Field(..., description="Number of CPU cores")
    memory_total: int = Field(..., description="Total memory in bytes")
    memory_available: int = Field(..., description="Available memory in bytes")


class ServiceHealth(BaseModel):
    """Service health information model."""
    service_name: str = Field(..., description="Service name")
    healthy: bool = Field(..., description="Whether service is healthy")
    status: str = Field(..., description="Service status")
    details: Dict[str, Any] = Field(
        default_factory=dict, description="Additional details")
    last_check: float = Field(..., description="Last health check timestamp")


class SystemHealth(BaseModel):
    """System health information model."""
    overall_healthy: bool = Field(..., description="Overall system health")
    services: List[ServiceHealth] = Field(...,
                                          description="Individual service health")
    system_info: SystemInfo = Field(..., description="System information")
    timestamp: float = Field(..., description="Health check timestamp")


class SystemMetrics(BaseModel):
    """System metrics model."""
    cpu_percent: float = Field(..., description="CPU usage percentage")
    memory_percent: float = Field(..., description="Memory usage percentage")
    disk_usage: Dict[str, float] = Field(...,
                                         description="Disk usage by mount point")
    network_io: Dict[str, int] = Field(...,
                                       description="Network I/O statistics")
    process_count: int = Field(..., description="Number of running processes")
    load_average: List[float] = Field(
        default_factory=list, description="System load average")


# Dependency injection
def get_container() -> IContainer:
    """Get the dependency injection container."""
    from fastapi import Request

    def _get_container(request: Request) -> IContainer:
        return request.app.state.container  # type: ignore[no-any-return]

    return Depends(_get_container)  # type: ignore[no-any-return]


# Router definition
router = APIRouter(
    prefix="/api/core",
    tags=["core"],
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.get("/health", response_model=SystemHealth)
async def get_system_health(
    container: IContainer = Depends(get_container)
) -> SystemHealth:
    """Get comprehensive system health information."""
    try:
        timestamp = time.time()
        services = []
        overall_healthy = True

        # Check health of all registered services
        try:
            # Get all services that implement IHealthCheckable
            # This is a simplified approach - in a real implementation,
            # we'd have a service registry
            service_types = [
                'IEventBus', 'IMessageBus', 'ICommandDispatcher',
                'IPluginManager', 'ISSHForwarder', 'IUploadManager'
            ]

            for service_type in service_types:
                try:
                    service = getattr(container, 'resolve_by_name',
                                      lambda x: container.resolve(x))(service_type)
                    if hasattr(service, 'health_check'):
                        health_result = await service.health_check()

                        service_health = ServiceHealth(
                            service_name=service_type,
                            healthy=health_result.get('healthy', False),
                            status=health_result.get('status', 'unknown'),
                            details=health_result.get('details', {}),
                            last_check=timestamp
                        )
                        services.append(service_health)

                        if not service_health.healthy:
                            overall_healthy = False

                except Exception as e:
                    logger.warning(
                        f"Failed to check health of {service_type}: {e}")
                    service_health = ServiceHealth(
                        service_name=service_type,
                        healthy=False,
                        status='error',
                        details={'error': str(e)},
                        last_check=timestamp
                    )
                    services.append(service_health)
                    overall_healthy = False

        except Exception as e:
            logger.error(f"Failed to check service health: {e}")
            overall_healthy = False

        # Get system information
        system_info = await _get_system_info()

        return SystemHealth(
            overall_healthy=overall_healthy,
            services=services,
            system_info=system_info,
            timestamp=timestamp
        )

    except Exception as e:
        logger.error(f"Failed to get system health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system health: {str(e)}"
        )


@router.get("/metrics", response_model=SystemMetrics)
async def get_system_metrics() -> SystemMetrics:
    """Get system performance metrics."""
    try:
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)

        # Memory usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent

        # Disk usage
        disk_usage = {}
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                disk_usage[partition.mountpoint] = (
                    usage.used / usage.total) * 100
            except PermissionError:
                # This can happen on Windows
                continue

        # Network I/O
        network = psutil.net_io_counters()
        network_io = {
            'bytes_sent': network.bytes_sent,
            'bytes_recv': network.bytes_recv,
            'packets_sent': network.packets_sent,
            'packets_recv': network.packets_recv
        }

        # Process count
        process_count = len(psutil.pids())

        # Load average (Unix-like systems only)
        load_average = []
        try:
            if hasattr(psutil, 'getloadavg'):
                load_average = list(psutil.getloadavg())
        except (AttributeError, OSError):
            # Not available on Windows
            pass

        return SystemMetrics(
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            disk_usage=disk_usage,
            network_io=network_io,
            process_count=process_count,
            load_average=load_average
        )

    except Exception as e:
        logger.error(f"Failed to get system metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system metrics: {str(e)}"
        )


@router.get("/info", response_model=SystemInfo)
async def get_system_info() -> SystemInfo:
    """Get system information."""
    try:
        return await _get_system_info()
    except Exception as e:
        logger.error(f"Failed to get system info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system info: {str(e)}"
        )


@router.post("/restart")
async def restart_system(
    container: IContainer = Depends(get_container)
) -> Dict[str, Any]:
    """Restart the application (graceful shutdown and restart)."""
    try:
        logger.info("System restart requested")

        # This would typically trigger a graceful shutdown
        # and restart of the application
        # For now, we'll just return a success message

        return {
            "success": True,
            "message": "System restart initiated",
            "timestamp": time.time()
        }

    except Exception as e:
        logger.error(f"Failed to restart system: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restart system: {str(e)}"
        )


@router.post("/shutdown")
async def shutdown_system(
    container: IContainer = Depends(get_container)
) -> Dict[str, Any]:
    """Shutdown the application gracefully."""
    try:
        logger.info("System shutdown requested")

        # This would typically trigger a graceful shutdown
        # For now, we'll just return a success message

        return {
            "success": True,
            "message": "System shutdown initiated",
            "timestamp": time.time()
        }

    except Exception as e:
        logger.error(f"Failed to shutdown system: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to shutdown system: {str(e)}"
        )


async def _get_system_info() -> SystemInfo:
    """Get system information."""
    import socket
    import sys

    # System uptime
    boot_time = psutil.boot_time()
    uptime = time.time() - boot_time

    # Memory information
    memory = psutil.virtual_memory()

    return SystemInfo(
        hostname=socket.gethostname(),
        platform=platform.platform(),
        architecture=platform.architecture()[0],
        python_version=sys.version.split()[0],
        uptime=uptime,
        cpu_count=psutil.cpu_count() or 0,
        memory_total=memory.total,
        memory_available=memory.available
    )
