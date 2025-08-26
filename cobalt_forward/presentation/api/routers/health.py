"""
Health check API endpoints.

This module provides health check endpoints for monitoring
application and component status.
"""

from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Depends

from ....application.container import IContainer
from ....infrastructure.config.models import ApplicationConfig
from ....core.interfaces.lifecycle import IComponent
from ..dependencies import get_container, get_config

router = APIRouter()


@router.get("/")
async def health_check(
    container: IContainer = Depends(get_container),
    config: ApplicationConfig = Depends(get_config)
) -> Dict[str, Any]:
    """
    Basic health check endpoint.
    
    Returns overall application health status.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "application": {
            "name": config.name,
            "version": config.version,
            "environment": config.environment
        }
    }


@router.get("/detailed")
async def detailed_health_check(
    container: IContainer = Depends(get_container),
    config: ApplicationConfig = Depends(get_config)
) -> Dict[str, Any]:
    """
    Detailed health check with component status.
    
    Returns health status for all registered components.
    """
    components_health = {}
    overall_healthy = True
    
    # Check health of all registered components
    registrations = container.get_registrations()
    
    for service_type, registration in registrations.items():
        component_name = service_type.__name__
        
        try:
            component = container.resolve(service_type)
            
            if isinstance(component, IComponent):
                health_info = await component.check_health()
                components_health[component_name] = health_info
                
                if not health_info.get("healthy", True):
                    overall_healthy = False
            else:
                # For non-component services, just check if they exist
                components_health[component_name] = {
                    "healthy": True,
                    "status": "available",
                    "details": {"type": "service"}
                }
        
        except Exception as e:
            components_health[component_name] = {
                "healthy": False,
                "status": "error",
                "details": {"error": str(e)}
            }
            overall_healthy = False
    
    return {
        "status": "healthy" if overall_healthy else "degraded",
        "timestamp": datetime.utcnow().isoformat(),
        "application": {
            "name": config.name,
            "version": config.version,
            "environment": config.environment
        },
        "components": components_health
    }


@router.get("/ready")
async def readiness_check(
    container: IContainer = Depends(get_container)
) -> Dict[str, Any]:
    """
    Readiness check endpoint.
    
    Indicates if the application is ready to serve requests.
    """
    # Check if essential services are available
    essential_services = [
        "ConfigManager",
        "LoggingManager"
    ]
    
    ready = True
    missing_services = []
    
    registrations = container.get_registrations()
    available_services = [st.__name__ for st in registrations.keys()]
    
    for service in essential_services:
        if service not in available_services:
            ready = False
            missing_services.append(service)
    
    return {
        "ready": ready,
        "timestamp": datetime.utcnow().isoformat(),
        "missing_services": missing_services
    }


@router.get("/live")
async def liveness_check() -> Dict[str, Any]:
    """
    Liveness check endpoint.
    
    Simple endpoint to indicate the application is running.
    """
    return {
        "alive": True,
        "timestamp": datetime.utcnow().isoformat()
    }
