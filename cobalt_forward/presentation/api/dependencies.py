"""
FastAPI dependency injection utilities.

This module provides dependency injection functions for FastAPI routes
to access application services and components.
"""

from typing import Any, Dict, Type, TypeVar

from fastapi import Depends, HTTPException, Request, status

from ...application.container import Container, IContainer
from ...infrastructure.config.models import ApplicationConfig
from ...core.interfaces.messaging import IEventBus, IMessageBus
from ...core.interfaces.commands import ICommandDispatcher
from ...core.interfaces.plugins import IPluginManager

T = TypeVar('T')


def get_container(request: Request) -> IContainer:
    """
    Get the dependency injection container from the request.
    
    Args:
        request: FastAPI request object
        
    Returns:
        Dependency injection container
        
    Raises:
        HTTPException: If container is not available
    """
    if not hasattr(request.app.state, "container"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Application container not available"
        )
    
    return request.app.state.container


def get_config(request: Request) -> ApplicationConfig:
    """
    Get the application configuration from the request.
    
    Args:
        request: FastAPI request object
        
    Returns:
        Application configuration
        
    Raises:
        HTTPException: If configuration is not available
    """
    if not hasattr(request.app.state, "config"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Application configuration not available"
        )
    
    return request.app.state.config


def get_component(service_type: Type[T]) -> Any:
    """
    Create a dependency function to get a specific component type.
    
    Args:
        service_type: Type of service to resolve
        
    Returns:
        Dependency function that resolves the service
    """
    def _get_component(container: IContainer = Depends(get_container)) -> T:
        try:
            return container.resolve(service_type)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Service {service_type.__name__} not available: {str(e)}"
            )
    
    return _get_component


# Pre-defined dependency functions for common services
get_event_bus = get_component(IEventBus)
get_message_bus = get_component(IMessageBus)
get_command_dispatcher = get_component(ICommandDispatcher)
get_plugin_manager = get_component(IPluginManager)


def require_api_key(request: Request, config: ApplicationConfig = Depends(get_config)) -> bool:
    """
    Dependency to require API key authentication.
    
    Args:
        request: FastAPI request object
        config: Application configuration
        
    Returns:
        True if authentication is successful
        
    Raises:
        HTTPException: If authentication fails
    """
    if not config.security.api_key_required:
        return True
    
    api_key = request.headers.get(config.security.api_key_header)
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "ApiKey"}
        )
    
    # In a real implementation, you would validate the API key
    # against a database or configuration
    # For now, we'll just check if it's not empty
    if not api_key.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"}
        )
    
    return True


def get_user_context(request: Request) -> Dict[str, Any]:
    """
    Get user context from request headers.
    
    Args:
        request: FastAPI request object
        
    Returns:
        User context dictionary
    """
    return {
        "user_id": request.headers.get("X-User-ID"),
        "session_id": request.headers.get("X-Session-ID"),
        "client_ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("User-Agent"),
        "correlation_id": request.headers.get("X-Correlation-ID")
    }
