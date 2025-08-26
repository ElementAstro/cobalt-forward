"""
FastAPI application factory and configuration.

This module creates and configures the FastAPI application with
proper middleware, error handling, and route registration.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from ...application.container import Container
from ...infrastructure.config.models import ApplicationConfig
from .middleware import ErrorHandlerMiddleware, PerformanceMiddleware, SecurityMiddleware
from .routers import health, system, websocket, ssh, config, commands, plugin, core

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    Handles startup and shutdown of application components.
    """
    logger.info("Application starting up...")
    
    # Application startup is handled by the main startup process
    # The container and components are already initialized
    
    yield
    
    # Shutdown is handled by the main shutdown process
    logger.info("Application shutting down...")


def create_app(container: Container, config: ApplicationConfig) -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Args:
        container: Dependency injection container
        config: Application configuration
        
    Returns:
        Configured FastAPI application
    """
    # Create FastAPI app
    app = FastAPI(
        title=config.name,
        version=config.version,
        description="High-performance TCP-WebSocket forwarder with command dispatching capabilities",
        debug=config.debug,
        lifespan=lifespan
    )
    
    # Store container and config in app state
    app.state.container = container
    app.state.config = config
    
    # Configure middleware
    _configure_middleware(app, config)
    
    # Register routes
    _register_routes(app)
    
    logger.info(f"FastAPI application created: {config.name} v{config.version}")
    return app


def create_app_from_config() -> FastAPI:
    """
    Create app from configuration (for uvicorn reload).
    
    This is a simplified factory for development mode with auto-reload.
    """
    from ...infrastructure.config.loader import ConfigLoader
    from ...application.container import Container
    from ...application.startup import ApplicationStartup
    
    # Load configuration
    config_loader = ConfigLoader()
    config = config_loader.load_config("config.yaml")
    
    # Create container (simplified for reload)
    container = Container()
    
    return create_app(container, config)


def _configure_middleware(app: FastAPI, config: ApplicationConfig) -> None:
    """Configure application middleware."""
    
    # Security middleware
    app.add_middleware(SecurityMiddleware, config=config.security)
    
    # Performance monitoring middleware
    if config.performance.enabled:
        app.add_middleware(PerformanceMiddleware, config=config.performance)
    
    # Error handling middleware
    app.add_middleware(ErrorHandlerMiddleware)
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.security.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Trusted host middleware (for production)
    if config.environment == "production":
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=["localhost", "127.0.0.1", config.websocket.host]
        )
    
    logger.debug("Middleware configured")


def _register_routes(app: FastAPI) -> None:
    """Register API routes."""
    
    # Health check routes
    app.include_router(
        health.router,
        prefix="/health",
        tags=["health"]
    )
    
    # System management routes
    app.include_router(
        system.router,
        prefix="/api/v1/system",
        tags=["system"]
    )
    
    # WebSocket routes
    app.include_router(
        websocket.router,
        prefix="/ws",
        tags=["websocket"]
    )

    # SSH management routes
    app.include_router(
        ssh.router,
        tags=["ssh"]
    )

    # Configuration management routes
    app.include_router(
        config.router,
        tags=["configuration"]
    )

    # Command management routes
    app.include_router(
        commands.router,
        tags=["commands"]
    )

    # Plugin management routes
    app.include_router(
        plugin.router,
        tags=["plugins"]
    )

    # Core system routes
    app.include_router(
        core.router,
        tags=["core"]
    )
    
    # Root endpoint
    @app.get("/", tags=["root"])
    async def root():
        """Root endpoint with basic application information."""
        return {
            "name": app.title,
            "version": app.version,
            "status": "running",
            "docs_url": "/docs",
            "health_url": "/health"
        }
    
    logger.debug("Routes registered")
