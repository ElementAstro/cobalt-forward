"""
Main entry point for the Cobalt Forward application.

This module provides the command-line interface and application startup logic
for the new modular architecture.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional, Any

import aiohttp
import typer
import uvicorn

from .application.container import Container
from .application.startup import ApplicationStartup
from .infrastructure.config.loader import ConfigLoader
from .infrastructure.config.models import ApplicationConfig
from .infrastructure.logging.setup import setup_logging
from .presentation.api.app import create_app

# Create CLI application
cli = typer.Typer(
    name="cobalt-forward",
    help="High-performance TCP-WebSocket forwarder with command dispatching capabilities"
)

logger = logging.getLogger(__name__)


@cli.command()
def start(
    config_file: Optional[str] = typer.Option(
        None, "--config", "-c", help="Configuration file path"
    ),
    host: Optional[str] = typer.Option(
        None, "--host", help="Server host address"
    ),
    port: Optional[int] = typer.Option(
        None, "--port", "-p", help="Server port"
    ),
    log_level: Optional[str] = typer.Option(
        None, "--log-level", help="Logging level"
    ),
    debug: bool = typer.Option(
        False, "--debug", help="Enable debug mode"
    )
) -> None:
    """Start the Cobalt Forward server."""

    # Load configuration
    config_loader = ConfigLoader()
    config = config_loader.load_config(config_file)

    # Override with command line arguments
    if host:
        config.websocket.host = host
    if port:
        config.websocket.port = port
    if log_level:
        config.logging.level = log_level.upper()
    if debug:
        config.debug = True
        config.logging.level = "DEBUG"

    # Setup logging
    setup_logging(config.logging)

    logger.info(f"Starting {config.name} v{config.version}")
    logger.info(f"Environment: {config.environment}")
    logger.info(f"Debug mode: {config.debug}")

    # Create and run application
    try:
        asyncio.run(run_application(config))
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        sys.exit(1)


@cli.command()
def dev(
    config_file: Optional[str] = typer.Option(
        "config.yaml", "--config", "-c", help="Configuration file path"
    ),
    port: int = typer.Option(
        8000, "--port", "-p", help="Server port"
    ),
    reload: bool = typer.Option(
        True, "--reload/--no-reload", help="Enable auto-reload"
    )
) -> None:
    """Start the server in development mode with auto-reload."""

    # Load configuration
    config_loader = ConfigLoader()
    config = config_loader.load_config(config_file)

    # Set development mode
    config.debug = True
    config.environment = "development"
    config.logging.level = "DEBUG"
    config.websocket.port = port

    # Setup logging
    setup_logging(config.logging)

    logger.info(f"Starting {config.name} in development mode")

    # Use uvicorn with reload for development
    uvicorn.run(
        "cobalt_forward.presentation.api.app:create_app_from_config",
        host=config.websocket.host,
        port=config.websocket.port,
        reload=reload,
        reload_dirs=["cobalt_forward"],
        log_level=config.logging.level.lower(),
        access_log=True
    )


@cli.command()
def init_config(
    output: str = typer.Option(
        "config.yaml", "--output", "-o", help="Output configuration file"
    ),
    format: str = typer.Option(
        "yaml", "--format", "-f", help="Configuration format (yaml/json)"
    )
) -> None:
    """Generate a default configuration file."""

    config = ApplicationConfig()
    config_loader = ConfigLoader()

    try:
        config_loader.save_config(config, output, format)
        typer.echo(f"Default configuration saved to {output}")
    except Exception as e:
        typer.echo(f"Error saving configuration: {e}", err=True)
        sys.exit(1)


@cli.command()
def validate_config(
    config_file: str = typer.Argument(...,
                                      help="Configuration file to validate")
) -> None:
    """Validate a configuration file."""

    config_loader = ConfigLoader()

    try:
        config = config_loader.load_config(config_file)
        typer.echo(f"Configuration file {config_file} is valid")
        typer.echo(f"Application: {config.name} v{config.version}")
        typer.echo(f"Environment: {config.environment}")
    except Exception as e:
        typer.echo(f"Configuration validation failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def health_check(
    host: str = typer.Option("localhost", "--host", help="Server host"),
    port: int = typer.Option(8000, "--port", help="Server port"),
    timeout: float = typer.Option(10.0, "--timeout", help="Request timeout")
) -> None:
    """Check the health of a running server."""

    async def check_health() -> bool:
        url = f"http://{host}:{port}/health"
        timeout_config = aiohttp.ClientTimeout(total=timeout)

        try:
            async with aiohttp.ClientSession(timeout=timeout_config) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        typer.echo(
                            f"Server is healthy: {data.get('status', 'unknown')}")
                        return True
                    else:
                        typer.echo(f"Server returned status {response.status}")
                        return False
        except Exception as e:
            typer.echo(f"Health check failed: {e}")
            return False

    result = asyncio.run(check_health())
    if not result:
        sys.exit(1)


async def run_application(config: ApplicationConfig) -> None:
    """
    Run the application with the given configuration.

    Args:
        config: Application configuration
    """
    # Create dependency injection container
    container = Container()

    # Create application startup manager
    startup = ApplicationStartup(container)

    try:
        # Configure services
        await startup.configure_services(config.to_dict())

        # Start application components
        await startup.start_application()

        # Create and configure FastAPI app
        app = create_app(container, config)

        # Run the server
        server_config = uvicorn.Config(
            app=app,
            host=config.websocket.host,
            port=config.websocket.port,
            log_level=config.logging.level.lower(),
            access_log=config.debug,
            reload=False  # Reload is handled by uvicorn CLI in dev mode
        )

        server = uvicorn.Server(server_config)

        # Setup graceful shutdown
        import signal

        def signal_handler(signum: int, frame: Any) -> None:
            logger.info(f"Received signal {signum}, initiating shutdown...")
            asyncio.create_task(shutdown_handler(startup))

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start server
        await server.serve()

    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        # Ensure cleanup
        await startup.stop_application()


async def shutdown_handler(startup: ApplicationStartup) -> None:
    """Handle graceful shutdown."""
    try:
        await startup.stop_application()
        logger.info("Application shutdown completed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


def main() -> None:
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
