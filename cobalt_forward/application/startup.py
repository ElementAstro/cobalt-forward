"""
Application startup and configuration logic.

This module handles the initialization and configuration of all application
components, including dependency registration and service startup.
"""

import logging
from typing import Any, Dict, List, Optional

from .container import Container, IContainer, ServiceLifetime
from ..core.interfaces.lifecycle import IStartable, IStoppable, IComponent
from ..core.interfaces.messaging import IEventBus, IMessageBus
from ..core.interfaces.commands import ICommandDispatcher
from ..core.interfaces.plugins import IPluginManager
from ..infrastructure.config.manager import ConfigManager

logger = logging.getLogger(__name__)


class ApplicationStartup:
    """
    Manages application startup and service configuration.

    This class is responsible for registering services with the DI container,
    configuring components, and managing the startup sequence.
    """

    def __init__(self, container: IContainer) -> None:
        self._container = container
        self._started_components: List[IComponent] = []
        self._startup_order: List[str] = [
            'config_manager',
            'event_bus',
            'message_bus',
            'command_dispatcher',
            'plugin_manager',
            'ssh_forwarder',
            'upload_manager',
            'tcp_client',
            'websocket_manager',
            'ssh_client',
            'ftp_server'
        ]

    async def configure_services(self, config: Dict[str, Any]) -> None:
        """
        Configure and register all application services.

        Args:
            config: Application configuration dictionary
        """
        logger.info("Configuring application services...")

        # Register configuration as singleton
        self._container.register_instance(dict, config)

        # Register core services
        await self._register_core_services(config)

        # Register infrastructure services
        await self._register_infrastructure_services(config)

        # Register application services
        await self._register_application_services(config)

        logger.info("Service configuration completed")

    async def start_application(self) -> None:
        """
        Start all application components in the correct order.

        This method resolves and starts components according to their
        dependencies and the defined startup order.
        """
        logger.info("Starting application components...")

        for component_name in self._startup_order:
            try:
                component = self._get_component_by_name(component_name)
                if component and isinstance(component, IStartable):
                    logger.debug(f"Starting component: {component_name}")
                    await component.start()

                    if isinstance(component, IComponent):
                        self._started_components.append(component)

                    logger.info(f"Started component: {component_name}")

            except Exception as e:
                logger.error(
                    f"Failed to start component {component_name}: {e}")
                # Attempt to stop already started components
                await self._stop_started_components()
                raise

        logger.info("Application startup completed successfully")

    async def stop_application(self) -> None:
        """
        Stop all application components in reverse order.
        """
        logger.info("Stopping application components...")

        # Stop components in reverse order
        for component in reversed(self._started_components):
            try:
                if isinstance(component, IStoppable):
                    logger.debug(f"Stopping component: {component.name}")
                    await component.stop()
                    logger.info(f"Stopped component: {component.name}")

            except Exception as e:
                logger.error(f"Error stopping component {component.name}: {e}")
                # Continue stopping other components

        self._started_components.clear()
        logger.info("Application shutdown completed")

    async def _register_core_services(self, config: Dict[str, Any]) -> None:
        """Register core domain services."""
        from ..core.services.event_bus import EventBus
        from ..core.services.message_bus import MessageBus
        from ..core.services.command_dispatcher import CommandDispatcher

        # Register core services with their interfaces
        self._container.register(
            IEventBus, EventBus, ServiceLifetime.SINGLETON)  # type: ignore[type-abstract]
        self._container.register(
            IMessageBus, MessageBus, ServiceLifetime.SINGLETON)  # type: ignore[type-abstract]
        self._container.register(
            ICommandDispatcher, CommandDispatcher, ServiceLifetime.SINGLETON)  # type: ignore[type-abstract]

        logger.debug("Registered core services")

    async def _register_infrastructure_services(self, config: Dict[str, Any]) -> None:
        """Register infrastructure services."""
        from ..infrastructure.config.manager import ConfigManager
        from ..infrastructure.logging.setup import LoggingManager
        from ..infrastructure.services.ssh.forwarder import SSHForwarder
        from ..infrastructure.services.upload.manager import UploadManager
        from ..core.interfaces.ssh import ISSHForwarder
        from ..core.interfaces.upload import IUploadManager

        # Configuration manager
        config_manager = ConfigManager(config)
        self._container.register_instance(ConfigManager, config_manager)

        # Logging manager
        logging_manager = LoggingManager(config.get('logging', {}))
        self._container.register_instance(LoggingManager, logging_manager)

        # SSH Forwarder service
        self._container.register(
            ISSHForwarder, SSHForwarder, ServiceLifetime.SINGLETON)  # type: ignore[type-abstract]

        # Upload Manager service
        self._container.register(
            IUploadManager, UploadManager, ServiceLifetime.SINGLETON)  # type: ignore[type-abstract]

        # Register client services based on configuration
        await self._register_client_services(config)

        logger.debug("Registered infrastructure services")

    async def _register_client_services(self, config: Dict[str, Any]) -> None:
        """Register client services based on configuration."""
        # For now, skip client registration as they're not implemented yet
        # This will be implemented in the next phase
        _ = config  # Suppress unused parameter warning
        logger.debug(
            "Client services registration skipped (not implemented yet)")

    async def _register_application_services(self, config: Dict[str, Any]) -> None:
        """Register application layer services."""
        try:
            from ..plugins.manager import PluginManager

            # Plugin manager
            plugin_config = config.get('plugins', {})
            plugin_manager = PluginManager(plugin_config, self._container)
            self._container.register_instance(IPluginManager, plugin_manager)  # type: ignore[type-abstract]

            logger.debug("Registered application services")
        except ImportError as e:
            logger.warning(f"Plugin manager not available: {e}")
            logger.debug(
                "Application services registration completed (with warnings)")

    def _get_component_by_name(self, component_name: str) -> Optional[Any]:
        """Get a component by its name from the container."""
        from ..core.interfaces.ssh import ISSHForwarder
        from ..core.interfaces.upload import IUploadManager

        component_map = {
            'config_manager': ConfigManager,
            'event_bus': IEventBus,
            'message_bus': IMessageBus,
            'command_dispatcher': ICommandDispatcher,
            'plugin_manager': IPluginManager,
            'ssh_forwarder': ISSHForwarder,
            'upload_manager': IUploadManager,
        }

        # Try to get from predefined map first
        if component_name in component_map:
            return self._container.try_resolve(component_map[component_name])  # type: ignore[arg-type]

        # Try to resolve by name (for dynamically registered services)
        try:
            # This is a simplified approach - in a real implementation,
            # you might want a more sophisticated service locator
            if hasattr(self._container, 'get_registrations'):
                for service_type, _ in self._container.get_registrations().items():
                    if hasattr(service_type, '__name__') and service_type.__name__.lower().endswith(component_name):
                        return self._container.resolve(service_type)
        except Exception:
            pass

        return None

    async def _stop_started_components(self) -> None:
        """Stop all components that have been started so far."""
        for component in reversed(self._started_components):
            try:
                if isinstance(component, IStoppable):
                    await component.stop()
            except Exception as e:
                logger.error(f"Error stopping component during cleanup: {e}")

        self._started_components.clear()
