"""
Configuration file watcher for the Cobalt Forward application.

This module provides file system monitoring capabilities to automatically
reload configuration when files change, with debouncing and error handling.
"""

import asyncio
from pathlib import Path
import logging
import time
from typing import Optional, Callable, Any, Dict, TYPE_CHECKING
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileSystemEvent

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileSystemEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False
    # Create dummy classes for type checking when watchdog is not available

    class Observer:  # type: ignore
        pass

    class FileSystemEventHandler:  # type: ignore
        pass

    class FileSystemEvent:  # type: ignore
        pass

logger = logging.getLogger(__name__)


class IConfigWatcher(ABC):
    """Interface for configuration file watchers."""

    @abstractmethod
    async def start(self) -> None:
        """Start watching for configuration changes."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop watching for configuration changes."""
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """Check if the watcher is currently running."""
        pass


class ConfigFileHandler(FileSystemEventHandler):
    """
    Handles file system events for configuration files.

    Implements debouncing to prevent multiple reloads for rapid changes
    and provides error handling for reload operations.
    """

    def __init__(
        self,
        config_path: Path,
        reload_callback: Callable[[], Any],
        debounce_delay: float = 0.5
    ):
        """
        Initialize the file handler.

        Args:
            config_path: Path to the configuration file to watch
            reload_callback: Callback function to call when file changes
            debounce_delay: Delay in seconds to debounce rapid changes
        """
        super().__init__()
        self.config_path = config_path
        self.reload_callback = reload_callback
        self.debounce_delay = debounce_delay
        self.last_modified: float = 0.0
        self._pending_task: Optional[asyncio.Task[None]] = None

    def on_modified(self, event: FileSystemEvent) -> None:
        """
        Handle file modification events with debounce protection.

        Args:
            event: File system event
        """
        if not event.is_directory and Path(str(event.src_path)) == self.config_path:
            current_time = time.time()
            if current_time - self.last_modified > self.debounce_delay:
                self.last_modified = current_time
                logger.debug(
                    f"Configuration file change detected: {self.config_path}")

                # Cancel previous pending task if exists
                if self._pending_task and not self._pending_task.done():
                    self._pending_task.cancel()

                # Create new debounced reload task
                self._pending_task = asyncio.create_task(
                    self._debounced_reload())

    def on_moved(self, event: FileSystemEvent) -> None:
        """
        Handle file move events.

        Args:
            event: File system event
        """
        if not event.is_directory and Path(str(event.dest_path)) == self.config_path:
            logger.debug(f"Configuration file moved to: {self.config_path}")
            self.on_modified(event)

    async def _debounced_reload(self) -> None:
        """Execute debounced reload with error handling."""
        try:
            await asyncio.sleep(self.debounce_delay)
            logger.info(f"Reloading configuration file: {self.config_path}")

            # Call the reload callback
            if asyncio.iscoroutinefunction(self.reload_callback):
                await self.reload_callback()
            else:
                self.reload_callback()

            logger.info("Configuration reloaded successfully")

        except asyncio.CancelledError:
            logger.debug(
                "Configuration reload cancelled, waiting for new changes")
        except Exception as e:
            logger.error(f"Error reloading configuration: {e}")


class ConfigWatcher(IConfigWatcher):
    """
    Configuration file watcher using watchdog library.

    Monitors configuration files for changes and triggers reload callbacks
    with debouncing and error handling.
    """

    def __init__(
        self,
        config_path: Path,
        reload_callback: Callable[[], Any],
        debounce_delay: float = 0.5
    ):
        """
        Initialize the configuration watcher.

        Args:
            config_path: Path to the configuration file to watch
            reload_callback: Callback function to call when file changes
            debounce_delay: Delay in seconds to debounce rapid changes
        """
        if not WATCHDOG_AVAILABLE:
            raise ImportError("watchdog library is required for file watching")

        self.config_path = config_path.resolve()
        self.reload_callback = reload_callback
        self.debounce_delay = debounce_delay

        self._observer: Optional[Any] = None
        self._handler: Optional[ConfigFileHandler] = None
        self._running = False

    async def start(self) -> None:
        """Start watching for configuration file changes."""
        if self._running:
            logger.warning("Configuration watcher is already running")
            return

        try:
            # Ensure the config file exists
            if not self.config_path.exists():
                logger.warning(
                    f"Configuration file does not exist: {self.config_path}")
                return

            # Create file handler
            self._handler = ConfigFileHandler(
                config_path=self.config_path,
                reload_callback=self.reload_callback,
                debounce_delay=self.debounce_delay
            )

            # Create and start observer
            self._observer = Observer()
            self._observer.schedule(
                self._handler,
                str(self.config_path.parent),
                recursive=False
            )

            self._observer.start()
            self._running = True

            logger.info(
                f"Started watching configuration file: {self.config_path}")

        except Exception as e:
            logger.error(f"Failed to start configuration watcher: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop watching for configuration file changes."""
        if not self._running:
            return

        try:
            if self._observer:
                self._observer.stop()
                self._observer.join(timeout=5.0)
                self._observer = None

            # Cancel any pending reload tasks
            if self._handler and self._handler._pending_task:
                if not self._handler._pending_task.done():
                    self._handler._pending_task.cancel()
                    try:
                        await self._handler._pending_task
                    except asyncio.CancelledError:
                        pass

            self._handler = None
            self._running = False

            logger.info("Stopped configuration file watcher")

        except Exception as e:
            logger.error(f"Error stopping configuration watcher: {e}")

    def is_running(self) -> bool:
        """Check if the watcher is currently running."""
        return self._running and self._observer is not None and self._observer.is_alive()


class PollingConfigWatcher(IConfigWatcher):
    """
    Fallback configuration watcher using polling.

    Used when watchdog is not available or file system events are not supported.
    """

    def __init__(
        self,
        config_path: Path,
        reload_callback: Callable[[], Any],
        poll_interval: float = 1.0
    ):
        """
        Initialize the polling watcher.

        Args:
            config_path: Path to the configuration file to watch
            reload_callback: Callback function to call when file changes
            poll_interval: Interval in seconds between polls
        """
        self.config_path = config_path.resolve()
        self.reload_callback = reload_callback
        self.poll_interval = poll_interval

        self._running = False
        self._task: Optional[asyncio.Task[None]] = None
        self._last_mtime: Optional[float] = None

    async def start(self) -> None:
        """Start polling for configuration file changes."""
        if self._running:
            logger.warning("Polling configuration watcher is already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())

        logger.info(f"Started polling configuration file: {self.config_path}")

    async def stop(self) -> None:
        """Stop polling for configuration file changes."""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        logger.info("Stopped polling configuration file watcher")

    def is_running(self) -> bool:
        """Check if the watcher is currently running."""
        return self._running and self._task is not None and not self._task.done()

    async def _poll_loop(self) -> None:
        """Main polling loop."""
        try:
            # Initialize last modification time
            if self.config_path.exists():
                self._last_mtime = self.config_path.stat().st_mtime

            while self._running:
                try:
                    if self.config_path.exists():
                        current_mtime = self.config_path.stat().st_mtime

                        if self._last_mtime is None or current_mtime > self._last_mtime:
                            self._last_mtime = current_mtime

                            if self._last_mtime is not None:  # Skip initial load
                                logger.info(
                                    f"Configuration file changed: {self.config_path}")

                                # Call reload callback
                                if asyncio.iscoroutinefunction(self.reload_callback):
                                    await self.reload_callback()
                                else:
                                    self.reload_callback()

                    await asyncio.sleep(self.poll_interval)

                except Exception as e:
                    logger.error(f"Error in polling loop: {e}")
                    await asyncio.sleep(self.poll_interval)

        except asyncio.CancelledError:
            logger.debug("Polling loop cancelled")
        except Exception as e:
            logger.error(f"Polling loop error: {e}")


def create_config_watcher(
    config_path: Path,
    reload_callback: Callable[[], Any],
    use_polling: bool = False,
    **kwargs: Any
) -> IConfigWatcher:
    """
    Create a configuration file watcher.

    Args:
        config_path: Path to the configuration file to watch
        reload_callback: Callback function to call when file changes
        use_polling: Force use of polling instead of file system events
        **kwargs: Additional arguments for the watcher

    Returns:
        Configuration watcher instance
    """
    if use_polling or not WATCHDOG_AVAILABLE:
        if not WATCHDOG_AVAILABLE:
            logger.warning("watchdog not available, using polling watcher")

        return PollingConfigWatcher(
            config_path=config_path,
            reload_callback=reload_callback,
            poll_interval=kwargs.get('poll_interval', 1.0)
        )
    else:
        return ConfigWatcher(
            config_path=config_path,
            reload_callback=reload_callback,
            debounce_delay=kwargs.get('debounce_delay', 0.5)
        )
