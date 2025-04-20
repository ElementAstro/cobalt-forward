import asyncio
from pathlib import Path
from watchdog.observers.api import BaseObserver as Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
import time
from typing import Optional


class ConfigFileHandler(FileSystemEventHandler):
    """
    Handles file system events for configuration files.
    Implements debouncing to prevent multiple reloads for rapid changes.
    """
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.last_modified = 0
        self.debounce_delay = 0.5  # 500ms debounce delay
        self._pending_task: Optional[asyncio.Task] = None

    def on_modified(self, event):
        """
        Handles file modification events with debounce protection
        """
        if not event.is_directory and event.src_path == str(self.config_manager.config_path):
            current_time = time.time()
            if current_time - self.last_modified > self.debounce_delay:
                self.last_modified = current_time
                logger.debug("Configuration file change detected, preparing to reload...")
                # Cancel previous pending task if exists
                if self._pending_task and not self._pending_task.done():
                    self._pending_task.cancel()
                # Create new task
                self._pending_task = asyncio.create_task(
                    self._debounced_reload())

    async def _debounced_reload(self):
        """Debounced reload implementation"""
        try:
            await asyncio.sleep(self.debounce_delay)
            logger.info("Reloading configuration file...")
            await self.config_manager._reload_config()
        except asyncio.CancelledError:
            logger.debug("Reload task cancelled, waiting for new changes")
        except Exception as e:
            logger.error(f"Error reloading configuration file: {e}")


class FileWatcher:
    """
    Watches configuration files for changes and triggers reloads.
    Implements retry logic for robustness.
    """
    def __init__(self, config_path: Path, config_manager):
        self.config_path = config_path
        self.config_manager = config_manager
        self._observer: Optional[Observer] = None
        self.watch_handler = None
        self._watch_delay = 1.0  # Watch delay
        self._retry_count = 3    # Number of retries
        self._is_running = False

    async def start_async(self):
        """Asynchronously start the file watcher"""
        if not self._is_running:
            await self._start_with_retry()

    async def _start_with_retry(self):
        """Start watcher with retry logic"""
        for attempt in range(self._retry_count):
            try:
                self.start()
                return
            except Exception as e:
                logger.error(
                    f"Failed to start watcher (attempt {attempt + 1}/{self._retry_count}): {e}")
                if self._observer:
                    try:
                        self._observer.stop()
                        self._observer.join(timeout=1.0)
                    except:
                        pass
                    self._observer = None
                await asyncio.sleep(self._watch_delay * (attempt + 1))  # Exponential backoff
        logger.error("Watcher startup failed, maximum retry count reached")

    def start(self):
        """Start file watcher"""
        if not self._observer:
            try:
                self.watch_handler = ConfigFileHandler(self.config_manager)
                self._observer = Observer()

                # Ensure directory exists
                watch_dir = self.config_path.parent
                if not watch_dir.exists():
                    watch_dir.mkdir(parents=True, exist_ok=True)

                self._observer.schedule(
                    self.watch_handler,
                    str(watch_dir),
                    recursive=False
                )
                self._observer.start()
                self._is_running = True
                logger.info("Configuration file watcher started")
            except Exception as e:
                self._is_running = False
                logger.error(f"Error starting watcher: {e}")
                raise

    def stop(self):
        """Stop file watcher"""
        if self._observer:
            try:
                self._observer.stop()
                self._observer.join(timeout=2.0)
                logger.info("Configuration file watcher stopped")
            except Exception as e:
                logger.error(f"Error stopping watcher: {e}")
            finally:
                self._observer = None
                self._is_running = False
