from typing import Dict, Any, Optional
import yaml
import json
from pathlib import Path
import asyncio
from watchdog.observers.api import BaseObserver as Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
from dataclasses import asdict, dataclass


@dataclass
class RuntimeConfig:
    tcp_host: str
    tcp_port: int
    websocket_host: str
    websocket_port: int
    log_level: str = "INFO"
    max_connections: int = 1000
    buffer_size: int = 8192
    enable_ssl: bool = False
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    metrics_enabled: bool = True
    hot_reload: bool = True


class ConfigManager:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.runtime_config: RuntimeConfig = self._load_default_config()
        self._observers: list = []
        self._file_observer: Optional[Observer] = None
        self.watch_handler = None
        self.observer = None
        logger.info(
            f"ConfigManager initialized with config path: {config_path}")

    def _load_default_config(self) -> RuntimeConfig:
        logger.debug("Loading default configuration")
        config = RuntimeConfig(
            tcp_host="localhost",
            tcp_port=8080,
            websocket_host="0.0.0.0",
            websocket_port=8000
        )
        logger.info("Default configuration loaded")
        return config

    async def load_config(self) -> None:
        """Load configuration from file"""
        try:
            if self.config_path.exists():
                logger.debug(f"Loading configuration from {self.config_path}")
                with open(self.config_path, 'r') as f:
                    config_data = yaml.safe_load(f)
                    self.runtime_config = RuntimeConfig(**config_data)
                logger.info("Configuration loaded successfully")
            else:
                logger.warning(
                    f"Config file not found at {self.config_path}, creating default")
                await self.save_config()
        except Exception as e:
            logger.error(
                f"Failed to load configuration: {str(e)}", exc_info=True)

    async def save_config(self) -> None:
        """Save current configuration to file"""
        try:
            logger.debug(f"Saving configuration to {self.config_path}")
            with open(self.config_path, 'w') as f:
                yaml.dump(asdict(self.runtime_config), f)
            logger.info("Configuration saved successfully")
        except Exception as e:
            logger.error(
                f"Failed to save configuration: {str(e)}", exc_info=True)

    async def update_config(self, new_config: Dict[str, Any]) -> None:
        """Update configuration with new values"""
        logger.debug(f"Updating configuration with: {new_config}")
        current_config = asdict(self.runtime_config)
        current_config.update(new_config)
        self.runtime_config = RuntimeConfig(**current_config)
        await self.save_config()
        logger.info("Configuration updated, notifying observers")
        await self._notify_observers()

    def register_observer(self, callback) -> None:
        """Register a callback for config changes"""
        logger.debug(f"Registering new observer: {callback.__name__}")
        self._observers.append(callback)
        logger.info(f"Observer count: {len(self._observers)}")

    async def _notify_observers(self) -> None:
        """Notify all observers of config changes"""
        logger.debug(f"Notifying {len(self._observers)} observers")
        for observer in self._observers:
            try:
                await observer(self.runtime_config)
                logger.trace(
                    f"Observer {observer.__name__} notified successfully")
            except Exception as e:
                logger.error(
                    f"Failed to notify observer {observer.__name__}: {str(e)}", exc_info=True)

    def start_file_watching(self) -> None:
        """Start watching config file for changes"""
        logger.info("Starting file watching")

        class ConfigFileHandler(FileSystemEventHandler):
            def __init__(self, manager):
                self.manager = manager

            def on_modified(self, event):
                if event.src_path == str(self.manager.config_path):
                    logger.debug(f"Config file modified: {event.src_path}")
                    asyncio.create_task(self.manager.load_config())

        if not self._file_observer:
            self._file_observer = Observer()
            self._file_observer.schedule(
                ConfigFileHandler(self),
                str(self.config_path.parent),
                recursive=False
            )
            self._file_observer.start()
            logger.info("File watching started successfully")

    def stop_file_watching(self) -> None:
        """Stop watching config file"""
        if self._file_observer:
            logger.info("Stopping file watching")
            self._file_observer.stop()
            self._file_observer.join()
            self._file_observer = None
            logger.info("File watching stopped successfully")

    def _setup_file_watcher(self):
        """设置文件监视器"""
        if not self.observer:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            class ConfigFileHandler(FileSystemEventHandler):
                def __init__(self, config_manager):
                    self.config_manager = config_manager

                def on_modified(self, event):
                    if not event.is_directory and event.src_path == str(self.config_manager.config_path):
                        logger.info("检测到配置文件变更，正在重新加载...")
                        asyncio.create_task(
                            self.config_manager._reload_config())

            self.watch_handler = ConfigFileHandler(self)
            self.observer = Observer()
            self.observer.schedule(self.watch_handler, str(
                self.config_path.parent), recursive=False)
            self.observer.start()
            logger.info("配置文件监视器已启动")

    async def _reload_config(self):
        """重新加载配置文件并通知观察者"""
        try:
            await self.load_config()
            await self._notify_observers()
            logger.success("配置重新加载完成")
        except Exception as e:
            logger.error(f"重新加载配置失败: {e}")

    def start_hot_reload(self):
        """启动热重载支持"""
        logger.info("正在启动配置热重载...")
        self._setup_file_watcher()

    def stop_hot_reload(self):
        """停止热重载支持"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
            logger.info("配置热重载已停止")
