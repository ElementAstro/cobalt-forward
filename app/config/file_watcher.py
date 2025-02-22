import asyncio
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
from functools import partial
import time

class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.last_modified = 0
        self.debounce_delay = 0.5  # 500ms 防抖动延迟

    def on_modified(self, event):
        if not event.is_directory and event.src_path == str(self.config_manager.config_path):
            current_time = time.time()
            if current_time - self.last_modified > self.debounce_delay:
                self.last_modified = current_time
                logger.info("检测到配置文件变更，正在重新加载...")
                asyncio.create_task(self._debounced_reload())

    async def _debounced_reload(self):
        await asyncio.sleep(self.debounce_delay)
        await self.config_manager._reload_config()

class FileWatcher:
    def __init__(self, config_path: Path, config_manager):
        self.config_path = config_path
        self.config_manager = config_manager
        self.observer = None
        self.watch_handler = None
        self._watch_delay = 1.0  # 监视延迟
        self._retry_count = 3    # 重试次数

    async def _start_with_retry(self):
        """带重试的启动监视器"""
        for attempt in range(self._retry_count):
            try:
                self.start()
                return
            except Exception as e:
                logger.error(f"启动监视器失败 (尝试 {attempt + 1}/{self._retry_count}): {e}")
                await asyncio.sleep(self._watch_delay)
        logger.error("监视器启动失败，已达到最大重试次数")

    def start(self):
        """启动文件监视器"""
        if not self.observer:
            self.watch_handler = ConfigFileHandler(self.config_manager)
            self.observer = Observer()
            self.observer.schedule(
                self.watch_handler,
                str(self.config_path.parent),
                recursive=False
            )
            self.observer.start()
            logger.info("配置文件监视器已启动")

    def stop(self):
        """停止文件监视器"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
            logger.info("配置文件监视器已停止")
