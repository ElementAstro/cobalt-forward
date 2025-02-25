import asyncio
from pathlib import Path
from watchdog.observers.api import BaseObserver as Observer  # 修改这行
from watchdog.events import FileSystemEventHandler
from loguru import logger
import time
from typing import Optional


class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.last_modified = 0
        self.debounce_delay = 0.5  # 500ms 防抖动延迟
        self._pending_task: Optional[asyncio.Task] = None

    def on_modified(self, event):
        if not event.is_directory and event.src_path == str(self.config_manager.config_path):
            current_time = time.time()
            if current_time - self.last_modified > self.debounce_delay:
                self.last_modified = current_time
                logger.debug("检测到配置文件变更，准备重新加载...")
                # 取消上一个待处理的任务（如果存在）
                if self._pending_task and not self._pending_task.done():
                    self._pending_task.cancel()
                # 创建新任务
                self._pending_task = asyncio.create_task(
                    self._debounced_reload())

    async def _debounced_reload(self):
        """防抖动重载实现"""
        try:
            await asyncio.sleep(self.debounce_delay)
            logger.info("正在重新加载配置文件...")
            await self.config_manager._reload_config()
        except asyncio.CancelledError:
            logger.debug("重新加载任务已取消，等待新的更改")
        except Exception as e:
            logger.error(f"重新加载配置文件时发生错误: {e}")


class FileWatcher:
    def __init__(self, config_path: Path, config_manager):
        self.config_path = config_path
        self.config_manager = config_manager
        self._observer: Optional[Observer] = None  # 修改这行
        self.watch_handler = None
        self._watch_delay = 1.0  # 监视延迟
        self._retry_count = 3    # 重试次数
        self._is_running = False

    async def start_async(self):
        """异步启动文件监视器"""
        if not self._is_running:
            await self._start_with_retry()

    async def _start_with_retry(self):
        """带重试的启动监视器"""
        for attempt in range(self._retry_count):
            try:
                self.start()
                return
            except Exception as e:
                logger.error(
                    f"启动监视器失败 (尝试 {attempt + 1}/{self._retry_count}): {e}")
                if self._observer:  # 修改这行
                    try:
                        self._observer.stop()  # 修改这行
                        self._observer.join(timeout=1.0)  # 修改这行
                    except:
                        pass
                    self._observer = None  # 修改这行
                await asyncio.sleep(self._watch_delay * (attempt + 1))  # 指数退避
        logger.error("监视器启动失败，已达到最大重试次数")

    def start(self):
        """启动文件监视器"""
        if not self._observer:  # 修改这行
            try:
                self.watch_handler = ConfigFileHandler(self.config_manager)
                self._observer = Observer()  # 修改这行

                # 确保目录存在
                watch_dir = self.config_path.parent
                if not watch_dir.exists():
                    watch_dir.mkdir(parents=True, exist_ok=True)

                self._observer.schedule(  # 修改这行
                    self.watch_handler,
                    str(watch_dir),
                    recursive=False
                )
                self._observer.start()  # 修改这行
                self._is_running = True
                logger.info("配置文件监视器已启动")
            except Exception as e:
                self._is_running = False
                logger.error(f"启动监视器时出错: {e}")
                raise

    def stop(self):
        """停止文件监视器"""
        if self._observer:  # 修改这行
            try:
                self._observer.stop()  # 修改这行
                self._observer.join(timeout=2.0)  # 修改这行
                logger.info("配置文件监视器已停止")
            except Exception as e:
                logger.error(f"停止监视器时出错: {e}")
            finally:
                self._observer = None  # 修改这行
                self._is_running = False
