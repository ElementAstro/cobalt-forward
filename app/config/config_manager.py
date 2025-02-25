from datetime import datetime
from typing import Dict, Any, Optional, List, Callable, Coroutine
import yaml
from pathlib import Path
import asyncio
from loguru import logger
from dataclasses import asdict
import jsonschema
import zlib
import os
import copy

from app.models.config_model import RuntimeConfig
from .file_watcher import FileWatcher
from .config_crypto import ConfigCrypto
from .config_backup import ConfigBackup

# 类型别名定义
ObserverCallable = Callable[[RuntimeConfig], Coroutine]


class ConfigManager:
    def __init__(self, config_path: str, encrypt: bool = False):
        self.config_path = Path(config_path).resolve()
        self.config_dir = self.config_path.parent
        self._observers: List[ObserverCallable] = []
        self._lock = asyncio.Lock()  # 添加锁以防止并发问题
        self._config_loaded = asyncio.Event()

        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)

        # 初始化加密工具
        self.encrypt_enabled = encrypt
        self.crypto = ConfigCrypto(
            self.config_dir / ".config.key") if encrypt else None

        # 初始化备份工具
        self.backup_manager = ConfigBackup(self.config_dir / "backups")

        # 加载配置
        self.runtime_config: RuntimeConfig = self._load_default_config()
        self._cache = {}
        self._version_history: List[Dict[str, Any]] = []
        self._max_history = 20  # 最大历史记录
        self._load_schema()
        self.file_watcher = FileWatcher(self.config_path, self)

        logger.info(f"ConfigManager 已初始化，配置路径: {config_path}")

    def _load_schema(self):
        """加载配置验证模式"""
        try:
            self.schema = {
                "type": "object",
                "properties": {
                    "version": {"type": "string"},
                    "tcp_host": {"type": "string"},
                    "tcp_port": {"type": "integer", "minimum": 1, "maximum": 65535},
                    "websocket_host": {"type": "string"},
                    "websocket_port": {"type": "integer", "minimum": 1, "maximum": 65535}
                },
                "required": ["tcp_host", "tcp_port", "websocket_host", "websocket_port"]
            }
        except Exception as e:
            logger.error(f"加载模式失败: {e}")
            raise

    def get_config_value(self, key: str, default=None) -> Any:
        """获取配置值"""
        # 使用缓存机制
        if key in self._cache:
            return self._cache[key]

        value = getattr(self.runtime_config, key, default)
        self._cache[key] = value
        return value

    def _validate_config(self, config_data: dict) -> bool:
        """验证配置数据"""
        try:
            jsonschema.validate(instance=config_data, schema=self.schema)
            return True
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"配置验证失败: {e}")
            return False

    def _compress_config(self, config_data: str) -> bytes:
        """压缩配置数据"""
        try:
            return zlib.compress(config_data.encode(), level=9)  # 使用最高压缩级别
        except Exception as e:
            logger.error(f"压缩配置失败: {e}")
            return config_data.encode()  # 失败时返回未压缩数据

    def _decompress_config(self, compressed_data: bytes) -> str:
        """解压配置数据"""
        try:
            return zlib.decompress(compressed_data).decode()
        except zlib.error:
            # 可能是未压缩数据
            logger.warning("解压配置失败，尝试直接解码")
            return compressed_data.decode()
        except Exception as e:
            logger.error(f"解压配置失败: {e}")
            raise

    async def create_backup(self) -> Path:
        """创建配置备份"""
        metadata = {
            "version": getattr(self.runtime_config, "version", "unknown"),
            "timestamp": datetime.now().isoformat(),
            "encrypted": self.encrypt_enabled
        }
        return self.backup_manager.create_backup(self.config_path, metadata)

    async def restore_from_backup(self, backup_path: Path) -> None:
        """从备份恢复配置"""
        async with self._lock:
            metadata = self.backup_manager.restore_backup(
                backup_path, self.config_path)
            await self.load_config()
            logger.info(f"配置已从备份恢复，版本: {metadata.get('version', 'unknown')}")
            self._config_loaded.set()

    def _load_default_config(self) -> RuntimeConfig:
        """加载默认配置"""
        logger.debug("加载默认配置")
        config = RuntimeConfig(
            tcp_host="localhost",
            tcp_port=8080,
            websocket_host="0.0.0.0",
            websocket_port=8000,
            version=datetime.now().isoformat()
        )
        logger.info("默认配置已加载")
        return config

    async def load_config(self) -> None:
        """加载配置文件"""
        async with self._lock:
            try:
                if self.config_path.exists() and os.path.getsize(self.config_path) > 0:
                    logger.debug(f"从 {self.config_path} 加载配置")

                    # 读取文件内容
                    with open(self.config_path, 'rb') as f:
                        content = f.read()

                    # 如果启用加密，解密内容
                    if self.encrypt_enabled and self.crypto:
                        content = self.crypto.decrypt_bytes(content)

                    # 解压配置
                    yaml_content = self._decompress_config(content)

                    # 解析YAML
                    config_data = yaml.safe_load(yaml_content)

                    # 验证配置
                    if not self._validate_config(config_data):
                        logger.warning("配置验证失败，使用默认配置")
                        self.runtime_config = self._load_default_config()
                    else:
                        self.runtime_config = RuntimeConfig(**config_data)

                    # 清除缓存
                    self._cache.clear()
                    logger.info("配置加载成功")
                else:
                    logger.warning(f"配置文件不存在或为空: {self.config_path}, 创建默认配置")
                    self.runtime_config = self._load_default_config()
                    await self.save_config()

                self._config_loaded.set()

            except Exception as e:
                logger.error(f"加载配置失败: {str(e)}", exc_info=True)
                # 加载失败时使用默认配置
                self.runtime_config = self._load_default_config()
                self._config_loaded.set()

    async def save_config(self) -> None:
        """保存配置到文件"""
        async with self._lock:
            try:
                logger.debug(f"保存配置到 {self.config_path}")

                # 创建父目录（如果不存在）
                self.config_path.parent.mkdir(parents=True, exist_ok=True)

                # 准备配置数据
                config_data = asdict(self.runtime_config)

                # 验证配置
                if not self._validate_config(config_data):
                    raise ValueError("配置验证失败")

                # 版本控制
                config_data['version'] = datetime.now().isoformat()

                # 管理版本历史
                self._version_history.append(copy.deepcopy(config_data))
                if len(self._version_history) > self._max_history:
                    self._version_history.pop(0)  # 移除最旧的版本

                # 转换为YAML
                yaml_content = yaml.dump(config_data, default_flow_style=False)

                # 压缩配置
                compressed = self._compress_config(yaml_content)

                # 如果启用加密，加密配置内容
                if self.encrypt_enabled and self.crypto:
                    compressed = self.crypto.encrypt_bytes(compressed)

                # 写入临时文件然后重命名，确保原子性写入
                temp_path = self.config_path.with_suffix('.tmp')
                with open(temp_path, 'wb') as f:
                    f.write(compressed)
                    f.flush()
                    os.fsync(f.fileno())  # 确保数据写入磁盘

                # 重命名为目标文件
                temp_path.replace(self.config_path)

                # 清除缓存
                self._cache.clear()

                # 创建自动备份
                await self.create_backup()
                logger.info("配置保存成功")

            except Exception as e:
                logger.error(f"保存配置失败: {str(e)}", exc_info=True)
                raise

    async def update_config(self, new_config: Dict[str, Any]) -> None:
        """更新配置"""
        async with self._lock:
            logger.debug(f"更新配置: {new_config}")
            current_config = asdict(self.runtime_config)
            current_config.update(new_config)

            # 验证更新后的配置
            if not self._validate_config(current_config):
                logger.error("更新配置失败：无效的配置值")
                return

            self.runtime_config = RuntimeConfig(**current_config)
            await self.save_config()

            logger.info("配置已更新，通知观察者")
            await self._notify_observers()

    async def rollback_to_version(self, version: str) -> None:
        """回滚到指定版本"""
        async with self._lock:
            for config in reversed(self._version_history):
                if config.get('version') == version:
                    await self.update_config(config)
                    logger.info(f"已回滚到版本 {version}")
                    return
            raise ValueError(f"未找到版本 {version}")

    async def repair_config(self) -> None:
        """尝试修复损坏的配置"""
        async with self._lock:
            try:
                # 尝试加载最新的备份
                backups = self.backup_manager.list_backups()
                if backups:
                    logger.info(f"尝试从备份恢复: {backups[0]}")
                    await self.restore_from_backup(backups[0])
                    logger.info("配置已从最新备份修复")
                else:
                    # 如果没有备份，加载默认配置
                    self.runtime_config = self._load_default_config()
                    await self.save_config()
                    logger.info("配置已重置为默认值")

                # 清除缓存
                self._cache.clear()

            except Exception as e:
                logger.error(f"配置修复失败: {str(e)}")
                # 确保即使修复失败，也设置配置加载完成
                self._config_loaded.set()

    def register_observer(self, callback: ObserverCallable) -> None:
        """注册配置变更观察者"""
        if callback not in self._observers:  # 防止重复注册
            logger.debug(
                f"注册新观察者: {callback.__name__ if hasattr(callback, '__name__') else str(callback)}")
            self._observers.append(callback)
            logger.info(f"观察者数量: {len(self._observers)}")

    def unregister_observer(self, callback: ObserverCallable) -> None:
        """注销配置变更观察者"""
        if callback in self._observers:
            self._observers.remove(callback)
            logger.debug(
                f"已注销观察者: {callback.__name__ if hasattr(callback, '__name__') else str(callback)}")

    async def _notify_observers(self) -> None:
        """通知所有观察者配置已变更"""
        logger.debug(f"通知 {len(self._observers)} 个观察者")

        # 创建所有观察者的任务
        tasks = []
        for observer in self._observers:
            tasks.append(self._notify_single_observer(observer))

        # 等待所有任务完成，但不让单个失败影响其他
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_single_observer(self, observer: ObserverCallable) -> None:
        """通知单个观察者，带错误处理"""
        observer_name = observer.__name__ if hasattr(
            observer, '__name__') else str(observer)
        try:
            await observer(self.runtime_config)
            logger.debug(f"观察者 {observer_name} 通知成功")
        except Exception as e:
            logger.error(f"通知观察者 {observer_name} 失败: {str(e)}", exc_info=True)

    async def _reload_config(self):
        """重新加载配置并通知观察者"""
        async with self._lock:
            try:
                # 重新加载前先创建备份
                await self.create_backup()

                # 加载配置
                await self.load_config()

                # 通知观察者
                await self._notify_observers()
                logger.success("配置重新加载完成")
            except Exception as e:
                logger.error(f"重新加载配置失败: {e}")

    async def wait_for_config(self, timeout: float = None) -> bool:
        """等待配置加载完成"""
        return await asyncio.wait_for(self._config_loaded.wait(), timeout=timeout) if timeout else await self._config_loaded.wait()

    def start_hot_reload(self):
        """启动热重载支持"""
        logger.info("正在启动配置热重载...")
        self.file_watcher.start()

    def stop_hot_reload(self):
        """停止热重载支持"""
        self.file_watcher.stop()

    async def __aenter__(self):
        """支持异步上下文管理器"""
        await self.load_config()
        self.start_hot_reload()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """支持异步上下文管理器"""
        self.stop_hot_reload()
