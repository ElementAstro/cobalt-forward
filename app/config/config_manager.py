from datetime import datetime
from typing import Dict, Any, Optional
import yaml
from pathlib import Path
import asyncio
from loguru import logger
from dataclasses import asdict
import json
from functools import lru_cache
import jsonschema
import zlib

from app.models.config_model import RuntimeConfig
from file_watcher import FileWatcher
from config_crypto import ConfigCrypto
from config_backup import ConfigBackup


class ConfigManager:
    def __init__(self, config_path: str, encrypt: bool = False):
        self.config_path = Path(config_path)
        self.config_dir = self.config_path.parent
        self._observers: list = []
        self.file_watcher = FileWatcher(self.config_path, self)

        # 初始化加密工具
        self.encrypt_enabled = encrypt
        if encrypt:
            self.crypto = ConfigCrypto(self.config_dir / ".config.key")

        # 初始化备份工具
        self.backup_manager = ConfigBackup(self.config_dir / "backups")

        # 加载配置
        self.runtime_config: RuntimeConfig = self._load_default_config()
        self._cache = {}
        self._version_history = []
        self._load_schema()
        logger.info(
            f"ConfigManager initialized with config path: {config_path}")

    def _load_schema(self):
        """加载配置验证模式"""
        self.schema = {
            "type": "object",
            "properties": {
                "version": {"type": "string"},
                "tcp_host": {"type": "string"},
                "tcp_port": {"type": "integer"},
                "websocket_host": {"type": "string"},
                "websocket_port": {"type": "integer"}
            },
            "required": ["tcp_host", "tcp_port", "websocket_host", "websocket_port"]
        }

    @lru_cache(maxsize=128)
    def get_config_value(self, key: str) -> Any:
        """获取配置值（带缓存）"""
        return getattr(self.runtime_config, key, None)

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
        return zlib.compress(config_data.encode())

    def _decompress_config(self, compressed_data: bytes) -> str:
        """解压配置数据"""
        return zlib.decompress(compressed_data).decode()

    async def create_backup(self) -> Path:
        """创建配置备份"""
        metadata = {
            "version": self.runtime_config.version,
            "timestamp": datetime.now().isoformat(),
            "encrypted": self.encrypt_enabled
        }
        return self.backup_manager.create_backup(self.config_path, metadata)

    async def restore_from_backup(self, backup_path: Path) -> None:
        """从备份恢复配置"""
        metadata = self.backup_manager.restore_backup(
            backup_path, self.config_path)
        await self.load_config()
        logger.info(f"配置已从备份恢复，版本: {metadata.get('version', 'unknown')}")

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
        """加载配置文件"""
        try:
            if self.config_path.exists():
                logger.debug(f"从 {self.config_path} 加载配置")

                # 读取文件内容
                with open(self.config_path, 'r') as f:
                    content = f.read()

                # 如果启用加密，解密内容
                if self.encrypt_enabled:
                    content = self.crypto.decrypt(content)

                # 解压配置
                content = self._decompress_config(content)

                # 解析YAML
                config_data = yaml.safe_load(content)
                self.runtime_config = RuntimeConfig(**config_data)
                logger.info("配置加载成功")
            else:
                logger.warning(f"配置文件不存在: {self.config_path}, 创建默认配置")
                await self.save_config()

        except Exception as e:
            logger.error(f"加载配置失败: {str(e)}", exc_info=True)

    async def save_config(self) -> None:
        """保存配置到文件"""
        try:
            logger.debug(f"保存配置到 {self.config_path}")
            config_data = asdict(self.runtime_config)

            # 验证配置
            if not self._validate_config(config_data):
                raise ValueError("配置验证失败")

            # 版本控制
            config_data['version'] = datetime.now().isoformat()
            self._version_history.append(config_data.copy())

            # 转换为YAML
            yaml_content = yaml.dump(config_data)

            # 压缩配置
            compressed = self._compress_config(yaml_content)

            # 如果启用加密，加密配置内容
            if self.encrypt_enabled:
                compressed = self.crypto.encrypt(compressed)

            # 写入文件
            with open(self.config_path, 'wb') as f:
                f.write(compressed)

            # 清除缓存
            self.get_config_value.cache_clear()

            # 创建自动备份
            await self.create_backup()
            logger.info("配置保存成功")

        except Exception as e:
            logger.error(f"保存配置失败: {str(e)}", exc_info=True)

    async def update_config(self, new_config: Dict[str, Any]) -> None:
        logger.debug(f"Updating configuration with: {new_config}")
        current_config = asdict(self.runtime_config)
        current_config.update(new_config)
        self.runtime_config = RuntimeConfig(**current_config)
        await self.save_config()
        logger.info("Configuration updated, notifying observers")
        await self._notify_observers()

    async def rollback_to_version(self, version: str) -> None:
        """回滚到指定版本"""
        for config in reversed(self._version_history):
            if config['version'] == version:
                await self.update_config(config)
                logger.info(f"已回滚到版本 {version}")
                return
        raise ValueError(f"未找到版本 {version}")

    async def repair_config(self) -> None:
        """尝试修复损坏的配置"""
        try:
            # 尝试加载最新的备份
            backups = self.backup_manager.list_backups()
            if backups:
                await self.restore_from_backup(backups[0])
                logger.info("配置已从最新备份修复")
            else:
                # 如果没有备份，加载默认配置
                self.runtime_config = self._load_default_config()
                await self.save_config()
                logger.info("配置已重置为默认值")
        except Exception as e:
            logger.error(f"配置修复失败: {str(e)}")

    def register_observer(self, callback) -> None:
        logger.debug(f"Registering new observer: {callback.__name__}")
        self._observers.append(callback)
        logger.info(f"Observer count: {len(self._observers)}")

    async def _notify_observers(self) -> None:
        logger.debug(f"Notifying {len(self._observers)} observers")
        for observer in self._observers:
            try:
                await observer(self.runtime_config)
                logger.trace(
                    f"Observer {observer.__name__} notified successfully")
            except Exception as e:
                logger.error(
                    f"Failed to notify observer {observer.__name__}: {str(e)}", exc_info=True)

    async def _reload_config(self):
        try:
            await self.load_config()
            await self._notify_observers()
            logger.success("配置重新加载完成")
        except Exception as e:
            logger.error(f"重新加载配置失败: {e}")

    def start_hot_reload(self):
        """启动热重载支持"""
        logger.info("正在启动配置热重载...")
        self.file_watcher.start()

    def stop_hot_reload(self):
        """停止热重载支持"""
        self.file_watcher.stop()
