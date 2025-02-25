from pathlib import Path
import json
from datetime import datetime
import zipfile
from loguru import logger
import os
import shutil
import tempfile
import hashlib
from typing import List, Dict, Any, Optional


class ConfigBackup:
    def __init__(self, backup_dir: Path, max_backups: int = 10):
        self.backup_dir = backup_dir
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.max_backups = max_backups
        self.compression = zipfile.ZIP_DEFLATED

    def _rotate_backups(self):
        """轮转旧的备份文件"""
        backups = self.list_backups()
        while len(backups) >= self.max_backups:
            oldest_backup = backups.pop()
            try:
                oldest_backup.unlink()
                logger.info(f"删除旧备份: {oldest_backup}")
            except Exception as e:
                logger.error(f"删除旧备份失败: {e}")

    def _calculate_checksum(self, file_path: Path) -> str:
        """计算文件的SHA256校验和"""
        h = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                h.update(chunk)
        return h.hexdigest()

    def create_backup(self, config_path: Path, metadata: dict = None) -> Path:
        """创建配置备份"""
        if not config_path.exists():
            logger.warning(f"配置文件不存在: {config_path}，无法创建备份")
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        # 生成时间戳和备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"config_backup_{timestamp}.zip"

        # 准备元数据
        if metadata is None:
            metadata = {}

        metadata.update({
            "backup_time": datetime.now().isoformat(),
            "source_file": str(config_path),
            "checksum": self._calculate_checksum(config_path)
        })

        # 创建临时备份文件
        temp_dir = tempfile.mkdtemp()
        temp_backup = Path(temp_dir) / backup_path.name

        try:
            with zipfile.ZipFile(temp_backup, 'w', compression=self.compression) as backup_zip:
                # 添加配置文件
                backup_zip.write(config_path, arcname=config_path.name)
                # 添加元数据
                backup_zip.writestr(
                    'metadata.json', json.dumps(metadata, indent=2))

            # 移动临时备份文件到目标位置
            if backup_path.exists():
                backup_path.unlink()
            shutil.move(temp_backup, backup_path)

            logger.info(f"配置备份已创建: {backup_path}")
            self._rotate_backups()
            return backup_path

        except Exception as e:
            logger.error(f"创建备份失败: {e}")
            raise
        finally:
            # 清理临时目录
            shutil.rmtree(temp_dir, ignore_errors=True)

    def verify_backup(self, backup_path: Path) -> bool:
        """验证备份文件的完整性"""
        try:
            with zipfile.ZipFile(backup_path, 'r') as backup_zip:
                # 检查备份文件结构
                file_list = backup_zip.namelist()
                if 'metadata.json' not in file_list:
                    logger.error(f"备份验证失败: {backup_path} 缺少元数据")
                    return False

                # 加载元数据
                metadata = json.loads(backup_zip.read('metadata.json'))

                # 确保至少有一个配置文件
                config_files = [f for f in file_list if f != 'metadata.json']
                if not config_files:
                    logger.error(f"备份验证失败: {backup_path} 不包含配置文件")
                    return False

            logger.debug(f"备份验证成功: {backup_path}")
            return True

        except Exception as e:
            logger.error(f"备份验证失败: {e}")
            return False

    def restore_backup(self, backup_path: Path, target_path: Path) -> dict:
        """从备份恢复配置"""
        with zipfile.ZipFile(backup_path, 'r') as backup_zip:
            config_file = next(
                name for name in backup_zip.namelist() if name != 'metadata.json')
            backup_zip.extract(config_file, target_path.parent)

            metadata = {}
            if 'metadata.json' in backup_zip.namelist():
                metadata = json.loads(backup_zip.read('metadata.json'))

        logger.info(f"配置已从备份恢复: {backup_path}")
        return metadata

    def list_backups(self) -> list:
        return sorted(
            [f for f in self.backup_dir.glob("config_backup_*.zip")],
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
