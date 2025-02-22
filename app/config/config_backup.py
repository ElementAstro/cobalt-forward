from pathlib import Path
import shutil
import json
from datetime import datetime
import zipfile
from loguru import logger

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
            oldest_backup.unlink()
            logger.info(f"删除旧备份: {oldest_backup}")

    def create_backup(self, config_path: Path, metadata: dict = None) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"config_backup_{timestamp}.zip"
        
        with zipfile.ZipFile(backup_path, 'w', compression=self.compression) as backup_zip:
            backup_zip.write(config_path, arcname=config_path.name)
            if metadata:
                backup_zip.writestr('metadata.json', json.dumps(metadata))
        
        logger.info(f"配置备份已创建: {backup_path}")
        self._rotate_backups()
        return backup_path

    def restore_backup(self, backup_path: Path, target_path: Path) -> dict:
        with zipfile.ZipFile(backup_path, 'r') as backup_zip:
            config_file = next(name for name in backup_zip.namelist() if name != 'metadata.json')
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
