from pathlib import Path
import json
from datetime import datetime
import zipfile
from loguru import logger
import shutil
import tempfile
import hashlib
from typing import Dict, Any, Optional, List


class ConfigBackup:
    """
    Configuration backup management system.
    Handles creating, verifying, and restoring configuration backups with metadata.
    """

    def __init__(self, backup_dir: Path, max_backups: int = 10):
        """
        Initialize the backup manager.

        Args:
            backup_dir: Directory to store backups
            max_backups: Maximum number of backups to retain
        """
        self.backup_dir = backup_dir
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.max_backups = max_backups
        self.compression = zipfile.ZIP_DEFLATED

    def _rotate_backups(self) -> None:
        """
        Rotate old backup files, removing the oldest when max_backups is reached.
        """
        backups = self.list_backups()
        while len(backups) >= self.max_backups:
            oldest_backup = backups.pop()
            try:
                oldest_backup.unlink()
                logger.info(f"Deleted old backup: {oldest_backup}")
            except Exception as e:
                logger.error(f"Failed to delete old backup: {e}")

    def _calculate_checksum(self, file_path: Path) -> str:
        """
        Calculate SHA256 checksum of a file.

        Args:
            file_path: Path to the file

        Returns:
            Hexadecimal checksum string
        """
        h = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                h.update(chunk)
        return h.hexdigest()

    def create_backup(self, config_path: Path, metadata: Optional[Dict[str, Any]] = None) -> Path:
        """
        Create a configuration backup.

        Args:
            config_path: Path to the configuration file to backup
            metadata: Additional metadata to store with backup

        Returns:
            Path to the created backup file

        Raises:
            FileNotFoundError: If the config file doesn't exist
        """
        if not config_path.exists():
            logger.warning(
                f"Config file doesn't exist: {config_path}, cannot create backup")
            raise FileNotFoundError(
                f"Config file doesn't exist: {config_path}")

        # Generate timestamp and backup filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"config_backup_{timestamp}.zip"

        # Prepare metadata
        if metadata is None:
            metadata = {}

        metadata.update({
            "backup_time": datetime.now().isoformat(),
            "source_file": str(config_path),
            "checksum": self._calculate_checksum(config_path)
        })

        # Create temporary backup file
        temp_dir = tempfile.mkdtemp()
        temp_backup = Path(temp_dir) / backup_path.name

        try:
            with zipfile.ZipFile(temp_backup, 'w', compression=self.compression) as backup_zip:
                # Add config file
                backup_zip.write(config_path, arcname=config_path.name)
                # Add metadata
                backup_zip.writestr(
                    'metadata.json', json.dumps(metadata, indent=2))

            # Move temporary backup file to target location
            if backup_path.exists():
                backup_path.unlink()
            shutil.move(str(temp_backup), str(backup_path))

            logger.info(f"Configuration backup created: {backup_path}")
            self._rotate_backups()
            return backup_path

        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)

    def verify_backup(self, backup_path: Path) -> bool:
        """
        Verify the integrity of a backup file.

        Args:
            backup_path: Path to the backup file

        Returns:
            True if backup is valid, False otherwise
        """
        try:
            with zipfile.ZipFile(backup_path, 'r') as backup_zip:
                # Check backup file structure
                file_list = backup_zip.namelist()
                if 'metadata.json' not in file_list:
                    logger.error(
                        f"Backup verification failed: {backup_path} is missing metadata")
                    return False

                # Load metadata
                metadata_content = backup_zip.read('metadata.json')
                metadata = json.loads(metadata_content)

                # Ensure at least one config file exists
                config_files = [f for f in file_list if f != 'metadata.json']
                if not config_files:
                    logger.error(
                        f"Backup verification failed: {backup_path} doesn't contain config files")
                    return False

            logger.debug(f"Backup verification successful: {backup_path}")
            return True

        except Exception as e:
            logger.error(f"Backup verification failed: {e}")
            return False

    def restore_backup(self, backup_path: Path, target_path: Path) -> Dict[str, Any]:
        """
        Restore configuration from backup.

        Args:
            backup_path: Path to the backup file
            target_path: Path where the config should be restored

        Returns:
            Metadata dictionary from the backup
        """
        with zipfile.ZipFile(backup_path, 'r') as backup_zip:
            config_file = next(
                name for name in backup_zip.namelist() if name != 'metadata.json')
            backup_zip.extract(config_file, target_path.parent)

            metadata: Dict[str, Any] = {}
            if 'metadata.json' in backup_zip.namelist():
                metadata_content = backup_zip.read('metadata.json')
                metadata = json.loads(metadata_content)

        logger.info(f"Configuration restored from backup: {backup_path}")
        return metadata

    def list_backups(self) -> List[Path]:
        """
        List all available backups, sorted by modification time (newest first).

        Returns:
            List of backup file paths
        """
        return sorted(
            [f for f in self.backup_dir.glob("config_backup_*.zip")],
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
