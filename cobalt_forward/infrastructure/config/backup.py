"""
Configuration backup management for the Cobalt Forward application.

This module provides functionality for creating, managing, and restoring
configuration backups with metadata and integrity verification.
"""

from pathlib import Path
import json
from datetime import datetime
import zipfile
import logging
import shutil
import tempfile
import hashlib
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


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
        """Rotate old backup files, removing the oldest when max_backups is reached."""
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
            SHA256 checksum as hex string
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def create_backup(
        self,
        config_data: Dict[str, Any],
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a backup of the configuration.

        Args:
            config_data: Configuration data to backup
            description: Optional description for the backup
            metadata: Additional metadata to include

        Returns:
            Backup information dictionary
        """
        try:
            timestamp = datetime.now()
            backup_id = timestamp.strftime("%Y%m%d_%H%M%S")
            backup_filename = f"config_backup_{backup_id}.zip"
            backup_path = self.backup_dir / backup_filename

            # Rotate old backups before creating new one
            self._rotate_backups()

            # Create backup metadata
            backup_metadata = {
                "backup_id": backup_id,
                "created_at": timestamp.isoformat(),
                "description": description or f"Automatic backup {backup_id}",
                "version": "1.0.0",
                "config_checksum": None,
                "file_count": 1,
                "compressed_size": 0,
                "original_size": 0
            }

            if metadata:
                backup_metadata.update(metadata)

            # Create temporary directory for backup preparation
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Write configuration to temporary file
                config_file = temp_path / "config.json"
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, indent=2, ensure_ascii=False)
                
                # Calculate checksum
                backup_metadata["config_checksum"] = self._calculate_checksum(config_file)
                backup_metadata["original_size"] = config_file.stat().st_size

                # Write metadata
                metadata_file = temp_path / "backup_metadata.json"
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(backup_metadata, f, indent=2, ensure_ascii=False)

                # Create ZIP archive
                with zipfile.ZipFile(backup_path, 'w', self.compression) as zipf:
                    zipf.write(config_file, "config.json")
                    zipf.write(metadata_file, "backup_metadata.json")

                backup_metadata["compressed_size"] = backup_path.stat().st_size

            logger.info(f"Created configuration backup: {backup_filename}")
            
            return {
                "backup_id": backup_id,
                "backup_path": str(backup_path),
                "created_at": timestamp,
                "description": backup_metadata["description"],
                "size": backup_metadata["compressed_size"],
                "checksum": backup_metadata["config_checksum"],
                "version": backup_metadata["version"]
            }

        except Exception as e:
            logger.error(f"Failed to create configuration backup: {e}")
            raise

    def restore_backup(self, backup_id: str) -> Dict[str, Any]:
        """
        Restore configuration from a backup.

        Args:
            backup_id: ID of the backup to restore

        Returns:
            Restored configuration data

        Raises:
            FileNotFoundError: If backup file doesn't exist
            ValueError: If backup is corrupted or invalid
        """
        backup_filename = f"config_backup_{backup_id}.zip"
        backup_path = self.backup_dir / backup_filename

        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_filename}")

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Extract backup
                with zipfile.ZipFile(backup_path, 'r') as zipf:
                    zipf.extractall(temp_path)

                # Load metadata
                metadata_file = temp_path / "backup_metadata.json"
                if not metadata_file.exists():
                    raise ValueError("Backup metadata not found")

                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                # Load configuration
                config_file = temp_path / "config.json"
                if not config_file.exists():
                    raise ValueError("Configuration file not found in backup")

                # Verify checksum if available
                if metadata.get("config_checksum"):
                    actual_checksum = self._calculate_checksum(config_file)
                    if actual_checksum != metadata["config_checksum"]:
                        raise ValueError("Backup integrity check failed: checksum mismatch")

                with open(config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)

                logger.info(f"Restored configuration from backup: {backup_id}")
                return config_data

        except Exception as e:
            logger.error(f"Failed to restore backup {backup_id}: {e}")
            raise

    def list_backups(self) -> List[Path]:
        """
        List all available backup files, sorted by creation time (newest first).

        Returns:
            List of backup file paths
        """
        try:
            backup_files = list(self.backup_dir.glob("config_backup_*.zip"))
            # Sort by modification time, newest first
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            return backup_files
        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
            return []

    def get_backup_info(self, backup_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific backup.

        Args:
            backup_id: ID of the backup

        Returns:
            Backup information dictionary or None if not found
        """
        backup_filename = f"config_backup_{backup_id}.zip"
        backup_path = self.backup_dir / backup_filename

        if not backup_path.exists():
            return None

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Extract metadata only
                with zipfile.ZipFile(backup_path, 'r') as zipf:
                    try:
                        zipf.extract("backup_metadata.json", temp_path)
                    except KeyError:
                        # Fallback for backups without metadata
                        return {
                            "backup_id": backup_id,
                            "created_at": datetime.fromtimestamp(backup_path.stat().st_mtime).isoformat(),
                            "description": f"Legacy backup {backup_id}",
                            "size": backup_path.stat().st_size,
                            "version": "unknown"
                        }

                metadata_file = temp_path / "backup_metadata.json"
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                # Add file system information
                metadata["file_path"] = str(backup_path)
                metadata["file_size"] = backup_path.stat().st_size
                
                return metadata

        except Exception as e:
            logger.error(f"Failed to get backup info for {backup_id}: {e}")
            return None

    def delete_backup(self, backup_id: str) -> bool:
        """
        Delete a specific backup.

        Args:
            backup_id: ID of the backup to delete

        Returns:
            True if successful, False otherwise
        """
        backup_filename = f"config_backup_{backup_id}.zip"
        backup_path = self.backup_dir / backup_filename

        if not backup_path.exists():
            logger.warning(f"Backup file not found: {backup_filename}")
            return False

        try:
            backup_path.unlink()
            logger.info(f"Deleted backup: {backup_filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete backup {backup_id}: {e}")
            return False

    def cleanup_old_backups(self, days: int = 30) -> int:
        """
        Clean up backups older than specified days.

        Args:
            days: Number of days to keep backups

        Returns:
            Number of backups deleted
        """
        cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
        deleted_count = 0

        try:
            for backup_path in self.list_backups():
                if backup_path.stat().st_mtime < cutoff_time:
                    try:
                        backup_path.unlink()
                        deleted_count += 1
                        logger.info(f"Deleted old backup: {backup_path.name}")
                    except Exception as e:
                        logger.error(f"Failed to delete old backup {backup_path.name}: {e}")

            logger.info(f"Cleaned up {deleted_count} old backups")
            return deleted_count

        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}")
            return 0
