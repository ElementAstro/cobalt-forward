from typing import Dict, Any, Optional
import os
import pickle
import uuid
import time
from loguru import logger


class ResumableTransferManager:
    def __init__(self, storage_path: str = 'transfer_state.pkl'):
        """Initialize resumable transfer manager

        Args:
            storage_path: Path to file for storing transfer state
        """
        self.storage_path = storage_path
        self.transfers = self._load_state()
        self._create_storage_dir()

    def _create_storage_dir(self):
        """Ensure storage directory exists"""
        storage_dir = os.path.dirname(self.storage_path)
        if storage_dir and not os.path.exists(storage_dir):
            os.makedirs(storage_dir, exist_ok=True)

    def _load_state(self) -> Dict[str, Any]:
        """Load transfer state from storage

        Returns:
            Dictionary containing transfer state information
        """
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'rb') as f:
                    return pickle.load(f)
            except (pickle.PickleError, EOFError) as e:
                logger.warning(f"Failed to load transfer state: {e}")
                return {}
        return {}

    def _save_state(self) -> None:
        """Save transfer state to storage"""
        try:
            with open(self.storage_path, 'wb') as f:
                pickle.dump(self.transfers, f)
        except Exception as e:
            logger.error(f"Failed to save transfer state: {e}")

    def register_transfer(self, file_path: str, remote_path: str, file_size: int) -> str:
        """Register a new file transfer

        Args:
            file_path: Local file path
            remote_path: Remote file path
            file_size: Size of the file in bytes

        Returns:
            Transfer ID as string
        """
        transfer_id = str(uuid.uuid4())
        self.transfers[transfer_id] = {
            'file_path': file_path,
            'remote_path': remote_path,
            'file_size': file_size,
            'transferred': 0,
            'status': 'initialized',
            'created_at': time.time(),
            'last_updated': time.time()
        }
        self._save_state()
        return transfer_id

    def update_progress(self, transfer_id: str, bytes_transferred: int) -> None:
        """Update transfer progress

        Args:
            transfer_id: Transfer ID
            bytes_transferred: Number of bytes transferred so far

        Raises:
            KeyError: If transfer ID does not exist
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"Transfer ID does not exist: {transfer_id}")

        self.transfers[transfer_id]['transferred'] = bytes_transferred
        self.transfers[transfer_id]['last_updated'] = time.time()
        self.transfers[transfer_id]['status'] = 'in_progress'
        self._save_state()

    def complete_transfer(self, transfer_id: str) -> None:
        """Mark transfer as completed

        Args:
            transfer_id: Transfer ID

        Raises:
            KeyError: If transfer ID does not exist
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"Transfer ID does not exist: {transfer_id}")

        self.transfers[transfer_id]['status'] = 'completed'
        self.transfers[transfer_id]['transferred'] = self.transfers[transfer_id]['file_size']
        self.transfers[transfer_id]['last_updated'] = time.time()
        self._save_state()

    def fail_transfer(self, transfer_id: str, error: str) -> None:
        """Mark transfer as failed

        Args:
            transfer_id: Transfer ID
            error: Error message

        Raises:
            KeyError: If transfer ID does not exist
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"Transfer ID does not exist: {transfer_id}")

        self.transfers[transfer_id]['status'] = 'failed'
        self.transfers[transfer_id]['error'] = error
        self.transfers[transfer_id]['last_updated'] = time.time()
        self._save_state()

    def get_transfer_info(self, transfer_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a transfer

        Args:
            transfer_id: Transfer ID

        Returns:
            Dictionary with transfer information or None if not found
        """
        return self.transfers.get(transfer_id)

    def get_resume_point(self, transfer_id: str) -> int:
        """Get byte position for resuming a transfer

        Args:
            transfer_id: Transfer ID

        Returns:
            Byte position for resuming transfer

        Raises:
            KeyError: If transfer ID does not exist
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"Transfer ID does not exist: {transfer_id}")

        return self.transfers[transfer_id].get('transferred', 0)

    def list_transfers(self, status: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """List all transfers, optionally filtered by status

        Args:
            status: Optional status filter

        Returns:
            Dictionary of transfer information
        """
        if not status:
            return self.transfers

        return {k: v for k, v in self.transfers.items() if v.get('status') == status}

    def clean_completed_transfers(self, older_than_days: int = 7) -> int:
        """Clean up completed transfer records

        Args:
            older_than_days: Remove records older than this many days

        Returns:
            Number of records removed
        """
        current_time = time.time()
        cutoff_time = current_time - \
            (older_than_days * 86400)  # 86400 seconds = 1 day

        to_remove = [
            transfer_id for transfer_id, info in self.transfers.items()
            if info['status'] == 'completed' and info['last_updated'] < cutoff_time
        ]

        for transfer_id in to_remove:
            del self.transfers[transfer_id]

        if to_remove:
            self._save_state()

        return len(to_remove)

    def get_transfer_progress(self, transfer_id: str) -> float:
        """Get transfer progress percentage

        Args:
            transfer_id: Transfer ID

        Returns:
            Completion percentage (0-100)

        Raises:
            KeyError: If transfer ID does not exist
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"Transfer ID does not exist: {transfer_id}")

        info = self.transfers[transfer_id]
        if info['file_size'] == 0:
            return 100.0

        return (info['transferred'] / info['file_size']) * 100
