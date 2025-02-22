import hashlib
import pickle
from typing import Dict, Any


class ResumableTransferManager:
    def __init__(self, storage_path: str = 'transfer_state.pkl'):
        self.storage_path = storage_path
        self.transfers = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """加载传输状态"""
        try:
            with open(self.storage_path, 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return {}

    def _save_state(self):
        """保存传输状态"""
        with open(self.storage_path, 'wb') as f:
            pickle.dump(self.transfers, f)

    def register_transfer(self, file_path: str, remote_path: str, file_size: int):
        """注册新的传输任务"""
        transfer_id = hashlib.md5(
            f"{file_path}{remote_path}".encode()).hexdigest()
        self.transfers[transfer_id] = {
            'file_path': file_path,
            'remote_path': remote_path,
            'file_size': file_size,
            'transferred': 0,
            'status': 'pending'
        }
        self._save_state()
        return transfer_id

    def update_progress(self, transfer_id: str, transferred: int):
        """更新传输进度"""
        if transfer_id in self.transfers:
            self.transfers[transfer_id]['transferred'] = transferred
            self._save_state()

    def get_resume_point(self, transfer_id: str) -> int:
        """获取续传点"""
        return self.transfers.get(transfer_id, {}).get('transferred', 0)
