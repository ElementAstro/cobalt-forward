from typing import Dict, Any, Optional
import os
import pickle
import uuid
import time
from pathlib import Path
from loguru import logger


class ResumableTransferManager:
    def __init__(self, storage_path: str = 'transfer_state.pkl'):
        """初始化可恢复传输管理器

        Args:
            storage_path: 存储传输状态的文件路径
        """
        self.storage_path = storage_path
        self.transfers = self._load_state()
        self._create_storage_dir()

    def _create_storage_dir(self):
        """确保存储目录存在"""
        storage_dir = os.path.dirname(self.storage_path)
        if storage_dir and not os.path.exists(storage_dir):
            os.makedirs(storage_dir, exist_ok=True)

    def _load_state(self) -> Dict[str, Any]:
        """加载传输状态"""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'rb') as f:
                    return pickle.load(f)
            except (pickle.PickleError, EOFError) as e:
                logger.warning(f"无法加载传输状态: {e}")
                return {}
        return {}

    def _save_state(self) -> None:
        """保存传输状态"""
        try:
            with open(self.storage_path, 'wb') as f:
                pickle.dump(self.transfers, f)
        except Exception as e:
            logger.error(f"保存传输状态失败: {e}")

    def register_transfer(self, file_path: str, remote_path: str, file_size: int) -> str:
        """注册新的文件传输

        Args:
            file_path: 本地文件路径
            remote_path: 远程文件路径
            file_size: 文件大小

        Returns:
            传输ID
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
        """更新传输进度

        Args:
            transfer_id: 传输ID
            bytes_transferred: 已传输的字节数
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"传输ID不存在: {transfer_id}")

        self.transfers[transfer_id]['transferred'] = bytes_transferred
        self.transfers[transfer_id]['last_updated'] = time.time()
        self.transfers[transfer_id]['status'] = 'in_progress'
        self._save_state()

    def complete_transfer(self, transfer_id: str) -> None:
        """标记传输为完成状态

        Args:
            transfer_id: 传输ID
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"传输ID不存在: {transfer_id}")

        self.transfers[transfer_id]['status'] = 'completed'
        self.transfers[transfer_id]['transferred'] = self.transfers[transfer_id]['file_size']
        self.transfers[transfer_id]['last_updated'] = time.time()
        self._save_state()

    def fail_transfer(self, transfer_id: str, error: str) -> None:
        """标记传输为失败状态

        Args:
            transfer_id: 传输ID
            error: 错误信息
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"传输ID不存在: {transfer_id}")

        self.transfers[transfer_id]['status'] = 'failed'
        self.transfers[transfer_id]['error'] = error
        self.transfers[transfer_id]['last_updated'] = time.time()
        self._save_state()

    def get_transfer_info(self, transfer_id: str) -> Optional[Dict[str, Any]]:
        """获取传输信息

        Args:
            transfer_id: 传输ID

        Returns:
            传输信息字典
        """
        return self.transfers.get(transfer_id)

    def get_resume_point(self, transfer_id: str) -> int:
        """获取恢复点位置

        Args:
            transfer_id: 传输ID

        Returns:
            恢复传输的字节位置
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"传输ID不存在: {transfer_id}")

        return self.transfers[transfer_id].get('transferred', 0)

    def list_transfers(self, status: str = None) -> Dict[str, Dict[str, Any]]:
        """列出所有传输

        Args:
            status: 可选筛选状态

        Returns:
            传输信息字典
        """
        if not status:
            return self.transfers

        return {k: v for k, v in self.transfers.items() if v.get('status') == status}

    def clean_completed_transfers(self, older_than_days: int = 7) -> int:
        """清理已完成的传输记录

        Args:
            older_than_days: 清理多少天前的记录

        Returns:
            清理的记录数量
        """
        current_time = time.time()
        cutoff_time = current_time - (older_than_days * 86400)  # 86400秒 = 1天

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
        """获取传输进度百分比

        Args:
            transfer_id: 传输ID

        Returns:
            完成百分比（0-100）
        """
        if transfer_id not in self.transfers:
            raise KeyError(f"传输ID不存在: {transfer_id}")

        info = self.transfers[transfer_id]
        if info['file_size'] == 0:
            return 100.0

        return (info['transferred'] / info['file_size']) * 100
