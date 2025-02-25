from collections import deque
import time
from typing import Dict, Optional
import psutil
from functools import lru_cache
import threading
from typing import Dict, List, Optional, Tuple
import os
import json
from datetime import datetime
from loguru import logger


class TransferMonitor:
    def __init__(self, history_size: int = 1000, update_interval: float = 1.0):
        self.transfer_history = deque(maxlen=history_size)
        self.start_time = None
        self.end_time = None
        self.update_interval = update_interval
        self.transfers = {}
        self.monitoring = False
        self.lock = threading.RLock()
        self.monitor_thread = None
        self.history = []
        self.max_history_size = 100

    @lru_cache(maxsize=128)
    def _get_system_metrics(self) -> Dict[str, float]:
        """获取系统指标，带缓存"""
        return {
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent,
            'io_counters': psutil.disk_io_counters()._asdict()
        }

    def start_monitoring(self):
        """开始监控"""
        self.start_time = time.time()
        self.transfer_history.clear()
        with self.lock:
            if self.monitoring:
                return

            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            logger.debug("传输监控已启动")

    def record_transfer(self, transferred: int, total: int, filename: str):
        """优化的记录传输"""
        current_time = time.time()
        metrics = self._get_system_metrics()

        self.transfer_history.append({
            'timestamp': current_time,
            'transferred': transferred,
            'total': total,
            'filename': filename,
            **metrics
        })

    def stop_monitoring(self):
        """停止监控"""
        self.end_time = time.time()
        with self.lock:
            self.monitoring = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=2.0)
                self.monitor_thread = None
            logger.debug("传输监控已停止")

    def get_statistics(self) -> Dict:
        """优化的统计信息计算"""
        if not self.transfer_history:
            return {}

        records = list(self.transfer_history)
        total_time = self.end_time - self.start_time
        total_transferred = sum(r['transferred'] for r in records)

        return {
            'total_time': total_time,
            'total_transferred': total_transferred,
            'average_speed': total_transferred / total_time if total_time > 0 else 0,
            'peak_cpu_usage': max(r['cpu_usage'] for r in records),
            'peak_memory_usage': max(r['memory_usage'] for r in records),
            'transfer_efficiency': self._calculate_efficiency(records)
        }

    def _calculate_efficiency(self, records: list) -> float:
        """计算传输效率"""
        if not records:
            return 0.0
        successful_transfers = sum(1 for r in records if r['transferred'] > 0)
        return successful_transfers / len(records)

    def _monitor_loop(self):
        """监控循环，计算传输速度"""
        while self.monitoring:
            with self.lock:
                current_time = time.time()
                for transfer_id, transfer in list(self.transfers.items()):
                    if transfer['status'] in ('completed', 'failed'):
                        continue

                    # 计算经过的时间
                    elapsed = current_time - transfer['last_update_time']
                    if elapsed > 0:
                        # 计算传输速度 (bytes/s)
                        transfer['speed'] = (
                            transfer['current_bytes'] - transfer['last_bytes']) / elapsed
                        transfer['last_bytes'] = transfer['current_bytes']
                        transfer['last_update_time'] = current_time

                        # 估计剩余时间
                        if transfer['speed'] > 0 and transfer['total_bytes'] > 0:
                            remaining_bytes = transfer['total_bytes'] - \
                                transfer['current_bytes']
                            transfer['estimated_time'] = remaining_bytes / \
                                transfer['speed']

                        # 更新进度
                        if transfer['total_bytes'] > 0:
                            transfer['progress'] = (
                                transfer['current_bytes'] / transfer['total_bytes']) * 100

                        # 记录历史数据
                        self._record_history(transfer_id, transfer)

            time.sleep(self.update_interval)

    def _record_history(self, transfer_id: str, transfer: Dict):
        """记录传输历史数据"""
        history_entry = {
            'timestamp': time.time(),
            'transfer_id': transfer_id,
            'speed': transfer.get('speed', 0),
            'progress': transfer.get('progress', 0),
            'current_bytes': transfer['current_bytes'],
            'system_metrics': self._get_system_metrics()
        }

        self.history.append(history_entry)

        # 限制历史记录大小
        if len(self.history) > self.max_history_size:
            self.history.pop(0)

    def create_transfer(self, transfer_id: str, total_bytes: int, filename: str) -> Dict:
        """创建新的传输记录"""
        transfer = {
            'start_time': time.time(),
            'last_update_time': time.time(),
            'filename': filename,
            'total_bytes': total_bytes,
            'current_bytes': 0,
            'last_bytes': 0,
            'speed': 0,
            'progress': 0,
            'status': 'running',
            'estimated_time': None,
            'error': None
        }

        with self.lock:
            self.transfers[transfer_id] = transfer

        return transfer

    def update_transfer(self, transfer_id: str, current_bytes: int, status: Optional[str] = None):
        """更新传输进度"""
        with self.lock:
            if transfer_id not in self.transfers:
                return

            transfer = self.transfers[transfer_id]
            transfer['current_bytes'] = current_bytes

            if status:
                transfer['status'] = status

            if current_bytes >= transfer['total_bytes']:
                transfer['status'] = 'completed'
                transfer['progress'] = 100
                transfer['end_time'] = time.time()

    def get_transfer_info(self, transfer_id: str) -> Optional[Dict]:
        """获取传输信息"""
        with self.lock:
            if transfer_id not in self.transfers:
                return None

            transfer = self.transfers[transfer_id]
            return {
                'filename': transfer['filename'],
                'total_bytes': transfer['total_bytes'],
                'current_bytes': transfer['current_bytes'],
                'speed': transfer.get('speed', 0),
                'progress': transfer.get('progress', 0),
                'status': transfer['status'],
                'estimated_time': transfer.get('estimated_time'),
                'start_time': transfer['start_time'],
                'end_time': transfer.get('end_time')
            }

    def get_active_transfers(self) -> List[Tuple[str, Dict]]:
        """获取所有活动的传输"""
        with self.lock:
            return [(tid, transfer) for tid, transfer in self.transfers.items()
                    if transfer['status'] == 'running']

    def cleanup_completed(self, age_threshold: float = 3600):
        """清理已完成的传输记录"""
        current_time = time.time()
        with self.lock:
            for transfer_id in list(self.transfers.keys()):
                transfer = self.transfers[transfer_id]
                if transfer['status'] in ('completed', 'failed'):
                    if current_time - transfer.get('end_time', 0) > age_threshold:
                        del self.transfers[transfer_id]

    def export_history(self, filename: str):
        """导出传输历史到文件"""
        with self.lock:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'transfers': self.transfers,
                    'history': self.history,
                    'export_time': datetime.now().isoformat()
                }, f, indent=2)
