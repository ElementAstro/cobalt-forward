from collections import deque
import time
from typing import Dict, Optional
import psutil
from functools import lru_cache


class TransferMonitor:
    def __init__(self, history_size: int = 1000):
        self.transfer_history = deque(maxlen=history_size)
        self.start_time = None
        self.end_time = None

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
