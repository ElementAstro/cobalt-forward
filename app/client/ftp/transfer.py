from collections import deque
import time
from typing import Dict

import psutil


class TransferMonitor:
    def __init__(self):
        self.transfer_history = deque(maxlen=1000)
        self.start_time = None
        self.end_time = None

    def start_monitoring(self):
        """开始监控"""
        self.start_time = time.time()
        self.transfer_history.clear()

    def record_transfer(self, transferred: int, total: int, filename: str):
        """记录传输"""
        current_time = time.time()
        self.transfer_history.append({
            'timestamp': current_time,
            'transferred': transferred,
            'total': total,
            'filename': filename,
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent
        })

    def stop_monitoring(self):
        """停止监控"""
        self.end_time = time.time()

    def get_statistics(self) -> Dict:
        """获取统计信息"""
        if not self.transfer_history:
            return {}

        total_time = self.end_time - self.start_time
        total_transferred = sum(record['transferred']
                                for record in self.transfer_history)
        avg_speed = total_transferred / total_time if total_time > 0 else 0

        return {
            'total_time': total_time,
            'total_transferred': total_transferred,
            'average_speed': avg_speed,
            'peak_cpu_usage': max(record['cpu_usage'] for record in self.transfer_history),
            'peak_memory_usage': max(record['memory_usage'] for record in self.transfer_history)
        }
