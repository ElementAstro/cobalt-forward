from collections import deque
import time
from typing import Dict, List, Optional, Tuple
import psutil
from functools import lru_cache
import threading
import os
import json
from datetime import datetime
from loguru import logger


class TransferMonitor:
    def __init__(self, history_size: int = 1000, update_interval: float = 1.0):
        """Initialize transfer monitor with specified history size and update interval.
        
        Args:
            history_size: Maximum number of history records to keep
            update_interval: Update frequency in seconds
        """
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
        """Get system metrics with caching"""
        return {
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent,
            'io_counters': psutil.disk_io_counters()._asdict()
        }

    def start_monitoring(self):
        """Start monitoring transfer operations"""
        self.start_time = time.time()
        self.transfer_history.clear()
        with self.lock:
            if self.monitoring:
                return

            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            logger.debug("Transfer monitoring started")

    def record_transfer(self, transferred: int, total: int, filename: str):
        """Record transfer progress with optimized metrics
        
        Args:
            transferred: Number of bytes transferred
            total: Total number of bytes
            filename: Name of the file being transferred
        """
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
        """Stop transfer monitoring"""
        self.end_time = time.time()
        with self.lock:
            self.monitoring = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=2.0)
                self.monitor_thread = None
            logger.debug("Transfer monitoring stopped")

    def get_statistics(self) -> Dict:
        """Calculate and return optimized transfer statistics
        
        Returns:
            Dictionary containing transfer statistics
        """
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
        """Calculate transfer efficiency ratio
        
        Args:
            records: List of transfer records
            
        Returns:
            Efficiency ratio as a float
        """
        if not records:
            return 0.0
        successful_transfers = sum(1 for r in records if r['transferred'] > 0)
        return successful_transfers / len(records)

    def _monitor_loop(self):
        """Monitoring loop that calculates transfer speed"""
        while self.monitoring:
            with self.lock:
                current_time = time.time()
                for transfer_id, transfer in list(self.transfers.items()):
                    if transfer['status'] in ('completed', 'failed'):
                        continue

                    # Calculate elapsed time
                    elapsed = current_time - transfer['last_update_time']
                    if elapsed > 0:
                        # Calculate transfer speed (bytes/s)
                        transfer['speed'] = (
                            transfer['current_bytes'] - transfer['last_bytes']) / elapsed
                        transfer['last_bytes'] = transfer['current_bytes']
                        transfer['last_update_time'] = current_time

                        # Estimate remaining time
                        if transfer['speed'] > 0 and transfer['total_bytes'] > 0:
                            remaining_bytes = transfer['total_bytes'] - \
                                transfer['current_bytes']
                            transfer['estimated_time'] = remaining_bytes / \
                                transfer['speed']

                        # Update progress
                        if transfer['total_bytes'] > 0:
                            transfer['progress'] = (
                                transfer['current_bytes'] / transfer['total_bytes']) * 100

                        # Record history data
                        self._record_history(transfer_id, transfer)

            time.sleep(self.update_interval)

    def _record_history(self, transfer_id: str, transfer: Dict):
        """Record transfer history data
        
        Args:
            transfer_id: Unique transfer identifier
            transfer: Transfer data dictionary
        """
        history_entry = {
            'timestamp': time.time(),
            'transfer_id': transfer_id,
            'speed': transfer.get('speed', 0),
            'progress': transfer.get('progress', 0),
            'current_bytes': transfer['current_bytes'],
            'system_metrics': self._get_system_metrics()
        }

        self.history.append(history_entry)

        # Limit history size
        if len(self.history) > self.max_history_size:
            self.history.pop(0)

    def create_transfer(self, transfer_id: str, total_bytes: int, filename: str) -> Dict:
        """Create a new transfer record
        
        Args:
            transfer_id: Unique transfer identifier
            total_bytes: Total size of the transfer in bytes
            filename: Name of the file being transferred
            
        Returns:
            Newly created transfer record
        """
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
        """Update transfer progress
        
        Args:
            transfer_id: Unique transfer identifier
            current_bytes: Current number of bytes transferred
            status: Optional new status
        """
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
        """Get transfer information
        
        Args:
            transfer_id: Unique transfer identifier
            
        Returns:
            Transfer information or None if not found
        """
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
        """Get all active transfers
        
        Returns:
            List of (transfer_id, transfer_info) tuples for active transfers
        """
        with self.lock:
            return [(tid, transfer) for tid, transfer in self.transfers.items()
                    if transfer['status'] == 'running']

    def cleanup_completed(self, age_threshold: float = 3600):
        """Clean up completed transfer records
        
        Args:
            age_threshold: Age in seconds after which to remove completed records
        """
        current_time = time.time()
        with self.lock:
            for transfer_id in list(self.transfers.keys()):
                transfer = self.transfers[transfer_id]
                if transfer['status'] in ('completed', 'failed'):
                    if current_time - transfer.get('end_time', 0) > age_threshold:
                        del self.transfers[transfer_id]

    def export_history(self, filename: str):
        """Export transfer history to a file
        
        Args:
            filename: Path to export the history data
        """
        with self.lock:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'transfers': self.transfers,
                    'history': self.history,
                    'export_time': datetime.now().isoformat()
                }, f, indent=2)
