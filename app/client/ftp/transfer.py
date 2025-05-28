from collections import deque
import time
from typing import Dict, List, Optional, Tuple, Any, Deque, TypedDict 
import psutil
from functools import lru_cache
import threading
import json
from datetime import datetime
from loguru import logger

# Define TypedDicts for better structure and type safety
class SystemMetrics(TypedDict):
    cpu_usage: float
    memory_usage: float
    io_counters: Dict[str, Any] # psutil's _asdict() can have various numeric types

class TransferRecordMain(TypedDict): # For self.transfer_history (raw chunk/progress log)
    timestamp: float
    transferred: int # Bytes transferred in this record/chunk
    total: int       # Total bytes for the file associated with this record
    filename: str
    cpu_usage: float
    memory_usage: float
    io_counters: Dict[str, Any]

class TransferInfo(TypedDict): # For self.transfers (state of an ongoing/completed transfer)
    start_time: float
    last_update_time: float
    filename: str
    total_bytes: int
    current_bytes: int
    last_bytes: int
    speed: float
    progress: float
    status: str # e.g., 'running', 'completed', 'failed'
    estimated_time: Optional[float]
    error: Optional[str]
    end_time: Optional[float]

class HistoryEntry(TypedDict): # For self.history (periodic snapshots of transfers)
    timestamp: float
    transfer_id: str
    speed: float
    progress: float
    current_bytes: int
    system_metrics: SystemMetrics

class TransferInfoSubset(TypedDict): # For get_transfer_info return
    filename: str
    total_bytes: int
    current_bytes: int
    speed: float
    progress: float
    status: str
    estimated_time: Optional[float]
    start_time: float
    end_time: Optional[float]

class StatisticsDict(TypedDict):
    total_time: float
    total_transferred: int
    average_speed: float
    peak_cpu_usage: float
    peak_memory_usage: float
    transfer_efficiency: float


class TransferMonitor:
    def __init__(self, history_size: int = 1000, update_interval: float = 1.0, detailed_history_max_size: int = 100):
        """Initialize transfer monitor with specified history size and update interval.
        
        Args:
            history_size: Maximum number of summary history records to keep for get_statistics.
            update_interval: Update frequency in seconds for monitoring loop.
            detailed_history_max_size: Maximum number of detailed periodic history entries.
        """
        self.transfer_history: Deque[TransferRecordMain] = deque(maxlen=history_size)
        self.start_time: Optional[float] = None # Global monitoring start time
        self.end_time: Optional[float] = None   # Global monitoring end time
        self.update_interval: float = update_interval
        self.transfers: Dict[str, TransferInfo] = {}
        self.monitoring: bool = False
        self.lock: threading.RLock = threading.RLock()
        self.monitor_thread: Optional[threading.Thread] = None
        
        self.max_history_size: int = detailed_history_max_size 
        self.history: Deque[HistoryEntry] = deque(maxlen=self.max_history_size)


    @lru_cache(maxsize=128)
    def _get_system_metrics(self) -> SystemMetrics:
        """Get system metrics with caching"""
        disk_counters_obj = psutil.disk_io_counters()
        io_counters_data: Dict[str, Any] = {}
        if disk_counters_obj:
            io_counters_data = disk_counters_obj._asdict()
        
        return SystemMetrics(
            cpu_usage=psutil.cpu_percent(),
            memory_usage=psutil.virtual_memory().percent,
            io_counters=io_counters_data
        )

    def start_monitoring(self):
        """Start monitoring transfer operations"""
        self.start_time = time.time()
        # self.transfer_history.clear() # Cleared on demand or by maxlen
        with self.lock:
            if self.monitoring:
                logger.debug("Monitoring is already active.")
                return

            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, name="TransferMonitorThread")
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            logger.debug("Transfer monitoring started")

    def record_transfer(self, transferred: int, total: int, filename: str):
        """Record transfer progress with optimized metrics.
        This method logs individual transfer chunks/progress points into self.transfer_history.
        
        Args:
            transferred: Number of bytes transferred in this event.
            total: Total number of bytes for the file.
            filename: Name of the file being transferred.
        """
        current_time = time.time()
        metrics = self._get_system_metrics()

        record: TransferRecordMain = TransferRecordMain(
            timestamp=current_time,
            transferred=transferred,
            total=total,
            filename=filename,
            cpu_usage=metrics['cpu_usage'],
            memory_usage=metrics['memory_usage'],
            io_counters=metrics['io_counters']
        )
        self.transfer_history.append(record)

    def stop_monitoring(self):
        """Stop transfer monitoring"""
        self.end_time = time.time() 
        with self.lock:
            if not self.monitoring:
                logger.debug("Monitoring is not active or already stopped.")
                return
            self.monitoring = False # Signal the loop to stop
            
            monitor_thread = self.monitor_thread # Local variable for thread safety
            if monitor_thread and monitor_thread.is_alive():
                try:
                    monitor_thread.join(timeout=max(1.0, self.update_interval * 2)) 
                    if monitor_thread.is_alive():
                        logger.warning("Monitor thread did not join in time.")
                except RuntimeError: 
                    logger.warning("RuntimeError during monitor thread join (e.g., thread not started).")
            self.monitor_thread = None
            logger.debug("Transfer monitoring stopped")

    def get_statistics(self) -> StatisticsDict:
        """Calculate and return optimized transfer statistics based on self.transfer_history.
        
        Returns:
            Dictionary containing transfer statistics.
        """
        if not self.transfer_history:
            return StatisticsDict(
                total_time=0.0,
                total_transferred=0,
                average_speed=0.0,
                peak_cpu_usage=0.0,
                peak_memory_usage=0.0,
                transfer_efficiency=0.0
            )

        records: List[TransferRecordMain] = list(self.transfer_history)
        
        total_time_val: float = 0.0
        # Use self.end_time if monitoring stopped, otherwise current time for ongoing duration
        current_eval_time = self.end_time if self.end_time is not None else time.time()

        if self.start_time is not None:
            if current_eval_time >= self.start_time:
                 total_time_val = current_eval_time - self.start_time
        
        total_transferred_val: int = sum(r['transferred'] for r in records)

        return StatisticsDict(
            total_time=total_time_val,
            total_transferred=total_transferred_val,
            average_speed=total_transferred_val / total_time_val if total_time_val > 0 else 0.0,
            peak_cpu_usage=max(r['cpu_usage'] for r in records) if records else 0.0,
            peak_memory_usage=max(r['memory_usage'] for r in records) if records else 0.0,
            transfer_efficiency=self._calculate_efficiency(records)
        )

    def _calculate_efficiency(self, records: List[TransferRecordMain]) -> float:
        """Calculate transfer efficiency ratio
        
        Args:
            records: List of transfer records from self.transfer_history
            
        Returns:
            Efficiency ratio as a float
        """
        if not records:
            return 0.0
        successful_records: int = sum(1 for r in records if r['transferred'] > 0) # Assuming 'transferred' > 0 means a successful chunk
        return successful_records / len(records) if len(records) > 0 else 0.0

    def _monitor_loop(self):
        """Monitoring loop that calculates transfer speed for items in self.transfers"""
        while self.monitoring:
            with self.lock:
                current_time = time.time()
                for transfer_id, transfer_info in list(self.transfers.items()): # Iterate on a copy
                    if transfer_info['status'] not in ('running', 'pending'): # Check for active states
                        continue

                    elapsed: float = current_time - transfer_info['last_update_time']
                    
                    if elapsed >= self.update_interval or (transfer_info['current_bytes'] != transfer_info['last_bytes']): # Update if interval passed or bytes changed
                        if elapsed > 0.001: # Avoid division by zero or tiny elapsed times
                            bytes_diff = transfer_info['current_bytes'] - transfer_info['last_bytes']
                            transfer_info['speed'] = bytes_diff / elapsed
                        # If elapsed is too small, speed might not be updated to avoid extreme values, or keep previous
                        
                        transfer_info['last_bytes'] = transfer_info['current_bytes']
                        transfer_info['last_update_time'] = current_time

                        if transfer_info['total_bytes'] > 0:
                            transfer_info['progress'] = (transfer_info['current_bytes'] / transfer_info['total_bytes']) * 100
                            if transfer_info['speed'] > 0:
                                remaining_bytes: int = transfer_info['total_bytes'] - transfer_info['current_bytes']
                                if remaining_bytes > 0:
                                    transfer_info['estimated_time'] = remaining_bytes / transfer_info['speed']
                                else:
                                    transfer_info['estimated_time'] = 0.0 # Completed
                            else: # No speed, cannot estimate if not completed
                                transfer_info['estimated_time'] = None if transfer_info['progress'] < 100 else 0.0
                        else: # total_bytes is 0 or unknown
                            transfer_info['progress'] = 0 if transfer_info['current_bytes'] == 0 else 100 # Or undefined
                            transfer_info['estimated_time'] = 0.0 # Assume 0-byte files are instant

                        self._record_history(transfer_id, transfer_info)
            
            time.sleep(self.update_interval)


    def _record_history(self, transfer_id: str, transfer: TransferInfo):
        """Record transfer history data into self.history
        
        Args:
            transfer_id: Unique transfer identifier
            transfer: Transfer data dictionary (TransferInfo)
        """
        history_item: HistoryEntry = HistoryEntry(
            timestamp=time.time(),
            transfer_id=transfer_id,
            speed=transfer.get('speed', 0.0),
            progress=transfer.get('progress', 0.0),
            current_bytes=transfer['current_bytes'],
            system_metrics=self._get_system_metrics()
        )
        self.history.append(history_item)

    def create_transfer(self, transfer_id: str, total_bytes: int, filename: str) -> TransferInfo:
        """Create a new transfer record in self.transfers
        
        Args:
            transfer_id: Unique transfer identifier
            total_bytes: Total size of the transfer in bytes
            filename: Name of the file being transferred
            
        Returns:
            Newly created transfer record (TransferInfo)
        """
        current_time = time.time()
        new_transfer: TransferInfo = TransferInfo(
            start_time=current_time,
            last_update_time=current_time,
            filename=filename,
            total_bytes=total_bytes,
            current_bytes=0,
            last_bytes=0,
            speed=0.0,
            progress=0.0,
            status='running', 
            estimated_time=None if total_bytes > 0 else 0.0, # ETA 0 for 0-byte file
            error=None,
            end_time=None
        )

        with self.lock:
            self.transfers[transfer_id] = new_transfer
        logger.info(f"Created transfer {transfer_id} for {filename} ({total_bytes} bytes)")
        return new_transfer

    def update_transfer(self, transfer_id: str, current_bytes: int, status: Optional[str] = None, error_message: Optional[str] = None):
        """Update transfer progress in self.transfers
        
        Args:
            transfer_id: Unique transfer identifier
            current_bytes: Current number of bytes transferred
            status: Optional new status (e.g., 'completed', 'failed', 'paused')
            error_message: Optional error message if status is 'failed'
        """
        with self.lock:
            if transfer_id not in self.transfers:
                logger.warning(f"Attempted to update non-existent transfer: {transfer_id}")
                return

            transfer = self.transfers[transfer_id]
            
            # Only update if status is active, or if a new status is forcing a change
            if transfer['status'] not in ('completed', 'failed') or status is not None:
                transfer['current_bytes'] = current_bytes
                # last_update_time is updated by _monitor_loop or here if status changes
                # transfer['last_update_time'] = time.time() 

                if status:
                    if transfer['status'] != status: # Log status change
                         logger.info(f"Transfer {transfer_id} status changed from {transfer['status']} to {status}")
                    transfer['status'] = status
                    transfer['last_update_time'] = time.time() # Ensure update time reflects status change
                    if status == 'failed' and error_message:
                        transfer['error'] = error_message
                    if status in ('completed', 'failed'):
                        transfer['end_time'] = transfer.get('end_time') or time.time() # Set end_time if not already set
                        if status == 'completed':
                             transfer['progress'] = 100.0
                             transfer['estimated_time'] = 0.0
                             if transfer['total_bytes'] == 0 and transfer['current_bytes'] == 0: # Ensure 0-byte files are 100%
                                 transfer['current_bytes'] = 0 # Keep as 0
                
                # Auto-completion logic based on bytes
                if transfer['status'] == 'running': # Only if still running
                    if transfer['total_bytes'] > 0 and current_bytes >= transfer['total_bytes']:
                        transfer['status'] = 'completed'
                        transfer['progress'] = 100.0
                        transfer['end_time'] = transfer.get('end_time') or time.time()
                        transfer['estimated_time'] = 0.0
                        logger.info(f"Transfer {transfer_id} completed by byte count.")
                    elif transfer['total_bytes'] == 0: # Auto-complete zero-byte files
                        transfer['status'] = 'completed'
                        transfer['progress'] = 100.0
                        transfer['current_bytes'] = 0 # Ensure current_bytes is 0 for 0-byte file
                        transfer['end_time'] = transfer.get('end_time') or time.time()
                        transfer['estimated_time'] = 0.0
                        logger.info(f"Transfer {transfer_id} (0 bytes) completed.")


    def get_transfer_info(self, transfer_id: str) -> Optional[TransferInfoSubset]:
        """Get transfer information from self.transfers
        
        Args:
            transfer_id: Unique transfer identifier
            
        Returns:
            Transfer information (TransferInfoSubset) or None if not found
        """
        with self.lock:
            transfer = self.transfers.get(transfer_id)
            if not transfer:
                return None

            return TransferInfoSubset(
                filename=transfer['filename'],
                total_bytes=transfer['total_bytes'],
                current_bytes=transfer['current_bytes'],
                speed=transfer.get('speed', 0.0),
                progress=transfer.get('progress', 0.0),
                status=transfer['status'],
                estimated_time=transfer.get('estimated_time'),
                start_time=transfer['start_time'],
                end_time=transfer.get('end_time')
            )

    def get_active_transfers(self) -> List[Tuple[str, TransferInfo]]:
        """Get all active (running or pending) transfers from self.transfers
        
        Returns:
            List of (transfer_id, TransferInfo) tuples for active transfers
        """
        with self.lock:
            return [(tid, transfer) for tid, transfer in self.transfers.items()
                    if transfer['status'] in ('running', 'pending')] # Include pending if that's a possible state

    def cleanup_completed(self, age_threshold: float = 3600):
        """Clean up completed or failed transfer records from self.transfers
        
        Args:
            age_threshold: Age in seconds after which to remove completed/failed records
        """
        current_time = time.time()
        with self.lock:
            ids_to_remove: List[str] = []
            for transfer_id, transfer in self.transfers.items():
                if transfer['status'] in ('completed', 'failed'):
                    end_t = transfer.get('end_time')
                    if end_t is not None and (current_time - end_t) > age_threshold:
                        ids_to_remove.append(transfer_id)
            
            for tid in ids_to_remove:
                if tid in self.transfers:
                    del self.transfers[tid]
                    logger.debug(f"Cleaned up transfer record: {tid}")


    def export_history(self, filename: str):
        """Export transfer data (current states and periodic history) to a file
        
        Args:
            filename: Path to export the history data
        """
        with self.lock:
            history_list = list(self.history) # Convert deque to list for JSON
            
            serializable_transfers: Dict[str, Dict[str, Any]] = {}
            for tid, t_info_typed in self.transfers.items():
                # Convert TypedDict to a plain dict for JSON serialization
                serializable_transfers[tid] = {k: v for k, v in t_info_typed.items()}


            data_to_export = {
                'transfers_snapshot': serializable_transfers, 
                'periodic_history': history_list, 
                'export_time': datetime.now().isoformat()
            }
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data_to_export, f, indent=2, default=str) # Add default=str for any non-serializable types
                logger.info(f"Transfer history exported to {filename}")
            except IOError as e:
                logger.error(f"Failed to export transfer history to {filename}: {e}")
            except TypeError as e:
                logger.error(f"TypeError during JSON serialization for export: {e}")
