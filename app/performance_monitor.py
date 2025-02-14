from typing import Dict, List, Optional, Any
import time
import psutil
import asyncio
from loguru import logger
from dataclasses import dataclass
from collections import deque


@dataclass
class PerformanceMetrics:
    cpu_percent: float
    memory_percent: float
    connection_count: int
    message_rate: float
    average_latency: float


class PerformanceMonitor:
    def __init__(self, history_size: int = 100):
        self.start_time = time.time()
        self.message_timestamps = deque(maxlen=history_size)
        self.latencies = deque(maxlen=history_size)
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start(self):
        """启动性能监控"""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Performance monitoring started")

    async def stop(self):
        """停止性能监控"""
        self._running = False
        if self._monitor_task:
            await self._monitor_task
        logger.info("Performance monitoring stopped")

    def record_message(self, latency: float):
        """记录消息处理时间"""
        self.message_timestamps.append(time.time())
        self.latencies.append(latency)

    async def _monitor_loop(self):
        """监控循环"""
        while self._running:
            try:
                metrics = self.get_current_metrics()
                self._log_metrics(metrics)
                await asyncio.sleep(5)  # 每5秒收集一次指标
            except Exception as e:
                logger.error(f"Error in performance monitoring: {e}")

    def get_current_metrics(self) -> PerformanceMetrics:
        """获取当前性能指标"""
        now = time.time()

        # 计算消息率
        recent_messages = sum(1 for t in self.message_timestamps
                              if now - t <= 60)  # 最近1分钟的消息数
        message_rate = recent_messages / 60 if recent_messages else 0

        # 计算平均延迟
        avg_latency = sum(self.latencies) / \
            len(self.latencies) if self.latencies else 0

        return PerformanceMetrics(
            cpu_percent=psutil.cpu_percent(),
            memory_percent=psutil.virtual_memory().percent,
            connection_count=len(psutil.net_connections()),
            message_rate=message_rate,
            average_latency=avg_latency
        )

    def get_detailed_metrics(self) -> Dict[str, Any]:
        """获取详细的性能指标"""
        basic_metrics = self.get_current_metrics()
        return {
            **basic_metrics.__dict__,
            "memory_details": self._get_memory_details(),
            "network_stats": self._get_network_stats(),
            "system_load": self._get_system_load()
        }

    def _get_memory_details(self) -> Dict[str, float]:
        memory = psutil.virtual_memory()
        return {
            "total": memory.total / (1024 * 1024 * 1024),  # GB
            "available": memory.available / (1024 * 1024 * 1024),  # GB
            "used": memory.used / (1024 * 1024 * 1024),  # GB
            "percent": memory.percent
        }

    def _get_network_stats(self) -> Dict[str, int]:
        net_io = psutil.net_io_counters()
        return {
            "bytes_sent": net_io.bytes_sent,
            "bytes_recv": net_io.bytes_recv,
            "packets_sent": net_io.packets_sent,
            "packets_recv": net_io.packets_recv
        }

    def _get_system_load(self) -> List[float]:
        return [x / psutil.cpu_count() * 100 for x in psutil.getloadavg()]

    def _log_metrics(self, metrics: PerformanceMetrics):
        """记录性能指标"""
        logger.info(
            "Performance Metrics:\n"
            f"CPU Usage: {metrics.cpu_percent}%\n"
            f"Memory Usage: {metrics.memory_percent}%\n"
            f"Connections: {metrics.connection_count}\n"
            f"Message Rate: {metrics.message_rate:.2f} msg/s\n"
            f"Avg Latency: {metrics.average_latency:.2f} ms"
        )
