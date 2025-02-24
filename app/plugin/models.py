from dataclasses import dataclass
import time
from typing import Dict, List, Any


@dataclass
class PluginMetadata:
    """插件元数据"""
    name: str
    version: str
    author: str
    description: str
    dependencies: List[str] = None
    required_version: str = None
    load_priority: int = 100
    config_schema: Dict = None


class PluginState:
    """插件状态"""
    UNLOADED = "UNLOADED"
    LOADING = "LOADING"
    ACTIVE = "ACTIVE"
    ERROR = "ERROR"
    DISABLED = "DISABLED"


class PluginMetrics:
    """插件性能指标"""

    def __init__(self):
        self.execution_times: List[float] = []
        self.memory_usage: List[float] = []
        self.error_count: int = 0
        self.last_execution: float = 0
        self.total_calls: int = 0

    def record_execution(self, execution_time: float, memory_used: float):
        """记录执行数据"""
        self.execution_times.append(execution_time)
        self.memory_usage.append(memory_used)
        self.last_execution = time.time()
        self.total_calls += 1

    def get_average_execution_time(self) -> float:
        """获取平均执行时间"""
        return sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0
