from dataclasses import dataclass, field, asdict
import time
from typing import Dict, List, Any, Optional
import json
from datetime import datetime
import threading


@dataclass
class PluginMetadata:
    """插件元数据"""
    name: str
    version: str
    author: str
    description: str
    dependencies: List[str] = field(default_factory=list)
    required_version: str = None
    load_priority: int = 100
    config_schema: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """将元数据转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PluginMetadata':
        """从字典创建元数据"""
        # 过滤掉不在数据类定义中的字段
        valid_fields = {f.name for f in field(cls)}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)


class PluginState:
    """插件状态"""
    UNLOADED = "UNLOADED"
    LOADING = "LOADING"
    ACTIVE = "ACTIVE"
    ERROR = "ERROR"
    DISABLED = "DISABLED"
    
    @classmethod
    def is_valid_state(cls, state: str) -> bool:
        """检查是否是有效的状态值"""
        return state in [cls.UNLOADED, cls.LOADING, cls.ACTIVE, cls.ERROR, cls.DISABLED]


class PluginMetrics:
    """插件性能指标，线程安全"""

    def __init__(self, max_samples: int = 100):
        self.execution_times: List[float] = []
        self.memory_usage: List[float] = []
        self.error_count: int = 0
        self.last_execution: float = 0
        self.total_calls: int = 0
        self.max_samples = max_samples
        self._lock = threading.RLock()

    def record_execution(self, execution_time: float, memory_used: float = 0):
        """记录执行数据，线程安全"""
        with self._lock:
            self.execution_times.append(execution_time)
            if memory_used > 0:
                self.memory_usage.append(memory_used)
            self.last_execution = time.time()
            self.total_calls += 1
            
            # 保持列表长度在合理范围内
            if len(self.execution_times) > self.max_samples:
                self.execution_times = self.execution_times[-self.max_samples:]
            if len(self.memory_usage) > self.max_samples:
                self.memory_usage = self.memory_usage[-self.max_samples:]

    def record_error(self):
        """记录错误，线程安全"""
        with self._lock:
            self.error_count += 1

    def get_average_execution_time(self) -> float:
        """获取平均执行时间，线程安全"""
        with self._lock:
            return sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """获取指标摘要，线程安全"""
        with self._lock:
            return {
                "avg_execution_time": self.get_average_execution_time(),
                "max_execution_time": max(self.execution_times) if self.execution_times else 0,
                "min_execution_time": min(self.execution_times) if self.execution_times else 0,
                "avg_memory_usage": sum(self.memory_usage) / len(self.memory_usage) if self.memory_usage else 0,
                "total_calls": self.total_calls,
                "error_count": self.error_count,
                "error_rate": self.error_count / self.total_calls if self.total_calls > 0 else 0,
                "last_execution": datetime.fromtimestamp(self.last_execution).isoformat() if self.last_execution else None
            }
    
    def reset(self):
        """重置所有指标，线程安全"""
        with self._lock:
            self.execution_times = []
            self.memory_usage = []
            self.error_count = 0
            self.total_calls = 0
