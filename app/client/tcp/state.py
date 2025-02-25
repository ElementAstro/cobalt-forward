from enum import Enum, auto
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List


class ClientState(Enum):
    """TCP客户端状态枚举"""
    DISCONNECTED = auto()  # 已断开连接
    CONNECTING = auto()    # 正在连接
    CONNECTED = auto()     # 已连接
    RECONNECTING = auto()  # 重新连接中
    ERROR = auto()         # 错误状态
    CLOSED = auto()        # 已关闭


@dataclass
class ConnectionStats:
    """连接统计信息"""
    created_at: float = field(default_factory=time.time)
    connection_time: Optional[float] = None
    last_activity: float = field(default_factory=time.time)
    bytes_sent: int = 0
    bytes_received: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    errors: int = 0
    reconnects: int = 0
    latency: List[float] = field(default_factory=list)  # 最近的延迟测量值（毫秒）

    def add_latency_sample(self, latency_ms: float) -> None:
        """添加延迟样本"""
        self.latency.append(latency_ms)
        # 保留最近的100个样本
        if len(self.latency) > 100:
            self.latency.pop(0)

    def get_avg_latency(self) -> Optional[float]:
        """获取平均延迟"""
        if not self.latency:
            return None
        return sum(self.latency) / len(self.latency)

    def get_max_latency(self) -> Optional[float]:
        """获取最大延迟"""
        if not self.latency:
            return None
        return max(self.latency)

    def get_min_latency(self) -> Optional[float]:
        """获取最小延迟"""
        if not self.latency:
            return None
        return min(self.latency)

    def update_activity(self) -> None:
        """更新最近活动时间"""
        self.last_activity = time.time()

    def get_idle_time(self) -> float:
        """获取空闲时间（秒）"""
        return time.time() - self.last_activity

    def get_uptime(self) -> Optional[float]:
        """获取连接持续时间（秒）"""
        if self.connection_time is None:
            return None
        return time.time() - self.connection_time

    def to_dict(self) -> Dict[str, Any]:
        """将统计信息转换为字典"""
        return {
            "created_at": self.created_at,
            "connection_time": self.connection_time,
            "last_activity": self.last_activity,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "errors": self.errors,
            "reconnects": self.reconnects,
            "avg_latency": self.get_avg_latency(),
            "max_latency": self.get_max_latency(),
            "min_latency": self.get_min_latency(),
            "idle_time": self.get_idle_time(),
            "uptime": self.get_uptime(),
        }


@dataclass
class ClientStats:
    """客户端全局统计信息"""
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    total_reconnects: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    total_messages_sent: int = 0
    total_messages_received: int = 0
    start_time: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """将统计信息转换为字典"""
        return {
            "total_connections": self.total_connections,
            "active_connections": self.active_connections,
            "failed_connections": self.failed_connections,
            "total_reconnects": self.total_reconnects,
            "total_bytes_sent": self.total_bytes_sent,
            "total_bytes_received": self.total_bytes_received,
            "total_messages_sent": self.total_messages_sent,
            "total_messages_received": self.total_messages_received,
            "start_time": self.start_time,
            "uptime": time.time() - self.start_time,
        }
