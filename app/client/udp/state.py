from enum import Enum, auto
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List


class ClientState(Enum):
    """UDP client state enumeration"""
    DISCONNECTED = auto()  # Connection is disconnected
    CONNECTING = auto()    # Establishing connection
    CONNECTED = auto()     # Connection established
    RECONNECTING = auto()  # Re-establishing connection
    ERROR = auto()         # Error state
    CLOSED = auto()        # Connection closed


@dataclass
class ConnectionStats:
    """Connection statistics"""
    created_at: float = field(default_factory=time.time)
    connection_time: Optional[float] = None
    last_activity: float = field(default_factory=time.time)
    bytes_sent: int = 0
    bytes_received: int = 0
    packets_sent: int = 0
    packets_received: int = 0
    errors: int = 0
    reconnects: int = 0
    dropped_packets: int = 0     # UDP specific: Dropped packets
    duplicate_packets: int = 0   # UDP specific: Duplicate packets
    out_of_order_packets: int = 0  # UDP specific: Out-of-order packets

    def add_packet_loss(self, count: int = 1) -> None:
        """Add packet loss record"""
        self.dropped_packets += count

    def update_activity(self) -> None:
        """Update the last activity time"""
        self.last_activity = time.time()

    def get_idle_time(self) -> float:
        """Get idle time (seconds)"""
        return time.time() - self.last_activity

    def get_uptime(self) -> Optional[float]:
        """Get connection uptime (seconds)"""
        if self.connection_time is None:
            return None
        return time.time() - self.connection_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to dictionary"""
        return {
            "created_at": self.created_at,
            "connection_time": self.connection_time,
            "last_activity": self.last_activity,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "packets_sent": self.packets_sent,
            "packets_received": self.packets_received,
            "errors": self.errors,
            "reconnects": self.reconnects,
            "dropped_packets": self.dropped_packets,
            "duplicate_packets": self.duplicate_packets,
            "out_of_order_packets": self.out_of_order_packets,
            "idle_time": self.get_idle_time(),
            "uptime": self.get_uptime(),
        }


@dataclass
class ClientStats:
    """Global client statistics"""
    total_connections: int = 0
    active_connections: int = 0
    total_packets_sent: int = 0
    total_packets_received: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    total_errors: int = 0
    packet_loss_rate: float = 0.0  # Packet loss rate
    avg_latency: float = 0.0       # Average latency

    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to dictionary"""
        return {
            "total_connections": self.total_connections,
            "active_connections": self.active_connections,
            "total_packets_sent": self.total_packets_sent,
            "total_packets_received": self.total_packets_received,
            "total_bytes_sent": self.total_bytes_sent,
            "total_bytes_received": self.total_bytes_received,
            "total_errors": self.total_errors,
            "packet_loss_rate": self.packet_loss_rate,
            "avg_latency": self.avg_latency,
        }
