from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional
import time


class ClientState(Enum):
    """TCP client states"""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    ERROR = auto()


@dataclass
class ConnectionStats:
    """Connection statistics"""
    bytes_sent: int = 0
    bytes_received: int = 0
    packets_sent: int = 0
    packets_received: int = 0
    errors: int = 0
    reconnects: int = 0
    last_heartbeat: Optional[float] = None
    connection_time: Optional[float] = None

    def reset(self):
        """Reset all statistics"""
        self.bytes_sent = 0
        self.bytes_received = 0
        self.packets_sent = 0
        self.packets_received = 0
        self.errors = 0
        self.reconnects = 0
        self.last_heartbeat = None
        self.connection_time = None

    def record_heartbeat(self):
        """Record heartbeat time"""
        self.last_heartbeat = time.time()
