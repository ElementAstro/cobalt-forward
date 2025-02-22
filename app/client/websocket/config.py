from dataclasses import dataclass
from typing import Optional, Dict, Any
import ssl

@dataclass
class WebSocketConfig:
    """WebSocket客户端配置"""
    uri: str
    ssl_context: Optional[ssl.SSLContext] = None
    headers: Optional[Dict[str, str]] = None
    subprotocols: Optional[list] = None
    extra_options: Optional[Dict[str, Any]] = None
    ping_interval: float = 20.0
    ping_timeout: float = 10.0
    close_timeout: float = 10.0
    max_size: int = 2**20  # 1MB
    compression: Optional[str] = None
    auto_reconnect: bool = False
    max_reconnect_attempts: int = -1
    reconnect_interval: float = 5.0
    receive_timeout: Optional[float] = None
