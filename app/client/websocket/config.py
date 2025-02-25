from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import ssl


@dataclass
class WebSocketConfig:
    """WebSocket客户端配置"""
    uri: str
    ssl_context: Optional[ssl.SSLContext] = None
    headers: Optional[Dict[str, str]] = None
    subprotocols: Optional[List[str]] = None
    extra_options: Optional[Dict[str, Any]] = field(default_factory=dict)
    ping_interval: float = 20.0
    ping_timeout: float = 10.0
    close_timeout: float = 10.0
    max_size: int = 2**20  # 1MB
    compression: Optional[str] = None
    auto_reconnect: bool = False
    max_reconnect_attempts: int = -1  # -1表示无限重试
    reconnect_interval: float = 5.0
    receive_timeout: Optional[float] = None

    def __post_init__(self):
        # 确保extra_options是字典
        if self.extra_options is None:
            self.extra_options = {}

        # 确保headers是字典
        if self.headers is None:
            self.headers = {}

        # 确保subprotocols是列表
        if self.subprotocols is None:
            self.subprotocols = []
