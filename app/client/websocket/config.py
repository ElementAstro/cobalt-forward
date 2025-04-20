from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import ssl


@dataclass
class WebSocketConfig:
    """WebSocket Client Configuration
    
    This class defines all configuration parameters for a WebSocket connection.
    """
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
    max_reconnect_attempts: int = -1  # -1 means unlimited retries
    reconnect_interval: float = 5.0
    receive_timeout: Optional[float] = None

    def __post_init__(self):
        # Ensure extra_options is a dictionary
        if self.extra_options is None:
            self.extra_options = {}

        # Ensure headers is a dictionary
        if self.headers is None:
            self.headers = {}

        # Ensure subprotocols is a list
        if self.subprotocols is None:
            self.subprotocols = []
