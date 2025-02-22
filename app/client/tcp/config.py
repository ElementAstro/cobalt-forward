from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class ClientConfig:
    """TCP client configuration"""
    host: str
    port: int
    timeout: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0
    buffer_size: int = 64 * 1024  # 64KB
    heartbeat_interval: float = 30.0
    compression_enabled: bool = False
    auto_reconnect: bool = True
    max_packet_size: int = 16 * 1024 * 1024  # 16MB
    compression_level: int = 6
    pool_size: int = 4
    write_batch_size: int = 8192

    # 高级配置
    advanced_settings: Dict[str, Any] = field(default_factory=lambda: {
        'tcp_nodelay': True,
        'keep_alive': True,
        'reuse_address': True,
        'socket_buffer_size': 32768,
        'use_zero_copy': True,
        'enable_batch_write': True,
    })
