from dataclasses import dataclass
from typing import Optional

@dataclass
class PoolConfig:
    min_size: int = 2
    max_size: int = 10
    timeout: float = 30.0

@dataclass
class SSHConfig:
    """增强的SSH配置数据类"""
    hostname: str
    username: str
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    port: int = 22
    timeout: int = 10
    compress: bool = True
    keep_alive: bool = True
    keep_alive_interval: int = 30
    banner_timeout: int = 60
    auth_timeout: int = 30
    channel_timeout: int = 30
    max_retries: int = 3
    retry_interval: int = 5
    pool: Optional[PoolConfig] = None
