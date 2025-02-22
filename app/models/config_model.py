from typing import Optional, Dict
from dataclasses import dataclass, field
from datetime import datetime
from pydantic import BaseModel, validator

class ConfigValidationModel(BaseModel):
    tcp_host: str
    tcp_port: int
    websocket_host: str
    websocket_port: int
    log_level: str
    max_connections: int
    buffer_size: int
    enable_ssl: bool
    ssl_cert: Optional[str]
    ssl_key: Optional[str]
    metrics_enabled: bool
    hot_reload: bool

    @validator('tcp_port', 'websocket_port')
    def validate_ports(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('端口号必须在1-65535之间')
        return v

    @validator('log_level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'日志级别必须是以下之一: {", ".join(valid_levels)}')
        return v.upper()

@dataclass
class RuntimeConfig(ConfigValidationModel):
    version: str = field(default="1.0.0")
    last_modified: datetime = field(default_factory=datetime.now)
    config_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d%H%M%S"))
