"""
Configuration models and data structures.

This module defines the configuration models used throughout the application,
providing type safety and validation for configuration values.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pathlib import Path


@dataclass
class TCPConfig:
    """TCP client configuration."""
    host: str = "localhost"
    port: int = 8080
    timeout: float = 30.0
    retry_attempts: int = 3
    keepalive: bool = True
    buffer_size: int = 8192


@dataclass
class WebSocketConfig:
    """WebSocket configuration."""
    host: str = "0.0.0.0"
    port: int = 8000
    path: str = "/ws"
    ssl_enabled: bool = False
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None
    max_connections: int = 100
    ping_interval: float = 30.0
    ping_timeout: float = 10.0


@dataclass
class SSHConfig:
    """SSH client configuration."""
    enabled: bool = False
    host: str = "localhost"
    port: int = 22
    username: Optional[str] = None
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    known_hosts_path: Optional[str] = None
    timeout: float = 30.0
    pool_size: int = 5


@dataclass
class FTPConfig:
    """FTP server configuration."""
    enabled: bool = False
    host: str = "localhost"
    port: int = 21
    root_directory: str = "/tmp/ftp"
    passive_ports_start: int = 60000
    passive_ports_end: int = 65535
    max_connections: int = 50
    timeout: float = 300.0


@dataclass
class MQTTConfig:
    """MQTT client configuration."""
    enabled: bool = False
    broker_host: str = "localhost"
    broker_port: int = 1883
    client_id: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    topics: List[str] = field(default_factory=list)
    qos: int = 1
    keepalive: int = 60


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_directory: str = "logs"
    max_file_size: str = "10MB"
    backup_count: int = 5
    console_enabled: bool = True
    file_enabled: bool = True


@dataclass
class PluginConfig:
    """Plugin system configuration."""
    enabled: bool = True
    plugin_directory: str = "plugins"
    auto_load: bool = True
    sandbox_enabled: bool = True
    max_execution_time: float = 30.0
    allowed_modules: List[str] = field(default_factory=list)
    blocked_modules: List[str] = field(default_factory=list)


@dataclass
class PerformanceConfig:
    """Performance monitoring configuration."""
    enabled: bool = True
    metrics_interval: float = 60.0
    history_size: int = 1000
    alert_thresholds: Dict[str, float] = field(default_factory=dict)


@dataclass
class SecurityConfig:
    """Security configuration."""
    api_key_required: bool = False
    api_key_header: str = "X-API-Key"
    allowed_origins: List[str] = field(default_factory=lambda: ["*"])
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: int = 60


@dataclass
class ApplicationConfig:
    """Main application configuration."""

    # Basic application settings
    name: str = "Cobalt Forward"
    version: str = "0.1.0"
    debug: bool = False
    environment: str = "production"

    # Component configurations
    tcp: TCPConfig = field(default_factory=TCPConfig)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    ssh: SSHConfig = field(default_factory=SSHConfig)
    ftp: FTPConfig = field(default_factory=FTPConfig)
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    plugins: PluginConfig = field(default_factory=PluginConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)

    # Additional settings
    config_file_path: Optional[str] = None
    data_directory: str = "data"
    temp_directory: str = "temp"

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate_paths()
        self._validate_ports()
        self._validate_timeouts()

    def _validate_paths(self) -> None:
        """Validate directory paths."""
        paths_to_check = [
            self.data_directory,
            self.temp_directory,
            self.logging.log_directory,
            self.plugins.plugin_directory
        ]

        for path_str in paths_to_check:
            if path_str:
                path = Path(path_str)
                try:
                    path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    raise ValueError(f"Cannot create directory {path}: {e}")

    def _validate_ports(self) -> None:
        """Validate port numbers."""
        ports = [
            ("TCP port", self.tcp.port),
            ("WebSocket port", self.websocket.port),
            ("SSH port", self.ssh.port),
            ("FTP port", self.ftp.port),
            ("MQTT port", self.mqtt.broker_port),
        ]

        for name, port in ports:
            if not (1 <= port <= 65535):
                raise ValueError(
                    f"{name} must be between 1 and 65535, got {port}")

    def _validate_timeouts(self) -> None:
        """Validate timeout values."""
        timeouts = [
            ("TCP timeout", self.tcp.timeout),
            ("SSH timeout", self.ssh.timeout),
            ("FTP timeout", self.ftp.timeout),
            ("WebSocket ping timeout", self.websocket.ping_timeout),
        ]

        for name, timeout in timeouts:
            if timeout <= 0:
                raise ValueError(f"{name} must be positive, got {timeout}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        result = {}

        for field_name, field_value in self.__dict__.items():
            if hasattr(field_value, '__dict__'):
                # Convert dataclass to dict
                result[field_name] = field_value.__dict__
            else:
                result[field_name] = field_value

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ApplicationConfig':
        """Create configuration from dictionary."""
        # Extract nested configurations
        tcp_config = TCPConfig(**data.get('tcp', {}))
        websocket_config = WebSocketConfig(**data.get('websocket', {}))
        ssh_config = SSHConfig(**data.get('ssh', {}))
        ftp_config = FTPConfig(**data.get('ftp', {}))
        mqtt_config = MQTTConfig(**data.get('mqtt', {}))
        logging_config = LoggingConfig(**data.get('logging', {}))
        plugin_config = PluginConfig(**data.get('plugins', {}))
        performance_config = PerformanceConfig(**data.get('performance', {}))
        security_config = SecurityConfig(**data.get('security', {}))

        # Create main configuration
        return cls(
            name=data.get('name', 'Cobalt Forward'),
            version=data.get('version', '0.1.0'),
            debug=data.get('debug', False),
            environment=data.get('environment', 'production'),
            tcp=tcp_config,
            websocket=websocket_config,
            ssh=ssh_config,
            ftp=ftp_config,
            mqtt=mqtt_config,
            logging=logging_config,
            plugins=plugin_config,
            performance=performance_config,
            security=security_config,
            config_file_path=data.get('config_file_path'),
            data_directory=data.get('data_directory', 'data'),
            temp_directory=data.get('temp_directory', 'temp')
        )
