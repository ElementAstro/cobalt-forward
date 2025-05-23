from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
from pydantic import BaseModel, field_validator, model_validator


class ConfigValidationModel(BaseModel):
    """
    Base model for configuration validation.
    Validates core configuration parameters needed by all components.
    """
    tcp_host: str
    tcp_port: int
    websocket_host: str
    websocket_port: int
    log_level: str = "INFO"
    max_connections: int = 100
    buffer_size: int = 4096
    enable_ssl: bool = False
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    metrics_enabled: bool = True
    hot_reload: bool = True
    plugins: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @field_validator('tcp_port', 'websocket_port')
    @classmethod
    def validate_ports(cls, v):
        """Validate port numbers are in valid range"""
        if not 1 <= v <= 65535:
            raise ValueError('Port number must be between 1-65535')
        return v

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level is one of the allowed values"""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(
                f'Log level must be one of: {", ".join(valid_levels)}')
        return v.upper()

    @model_validator(mode='after')
    def validate_ssl(self):
        """Validate SSL configuration is complete"""
        if self.enable_ssl and (not self.ssl_cert or not self.ssl_key):
            raise ValueError(
                'SSL certificate and key must be provided when SSL is enabled')
        return self


@dataclass
class RuntimeConfig:
    """
    Runtime configuration container.
    Stores the active configuration and provides access to it.
    Supports plugins through a dedicated plugins dictionary.
    """
    # Core required configurations
    tcp_host: str
    tcp_port: int
    websocket_host: str
    websocket_port: int

    # Optional configurations with defaults
    log_level: str = "INFO"
    max_connections: int = 100
    buffer_size: int = 4096
    enable_ssl: bool = False
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    metrics_enabled: bool = True
    hot_reload: bool = True

    # Metadata fields
    version: str = field(default="1.0.0")
    last_modified: datetime = field(default_factory=datetime.now)
    config_id: str = field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d%H%M%S"))

    # Plugin configurations
    plugins: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def get_plugin_config(self, plugin_id: str) -> Dict[str, Any]:
        """
        Get configuration for specific plugin

        Args:
            plugin_id: The unique identifier of the plugin

        Returns:
            Plugin configuration dictionary or empty dict if not found
        """
        return self.plugins.get(plugin_id, {})

    def set_plugin_config(self, plugin_id: str, config: Dict[str, Any]) -> None:
        """
        Set configuration for specific plugin

        Args:
            plugin_id: The unique identifier of the plugin
            config: The configuration dictionary for the plugin
        """
        if not self.plugins:
            self.plugins = {}
        self.plugins[plugin_id] = config
