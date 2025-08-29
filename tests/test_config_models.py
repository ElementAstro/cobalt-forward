"""
Tests for configuration models.

This module tests the configuration model classes including ApplicationConfig
and all component-specific configuration classes.
"""

import pytest
from typing import Any, Dict

from cobalt_forward.infrastructure.config.models import (
    ApplicationConfig, TCPConfig, WebSocketConfig, SSHConfig, FTPConfig,
    MQTTConfig, LoggingConfig, PluginConfig, PerformanceConfig, SecurityConfig
)


class TestTCPConfig:
    """Test cases for TCPConfig."""
    
    def test_tcp_config_defaults(self) -> None:
        """Test TCPConfig default values."""
        config = TCPConfig()
        
        assert config.host == "0.0.0.0"
        assert config.port == 8080
        assert config.enabled is True
        assert config.max_connections == 100
        assert config.timeout == 30.0
        assert config.buffer_size == 8192
    
    def test_tcp_config_custom_values(self) -> None:
        """Test TCPConfig with custom values."""
        config = TCPConfig(
            host="127.0.0.1",
            port=9090,
            enabled=False,
            max_connections=50,
            timeout=60.0,
            buffer_size=8192
        )
        
        assert config.host == "127.0.0.1"
        assert config.port == 9090
        assert config.enabled is False
        assert config.max_connections == 50
        assert config.timeout == 60.0
        assert config.buffer_size == 8192
    
    def test_tcp_config_from_dict(self) -> None:
        """Test creating TCPConfig from dictionary."""
        data = {
            "host": "192.168.1.100",
            "port": 8888,
            "enabled": True,
            "max_connections": 200,
            "timeout": 45.0,
            "buffer_size": 16384
        }
        
        config = TCPConfig.from_dict(data)
        
        assert config.host == "192.168.1.100"
        assert config.port == 8888
        assert config.enabled is True
        assert config.max_connections == 200
        assert config.timeout == 45.0
        assert config.buffer_size == 16384
    
    def test_tcp_config_to_dict(self) -> None:
        """Test converting TCPConfig to dictionary."""
        config = TCPConfig(host="test.com", port=1234)
        data = config.to_dict()
        
        assert data["host"] == "test.com"
        assert data["port"] == 1234
        assert data["enabled"] is True
        assert isinstance(data, dict)


class TestWebSocketConfig:
    """Test cases for WebSocketConfig."""
    
    def test_websocket_config_defaults(self) -> None:
        """Test WebSocketConfig default values."""
        config = WebSocketConfig()
        
        assert config.host == "0.0.0.0"
        assert config.port == 8000
        assert config.path == "/ws"
        assert config.ssl_enabled is False
        assert config.ssl_cert_path is None
        assert config.ssl_key_path is None
        assert config.max_connections == 100
        assert config.ping_interval == 30.0
        assert config.ping_timeout == 10.0
    
    def test_websocket_config_with_ssl(self) -> None:
        """Test WebSocketConfig with SSL configuration."""
        config = WebSocketConfig(
            ssl_enabled=True,
            ssl_cert_path="/path/to/cert.pem",
            ssl_key_path="/path/to/key.pem"
        )
        
        assert config.ssl_enabled is True
        assert config.ssl_cert_path == "/path/to/cert.pem"
        assert config.ssl_key_path == "/path/to/key.pem"


class TestSSHConfig:
    """Test cases for SSHConfig."""
    
    def test_ssh_config_defaults(self) -> None:
        """Test SSHConfig default values."""
        config = SSHConfig()
        
        assert config.enabled is False
        assert config.host == "localhost"
        assert config.port == 22
        assert config.username is None
        assert config.password is None
        assert config.private_key_path is None
        assert config.known_hosts_path is None
        assert config.timeout == 30.0
        assert config.pool_size == 5
    
    def test_ssh_config_with_credentials(self) -> None:
        """Test SSHConfig with authentication credentials."""
        config = SSHConfig(
            enabled=True,
            host="remote.server.com",
            port=2222,
            username="testuser",
            password="testpass",
            private_key_path="/home/user/.ssh/id_rsa"
        )
        
        assert config.enabled is True
        assert config.host == "remote.server.com"
        assert config.port == 2222
        assert config.username == "testuser"
        assert config.password == "testpass"
        assert config.private_key_path == "/home/user/.ssh/id_rsa"


class TestLoggingConfig:
    """Test cases for LoggingConfig."""
    
    def test_logging_config_defaults(self) -> None:
        """Test LoggingConfig default values."""
        config = LoggingConfig()
        
        assert config.level == "INFO"
        assert config.format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        assert config.log_directory == "logs"
        assert config.max_file_size == "10MB"
        assert config.backup_count == 5
        assert config.console_enabled is True
        assert config.file_enabled is True
    
    def test_logging_config_custom_values(self) -> None:
        """Test LoggingConfig with custom values."""
        config = LoggingConfig(
            level="DEBUG",
            format="%(levelname)s: %(message)s",
            log_directory="/var/log/app",
            max_file_size="50MB",
            backup_count=10,
            console_enabled=False,
            file_enabled=True
        )
        
        assert config.level == "DEBUG"
        assert config.format == "%(levelname)s: %(message)s"
        assert config.log_directory == "/var/log/app"
        assert config.max_file_size == "50MB"
        assert config.backup_count == 10
        assert config.console_enabled is False
        assert config.file_enabled is True


class TestPluginConfig:
    """Test cases for PluginConfig."""
    
    def test_plugin_config_defaults(self) -> None:
        """Test PluginConfig default values."""
        config = PluginConfig()
        
        assert config.enabled is True
        assert config.plugin_directory == "plugins"
        assert config.auto_load is True
        assert config.sandbox_enabled is True
        assert config.max_execution_time == 30.0
        assert config.allowed_modules == []
        assert config.blocked_modules == []
    
    def test_plugin_config_with_restrictions(self) -> None:
        """Test PluginConfig with module restrictions."""
        config = PluginConfig(
            allowed_modules=["os", "sys", "json"],
            blocked_modules=["subprocess", "socket"],
            sandbox_enabled=True,
            max_execution_time=10.0
        )
        
        assert config.allowed_modules == ["os", "sys", "json"]
        assert config.blocked_modules == ["subprocess", "socket"]
        assert config.sandbox_enabled is True
        assert config.max_execution_time == 10.0


class TestApplicationConfig:
    """Test cases for ApplicationConfig."""
    
    def test_application_config_defaults(self) -> None:
        """Test ApplicationConfig default values."""
        config = ApplicationConfig()
        
        assert config.name == "Cobalt Forward"
        assert config.version == "0.1.0"
        assert config.debug is False
        assert config.environment == "production"
        assert isinstance(config.tcp, TCPConfig)
        assert isinstance(config.websocket, WebSocketConfig)
        assert isinstance(config.ssh, SSHConfig)
        assert isinstance(config.logging, LoggingConfig)
        assert isinstance(config.plugins, PluginConfig)
        assert config.config_file_path is None
        assert config.data_directory == "data"
        assert config.temp_directory == "temp"
    
    def test_application_config_custom_values(self) -> None:
        """Test ApplicationConfig with custom values."""
        tcp_config = TCPConfig(port=9090)
        websocket_config = WebSocketConfig(port=8080)
        
        config = ApplicationConfig(
            name="Test App",
            version="2.0.0",
            debug=True,
            environment="development",
            tcp=tcp_config,
            websocket=websocket_config,
            data_directory="/app/data",
            temp_directory="/tmp/app"
        )
        
        assert config.name == "Test App"
        assert config.version == "2.0.0"
        assert config.debug is True
        assert config.environment == "development"
        assert config.tcp.port == 9090
        assert config.websocket.port == 8080
        assert config.data_directory == "/app/data"
        assert config.temp_directory == "/tmp/app"
    
    def test_application_config_from_dict_minimal(self) -> None:
        """Test creating ApplicationConfig from minimal dictionary."""
        data = {
            "name": "Minimal App",
            "debug": True
        }
        
        config = ApplicationConfig.from_dict(data)
        
        assert config.name == "Minimal App"
        assert config.debug is True
        assert config.version == "0.1.0"  # Default
        assert config.environment == "production"  # Default
        assert isinstance(config.tcp, TCPConfig)  # Default instance
    
    def test_application_config_from_dict_complete(self) -> None:
        """Test creating ApplicationConfig from complete dictionary."""
        data = {
            "name": "Complete App",
            "version": "3.0.0",
            "debug": False,
            "environment": "staging",
            "tcp": {
                "host": "127.0.0.1",
                "port": 8080,
                "enabled": True
            },
            "websocket": {
                "host": "0.0.0.0",
                "port": 9000,
                "path": "/websocket"
            },
            "ssh": {
                "enabled": True,
                "host": "ssh.example.com",
                "username": "user"
            },
            "logging": {
                "level": "DEBUG",
                "console_enabled": False
            },
            "plugins": {
                "enabled": False,
                "plugin_directory": "/custom/plugins"
            },
            "data_directory": "/app/data",
            "temp_directory": "/app/temp"
        }
        
        config = ApplicationConfig.from_dict(data)
        
        assert config.name == "Complete App"
        assert config.version == "3.0.0"
        assert config.debug is False
        assert config.environment == "staging"
        assert config.tcp.host == "127.0.0.1"
        assert config.tcp.port == 8080
        assert config.websocket.port == 9000
        assert config.websocket.path == "/websocket"
        assert config.ssh.enabled is True
        assert config.ssh.host == "ssh.example.com"
        assert config.ssh.username == "user"
        assert config.logging.level == "DEBUG"
        assert config.logging.console_enabled is False
        assert config.plugins.enabled is False
        assert config.plugins.plugin_directory == "/custom/plugins"
        assert config.data_directory == "/app/data"
        assert config.temp_directory == "/app/temp"
    
    def test_application_config_to_dict(self) -> None:
        """Test converting ApplicationConfig to dictionary."""
        config = ApplicationConfig(
            name="Test App",
            debug=True,
            tcp=TCPConfig(port=9090)
        )
        
        data = config.to_dict()
        
        assert data["name"] == "Test App"
        assert data["debug"] is True
        assert data["tcp"]["port"] == 9090
        assert isinstance(data, dict)
        assert isinstance(data["tcp"], dict)
        assert isinstance(data["websocket"], dict)
    
    def test_application_config_validation_success(self) -> None:
        """Test successful configuration validation."""
        config = ApplicationConfig()
        
        # Should not raise any exceptions
        is_valid = config.validate()
        assert is_valid is True
    
    def test_application_config_validation_invalid_port(self) -> None:
        """Test configuration validation with invalid port."""
        config = ApplicationConfig(tcp=TCPConfig(port=-1))
        
        with pytest.raises(ValueError, match="TCP port must be between 1 and 65535"):
            config.validate()
    
    def test_application_config_validation_invalid_websocket_port(self) -> None:
        """Test configuration validation with invalid WebSocket port."""
        config = ApplicationConfig(websocket=WebSocketConfig(port=70000))
        
        with pytest.raises(ValueError, match="WebSocket port must be between 1 and 65535"):
            config.validate()
    
    def test_application_config_validation_ssl_without_cert(self) -> None:
        """Test configuration validation with SSL enabled but no certificate."""
        config = ApplicationConfig(
            websocket=WebSocketConfig(ssl_enabled=True, ssl_cert_path=None)
        )
        
        with pytest.raises(ValueError, match="SSL certificate path is required when SSL is enabled"):
            config.validate()
    
    def test_application_config_validation_invalid_log_level(self) -> None:
        """Test configuration validation with invalid log level."""
        config = ApplicationConfig(logging=LoggingConfig(level="INVALID"))
        
        with pytest.raises(ValueError, match="Invalid log level"):
            config.validate()
    
    def test_application_config_nested_dict_creation(self) -> None:
        """Test creating nested configuration from dictionary."""
        data = {
            "tcp": {
                "host": "custom.host.com",
                "port": 8888,
                "timeout": 60.0
            }
        }
        
        config = ApplicationConfig.from_dict(data)
        
        assert config.tcp.host == "custom.host.com"
        assert config.tcp.port == 8888
        assert config.tcp.timeout == 60.0
        # Other TCP config values should be defaults
        assert config.tcp.enabled is True
        assert config.tcp.max_connections == 100
