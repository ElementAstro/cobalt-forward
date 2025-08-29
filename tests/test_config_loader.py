"""
Tests for configuration loader.

This module tests the ConfigLoader class including file loading,
environment variable processing, and configuration merging.
"""

import pytest
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import patch, mock_open

from cobalt_forward.infrastructure.config.loader import ConfigLoader
from cobalt_forward.infrastructure.config.models import ApplicationConfig


class TestConfigLoader:
    """Test cases for ConfigLoader class."""
    
    @pytest.fixture
    def config_loader(self) -> ConfigLoader:
        """Create a ConfigLoader instance."""
        return ConfigLoader()
    
    @pytest.fixture
    def sample_config_dict(self) -> Dict[str, Any]:
        """Sample configuration dictionary."""
        return {
            "name": "Test Application",
            "version": "1.0.0",
            "debug": True,
            "environment": "testing",
            "tcp": {
                "host": "127.0.0.1",
                "port": 8080
            },
            "websocket": {
                "host": "0.0.0.0",
                "port": 9000,
                "path": "/ws",
                "max_connections": 50
            },
            "logging": {
                "level": "DEBUG",
                "console_enabled": True,
                "file_enabled": False
            }
        }
    
    @pytest.fixture
    def temp_json_file(self, sample_config_dict: Dict[str, Any]) -> Generator[str, None, None]:
        """Create temporary JSON config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_config_dict, f)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    @pytest.fixture
    def temp_yaml_file(self, sample_config_dict: Dict[str, Any]) -> Generator[str, None, None]:
        """Create temporary YAML config file."""
        yaml_content = """
name: Test Application
version: 1.0.0
debug: true
environment: testing
tcp:
  host: 127.0.0.1
  port: 8080
websocket:
  host: 0.0.0.0
  port: 9000
  path: /ws
  max_connections: 50
logging:
  level: DEBUG
  console_enabled: true
  file_enabled: false
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content.strip())
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    def test_config_loader_initialization(self, config_loader: ConfigLoader) -> None:
        """Test ConfigLoader initialization."""
        assert config_loader._env_prefix == "COBALT_"
    
    def test_load_config_no_file(self, config_loader: ConfigLoader) -> None:
        """Test loading config without file (defaults only)."""
        config = config_loader.load_config()
        
        assert isinstance(config, ApplicationConfig)
        assert config.name == "Cobalt Forward"  # Default value
        assert config.version == "0.1.0"  # Default value
        assert config.debug is False  # Default value
        assert config.environment == "production"  # Default value
    
    def test_load_config_from_json_file(self, config_loader: ConfigLoader, temp_json_file: str) -> None:
        """Test loading config from JSON file."""
        config = config_loader.load_config(temp_json_file)
        
        assert config.name == "Test Application"
        assert config.version == "1.0.0"
        assert config.debug is True
        assert config.environment == "testing"
        assert config.tcp.host == "127.0.0.1"
        assert config.tcp.port == 8080
        assert config.websocket.port == 9000
        assert config.logging.level == "DEBUG"
        assert config.config_file_path == temp_json_file
    
    def test_load_config_from_yaml_file(self, config_loader: ConfigLoader, temp_yaml_file: str) -> None:
        """Test loading config from YAML file."""
        config = config_loader.load_config(temp_yaml_file)
        
        assert config.name == "Test Application"
        assert config.version == "1.0.0"
        assert config.debug is True
        assert config.environment == "testing"
        assert config.tcp.host == "127.0.0.1"
        assert config.tcp.port == 8080
        assert config.websocket.port == 9000
        assert config.logging.level == "DEBUG"
        assert config.config_file_path == temp_yaml_file
    
    def test_load_config_file_not_found(self, config_loader: ConfigLoader) -> None:
        """Test loading config from non-existent file."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            config_loader.load_config("/nonexistent/config.json")
    
    def test_load_config_unsupported_format(self, config_loader: ConfigLoader) -> None:
        """Test loading config from unsupported file format."""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="Unsupported configuration file format"):
                config_loader.load_config(temp_path)
        finally:
            os.unlink(temp_path)
    
    def test_load_config_invalid_json(self, config_loader: ConfigLoader) -> None:
        """Test loading config from invalid JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("{ invalid json }")
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="Invalid JSON"):
                config_loader.load_config(temp_path)
        finally:
            os.unlink(temp_path)
    
    def test_load_config_invalid_yaml(self, config_loader: ConfigLoader) -> None:
        """Test loading config from invalid YAML file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="Invalid YAML"):
                config_loader.load_config(temp_path)
        finally:
            os.unlink(temp_path)
    
    def test_load_config_with_environment_overrides(self, config_loader: ConfigLoader) -> None:
        """Test loading config with environment variable overrides."""
        env_vars = {
            "COBALT_DEBUG": "true",
            "COBALT_TCP_HOST": "192.168.1.100",
            "COBALT_TCP_PORT": "9090",
            "COBALT_WEBSOCKET_PORT": "8080",
            "COBALT_LOG_LEVEL": "ERROR"
        }
        
        with patch.dict(os.environ, env_vars):
            config = config_loader.load_config()
        
        assert config.debug is True
        assert config.tcp.host == "192.168.1.100"
        assert config.tcp.port == 9090
        assert config.websocket.port == 8080
        assert config.logging.level == "ERROR"
    
    def test_load_config_environment_type_conversion(self, config_loader: ConfigLoader) -> None:
        """Test environment variable type conversion."""
        env_vars = {
            "COBALT_DEBUG": "false",
            "COBALT_TCP_PORT": "8080",
            "COBALT_WEBSOCKET_MAX_CONNECTIONS": "200",
            "COBALT_PLUGINS_ENABLED": "true"
        }
        
        with patch.dict(os.environ, env_vars):
            config = config_loader.load_config()
        
        assert config.debug is False
        assert isinstance(config.debug, bool)
        assert config.tcp.port == 8080
        assert isinstance(config.tcp.port, int)
        assert config.websocket.max_connections == 200
        assert isinstance(config.websocket.max_connections, int)
        assert config.plugins.enabled is True
        assert isinstance(config.plugins.enabled, bool)
    
    def test_load_config_invalid_environment_value(self, config_loader: ConfigLoader) -> None:
        """Test handling of invalid environment variable values."""
        env_vars = {
            "COBALT_TCP_PORT": "not_a_number"
        }
        
        with patch.dict(os.environ, env_vars):
            with pytest.raises(ValueError, match="Invalid value for COBALT_TCP_PORT"):
                config_loader.load_config()
    
    def test_save_config_json(self, config_loader: ConfigLoader, sample_config_dict: Dict[str, Any]) -> None:
        """Test saving config to JSON file."""
        config = ApplicationConfig.from_dict(sample_config_dict)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            config_loader.save_config(config, temp_path, "json")
            
            # Verify file was created and contains correct data
            assert os.path.exists(temp_path)
            
            with open(temp_path, 'r') as f:
                saved_data = json.load(f)
            
            assert saved_data['name'] == "Test Application"
            assert saved_data['debug'] is True
            assert saved_data['tcp']['port'] == 8080
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_save_config_yaml(self, config_loader: ConfigLoader, sample_config_dict: Dict[str, Any]) -> None:
        """Test saving config to YAML file."""
        config = ApplicationConfig.from_dict(sample_config_dict)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = f.name
        
        try:
            config_loader.save_config(config, temp_path, "yaml")
            
            # Verify file was created
            assert os.path.exists(temp_path)
            
            # Load and verify content
            loaded_config = config_loader.load_config(temp_path)
            assert loaded_config.name == "Test Application"
            assert loaded_config.debug is True
            assert loaded_config.tcp.port == 8080
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_save_config_unsupported_format(self, config_loader: ConfigLoader) -> None:
        """Test saving config with unsupported format."""
        config = ApplicationConfig()
        
        with pytest.raises(ValueError, match="Unsupported format: xml"):
            config_loader.save_config(config, "test.xml", "xml")
    
    def test_merge_configs(self, config_loader: ConfigLoader) -> None:
        """Test configuration merging."""
        base_config = {
            "name": "Base App",
            "debug": False,
            "tcp": {
                "host": "localhost",
                "port": 8080
            }
        }
        
        override_config = {
            "debug": True,
            "tcp": {
                "port": 9090
            },
            "websocket": {
                "port": 8000
            }
        }
        
        merged = config_loader._merge_configs(base_config, override_config)
        
        assert merged["name"] == "Base App"  # Unchanged
        assert merged["debug"] is True  # Overridden
        assert merged["tcp"]["host"] == "localhost"  # Unchanged
        assert merged["tcp"]["port"] == 9090  # Overridden
        assert merged["websocket"]["port"] == 8000  # Added
    
    def test_parse_bool_values(self, config_loader: ConfigLoader) -> None:
        """Test boolean parsing from strings."""
        true_values = ["true", "True", "TRUE", "1", "yes", "Yes", "YES", "on", "On", "ON"]
        false_values = ["false", "False", "FALSE", "0", "no", "No", "NO", "off", "Off", "OFF"]
        
        for value in true_values:
            assert config_loader._parse_bool(value) is True
        
        for value in false_values:
            assert config_loader._parse_bool(value) is False
        
        # Test invalid values
        with pytest.raises(ValueError):
            config_loader._parse_bool("invalid")
    
    def test_set_nested_value(self, config_loader: ConfigLoader) -> None:
        """Test setting nested configuration values."""
        config: Dict[str, Any] = {}
        
        # Set simple value
        config_loader._set_nested_value(config, "debug", True)
        assert config["debug"] is True
        
        # Set nested value
        config_loader._set_nested_value(config, "tcp.host", "192.168.1.1")
        assert config["tcp"]["host"] == "192.168.1.1"
        
        # Set deeply nested value
        config_loader._set_nested_value(config, "logging.handlers.file.level", "ERROR")
        assert config["logging"]["handlers"]["file"]["level"] == "ERROR"
