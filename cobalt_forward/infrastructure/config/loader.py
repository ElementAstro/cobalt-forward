"""
Configuration loading and saving utilities.

This module provides functionality to load configuration from various sources
including files, environment variables, and command line arguments.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Callable, Union

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None  # type: ignore
    YAML_AVAILABLE = False

from .models import ApplicationConfig


class ConfigLoader:
    """Configuration loader supporting multiple formats and sources."""

    def __init__(self) -> None:
        self._env_prefix = "COBALT_"

    def load_config(self, config_file: Optional[str] = None) -> ApplicationConfig:
        """
        Load configuration from file and environment variables.

        Args:
            config_file: Path to configuration file (optional)

        Returns:
            Loaded and validated configuration
        """
        # Start with default configuration
        config_data = {}

        # Load from file if provided
        if config_file:
            config_data = self._load_from_file(config_file)

        # Override with environment variables
        env_overrides = self._load_from_environment()
        config_data = self._merge_configs(config_data, env_overrides)

        # Create and validate configuration
        config = ApplicationConfig.from_dict(config_data)
        config.config_file_path = config_file

        return config

    def save_config(self, config: ApplicationConfig, file_path: str, format: str = "yaml") -> None:
        """
        Save configuration to file.

        Args:
            config: Configuration to save
            file_path: Output file path
            format: File format (yaml or json)
        """
        config_data = config.to_dict()

        if format.lower() == "yaml":
            self._save_yaml(config_data, file_path)
        elif format.lower() == "json":
            self._save_json(config_data, file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _load_from_file(self, file_path: str) -> Dict[str, Any]:
        """Load configuration from file."""
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {file_path}")

        if path.suffix.lower() in ['.yaml', '.yml']:
            return self._load_yaml(file_path)
        elif path.suffix.lower() == '.json':
            return self._load_json(file_path)
        else:
            raise ValueError(
                f"Unsupported configuration file format: {path.suffix}")

    def _load_yaml(self, file_path: str) -> Dict[str, Any]:
        """Load YAML configuration file."""
        if not YAML_AVAILABLE:
            raise ValueError(
                "YAML support not available: PyYAML package not installed")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {file_path}: {e}")
        except Exception as e:
            raise ValueError(f"Error reading {file_path}: {e}")

    def _load_json(self, file_path: str) -> Dict[str, Any]:
        """Load JSON configuration file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)  # type: ignore[no-any-return]
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {file_path}: {e}")
        except Exception as e:
            raise ValueError(f"Error reading {file_path}: {e}")

    def _save_yaml(self, data: Dict[str, Any], file_path: str) -> None:
        """Save configuration as YAML."""
        if not YAML_AVAILABLE:
            raise ValueError(
                "YAML support not available: PyYAML package not installed")

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, indent=2)
        except Exception as e:
            raise ValueError(f"Error writing YAML to {file_path}: {e}")

    def _save_json(self, data: Dict[str, Any], file_path: str) -> None:
        """Save configuration as JSON."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            raise ValueError(f"Error writing JSON to {file_path}: {e}")

    def _load_from_environment(self) -> Dict[str, Any]:
        """Load configuration overrides from environment variables."""
        config: Dict[str, Any] = {}

        # Define environment variable mappings
        env_mappings = {
            f"{self._env_prefix}DEBUG": ("debug", self._parse_bool),
            f"{self._env_prefix}ENVIRONMENT": ("environment", str),
            f"{self._env_prefix}TCP_HOST": ("tcp.host", str),
            f"{self._env_prefix}TCP_PORT": ("tcp.port", int),
            f"{self._env_prefix}TCP_TIMEOUT": ("tcp.timeout", float),
            f"{self._env_prefix}WS_HOST": ("websocket.host", str),
            f"{self._env_prefix}WS_PORT": ("websocket.port", int),
            f"{self._env_prefix}SSH_ENABLED": ("ssh.enabled", self._parse_bool),
            f"{self._env_prefix}SSH_HOST": ("ssh.host", str),
            f"{self._env_prefix}SSH_PORT": ("ssh.port", int),
            f"{self._env_prefix}FTP_ENABLED": ("ftp.enabled", self._parse_bool),
            f"{self._env_prefix}FTP_HOST": ("ftp.host", str),
            f"{self._env_prefix}FTP_PORT": ("ftp.port", int),
            f"{self._env_prefix}LOG_LEVEL": ("logging.level", str),
            f"{self._env_prefix}LOG_DIR": ("logging.log_directory", str),
            f"{self._env_prefix}PLUGIN_DIR": ("plugins.plugin_directory", str),
            f"{self._env_prefix}PLUGINS_ENABLED": ("plugins.enabled", self._parse_bool),
        }

        for env_var, (config_path, converter) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    converted_value = converter(
                        value)  # type: ignore[operator]
                    self._set_nested_value(
                        config, config_path, converted_value)
                except (ValueError, TypeError) as e:
                    raise ValueError(
                        f"Invalid value for {env_var}: {value} ({e})")

        return config

    def _parse_bool(self, value: str) -> bool:
        """Parse boolean value from string."""
        return value.lower() in ('true', '1', 'yes', 'on', 'enabled')

    def _set_nested_value(self, config: Dict[str, Any], path: str, value: Any) -> None:
        """Set a nested configuration value using dot notation."""
        keys = path.split('.')
        current = config

        # Navigate to the parent of the target key
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        # Set the final value
        current[keys[-1]] = value

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge two configuration dictionaries."""
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value

        return result
