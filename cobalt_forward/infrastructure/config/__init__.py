"""
Configuration management infrastructure.

This module provides configuration loading, validation, and management
capabilities for the application including backup, encryption, and file watching.
"""

from .manager import ConfigManager
from .models import ApplicationConfig
from .loader import ConfigLoader
from .backup import ConfigBackup
from .crypto import ConfigCrypto
from .watcher import ConfigWatcher, PollingConfigWatcher, create_config_watcher, IConfigWatcher

__all__ = [
    "ConfigManager",
    "ApplicationConfig",
    "ConfigLoader",
    "ConfigBackup",
    "ConfigCrypto",
    "ConfigWatcher",
    "PollingConfigWatcher",
    "create_config_watcher",
    "IConfigWatcher",
]
