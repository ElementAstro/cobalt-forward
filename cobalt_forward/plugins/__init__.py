"""
Plugin system for extensible architecture.

This module provides plugin loading, management, and execution capabilities
with sandboxing and security features.
"""

from .manager import PluginManager
from .base import BasePlugin

__all__ = [
    "PluginManager",
    "BasePlugin",
]
