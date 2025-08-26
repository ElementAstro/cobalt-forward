"""
API router modules for different endpoints.

This module contains all the API route handlers organized by functionality.
"""

from . import health, system, websocket, ssh, config, commands, plugin, core

__all__ = [
    "health",
    "system",
    "websocket",
    "ssh",
    "config",
    "commands",
    "plugin",
    "core",
]
