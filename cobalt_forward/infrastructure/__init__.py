"""
Infrastructure layer containing external dependencies and I/O operations.

This layer handles all external concerns like configuration, logging,
persistence, and communication with external services.
"""

from .config.manager import ConfigManager
from .logging.setup import LoggingManager

__all__ = [
    "ConfigManager",
    "LoggingManager",
]
