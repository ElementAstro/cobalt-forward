"""
SSH client implementation for the Cobalt Forward application.

This module provides SSH client functionality with connection pooling,
file transfer, and command execution capabilities.
"""

from .client import SSHClient
from .config import SSHClientConfig

__all__ = [
    "SSHClient",
    "SSHClientConfig",
]
