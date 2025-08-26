"""
Presentation layer containing HTTP API and WebSocket interfaces.

This layer handles all user-facing interfaces including REST APIs,
WebSocket connections, and request/response processing.
"""

from .api.app import create_app

__all__ = [
    "create_app",
]
