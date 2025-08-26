"""
REST API components for the presentation layer.

This module contains FastAPI application setup, middleware,
and API route definitions.
"""

from .app import create_app
from .dependencies import get_container, get_config, get_component

__all__ = [
    "create_app",
    "get_container",
    "get_config", 
    "get_component",
]
