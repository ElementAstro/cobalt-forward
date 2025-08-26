"""
Application layer containing use cases, dependency injection, and startup logic.

This layer orchestrates the interaction between the core domain and
infrastructure layers, managing application lifecycle and dependencies.
"""

from .container import Container, IContainer, ServiceLifetime
from .startup import ApplicationStartup

__all__ = [
    "Container",
    "IContainer", 
    "ServiceLifetime",
    "ApplicationStartup",
]
