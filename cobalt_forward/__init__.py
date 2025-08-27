"""
Cobalt Forward - High-performance TCP-WebSocket forwarder with command dispatching capabilities.

This package provides a modular, extensible architecture for forwarding messages between
TCP and WebSocket connections with support for command processing, event handling,
and plugin systems.
"""

__version__ = "0.1.0"
__author__ = "Max Qian"
__email__ = "astro_air@126.com"

# Public API exports
from .core.interfaces.lifecycle import IStartable, IStoppable, IHealthCheckable
from .core.interfaces.messaging import IEventBus, IMessageBus
from .core.interfaces.commands import ICommandDispatcher, ICommandHandler
from .core.interfaces.ssh import ISSHForwarder, ISSHTunnel, ForwardType, TunnelStatus, ForwardConfig
from .core.interfaces.upload import IUploadManager, IUploadSession, UploadStatus, UploadStrategy
from .core.interfaces.clients import ISSHClient, IFTPClient, IMQTTClient
from .application.container import Container, IContainer

__all__ = [
    "IStartable",
    "IStoppable",
    "IHealthCheckable",
    "IEventBus",
    "IMessageBus",
    "ICommandDispatcher",
    "ICommandHandler",
    "Container",
    "IContainer",
]
