"""
Core interfaces defining the contracts for all major system components.

These interfaces provide the foundation for dependency inversion and enable
loose coupling between components.
"""

from .lifecycle import IStartable, IStoppable, IHealthCheckable
from .messaging import IEventBus, IMessageBus
from .commands import ICommandDispatcher, ICommandHandler
from .plugins import IPlugin, IPluginManager
from .ssh import ISSHForwarder, ISSHTunnel, ForwardType, TunnelStatus, ForwardConfig, TunnelInfo
from .upload import IUploadManager, IUploadSession, UploadStatus, UploadStrategy, UploadInfo, UploadChunk
from .clients import (
    IBaseClient, ISSHClient, IFTPClient, IMQTTClient,
    ITCPClient, IUDPClient, IWebSocketClient, ClientStatus
)

__all__ = [
    "IStartable",
    "IStoppable",
    "IHealthCheckable",
    "IEventBus",
    "IMessageBus",
    "ICommandDispatcher",
    "ICommandHandler",
    "IPlugin",
    "IPluginManager",
    "ISSHForwarder",
    "ISSHTunnel",
    "ForwardType",
    "TunnelStatus",
    "ForwardConfig",
    "TunnelInfo",
    "IUploadManager",
    "IUploadSession",
    "UploadStatus",
    "UploadStrategy",
    "UploadInfo",
    "UploadChunk",
    "IBaseClient",
    "ISSHClient",
    "IFTPClient",
    "IMQTTClient",
    "ITCPClient",
    "IUDPClient",
    "IWebSocketClient",
    "ClientStatus",
]
