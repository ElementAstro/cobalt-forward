"""
SSH service interfaces for the Cobalt Forward application.

This module defines the contracts for SSH-related services including
SSH forwarding, tunneling, and connection management.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Union
from enum import Enum
from dataclasses import dataclass

from .lifecycle import IStartable, IStoppable, IHealthCheckable


class ForwardType(Enum):
    """SSH forwarding types."""
    LOCAL = "local"
    REMOTE = "remote"
    DYNAMIC = "dynamic"


class TunnelStatus(Enum):
    """SSH tunnel status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass
class ForwardConfig:
    """SSH forwarding configuration."""
    forward_type: ForwardType
    listen_host: str
    listen_port: int
    dest_host: str
    dest_port: int
    description: Optional[str] = None


@dataclass
class TunnelInfo:
    """SSH tunnel information."""
    tunnel_id: str
    host: str
    port: int
    username: str
    status: TunnelStatus
    forwards: List[ForwardConfig]
    created_at: float
    last_connected: Optional[float] = None
    last_disconnected: Optional[float] = None
    connection_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class ISSHTunnel(ABC):
    """Interface for SSH tunnel management."""
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect the SSH tunnel."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect the SSH tunnel."""
        pass
    
    @abstractmethod
    async def add_forward(self, forward: ForwardConfig) -> int:
        """Add a port forward to the tunnel."""
        pass
    
    @abstractmethod
    async def remove_forward(self, forward_index: int) -> bool:
        """Remove a port forward from the tunnel."""
        pass
    
    @abstractmethod
    def get_info(self) -> TunnelInfo:
        """Get tunnel information."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if tunnel is connected."""
        pass


class ISSHForwarder(IStartable, IStoppable, IHealthCheckable):
    """
    Interface for SSH forwarding service.
    
    Provides SSH tunnel management with support for local, remote,
    and dynamic port forwarding.
    """
    
    @abstractmethod
    async def create_tunnel(
        self,
        host: str,
        port: int,
        username: str,
        password: Optional[str] = None,
        key_file: Optional[str] = None,
        key_passphrase: Optional[str] = None,
        forwards: Optional[List[ForwardConfig]] = None,
        auto_connect: bool = True,
        client_keys: Optional[List[str]] = None,
        keepalive_interval: int = 60,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new SSH tunnel.
        
        Args:
            host: SSH server hostname or IP
            port: SSH server port
            username: Username for authentication
            password: Password for authentication (optional)
            key_file: Private key file path (optional)
            key_passphrase: Private key passphrase (optional)
            forwards: List of port forwarding configurations
            auto_connect: Whether to automatically connect
            client_keys: List of client key paths
            keepalive_interval: Keep-alive interval in seconds
            metadata: Additional metadata
            
        Returns:
            Tunnel ID
        """
        pass
    
    @abstractmethod
    async def connect_tunnel(self, tunnel_id: str) -> bool:
        """Connect an SSH tunnel."""
        pass
    
    @abstractmethod
    async def disconnect_tunnel(self, tunnel_id: str) -> bool:
        """Disconnect an SSH tunnel."""
        pass
    
    @abstractmethod
    async def remove_tunnel(self, tunnel_id: str) -> bool:
        """Remove an SSH tunnel."""
        pass
    
    @abstractmethod
    async def add_forward(self, tunnel_id: str, forward: ForwardConfig) -> int:
        """Add a port forward to a tunnel."""
        pass
    
    @abstractmethod
    async def remove_forward(self, tunnel_id: str, forward_index: int) -> bool:
        """Remove a port forward from a tunnel."""
        pass
    
    @abstractmethod
    def get_tunnel_info(self, tunnel_id: str) -> Optional[TunnelInfo]:
        """Get information about a tunnel."""
        pass
    
    @abstractmethod
    def list_tunnels(self) -> List[TunnelInfo]:
        """List all tunnels."""
        pass
    
    @abstractmethod
    async def upload_file(
        self,
        tunnel_id: str,
        local_path: str,
        remote_path: str,
        callback: Optional[Callable[..., Any]] = None
    ) -> Dict[str, Any]:
        """Upload a file through an SSH tunnel."""
        pass
    
    @abstractmethod
    async def download_file(
        self,
        tunnel_id: str,
        remote_path: str,
        local_path: str,
        callback: Optional[Callable[..., Any]] = None
    ) -> Dict[str, Any]:
        """Download a file through an SSH tunnel."""
        pass
    
    @abstractmethod
    async def execute_command(
        self,
        tunnel_id: str,
        command: str,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """Execute a command through an SSH tunnel."""
        pass
