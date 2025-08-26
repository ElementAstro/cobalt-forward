"""
Client interfaces for various protocols.

This module defines the contracts for client implementations
including SSH, FTP, MQTT, TCP, UDP, and WebSocket clients.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Union
from enum import Enum

from .lifecycle import IStartable, IStoppable, IHealthCheckable


class ClientStatus(Enum):
    """Client connection status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


class IBaseClient(IStartable, IStoppable, IHealthCheckable):
    """Base interface for all client implementations."""
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the server."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the server."""
        pass
    
    @abstractmethod
    async def send(self, data: Any) -> bool:
        """Send data to the server."""
        pass
    
    @abstractmethod
    async def receive(self) -> Optional[Any]:
        """Receive data from the server."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if client is connected."""
        pass
    
    @abstractmethod
    def get_status(self) -> ClientStatus:
        """Get current client status."""
        pass


class ISSHClient(IBaseClient):
    """Interface for SSH client implementation."""
    
    @abstractmethod
    async def execute_command(
        self,
        command: str,
        timeout: Optional[float] = None,
        environment: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Execute a command on the remote server."""
        pass
    
    @abstractmethod
    async def upload_file(
        self,
        local_path: str,
        remote_path: str,
        callback: Optional[Callable[[str, str, float], None]] = None
    ) -> None:
        """Upload a file to the remote server."""
        pass
    
    @abstractmethod
    async def download_file(
        self,
        remote_path: str,
        local_path: str,
        callback: Optional[Callable[[str, str, float], None]] = None
    ) -> None:
        """Download a file from the remote server."""
        pass
    
    @abstractmethod
    async def create_sftp_session(self) -> Any:
        """Create an SFTP session."""
        pass
    
    @abstractmethod
    async def forward_local_port(
        self,
        local_host: str,
        local_port: int,
        remote_host: str,
        remote_port: int
    ) -> Any:
        """Create a local port forward."""
        pass
    
    @abstractmethod
    async def forward_remote_port(
        self,
        remote_host: str,
        remote_port: int,
        local_host: str,
        local_port: int
    ) -> Any:
        """Create a remote port forward."""
        pass


class IFTPClient(IBaseClient):
    """Interface for FTP client implementation."""
    
    @abstractmethod
    async def list_directory(self, path: str = ".") -> List[Dict[str, Any]]:
        """List directory contents."""
        pass
    
    @abstractmethod
    async def change_directory(self, path: str) -> bool:
        """Change current directory."""
        pass
    
    @abstractmethod
    async def create_directory(self, path: str) -> bool:
        """Create a directory."""
        pass
    
    @abstractmethod
    async def delete_file(self, path: str) -> bool:
        """Delete a file."""
        pass
    
    @abstractmethod
    async def upload_file(
        self,
        local_path: str,
        remote_path: str,
        callback: Optional[Callable[[int, int], None]] = None
    ) -> bool:
        """Upload a file."""
        pass
    
    @abstractmethod
    async def download_file(
        self,
        remote_path: str,
        local_path: str,
        callback: Optional[Callable[[int, int], None]] = None
    ) -> bool:
        """Download a file."""
        pass


class IMQTTClient(IBaseClient):
    """Interface for MQTT client implementation."""
    
    @abstractmethod
    async def subscribe(
        self,
        topic: str,
        qos: int = 0,
        callback: Optional[Callable[[str, bytes], None]] = None
    ) -> bool:
        """Subscribe to a topic."""
        pass
    
    @abstractmethod
    async def unsubscribe(self, topic: str) -> bool:
        """Unsubscribe from a topic."""
        pass
    
    @abstractmethod
    async def publish(
        self,
        topic: str,
        payload: Union[str, bytes],
        qos: int = 0,
        retain: bool = False
    ) -> bool:
        """Publish a message to a topic."""
        pass


class ITCPClient(IBaseClient):
    """Interface for TCP client implementation."""
    
    @abstractmethod
    async def send_raw(self, data: bytes) -> bool:
        """Send raw bytes."""
        pass
    
    @abstractmethod
    async def receive_raw(self, buffer_size: int = 4096) -> Optional[bytes]:
        """Receive raw bytes."""
        pass


class IUDPClient(IBaseClient):
    """Interface for UDP client implementation."""
    
    @abstractmethod
    async def send_to(self, data: bytes, address: tuple) -> bool:
        """Send data to specific address."""
        pass
    
    @abstractmethod
    async def receive_from(self, buffer_size: int = 4096) -> Optional[tuple]:
        """Receive data and sender address."""
        pass


class IWebSocketClient(IBaseClient):
    """Interface for WebSocket client implementation."""
    
    @abstractmethod
    async def send_text(self, message: str) -> bool:
        """Send text message."""
        pass
    
    @abstractmethod
    async def send_binary(self, data: bytes) -> bool:
        """Send binary data."""
        pass
    
    @abstractmethod
    async def receive_text(self) -> Optional[str]:
        """Receive text message."""
        pass
    
    @abstractmethod
    async def receive_binary(self) -> Optional[bytes]:
        """Receive binary data."""
        pass
    
    @abstractmethod
    async def ping(self, data: bytes = b"") -> bool:
        """Send ping frame."""
        pass
    
    @abstractmethod
    async def pong(self, data: bytes = b"") -> bool:
        """Send pong frame."""
        pass
