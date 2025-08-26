"""
SSH client implementation for the Cobalt Forward application.

This module provides SSH client functionality with connection pooling,
file transfer, and command execution capabilities.
"""

import asyncio
import logging
import time
import os
from typing import Any, Dict, List, Optional, Callable, AsyncContextManager
from contextlib import asynccontextmanager

try:
    import asyncssh
    from asyncssh import SSHClientConnection, SFTPClient
except ImportError:
    asyncssh = None
    SSHClientConnection = None
    SFTPClient = None

from ....core.interfaces.clients import ISSHClient, ClientStatus
from ....core.interfaces.messaging import IEventBus
from ..base import BaseClient
from .config import SSHClientConfig

logger = logging.getLogger(__name__)


class SSHConnectionPool:
    """SSH connection pool for managing multiple connections."""
    
    def __init__(self, config: SSHClientConfig):
        self._config = config
        self._connections: List[SSHClientConnection] = []
        self._available: asyncio.Queue[SSHClientConnection] = asyncio.Queue()
        self._in_use: set[SSHClientConnection] = set()
        self._lock = asyncio.Lock()
        self._created_count = 0
    
    async def get_connection(self) -> SSHClientConnection:
        """Get a connection from the pool."""
        # Try to get an available connection
        try:
            connection = self._available.get_nowait()
            if connection and not connection.is_closing():
                self._in_use.add(connection)
                return connection
        except asyncio.QueueEmpty:
            pass
        
        # Create new connection if pool not full
        async with self._lock:
            if self._created_count < self._config.pool_size:
                connection = await self._create_connection()
                if connection:
                    self._connections.append(connection)
                    self._in_use.add(connection)
                    self._created_count += 1
                    return connection
        
        # Wait for available connection
        connection = await self._available.get()
        self._in_use.add(connection)
        return connection
    
    async def return_connection(self, connection: SSHClientConnection) -> None:
        """Return a connection to the pool."""
        if connection in self._in_use:
            self._in_use.remove(connection)
            
            if not connection.is_closing():
                await self._available.put(connection)
            else:
                # Connection is closed, remove from pool
                if connection in self._connections:
                    self._connections.remove(connection)
                    self._created_count -= 1
    
    async def close_all(self) -> None:
        """Close all connections in the pool."""
        async with self._lock:
            for connection in self._connections:
                if not connection.is_closing():
                    connection.close()
                    await connection.wait_closed()
            
            self._connections.clear()
            self._in_use.clear()
            self._created_count = 0
            
            # Clear the queue
            while not self._available.empty():
                try:
                    self._available.get_nowait()
                except asyncio.QueueEmpty:
                    break
    
    async def _create_connection(self) -> Optional[SSHClientConnection]:
        """Create a new SSH connection."""
        if asyncssh is None:
            logger.error("asyncssh is not installed")
            return None
        
        try:
            kwargs = self._config.to_asyncssh_kwargs()
            connection = await asyncssh.connect(**kwargs)
            logger.debug(f"Created new SSH connection to {self._config.host}:{self._config.port}")
            return connection
        except Exception as e:
            logger.error(f"Failed to create SSH connection: {e}")
            return None


class SSHClient(BaseClient, ISSHClient):
    """
    SSH client implementation.
    
    Provides SSH functionality including command execution,
    file transfer, and port forwarding with connection pooling.
    """
    
    def __init__(
        self,
        config: SSHClientConfig,
        event_bus: Optional[IEventBus] = None,
        name: Optional[str] = None
    ):
        """
        Initialize SSH client.
        
        Args:
            config: SSH client configuration
            event_bus: Event bus for publishing events
            name: Client name for identification
        """
        super().__init__(config, event_bus, name)
        self._ssh_config = config
        self._pool: Optional[SSHConnectionPool] = None
    
    async def connect(self) -> bool:
        """Establish SSH connection pool."""
        if self.is_connected():
            return True
        
        if asyncssh is None:
            logger.error("asyncssh is not installed")
            return False
        
        try:
            self._update_status(ClientStatus.CONNECTING)
            
            # Create connection pool
            self._pool = SSHConnectionPool(self._ssh_config)
            
            # Test connection by creating one
            test_connection = await self._pool.get_connection()
            if test_connection:
                await self._pool.return_connection(test_connection)
                
                self._update_status(ClientStatus.CONNECTED)
                await self._start_keepalive()
                
                if self._event_bus:
                    await self._event_bus.publish("ssh_client.connected", {
                        "client_name": self._name,
                        "host": self._ssh_config.host,
                        "port": self._ssh_config.port,
                        "username": self._ssh_config.username
                    })
                
                logger.info(f"SSH client connected to {self._ssh_config.host}:{self._ssh_config.port}")
                return True
            else:
                self._update_status(ClientStatus.ERROR)
                return False
                
        except Exception as e:
            self._update_status(ClientStatus.ERROR)
            self._metrics.record_error(str(e))
            
            logger.error(f"SSH connection failed: {e}")
            
            if self._event_bus:
                await self._event_bus.publish("ssh_client.connection_failed", {
                    "client_name": self._name,
                    "host": self._ssh_config.host,
                    "port": self._ssh_config.port,
                    "error": str(e)
                })
            
            return False
    
    async def disconnect(self) -> None:
        """Disconnect SSH client."""
        if not self.is_connected():
            return
        
        try:
            await self._stop_keepalive()
            
            if self._pool:
                await self._pool.close_all()
                self._pool = None
            
            self._update_status(ClientStatus.DISCONNECTED)
            
            if self._event_bus:
                await self._event_bus.publish("ssh_client.disconnected", {
                    "client_name": self._name
                })
            
            logger.info(f"SSH client disconnected from {self._ssh_config.host}:{self._ssh_config.port}")
            
        except Exception as e:
            logger.error(f"Error during SSH disconnect: {e}")
    
    async def send(self, data: Any) -> bool:
        """Send data (not applicable for SSH client)."""
        logger.warning("send() method not applicable for SSH client")
        return False
    
    async def receive(self) -> Optional[Any]:
        """Receive data (not applicable for SSH client)."""
        logger.warning("receive() method not applicable for SSH client")
        return None
    
    @asynccontextmanager
    async def _get_connection(self) -> AsyncContextManager[SSHClientConnection]:
        """Get a connection from the pool as context manager."""
        if not self._pool:
            raise RuntimeError("SSH client not connected")
        
        connection = await self._pool.get_connection()
        try:
            yield connection
        finally:
            await self._pool.return_connection(connection)
    
    async def execute_command(
        self,
        command: str,
        timeout: Optional[float] = None,
        environment: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Execute a command on the remote server."""
        if not self.is_connected():
            return {"success": False, "error": "Not connected"}
        
        try:
            async with self._get_connection() as connection:
                start_time = time.time()
                
                # Prepare environment
                env = environment or {}
                
                # Execute command
                if timeout:
                    result = await asyncio.wait_for(
                        connection.run(command, env=env),
                        timeout=timeout
                    )
                else:
                    result = await connection.run(command, env=env)
                
                execution_time = time.time() - start_time
                
                # Record metrics
                self._metrics.record_request(result.exit_status == 0, execution_time)
                
                if self._event_bus:
                    await self._event_bus.publish("ssh_client.command_executed", {
                        "client_name": self._name,
                        "command": command,
                        "exit_status": result.exit_status,
                        "execution_time": execution_time
                    })
                
                return {
                    "success": True,
                    "command": command,
                    "exit_status": result.exit_status,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "execution_time": execution_time
                }
                
        except asyncio.TimeoutError:
            error_msg = f"Command timed out after {timeout} seconds"
            self._metrics.record_error(error_msg)
            return {"success": False, "error": error_msg}
            
        except Exception as e:
            error_msg = f"Command execution failed: {str(e)}"
            self._metrics.record_error(error_msg)
            return {"success": False, "error": error_msg}

    async def upload_file(
        self,
        local_path: str,
        remote_path: str,
        callback: Optional[Callable[[str, str, float], None]] = None
    ) -> None:
        """Upload a file to the remote server."""
        if not self.is_connected():
            raise RuntimeError("SSH client not connected")

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        try:
            async with self._get_connection() as connection:
                async with connection.start_sftp_client() as sftp:
                    file_size = os.path.getsize(local_path)

                    def progress_handler(bytes_sent: int, total_bytes: int) -> None:
                        if callback:
                            progress = (bytes_sent / total_bytes) * 100
                            callback(local_path, remote_path, progress)

                    await sftp.put(
                        local_path,
                        remote_path,
                        progress_handler=progress_handler if callback else None
                    )

                    if self._event_bus:
                        await self._event_bus.publish("ssh_client.file_uploaded", {
                            "client_name": self._name,
                            "local_path": local_path,
                            "remote_path": remote_path,
                            "file_size": file_size
                        })

                    logger.info(f"File uploaded: {local_path} -> {remote_path}")

        except Exception as e:
            error_msg = f"File upload failed: {str(e)}"
            self._metrics.record_error(error_msg)
            raise RuntimeError(error_msg)

    async def download_file(
        self,
        remote_path: str,
        local_path: str,
        callback: Optional[Callable[[str, str, float], None]] = None
    ) -> None:
        """Download a file from the remote server."""
        if not self.is_connected():
            raise RuntimeError("SSH client not connected")

        try:
            async with self._get_connection() as connection:
                async with connection.start_sftp_client() as sftp:
                    # Get file size
                    file_attrs = await sftp.stat(remote_path)
                    file_size = file_attrs.size if file_attrs else 0

                    def progress_handler(bytes_received: int, total_bytes: int) -> None:
                        if callback:
                            progress = (bytes_received / total_bytes) * 100
                            callback(remote_path, local_path, progress)

                    await sftp.get(
                        remote_path,
                        local_path,
                        progress_handler=progress_handler if callback else None
                    )

                    if self._event_bus:
                        await self._event_bus.publish("ssh_client.file_downloaded", {
                            "client_name": self._name,
                            "remote_path": remote_path,
                            "local_path": local_path,
                            "file_size": file_size
                        })

                    logger.info(f"File downloaded: {remote_path} -> {local_path}")

        except Exception as e:
            error_msg = f"File download failed: {str(e)}"
            self._metrics.record_error(error_msg)
            raise RuntimeError(error_msg)

    async def create_sftp_session(self) -> Any:
        """Create an SFTP session."""
        if not self.is_connected():
            raise RuntimeError("SSH client not connected")

        # Note: This returns a context manager, not a direct SFTP client
        async with self._get_connection() as connection:
            return connection.start_sftp_client()

    async def forward_local_port(
        self,
        local_host: str,
        local_port: int,
        remote_host: str,
        remote_port: int
    ) -> Any:
        """Create a local port forward."""
        if not self.is_connected():
            raise RuntimeError("SSH client not connected")

        try:
            async with self._get_connection() as connection:
                listener = await connection.forward_local_port(
                    local_host, local_port, remote_host, remote_port
                )

                if self._event_bus:
                    await self._event_bus.publish("ssh_client.local_forward_created", {
                        "client_name": self._name,
                        "local": f"{local_host}:{local_port}",
                        "remote": f"{remote_host}:{remote_port}"
                    })

                logger.info(f"Local port forward: {local_host}:{local_port} -> {remote_host}:{remote_port}")
                return listener

        except Exception as e:
            error_msg = f"Local port forward failed: {str(e)}"
            self._metrics.record_error(error_msg)
            raise RuntimeError(error_msg)

    async def forward_remote_port(
        self,
        remote_host: str,
        remote_port: int,
        local_host: str,
        local_port: int
    ) -> Any:
        """Create a remote port forward."""
        if not self.is_connected():
            raise RuntimeError("SSH client not connected")

        try:
            async with self._get_connection() as connection:
                listener = await connection.forward_remote_port(
                    remote_host, remote_port, local_host, local_port
                )

                if self._event_bus:
                    await self._event_bus.publish("ssh_client.remote_forward_created", {
                        "client_name": self._name,
                        "remote": f"{remote_host}:{remote_port}",
                        "local": f"{local_host}:{local_port}"
                    })

                logger.info(f"Remote port forward: {remote_host}:{remote_port} -> {local_host}:{local_port}")
                return listener

        except Exception as e:
            error_msg = f"Remote port forward failed: {str(e)}"
            self._metrics.record_error(error_msg)
            raise RuntimeError(error_msg)

    async def _send_keepalive(self) -> None:
        """Send SSH keepalive."""
        if self.is_connected():
            # SSH keepalive is handled by asyncssh automatically
            # We just update the last activity time
            self._last_activity = time.time()
