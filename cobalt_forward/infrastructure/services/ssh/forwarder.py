"""
SSH Forwarder implementation for the Cobalt Forward application.

This module provides SSH tunneling and port forwarding capabilities
with support for local, remote, and dynamic forwarding.
"""

import asyncio
import logging
import time
import uuid
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

try:
    import asyncssh
except ImportError:
    asyncssh = None

from ....core.interfaces.ssh import (
    ISSHForwarder, ISSHTunnel, ForwardType, TunnelStatus, 
    ForwardConfig, TunnelInfo
)
from ....core.interfaces.lifecycle import IComponent
from ....core.interfaces.messaging import IEventBus

logger = logging.getLogger(__name__)


class SSHTunnel(ISSHTunnel):
    """SSH tunnel implementation."""
    
    def __init__(self, tunnel_info: TunnelInfo, event_bus: Optional[IEventBus] = None):
        self._info = tunnel_info
        self._event_bus = event_bus
        self._connection: Optional[Any] = None
        self._listener_tasks: Dict[str, asyncio.Task] = {}
    
    async def connect(self) -> bool:
        """Connect the SSH tunnel."""
        if asyncssh is None:
            logger.error("asyncssh is not installed")
            return False
        
        try:
            self._info.status = TunnelStatus.CONNECTING
            
            # Prepare connection options
            connect_kwargs = {
                'host': self._info.host,
                'port': self._info.port,
                'username': self._info.username,
                'client_version': self._info.metadata.get('client_version', 'Cobalt_SSH_Forwarder_1.0'),
                'compression_algs': ['zlib@openssh.com', 'zlib'] if self._info.metadata.get('use_compression', True) else None,
                'keepalive_interval': self._info.metadata.get('keepalive_interval', 60),
            }
            
            # Add authentication
            if 'password' in self._info.metadata:
                connect_kwargs['password'] = self._info.metadata['password']
            
            if 'key_file' in self._info.metadata:
                connect_kwargs['client_keys'] = [self._info.metadata['key_file']]
            elif 'client_keys' in self._info.metadata:
                connect_kwargs['client_keys'] = self._info.metadata['client_keys']
            
            if 'key_passphrase' in self._info.metadata:
                connect_kwargs['passphrase'] = self._info.metadata['key_passphrase']
            
            # Establish connection
            self._connection = await asyncssh.connect(**connect_kwargs)
            
            # Start port forwards
            for i, forward in enumerate(self._info.forwards):
                await self._start_forward(i)
            
            self._info.status = TunnelStatus.CONNECTED
            self._info.last_connected = time.time()
            self._info.connection_count += 1
            
            if self._event_bus:
                await self._event_bus.publish("ssh.tunnel.connected", {
                    "tunnel_id": self._info.tunnel_id,
                    "host": self._info.host,
                    "port": self._info.port
                })
            
            logger.info(f"SSH tunnel connected: {self._info.tunnel_id}")
            return True
            
        except Exception as e:
            self._info.status = TunnelStatus.ERROR
            self._info.last_error = str(e)
            self._info.error_count += 1
            
            if self._event_bus:
                await self._event_bus.publish("ssh.tunnel.error", {
                    "tunnel_id": self._info.tunnel_id,
                    "error": str(e)
                })
            
            logger.error(f"Failed to connect SSH tunnel {self._info.tunnel_id}: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect the SSH tunnel."""
        try:
            # Stop all listener tasks
            for task_id, task in self._listener_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            self._listener_tasks.clear()
            
            # Close connection
            if self._connection:
                self._connection.close()
                await self._connection.wait_closed()
                self._connection = None
            
            self._info.status = TunnelStatus.DISCONNECTED
            self._info.last_disconnected = time.time()
            
            if self._event_bus:
                await self._event_bus.publish("ssh.tunnel.disconnected", {
                    "tunnel_id": self._info.tunnel_id
                })
            
            logger.info(f"SSH tunnel disconnected: {self._info.tunnel_id}")
            
        except Exception as e:
            logger.error(f"Error disconnecting SSH tunnel {self._info.tunnel_id}: {e}")
    
    async def add_forward(self, forward: ForwardConfig) -> int:
        """Add a port forward to the tunnel."""
        self._info.forwards.append(forward)
        forward_index = len(self._info.forwards) - 1
        
        if self._info.status == TunnelStatus.CONNECTED:
            await self._start_forward(forward_index)
        
        return forward_index
    
    async def remove_forward(self, forward_index: int) -> bool:
        """Remove a port forward from the tunnel."""
        if forward_index < 0 or forward_index >= len(self._info.forwards):
            return False
        
        # Stop the listener task
        task_id = f"forward_{forward_index}"
        if task_id in self._listener_tasks:
            task = self._listener_tasks[task_id]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self._listener_tasks[task_id]
        
        # Remove the forward config
        del self._info.forwards[forward_index]
        return True
    
    def get_info(self) -> TunnelInfo:
        """Get tunnel information."""
        return self._info
    
    def is_connected(self) -> bool:
        """Check if tunnel is connected."""
        return self._info.status == TunnelStatus.CONNECTED and self._connection is not None
    
    async def _start_forward(self, forward_index: int) -> None:
        """Start a port forward."""
        if not self._connection or forward_index >= len(self._info.forwards):
            return
        
        forward = self._info.forwards[forward_index]
        
        try:
            if forward.forward_type == ForwardType.LOCAL:
                # Local port forwarding
                listener = await self._connection.forward_local_port(
                    forward.listen_host,
                    forward.listen_port,
                    forward.dest_host,
                    forward.dest_port
                )
                
                logger.info(f"Started local port forward: {forward.listen_host}:{forward.listen_port} -> {forward.dest_host}:{forward.dest_port}")
                
            elif forward.forward_type == ForwardType.REMOTE:
                # Remote port forwarding
                listener = await self._connection.forward_remote_port(
                    forward.listen_host,
                    forward.listen_port,
                    forward.dest_host,
                    forward.dest_port
                )
                
                logger.info(f"Started remote port forward: {forward.listen_host}:{forward.listen_port} -> {forward.dest_host}:{forward.dest_port}")
            
            elif forward.forward_type == ForwardType.DYNAMIC:
                # Dynamic port forwarding (SOCKS proxy)
                listener = await self._connection.forward_socks(
                    forward.listen_host,
                    forward.listen_port
                )
                
                logger.info(f"Started SOCKS proxy: {forward.listen_host}:{forward.listen_port}")
            
            else:
                logger.warning(f"Unsupported forward type: {forward.forward_type}")
                return
            
            # Create task to maintain the listener
            task_id = f"forward_{forward_index}"
            self._listener_tasks[task_id] = asyncio.create_task(
                self._maintain_listener(task_id, listener)
            )
            
            if self._event_bus:
                await self._event_bus.publish("ssh.forward.created", {
                    "tunnel_id": self._info.tunnel_id,
                    "forward_type": forward.forward_type.value,
                    "listen": f"{forward.listen_host}:{forward.listen_port}",
                    "destination": f"{forward.dest_host}:{forward.dest_port}" if forward.dest_host else "dynamic"
                })
        
        except Exception as e:
            logger.error(f"Failed to start forward {forward_index}: {e}")
            if self._event_bus:
                await self._event_bus.publish("ssh.forward.error", {
                    "tunnel_id": self._info.tunnel_id,
                    "forward_index": forward_index,
                    "error": str(e)
                })
    
    async def _maintain_listener(self, task_id: str, listener: Any) -> None:
        """Maintain a port forward listener."""
        try:
            await listener.wait_closed()
        except Exception as e:
            logger.error(f"Listener {task_id} error: {e}")
        finally:
            if task_id in self._listener_tasks:
                del self._listener_tasks[task_id]


class SSHForwarder(ISSHForwarder, IComponent):
    """
    SSH forwarder service implementation.

    Provides SSH tunnel management with support for local, remote,
    and dynamic port forwarding.
    """

    def __init__(
        self,
        event_bus: Optional[IEventBus] = None,
        known_hosts_path: Optional[str] = None,
        keys_path: Optional[str] = None,
        reconnect_interval: int = 5,
        max_reconnect_attempts: int = 10
    ):
        """
        Initialize SSH forwarder.

        Args:
            event_bus: Event bus for publishing events
            known_hosts_path: Path to known hosts file
            keys_path: Path to SSH keys directory
            reconnect_interval: Reconnection interval in seconds
            max_reconnect_attempts: Maximum reconnection attempts
        """
        self._event_bus = event_bus
        self._known_hosts_path = known_hosts_path
        self._keys_path = keys_path
        self._reconnect_interval = reconnect_interval
        self._max_reconnect_attempts = max_reconnect_attempts

        self._tunnels: Dict[str, SSHTunnel] = {}
        self._running = False
        self._reconnect_tasks: Dict[str, asyncio.Task[None]] = {}

    async def start(self) -> None:
        """Start the SSH forwarder service."""
        if self._running:
            return

        self._running = True
        logger.info("SSH forwarder service started")

        if self._event_bus:
            await self._event_bus.publish("ssh.forwarder.started", {})

    async def stop(self) -> None:
        """Stop the SSH forwarder service."""
        if not self._running:
            return

        self._running = False

        # Stop all reconnect tasks
        for task in self._reconnect_tasks.values():
            if not task.done():
                task.cancel()

        # Disconnect all tunnels
        for tunnel in self._tunnels.values():
            await tunnel.disconnect()

        self._tunnels.clear()
        self._reconnect_tasks.clear()

        logger.info("SSH forwarder service stopped")

        if self._event_bus:
            await self._event_bus.publish("ssh.forwarder.stopped", {})

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        connected_count = sum(1 for tunnel in self._tunnels.values() if tunnel.is_connected())
        total_count = len(self._tunnels)

        return {
            "healthy": self._running,
            "tunnels_total": total_count,
            "tunnels_connected": connected_count,
            "tunnels_disconnected": total_count - connected_count,
            "details": {
                "running": self._running,
                "tunnel_status": {
                    tunnel_id: tunnel.get_info().status.value
                    for tunnel_id, tunnel in self._tunnels.items()
                }
            }
        }

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
        """Create a new SSH tunnel."""
        tunnel_id = str(uuid.uuid4())

        # Prepare metadata
        tunnel_metadata = metadata or {}
        if password:
            tunnel_metadata["password"] = password
        if key_file:
            tunnel_metadata["key_file"] = key_file
        if key_passphrase:
            tunnel_metadata["key_passphrase"] = key_passphrase
        if client_keys:
            tunnel_metadata["client_keys"] = client_keys

        tunnel_metadata.update({
            "client_version": "Cobalt_SSH_Forwarder_1.0",
            "use_compression": True,
            "keepalive_interval": keepalive_interval
        })

        # Create tunnel info
        tunnel_info = TunnelInfo(
            tunnel_id=tunnel_id,
            host=host,
            port=port,
            username=username,
            status=TunnelStatus.DISCONNECTED,
            forwards=forwards or [],
            created_at=time.time(),
            metadata=tunnel_metadata
        )

        # Create tunnel
        tunnel = SSHTunnel(tunnel_info, self._event_bus)
        self._tunnels[tunnel_id] = tunnel

        logger.info(f"Created SSH tunnel: {tunnel_id} ({username}@{host}:{port})")

        if self._event_bus:
            await self._event_bus.publish("ssh.tunnel.created", {
                "tunnel_id": tunnel_id,
                "host": host,
                "port": port,
                "username": username
            })

        # Auto-connect if requested
        if auto_connect:
            await self.connect_tunnel(tunnel_id)

        return tunnel_id

    async def connect_tunnel(self, tunnel_id: str) -> bool:
        """Connect an SSH tunnel."""
        if tunnel_id not in self._tunnels:
            logger.warning(f"Tunnel not found: {tunnel_id}")
            return False

        tunnel = self._tunnels[tunnel_id]
        return await tunnel.connect()

    async def disconnect_tunnel(self, tunnel_id: str) -> bool:
        """Disconnect an SSH tunnel."""
        if tunnel_id not in self._tunnels:
            logger.warning(f"Tunnel not found: {tunnel_id}")
            return False

        tunnel = self._tunnels[tunnel_id]
        await tunnel.disconnect()

        # Cancel reconnect task if running
        if tunnel_id in self._reconnect_tasks:
            task = self._reconnect_tasks[tunnel_id]
            if not task.done():
                task.cancel()
            del self._reconnect_tasks[tunnel_id]

        return True

    async def remove_tunnel(self, tunnel_id: str) -> bool:
        """Remove an SSH tunnel."""
        if tunnel_id not in self._tunnels:
            logger.warning(f"Tunnel not found: {tunnel_id}")
            return False

        # Disconnect first
        await self.disconnect_tunnel(tunnel_id)

        # Remove tunnel
        del self._tunnels[tunnel_id]

        logger.info(f"Removed SSH tunnel: {tunnel_id}")

        if self._event_bus:
            await self._event_bus.publish("ssh.tunnel.removed", {
                "tunnel_id": tunnel_id
            })

        return True

    async def add_forward(self, tunnel_id: str, forward: ForwardConfig) -> int:
        """Add a port forward to a tunnel."""
        if tunnel_id not in self._tunnels:
            logger.warning(f"Tunnel not found: {tunnel_id}")
            return -1

        tunnel = self._tunnels[tunnel_id]
        return await tunnel.add_forward(forward)

    async def remove_forward(self, tunnel_id: str, forward_index: int) -> bool:
        """Remove a port forward from a tunnel."""
        if tunnel_id not in self._tunnels:
            logger.warning(f"Tunnel not found: {tunnel_id}")
            return False

        tunnel = self._tunnels[tunnel_id]
        return await tunnel.remove_forward(forward_index)

    def get_tunnel_info(self, tunnel_id: str) -> Optional[TunnelInfo]:
        """Get information about a tunnel."""
        if tunnel_id not in self._tunnels:
            return None

        tunnel = self._tunnels[tunnel_id]
        return tunnel.get_info()

    def list_tunnels(self) -> List[TunnelInfo]:
        """List all tunnels."""
        return [tunnel.get_info() for tunnel in self._tunnels.values()]

    async def upload_file(
        self,
        tunnel_id: str,
        local_path: str,
        remote_path: str,
        callback: Optional[Callable[..., Any]] = None
    ) -> Dict[str, Any]:
        """Upload a file through an SSH tunnel."""
        if tunnel_id not in self._tunnels:
            return {"success": False, "error": "Tunnel not found"}

        tunnel = self._tunnels[tunnel_id]
        tunnel_info = tunnel.get_info()

        if tunnel_info.status != TunnelStatus.CONNECTED:
            return {"success": False, "error": "Tunnel not connected"}

        if not os.path.exists(local_path):
            return {"success": False, "error": "Local file not found"}

        try:
            # Get the connection from the tunnel
            connection = tunnel._connection
            if not connection:
                return {"success": False, "error": "No active connection"}

            # Create SFTP client
            async with connection.start_sftp_client() as sftp:
                # Get file size for progress tracking
                file_size = os.path.getsize(local_path)
                bytes_transferred = 0

                def progress_callback(bytes_sent: int, total_bytes: int) -> None:
                    nonlocal bytes_transferred
                    bytes_transferred = bytes_sent
                    if callback:
                        callback(local_path, remote_path, bytes_sent / total_bytes * 100)

                # Upload file
                await sftp.put(
                    local_path,
                    remote_path,
                    progress_handler=progress_callback if callback else None
                )

                logger.info(f"File uploaded: {local_path} -> {remote_path} ({file_size} bytes)")

                if self._event_bus:
                    await self._event_bus.publish("ssh.file.uploaded", {
                        "tunnel_id": tunnel_id,
                        "local_path": local_path,
                        "remote_path": remote_path,
                        "file_size": file_size
                    })

                return {
                    "success": True,
                    "local_path": local_path,
                    "remote_path": remote_path,
                    "file_size": file_size,
                    "bytes_transferred": bytes_transferred
                }

        except Exception as e:
            error_msg = f"Upload failed: {str(e)}"
            logger.error(error_msg)

            if self._event_bus:
                await self._event_bus.publish("ssh.file.upload_error", {
                    "tunnel_id": tunnel_id,
                    "local_path": local_path,
                    "remote_path": remote_path,
                    "error": str(e)
                })

            return {"success": False, "error": error_msg}

    async def download_file(
        self,
        tunnel_id: str,
        remote_path: str,
        local_path: str,
        callback: Optional[Callable[..., Any]] = None
    ) -> Dict[str, Any]:
        """Download a file through an SSH tunnel."""
        if tunnel_id not in self._tunnels:
            return {"success": False, "error": "Tunnel not found"}

        tunnel = self._tunnels[tunnel_id]
        tunnel_info = tunnel.get_info()

        if tunnel_info.status != TunnelStatus.CONNECTED:
            return {"success": False, "error": "Tunnel not connected"}

        try:
            # Get the connection from the tunnel
            connection = tunnel._connection
            if not connection:
                return {"success": False, "error": "No active connection"}

            # Create SFTP client
            async with connection.start_sftp_client() as sftp:
                # Get file size for progress tracking
                file_attrs = await sftp.stat(remote_path)
                file_size = file_attrs.size if file_attrs else 0
                bytes_transferred = 0

                def progress_callback(bytes_received: int, total_bytes: int) -> None:
                    nonlocal bytes_transferred
                    bytes_transferred = bytes_received
                    if callback:
                        callback(remote_path, local_path, bytes_received / total_bytes * 100)

                # Download file
                await sftp.get(
                    remote_path,
                    local_path,
                    progress_handler=progress_callback if callback else None
                )

                logger.info(f"File downloaded: {remote_path} -> {local_path} ({file_size} bytes)")

                if self._event_bus:
                    await self._event_bus.publish("ssh.file.downloaded", {
                        "tunnel_id": tunnel_id,
                        "remote_path": remote_path,
                        "local_path": local_path,
                        "file_size": file_size
                    })

                return {
                    "success": True,
                    "remote_path": remote_path,
                    "local_path": local_path,
                    "file_size": file_size,
                    "bytes_transferred": bytes_transferred
                }

        except Exception as e:
            error_msg = f"Download failed: {str(e)}"
            logger.error(error_msg)

            if self._event_bus:
                await self._event_bus.publish("ssh.file.download_error", {
                    "tunnel_id": tunnel_id,
                    "remote_path": remote_path,
                    "local_path": local_path,
                    "error": str(e)
                })

            return {"success": False, "error": error_msg}

    async def execute_command(
        self,
        tunnel_id: str,
        command: str,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """Execute a command through an SSH tunnel."""
        if tunnel_id not in self._tunnels:
            return {"success": False, "error": "Tunnel not found"}

        tunnel = self._tunnels[tunnel_id]
        tunnel_info = tunnel.get_info()

        if tunnel_info.status != TunnelStatus.CONNECTED:
            return {"success": False, "error": "Tunnel not connected"}

        try:
            # Get the connection from the tunnel
            connection = tunnel._connection
            if not connection:
                return {"success": False, "error": "No active connection"}

            # Execute command
            start_time = time.time()

            if timeout:
                result = await asyncio.wait_for(
                    connection.run(command),
                    timeout=timeout
                )
            else:
                result = await connection.run(command)

            execution_time = time.time() - start_time

            logger.info(f"Command executed on {tunnel_id}: {command} (exit code: {result.exit_status})")

            if self._event_bus:
                await self._event_bus.publish("ssh.command.executed", {
                    "tunnel_id": tunnel_id,
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
            logger.error(error_msg)

            if self._event_bus:
                await self._event_bus.publish("ssh.command.timeout", {
                    "tunnel_id": tunnel_id,
                    "command": command,
                    "timeout": timeout
                })

            return {"success": False, "error": error_msg}

        except Exception as e:
            error_msg = f"Command execution failed: {str(e)}"
            logger.error(error_msg)

            if self._event_bus:
                await self._event_bus.publish("ssh.command.error", {
                    "tunnel_id": tunnel_id,
                    "command": command,
                    "error": str(e)
                })

            return {"success": False, "error": error_msg}
