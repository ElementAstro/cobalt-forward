"""
Tests for SSH interfaces.

This module tests the SSH interfaces including ISSHForwarder, ISSHTunnel,
and related data classes and enums.
"""

import pytest
import time
from typing import Any, Dict, List, Optional, Callable
from unittest.mock import AsyncMock, Mock

from cobalt_forward.core.interfaces.ssh import (
    ISSHForwarder, ISSHTunnel, ForwardType, TunnelStatus, 
    ForwardConfig, TunnelInfo
)
from cobalt_forward.core.interfaces.lifecycle import IStartable, IStoppable, IHealthCheckable


class TestForwardType:
    """Test cases for ForwardType enum."""
    
    def test_forward_types_exist(self) -> None:
        """Test that all expected forward types exist."""
        expected_types = {'LOCAL', 'REMOTE', 'DYNAMIC'}
        actual_types = {ft.name for ft in ForwardType}
        assert actual_types == expected_types
    
    def test_forward_type_values(self) -> None:
        """Test forward type values."""
        assert ForwardType.LOCAL.value == "local"
        assert ForwardType.REMOTE.value == "remote"
        assert ForwardType.DYNAMIC.value == "dynamic"


class TestTunnelStatus:
    """Test cases for TunnelStatus enum."""
    
    def test_tunnel_statuses_exist(self) -> None:
        """Test that all expected tunnel statuses exist."""
        expected_statuses = {
            'DISCONNECTED', 'CONNECTING', 'CONNECTED', 'RECONNECTING', 'ERROR'
        }
        actual_statuses = {ts.name for ts in TunnelStatus}
        assert actual_statuses == expected_statuses
    
    def test_tunnel_status_values(self) -> None:
        """Test tunnel status values."""
        assert TunnelStatus.DISCONNECTED.value == "disconnected"
        assert TunnelStatus.CONNECTING.value == "connecting"
        assert TunnelStatus.CONNECTED.value == "connected"
        assert TunnelStatus.RECONNECTING.value == "reconnecting"
        assert TunnelStatus.ERROR.value == "error"


class TestForwardConfig:
    """Test cases for ForwardConfig dataclass."""
    
    def test_forward_config_creation_minimal(self) -> None:
        """Test creating ForwardConfig with minimal fields."""
        config = ForwardConfig(
            forward_type=ForwardType.LOCAL,
            listen_host="localhost",
            listen_port=8080,
            dest_host="remote.example.com",
            dest_port=80
        )
        
        assert config.forward_type == ForwardType.LOCAL
        assert config.listen_host == "localhost"
        assert config.listen_port == 8080
        assert config.dest_host == "remote.example.com"
        assert config.dest_port == 80
        assert config.description is None
    
    def test_forward_config_creation_full(self) -> None:
        """Test creating ForwardConfig with all fields."""
        config = ForwardConfig(
            forward_type=ForwardType.REMOTE,
            listen_host="0.0.0.0",
            listen_port=9090,
            dest_host="internal.server.com",
            dest_port=3306,
            description="MySQL database tunnel"
        )
        
        assert config.forward_type == ForwardType.REMOTE
        assert config.listen_host == "0.0.0.0"
        assert config.listen_port == 9090
        assert config.dest_host == "internal.server.com"
        assert config.dest_port == 3306
        assert config.description == "MySQL database tunnel"
    
    def test_forward_config_dynamic_type(self) -> None:
        """Test ForwardConfig with dynamic forwarding."""
        config = ForwardConfig(
            forward_type=ForwardType.DYNAMIC,
            listen_host="127.0.0.1",
            listen_port=1080,
            dest_host="",  # Not used for dynamic forwarding
            dest_port=0,   # Not used for dynamic forwarding
            description="SOCKS proxy"
        )
        
        assert config.forward_type == ForwardType.DYNAMIC
        assert config.description == "SOCKS proxy"


class TestTunnelInfo:
    """Test cases for TunnelInfo dataclass."""
    
    @pytest.fixture
    def sample_forwards(self) -> List[ForwardConfig]:
        """Create sample forward configurations."""
        return [
            ForwardConfig(
                forward_type=ForwardType.LOCAL,
                listen_host="localhost",
                listen_port=8080,
                dest_host="web.server.com",
                dest_port=80
            ),
            ForwardConfig(
                forward_type=ForwardType.REMOTE,
                listen_host="0.0.0.0",
                listen_port=9090,
                dest_host="db.server.com",
                dest_port=3306
            )
        ]
    
    def test_tunnel_info_creation_minimal(self, sample_forwards: List[ForwardConfig]) -> None:
        """Test creating TunnelInfo with minimal fields."""
        created_time = time.time()
        
        info = TunnelInfo(
            tunnel_id="tunnel-123",
            host="ssh.example.com",
            port=22,
            username="testuser",
            status=TunnelStatus.DISCONNECTED,
            forwards=sample_forwards,
            created_at=created_time
        )
        
        assert info.tunnel_id == "tunnel-123"
        assert info.host == "ssh.example.com"
        assert info.port == 22
        assert info.username == "testuser"
        assert info.status == TunnelStatus.DISCONNECTED
        assert info.forwards == sample_forwards
        assert info.created_at == created_time
        assert info.last_connected is None
        assert info.last_disconnected is None
        assert info.connection_count == 0
        assert info.error_count == 0
        assert info.last_error is None
        assert info.metadata == {}  # Should be initialized by __post_init__
    
    def test_tunnel_info_creation_full(self, sample_forwards: List[ForwardConfig]) -> None:
        """Test creating TunnelInfo with all fields."""
        created_time = time.time()
        connected_time = created_time + 10
        disconnected_time = created_time + 100
        metadata = {"client_version": "1.0", "keepalive": 60}
        
        info = TunnelInfo(
            tunnel_id="tunnel-456",
            host="secure.example.com",
            port=2222,
            username="admin",
            status=TunnelStatus.CONNECTED,
            forwards=sample_forwards,
            created_at=created_time,
            last_connected=connected_time,
            last_disconnected=disconnected_time,
            connection_count=5,
            error_count=2,
            last_error="Connection timeout",
            metadata=metadata
        )
        
        assert info.tunnel_id == "tunnel-456"
        assert info.host == "secure.example.com"
        assert info.port == 2222
        assert info.username == "admin"
        assert info.status == TunnelStatus.CONNECTED
        assert info.forwards == sample_forwards
        assert info.created_at == created_time
        assert info.last_connected == connected_time
        assert info.last_disconnected == disconnected_time
        assert info.connection_count == 5
        assert info.error_count == 2
        assert info.last_error == "Connection timeout"
        assert info.metadata == metadata
    
    def test_tunnel_info_post_init_metadata(self) -> None:
        """Test that __post_init__ initializes metadata if None."""
        info = TunnelInfo(
            tunnel_id="tunnel-789",
            host="test.example.com",
            port=22,
            username="user",
            status=TunnelStatus.DISCONNECTED,
            forwards=[],
            created_at=time.time(),
            metadata=None
        )
        
        assert info.metadata == {}


class MockSSHTunnel(ISSHTunnel):
    """Mock implementation of ISSHTunnel for testing."""
    
    def __init__(self, tunnel_info: TunnelInfo):
        self._info = tunnel_info
        self._connected = False
        
        # Call counters
        self.connect_called_count = 0
        self.disconnect_called_count = 0
        self.add_forward_called_count = 0
        self.remove_forward_called_count = 0
        self.get_info_called_count = 0
        self.is_connected_called_count = 0
        
        # Configuration
        self.should_fail_connect = False
        self.should_fail_disconnect = False
        self.should_fail_add_forward = False
        self.should_fail_remove_forward = False
    
    async def connect(self) -> bool:
        """Connect the SSH tunnel."""
        self.connect_called_count += 1
        if self.should_fail_connect:
            self._info.status = TunnelStatus.ERROR
            self._info.error_count += 1
            self._info.last_error = "Mock connection failure"
            return False
        
        self._connected = True
        self._info.status = TunnelStatus.CONNECTED
        self._info.last_connected = time.time()
        self._info.connection_count += 1
        return True
    
    async def disconnect(self) -> None:
        """Disconnect the SSH tunnel."""
        self.disconnect_called_count += 1
        if self.should_fail_disconnect:
            raise RuntimeError("Mock disconnect failure")
        
        self._connected = False
        self._info.status = TunnelStatus.DISCONNECTED
        self._info.last_disconnected = time.time()
    
    async def add_forward(self, forward: ForwardConfig) -> int:
        """Add a port forward to the tunnel."""
        self.add_forward_called_count += 1
        if self.should_fail_add_forward:
            return -1
        
        self._info.forwards.append(forward)
        return len(self._info.forwards) - 1
    
    async def remove_forward(self, forward_index: int) -> bool:
        """Remove a port forward from the tunnel."""
        self.remove_forward_called_count += 1
        if self.should_fail_remove_forward:
            return False
        
        if 0 <= forward_index < len(self._info.forwards):
            del self._info.forwards[forward_index]
            return True
        return False
    
    def get_info(self) -> TunnelInfo:
        """Get tunnel information."""
        self.get_info_called_count += 1
        return self._info
    
    def is_connected(self) -> bool:
        """Check if tunnel is connected."""
        self.is_connected_called_count += 1
        return self._connected


class MockSSHForwarder(ISSHForwarder):
    """Mock implementation of ISSHForwarder for testing."""
    
    def __init__(self):
        self.tunnels: Dict[str, MockSSHTunnel] = {}
        self.tunnel_counter = 0
        self._started = False
        self._health_status = True
        
        # Call counters
        self.create_tunnel_called_count = 0
        self.connect_tunnel_called_count = 0
        self.disconnect_tunnel_called_count = 0
        self.remove_tunnel_called_count = 0
        self.add_forward_called_count = 0
        self.remove_forward_called_count = 0
        self.get_tunnel_info_called_count = 0
        self.list_tunnels_called_count = 0
        self.upload_file_called_count = 0
        self.download_file_called_count = 0
        self.execute_command_called_count = 0
        self.start_called_count = 0
        self.stop_called_count = 0
        self.check_health_called_count = 0
        
        # Configuration
        self.should_fail_create_tunnel = False
        self.should_fail_connect_tunnel = False
        self.should_fail_disconnect_tunnel = False
        self.should_fail_remove_tunnel = False
        self.should_fail_add_forward = False
        self.should_fail_remove_forward = False
        self.should_fail_upload = False
        self.should_fail_download = False
        self.should_fail_execute = False
    
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
        self.create_tunnel_called_count += 1
        if self.should_fail_create_tunnel:
            raise RuntimeError("Mock tunnel creation failure")
        
        self.tunnel_counter += 1
        tunnel_id = f"tunnel-{self.tunnel_counter}"
        
        tunnel_info = TunnelInfo(
            tunnel_id=tunnel_id,
            host=host,
            port=port,
            username=username,
            status=TunnelStatus.DISCONNECTED,
            forwards=forwards or [],
            created_at=time.time(),
            metadata=metadata or {}
        )
        
        tunnel = MockSSHTunnel(tunnel_info)
        self.tunnels[tunnel_id] = tunnel
        
        if auto_connect:
            await self.connect_tunnel(tunnel_id)
        
        return tunnel_id

    async def connect_tunnel(self, tunnel_id: str) -> bool:
        """Connect an SSH tunnel."""
        self.connect_tunnel_called_count += 1
        if self.should_fail_connect_tunnel:
            return False

        tunnel = self.tunnels.get(tunnel_id)
        if not tunnel:
            return False

        return await tunnel.connect()

    async def disconnect_tunnel(self, tunnel_id: str) -> bool:
        """Disconnect an SSH tunnel."""
        self.disconnect_tunnel_called_count += 1
        if self.should_fail_disconnect_tunnel:
            return False

        tunnel = self.tunnels.get(tunnel_id)
        if not tunnel:
            return False

        await tunnel.disconnect()
        return True

    async def remove_tunnel(self, tunnel_id: str) -> bool:
        """Remove an SSH tunnel."""
        self.remove_tunnel_called_count += 1
        if self.should_fail_remove_tunnel:
            return False

        if tunnel_id in self.tunnels:
            await self.disconnect_tunnel(tunnel_id)
            del self.tunnels[tunnel_id]
            return True
        return False

    async def add_forward(self, tunnel_id: str, forward: ForwardConfig) -> int:
        """Add a port forward to a tunnel."""
        self.add_forward_called_count += 1
        if self.should_fail_add_forward:
            return -1

        tunnel = self.tunnels.get(tunnel_id)
        if not tunnel:
            return -1

        return await tunnel.add_forward(forward)

    async def remove_forward(self, tunnel_id: str, forward_index: int) -> bool:
        """Remove a port forward from a tunnel."""
        self.remove_forward_called_count += 1
        if self.should_fail_remove_forward:
            return False

        tunnel = self.tunnels.get(tunnel_id)
        if not tunnel:
            return False

        return await tunnel.remove_forward(forward_index)

    def get_tunnel_info(self, tunnel_id: str) -> Optional[TunnelInfo]:
        """Get information about a tunnel."""
        self.get_tunnel_info_called_count += 1
        tunnel = self.tunnels.get(tunnel_id)
        return tunnel.get_info() if tunnel else None

    def list_tunnels(self) -> List[TunnelInfo]:
        """List all tunnels."""
        self.list_tunnels_called_count += 1
        return [tunnel.get_info() for tunnel in self.tunnels.values()]

    async def upload_file(
        self,
        tunnel_id: str,
        local_path: str,
        remote_path: str,
        callback: Optional[Callable[..., Any]] = None
    ) -> Dict[str, Any]:
        """Upload a file through an SSH tunnel."""
        self.upload_file_called_count += 1
        if self.should_fail_upload:
            return {"success": False, "error": "Mock upload failure"}

        return {
            "success": True,
            "local_path": local_path,
            "remote_path": remote_path,
            "bytes_transferred": 1024,
            "duration": 1.5
        }

    async def download_file(
        self,
        tunnel_id: str,
        remote_path: str,
        local_path: str,
        callback: Optional[Callable[..., Any]] = None
    ) -> Dict[str, Any]:
        """Download a file through an SSH tunnel."""
        self.download_file_called_count += 1
        if self.should_fail_download:
            return {"success": False, "error": "Mock download failure"}

        return {
            "success": True,
            "remote_path": remote_path,
            "local_path": local_path,
            "bytes_transferred": 2048,
            "duration": 2.0
        }

    async def execute_command(
        self,
        tunnel_id: str,
        command: str,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """Execute a command through an SSH tunnel."""
        self.execute_command_called_count += 1
        if self.should_fail_execute:
            return {
                "success": False,
                "error": "Mock command execution failure",
                "exit_code": 1
            }

        return {
            "success": True,
            "command": command,
            "stdout": "Mock command output",
            "stderr": "",
            "exit_code": 0,
            "duration": 0.5
        }

    async def start(self) -> None:
        """Start the SSH forwarder."""
        self.start_called_count += 1
        self._started = True

    async def stop(self) -> None:
        """Stop the SSH forwarder."""
        self.stop_called_count += 1
        self._started = False

        # Disconnect all tunnels
        for tunnel_id in list(self.tunnels.keys()):
            await self.disconnect_tunnel(tunnel_id)

    async def check_health(self) -> Dict[str, Any]:
        """Check SSH forwarder health."""
        self.check_health_called_count += 1

        connected_tunnels = sum(1 for t in self.tunnels.values() if t.is_connected())

        return {
            'healthy': self._health_status,
            'status': 'running' if self._started else 'stopped',
            'details': {
                'total_tunnels': len(self.tunnels),
                'connected_tunnels': connected_tunnels,
                'disconnected_tunnels': len(self.tunnels) - connected_tunnels
            }
        }
