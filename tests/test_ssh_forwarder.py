"""
Tests for SSH forwarder functionality.

This module tests the SSH forwarder service integration
with the dependency injection container.
"""

import pytest
from unittest.mock import Mock, AsyncMock
from typing import Dict, Any

from cobalt_forward.application.container import Container
from cobalt_forward.application.startup import ApplicationStartup
from cobalt_forward.core.interfaces.ssh import ISSHForwarder, ForwardType, ForwardConfig
from cobalt_forward.core.interfaces.messaging import IEventBus
from cobalt_forward.infrastructure.services.ssh.forwarder import SSHForwarder


class TestSSHForwarderIntegration:
    """Test SSH forwarder integration with DI container."""
    
    @pytest.fixture
    def container(self) -> Container:
        """Create a test container."""
        return Container()
    
    @pytest.fixture
    def mock_event_bus(self) -> Mock:
        """Create a mock event bus."""
        mock_bus = Mock(spec=IEventBus)
        mock_bus.publish = AsyncMock()
        return mock_bus
    
    @pytest.fixture
    async def ssh_forwarder(self, container: Container, mock_event_bus: Mock) -> SSHForwarder:
        """Create SSH forwarder with dependencies."""
        # Register event bus
        container.register_instance(IEventBus, mock_event_bus)
        
        # Create SSH forwarder
        forwarder = SSHForwarder(
            event_bus=mock_event_bus,
            reconnect_interval=1,
            max_reconnect_attempts=3
        )
        
        return forwarder
    
    async def test_ssh_forwarder_creation(self, ssh_forwarder: SSHForwarder):
        """Test SSH forwarder can be created."""
        assert ssh_forwarder is not None
        assert isinstance(ssh_forwarder, ISSHForwarder)
    
    async def test_ssh_forwarder_lifecycle(self, ssh_forwarder: SSHForwarder):
        """Test SSH forwarder lifecycle methods."""
        # Test start
        await ssh_forwarder.start()
        
        # Test health check
        health = await ssh_forwarder.health_check()
        assert health["healthy"] is True
        assert health["tunnels_total"] == 0
        
        # Test stop
        await ssh_forwarder.stop()
    
    async def test_tunnel_creation(self, ssh_forwarder: SSHForwarder):
        """Test tunnel creation without actual SSH connection."""
        await ssh_forwarder.start()
        
        # Create tunnel (will fail to connect but should create the tunnel object)
        tunnel_id = await ssh_forwarder.create_tunnel(
            host="localhost",
            port=22,
            username="test",
            password="test",
            auto_connect=False  # Don't auto-connect to avoid actual SSH
        )
        
        assert tunnel_id is not None
        assert len(tunnel_id) > 0
        
        # Check tunnel info
        tunnel_info = ssh_forwarder.get_tunnel_info(tunnel_id)
        assert tunnel_info is not None
        assert tunnel_info.host == "localhost"
        assert tunnel_info.port == 22
        assert tunnel_info.username == "test"
        
        # List tunnels
        tunnels = ssh_forwarder.list_tunnels()
        assert len(tunnels) == 1
        assert tunnels[0].tunnel_id == tunnel_id
        
        await ssh_forwarder.stop()
    
    async def test_forward_configuration(self, ssh_forwarder: SSHForwarder):
        """Test port forward configuration."""
        await ssh_forwarder.start()
        
        # Create tunnel
        tunnel_id = await ssh_forwarder.create_tunnel(
            host="localhost",
            port=22,
            username="test",
            auto_connect=False
        )
        
        # Add port forward
        forward_config = ForwardConfig(
            forward_type=ForwardType.LOCAL,
            listen_host="127.0.0.1",
            listen_port=8080,
            dest_host="localhost",
            dest_port=80,
            description="Test forward"
        )
        
        forward_index = await ssh_forwarder.add_forward(tunnel_id, forward_config)
        assert forward_index >= 0
        
        # Check tunnel has the forward
        tunnel_info = ssh_forwarder.get_tunnel_info(tunnel_id)
        assert tunnel_info is not None
        assert len(tunnel_info.forwards) == 1
        assert tunnel_info.forwards[0].forward_type == ForwardType.LOCAL
        
        await ssh_forwarder.stop()
    
    async def test_container_integration(self, container: Container):
        """Test SSH forwarder registration in container."""
        # Register SSH forwarder
        container.register(ISSHForwarder, SSHForwarder)
        
        # Resolve SSH forwarder
        forwarder = container.resolve(ISSHForwarder)
        assert forwarder is not None
        assert isinstance(forwarder, SSHForwarder)
        
        # Test it's a singleton
        forwarder2 = container.resolve(ISSHForwarder)
        assert forwarder is forwarder2


class TestApplicationStartupWithSSH:
    """Test application startup with SSH services."""
    
    @pytest.fixture
    def container(self) -> Container:
        """Create a test container."""
        return Container()
    
    @pytest.fixture
    def startup(self, container: Container) -> ApplicationStartup:
        """Create application startup."""
        return ApplicationStartup(container)
    
    async def test_ssh_service_registration(self, startup: ApplicationStartup):
        """Test SSH services are registered during startup."""
        # Configure services
        config = {
            "name": "test-app",
            "version": "1.0.0",
            "logging": {"level": "INFO"},
            "ssh": {
                "enabled": True,
                "reconnect_interval": 5
            }
        }
        
        await startup.configure_services(config)
        
        # Check SSH forwarder is registered
        forwarder = startup._container.resolve(ISSHForwarder)
        assert forwarder is not None
        assert isinstance(forwarder, SSHForwarder)
    
    async def test_ssh_service_startup(self, startup: ApplicationStartup):
        """Test SSH services start correctly."""
        # Configure services
        config = {
            "name": "test-app",
            "version": "1.0.0",
            "logging": {"level": "INFO"}
        }
        
        await startup.configure_services(config)
        
        # Start application (this should start SSH forwarder)
        await startup.start_application()
        
        # Check SSH forwarder is running
        forwarder = startup._container.resolve(ISSHForwarder)
        health = await forwarder.health_check()
        assert health["healthy"] is True
        
        # Stop application
        await startup.stop_application()
