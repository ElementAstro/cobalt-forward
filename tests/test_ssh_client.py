"""
Tests for SSH client functionality.

This module tests the SSH client implementation and its integration
with the dependency injection container.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any

from cobalt_forward.application.container import Container
from cobalt_forward.core.interfaces.clients import ISSHClient, ClientStatus
from cobalt_forward.core.interfaces.messaging import IEventBus
from cobalt_forward.infrastructure.clients.ssh import SSHClient, SSHClientConfig


class TestSSHClientConfiguration:
    """Test SSH client configuration."""
    
    def test_ssh_config_creation(self):
        """Test SSH configuration creation."""
        config = SSHClientConfig(
            host="localhost",
            port=22,
            username="test",
            password="test123"
        )
        
        assert config.host == "localhost"
        assert config.port == 22
        assert config.username == "test"
        assert config.password == "test123"
        assert config.compression is True
        assert config.pool_size == 5
    
    def test_ssh_config_validation(self):
        """Test SSH configuration validation."""
        # Missing username should raise error
        with pytest.raises(ValueError, match="Username is required"):
            SSHClientConfig(
                host="localhost",
                port=22,
                username="",
                password="test"
            )
        
        # Missing authentication should raise error
        with pytest.raises(ValueError, match="Either password, key_file, or client_keys"):
            SSHClientConfig(
                host="localhost",
                port=22,
                username="test"
            )
    
    def test_ssh_config_to_asyncssh_kwargs(self):
        """Test conversion to asyncssh kwargs."""
        config = SSHClientConfig(
            host="localhost",
            port=22,
            username="test",
            password="test123",
            compression=True,
            known_hosts_file="/path/to/known_hosts"
        )
        
        kwargs = config.to_asyncssh_kwargs()
        
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 22
        assert kwargs["username"] == "test"
        assert kwargs["password"] == "test123"
        assert kwargs["compression_algs"] == ['zlib@openssh.com', 'zlib']
        assert kwargs["known_hosts"] == "/path/to/known_hosts"


class TestSSHClientIntegration:
    """Test SSH client integration with DI container."""
    
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
    def ssh_config(self) -> SSHClientConfig:
        """Create SSH configuration for testing."""
        return SSHClientConfig(
            host="localhost",
            port=22,
            username="test",
            password="test123",
            timeout=5.0,
            retry_attempts=1  # Reduce for faster tests
        )
    
    @pytest.fixture
    def ssh_client(self, ssh_config: SSHClientConfig, mock_event_bus: Mock) -> SSHClient:
        """Create SSH client with dependencies."""
        return SSHClient(
            config=ssh_config,
            event_bus=mock_event_bus,
            name="test_ssh_client"
        )
    
    async def test_ssh_client_creation(self, ssh_client: SSHClient):
        """Test SSH client can be created."""
        assert ssh_client is not None
        assert isinstance(ssh_client, ISSHClient)
        assert ssh_client.get_status() == ClientStatus.DISCONNECTED
    
    async def test_ssh_client_lifecycle(self, ssh_client: SSHClient):
        """Test SSH client lifecycle methods."""
        # Test start
        await ssh_client.start()
        
        # Test health check
        health = await ssh_client.health_check()
        assert health["healthy"] is False  # Not connected yet
        assert health["status"] == ClientStatus.DISCONNECTED.value
        
        # Test stop
        await ssh_client.stop()
    
    async def test_ssh_client_metrics(self, ssh_client: SSHClient):
        """Test SSH client metrics collection."""
        await ssh_client.start()
        
        metrics = ssh_client.get_metrics()
        assert metrics.total_requests == 0
        assert metrics.success_rate == 100.0  # No requests yet
        assert metrics.connection_count == 0
        
        await ssh_client.stop()
    
    @patch('cobalt_forward.infrastructure.clients.ssh.client.asyncssh')
    async def test_ssh_connection_mock(self, mock_asyncssh, ssh_client: SSHClient):
        """Test SSH connection with mocked asyncssh."""
        # Mock successful connection
        mock_connection = AsyncMock()
        mock_connection.is_closing.return_value = False
        mock_asyncssh.connect = AsyncMock(return_value=mock_connection)
        
        await ssh_client.start()
        
        # Test connection
        result = await ssh_client.connect()
        assert result is True
        assert ssh_client.get_status() == ClientStatus.CONNECTED
        
        # Test disconnect
        await ssh_client.disconnect()
        assert ssh_client.get_status() == ClientStatus.DISCONNECTED
        
        await ssh_client.stop()
    
    @patch('cobalt_forward.infrastructure.clients.ssh.client.asyncssh')
    async def test_ssh_command_execution_mock(self, mock_asyncssh, ssh_client: SSHClient):
        """Test SSH command execution with mocked asyncssh."""
        # Mock connection and command result
        mock_connection = AsyncMock()
        mock_connection.is_closing.return_value = False
        
        mock_result = Mock()
        mock_result.exit_status = 0
        mock_result.stdout = "test output"
        mock_result.stderr = ""
        
        mock_connection.run = AsyncMock(return_value=mock_result)
        mock_asyncssh.connect = AsyncMock(return_value=mock_connection)
        
        await ssh_client.start()
        await ssh_client.connect()
        
        # Test command execution
        result = await ssh_client.execute_command("echo test")
        
        assert result["success"] is True
        assert result["exit_status"] == 0
        assert result["stdout"] == "test output"
        assert "execution_time" in result
        
        await ssh_client.disconnect()
        await ssh_client.stop()
    
    async def test_container_integration(self, container: Container, ssh_config: SSHClientConfig):
        """Test SSH client registration in container."""
        # Register SSH client
        container.register(ISSHClient, lambda: SSHClient(ssh_config))
        
        # Resolve SSH client
        client = container.resolve(ISSHClient)
        assert client is not None
        assert isinstance(client, SSHClient)
        
        # Test it's a singleton (if registered as such)
        client2 = container.resolve(ISSHClient)
        # Note: This will be different instances since we used lambda
        # In real usage, we'd register as singleton


class TestSSHClientErrorHandling:
    """Test SSH client error handling."""
    
    @pytest.fixture
    def ssh_config(self) -> SSHClientConfig:
        """Create SSH configuration for testing."""
        return SSHClientConfig(
            host="nonexistent.host",
            port=22,
            username="test",
            password="test123",
            timeout=1.0,  # Short timeout for faster tests
            retry_attempts=1
        )
    
    @pytest.fixture
    def ssh_client(self, ssh_config: SSHClientConfig) -> SSHClient:
        """Create SSH client for error testing."""
        return SSHClient(config=ssh_config, name="error_test_client")
    
    async def test_connection_failure(self, ssh_client: SSHClient):
        """Test SSH connection failure handling."""
        await ssh_client.start()
        
        # This should fail due to nonexistent host
        result = await ssh_client.connect()
        assert result is False
        assert ssh_client.get_status() == ClientStatus.ERROR
        
        # Check metrics recorded the error
        metrics = ssh_client.get_metrics()
        assert metrics.error_count > 0
        assert metrics.last_error is not None
        
        await ssh_client.stop()
    
    async def test_command_execution_without_connection(self, ssh_client: SSHClient):
        """Test command execution without connection."""
        await ssh_client.start()
        
        # Try to execute command without connecting
        result = await ssh_client.execute_command("echo test")
        
        assert result["success"] is False
        assert "Not connected" in result["error"]
        
        await ssh_client.stop()


class TestSSHClientCallbacks:
    """Test SSH client callback functionality."""
    
    @pytest.fixture
    def ssh_client(self) -> SSHClient:
        """Create SSH client for callback testing."""
        config = SSHClientConfig(
            host="localhost",
            port=22,
            username="test",
            password="test123"
        )
        return SSHClient(config=config, name="callback_test_client")
    
    async def test_callback_registration(self, ssh_client: SSHClient):
        """Test callback registration and removal."""
        callback_called = False
        
        def test_callback(*args, **kwargs):
            nonlocal callback_called
            callback_called = True
        
        # Add callback
        ssh_client.add_callback("test_event", test_callback)
        
        # Trigger callback
        await ssh_client._trigger_callback("test_event")
        assert callback_called is True
        
        # Remove callback
        ssh_client.remove_callback("test_event", test_callback)
        
        # Reset and trigger again
        callback_called = False
        await ssh_client._trigger_callback("test_event")
        assert callback_called is False  # Should not be called after removal
