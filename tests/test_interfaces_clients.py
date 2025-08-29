"""
Tests for client interfaces.

This module tests the client interfaces including IBaseClient and all
protocol-specific client interfaces.
"""

import pytest
from typing import Any, Dict, List, Optional, Callable, Union, Tuple
from unittest.mock import AsyncMock, Mock

from cobalt_forward.core.interfaces.clients import (
    IBaseClient, ISSHClient, IFTPClient, IMQTTClient, ITCPClient, 
    IUDPClient, IWebSocketClient, ClientStatus
)
from cobalt_forward.core.interfaces.lifecycle import IStartable, IStoppable, IHealthCheckable


class TestClientStatus:
    """Test cases for ClientStatus enum."""
    
    def test_client_statuses_exist(self) -> None:
        """Test that all expected client statuses exist."""
        expected_statuses = {
            'DISCONNECTED', 'CONNECTING', 'CONNECTED', 'RECONNECTING', 'ERROR'
        }
        actual_statuses = {cs.name for cs in ClientStatus}
        assert actual_statuses == expected_statuses
    
    def test_client_status_values(self) -> None:
        """Test client status values."""
        assert ClientStatus.DISCONNECTED.value == "disconnected"
        assert ClientStatus.CONNECTING.value == "connecting"
        assert ClientStatus.CONNECTED.value == "connected"
        assert ClientStatus.RECONNECTING.value == "reconnecting"
        assert ClientStatus.ERROR.value == "error"


class MockBaseClient(IBaseClient):
    """Mock implementation of IBaseClient for testing."""
    
    def __init__(self):
        self._connected = False
        self._status = ClientStatus.DISCONNECTED
        self._started = False
        self.received_data = []
        self.sent_data = []
        
        # Call counters
        self.connect_called_count = 0
        self.disconnect_called_count = 0
        self.send_called_count = 0
        self.receive_called_count = 0
        self.is_connected_called_count = 0
        self.get_status_called_count = 0
        self.start_called_count = 0
        self.stop_called_count = 0
        self.check_health_called_count = 0
        
        # Configuration
        self.should_fail_connect = False
        self.should_fail_disconnect = False
        self.should_fail_send = False
        self.should_fail_start = False
        self.should_fail_stop = False
        self.should_fail_health_check = False
        self.data_to_receive = []
    
    async def connect(self) -> bool:
        """Establish connection to the server."""
        self.connect_called_count += 1
        if self.should_fail_connect:
            self._status = ClientStatus.ERROR
            return False
        
        self._status = ClientStatus.CONNECTING
        # Simulate connection process
        self._connected = True
        self._status = ClientStatus.CONNECTED
        return True
    
    async def disconnect(self) -> None:
        """Disconnect from the server."""
        self.disconnect_called_count += 1
        if self.should_fail_disconnect:
            raise RuntimeError("Mock disconnect failure")
        
        self._connected = False
        self._status = ClientStatus.DISCONNECTED
    
    async def send(self, data: Any) -> bool:
        """Send data to the server."""
        self.send_called_count += 1
        if self.should_fail_send:
            return False
        
        if not self._connected:
            return False
        
        self.sent_data.append(data)
        return True
    
    async def receive(self) -> Optional[Any]:
        """Receive data from the server."""
        self.receive_called_count += 1
        if not self._connected:
            return None
        
        if self.data_to_receive:
            return self.data_to_receive.pop(0)
        return None
    
    def is_connected(self) -> bool:
        """Check if client is connected."""
        self.is_connected_called_count += 1
        return self._connected
    
    def get_status(self) -> ClientStatus:
        """Get current client status."""
        self.get_status_called_count += 1
        return self._status
    
    async def start(self) -> None:
        """Start the client."""
        self.start_called_count += 1
        if self.should_fail_start:
            raise RuntimeError("Mock start failure")
        self._started = True
    
    async def stop(self) -> None:
        """Stop the client."""
        self.stop_called_count += 1
        if self.should_fail_stop:
            raise RuntimeError("Mock stop failure")
        
        if self._connected:
            await self.disconnect()
        self._started = False
    
    async def check_health(self) -> Dict[str, Any]:
        """Check client health."""
        self.check_health_called_count += 1
        if self.should_fail_health_check:
            raise RuntimeError("Mock health check failure")
        
        return {
            'healthy': self._connected,
            'status': self._status.value,
            'details': {
                'connected': self._connected,
                'started': self._started,
                'sent_messages': len(self.sent_data),
                'received_messages': len(self.received_data)
            }
        }


class MockSSHClient(ISSHClient):
    """Mock implementation of ISSHClient for testing."""
    
    def __init__(self):
        self.base_client = MockBaseClient()
        self.executed_commands = []
        self.uploaded_files = []
        self.downloaded_files = []
        
        # Call counters
        self.execute_command_called_count = 0
        self.upload_file_called_count = 0
        self.download_file_called_count = 0
        
        # Configuration
        self.should_fail_execute = False
        self.should_fail_upload = False
        self.should_fail_download = False
    
    # Delegate base client methods
    async def connect(self) -> bool:
        return await self.base_client.connect()
    
    async def disconnect(self) -> None:
        await self.base_client.disconnect()
    
    async def send(self, data: Any) -> bool:
        return await self.base_client.send(data)
    
    async def receive(self) -> Optional[Any]:
        return await self.base_client.receive()
    
    def is_connected(self) -> bool:
        return self.base_client.is_connected()
    
    def get_status(self) -> ClientStatus:
        return self.base_client.get_status()
    
    async def start(self) -> None:
        await self.base_client.start()
    
    async def stop(self) -> None:
        await self.base_client.stop()
    
    async def check_health(self) -> Dict[str, Any]:
        return await self.base_client.check_health()
    
    # SSH-specific methods
    async def execute_command(
        self,
        command: str,
        timeout: Optional[float] = None,
        environment: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Execute a command on the remote server."""
        self.execute_command_called_count += 1
        if self.should_fail_execute:
            return {
                "success": False,
                "exit_code": 1,
                "stdout": "",
                "stderr": "Mock command execution failure",
                "execution_time": 0.1
            }
        
        self.executed_commands.append({
            "command": command,
            "timeout": timeout,
            "environment": environment
        })
        
        return {
            "success": True,
            "exit_code": 0,
            "stdout": f"Mock output for: {command}",
            "stderr": "",
            "execution_time": 0.5
        }
    
    async def upload_file(
        self,
        local_path: str,
        remote_path: str,
        callback: Optional[Callable[[str, str, float], None]] = None
    ) -> None:
        """Upload a file to the remote server."""
        self.upload_file_called_count += 1
        if self.should_fail_upload:
            raise RuntimeError("Mock file upload failure")
        
        self.uploaded_files.append({
            "local_path": local_path,
            "remote_path": remote_path
        })
        
        if callback:
            callback(local_path, remote_path, 100.0)  # 100% progress
    
    async def download_file(
        self,
        remote_path: str,
        local_path: str,
        callback: Optional[Callable[[str, str, float], None]] = None
    ) -> None:
        """Download a file from the remote server."""
        self.download_file_called_count += 1
        if self.should_fail_download:
            raise RuntimeError("Mock file download failure")
        
        self.downloaded_files.append({
            "remote_path": remote_path,
            "local_path": local_path
        })
        
        if callback:
            callback(remote_path, local_path, 100.0)  # 100% progress


class MockMQTTClient(IMQTTClient):
    """Mock implementation of IMQTTClient for testing."""
    
    def __init__(self):
        self.base_client = MockBaseClient()
        self.subscriptions = {}
        self.published_messages = []
        
        # Call counters
        self.subscribe_called_count = 0
        self.unsubscribe_called_count = 0
        self.publish_called_count = 0
        
        # Configuration
        self.should_fail_subscribe = False
        self.should_fail_unsubscribe = False
        self.should_fail_publish = False
    
    # Delegate base client methods
    async def connect(self) -> bool:
        return await self.base_client.connect()
    
    async def disconnect(self) -> None:
        await self.base_client.disconnect()
    
    async def send(self, data: Any) -> bool:
        return await self.base_client.send(data)
    
    async def receive(self) -> Optional[Any]:
        return await self.base_client.receive()
    
    def is_connected(self) -> bool:
        return self.base_client.is_connected()
    
    def get_status(self) -> ClientStatus:
        return self.base_client.get_status()
    
    async def start(self) -> None:
        await self.base_client.start()
    
    async def stop(self) -> None:
        await self.base_client.stop()
    
    async def check_health(self) -> Dict[str, Any]:
        return await self.base_client.check_health()
    
    # MQTT-specific methods
    async def subscribe(
        self,
        topic: str,
        qos: int = 0,
        callback: Optional[Callable[[str, bytes], None]] = None
    ) -> bool:
        """Subscribe to a topic."""
        self.subscribe_called_count += 1
        if self.should_fail_subscribe:
            return False
        
        if not self.is_connected():
            return False
        
        self.subscriptions[topic] = {
            "qos": qos,
            "callback": callback
        }
        return True
    
    async def unsubscribe(self, topic: str) -> bool:
        """Unsubscribe from a topic."""
        self.unsubscribe_called_count += 1
        if self.should_fail_unsubscribe:
            return False
        
        if topic in self.subscriptions:
            del self.subscriptions[topic]
            return True
        return False
    
    async def publish(
        self,
        topic: str,
        payload: Union[str, bytes],
        qos: int = 0,
        retain: bool = False
    ) -> bool:
        """Publish a message to a topic."""
        self.publish_called_count += 1
        if self.should_fail_publish:
            return False
        
        if not self.is_connected():
            return False
        
        self.published_messages.append({
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain
        })
        return True
