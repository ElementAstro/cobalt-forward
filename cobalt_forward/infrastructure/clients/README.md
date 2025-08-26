# Client Implementations

This directory contains client implementations for various protocols used by the Cobalt Forward application.

## Completed Implementations

### SSH Client (`ssh/`)
- **Status**: ✅ Complete
- **Features**: 
  - Connection pooling
  - Command execution
  - File transfer (upload/download)
  - Port forwarding (local/remote)
  - SFTP support
  - Event bus integration
  - Comprehensive error handling

## Planned Implementations

### FTP Client (`ftp/`)
- **Status**: 🔄 Pending Migration
- **Source**: `app/client/ftp/`
- **Features to Migrate**:
  - Secure FTP connections
  - Resumable transfers
  - Directory operations
  - Plugin system integration
  - Progress monitoring

### MQTT Client (`mqtt/`)
- **Status**: 🔄 Pending Migration
- **Source**: `app/client/mqtt/`
- **Features to Migrate**:
  - Publish/Subscribe functionality
  - QoS support
  - Connection management
  - Message queuing
  - Event-driven architecture

### TCP Client (`tcp/`)
- **Status**: 🔄 Pending Migration
- **Source**: `app/client/tcp/`
- **Features to Migrate**:
  - Raw TCP connections
  - Stream handling
  - Connection pooling
  - Binary data support

### UDP Client (`udp/`)
- **Status**: 🔄 Pending Migration
- **Source**: `app/client/udp/`
- **Features to Migrate**:
  - Datagram operations
  - Broadcast support
  - Multicast support
  - Connection-less communication

### WebSocket Client (`websocket/`)
- **Status**: 🔄 Pending Migration
- **Source**: `app/client/websocket/`
- **Features to Migrate**:
  - WebSocket connections
  - Text/Binary messaging
  - Ping/Pong handling
  - Auto-reconnection
  - Subprotocol support

## Implementation Guidelines

### Directory Structure
Each client implementation should follow this structure:
```
protocol_name/
├── __init__.py          # Package exports
├── client.py            # Main client implementation
├── config.py            # Configuration classes
└── README.md            # Protocol-specific documentation
```

### Base Class Usage
All clients should inherit from `BaseClient` which provides:
- Common lifecycle management
- Metrics collection
- Event bus integration
- Error handling
- Retry logic
- Connection management

### Interface Implementation
Each client must implement the appropriate interface from `core.interfaces.clients`:
- `IBaseClient` (required for all)
- Protocol-specific interfaces (e.g., `IFTPClient`, `IMQTTClient`)

### Configuration
Each client should have a configuration class that extends `ClientConfig`:
```python
@dataclass
class ProtocolClientConfig(ClientConfig):
    # Protocol-specific settings
    protocol_setting: str = "default"
    # ... other settings
```

### Event Publishing
Clients should publish events for important operations:
- Connection/disconnection
- Data transfer progress
- Errors and warnings
- Performance metrics

### Testing
Each client should include comprehensive tests:
- Unit tests for individual methods
- Integration tests with DI container
- Mock tests for external dependencies
- Performance tests for critical operations

## Migration Priority

1. **High Priority**: SSH Client ✅ (Complete)
2. **Medium Priority**: 
   - FTP Client (file transfer capabilities)
   - WebSocket Client (real-time communication)
3. **Low Priority**:
   - MQTT Client (message queuing)
   - TCP/UDP Clients (raw networking)

## Usage Example

```python
from cobalt_forward.infrastructure.clients.ssh import SSHClient, SSHClientConfig
from cobalt_forward.core.interfaces.clients import ISSHClient

# Create configuration
config = SSHClientConfig(
    host="example.com",
    port=22,
    username="user",
    password="password"
)

# Create client
ssh_client = SSHClient(config)

# Use through dependency injection
container.register(ISSHClient, lambda: ssh_client)
client = container.resolve(ISSHClient)

# Connect and use
await client.connect()
result = await client.execute_command("ls -la")
await client.disconnect()
```

## Notes

- All clients use async/await for non-blocking operations
- Connection pooling is implemented where beneficial
- Comprehensive error handling and logging
- Integration with the application's event bus
- Metrics collection for monitoring and debugging
- Configuration-driven behavior
- Testable design with dependency injection
