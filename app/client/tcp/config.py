from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Union
from ..base import BaseConfig


@dataclass
class TCPSocketOptions:
    """TCP socket options configuration"""
    nodelay: bool = True                # Disable Nagle's algorithm
    keepalive: bool = True              # Enable TCP keepalive mechanism
    keepalive_idle: int = 60            # Seconds idle before sending keepalive probes
    keepalive_interval: int = 10        # Interval between keepalive probes
    keepalive_count: int = 5            # Number of unacknowledged probes before failing
    reuseaddr: bool = True              # Allow port reuse
    recvbuf_size: int = 262144          # Receive buffer size (256KB)
    sendbuf_size: int = 262144          # Send buffer size (256KB)
    linger: Optional[int] = None        # Delayed closing
    defer_accept: bool = True           # Delay accepting connections

    def to_dict(self) -> Dict[str, Any]:
        """Convert socket options to dictionary"""
        return {
            "nodelay": self.nodelay,
            "keepalive": self.keepalive,
            "keepalive_idle": self.keepalive_idle,
            "keepalive_interval": self.keepalive_interval,
            "keepalive_count": self.keepalive_count,
            "reuseaddr": self.reuseaddr,
            "recvbuf_size": self.recvbuf_size,
            "sendbuf_size": self.sendbuf_size,
            "linger": self.linger,
            "defer_accept": self.defer_accept
        }


@dataclass
class RetryPolicy:
    """Retry policy configuration"""
    max_retries: int = 5                # Maximum retry attempts
    base_delay: float = 1.0             # Base delay (seconds)
    max_delay: float = 30.0             # Maximum delay (seconds)
    jitter: float = 0.1                 # Jitter factor (0-1)
    backoff_factor: float = 2.0         # Backoff factor

    def get_retry_delay(self, attempt: int) -> float:
        """Calculate delay for the nth attempt using exponential backoff
        
        Args:
            attempt: Attempt number (0-based)
            
        Returns:
            Delay in seconds
        """
        import random
        # Calculate exponential backoff delay
        delay = min(self.max_delay, self.base_delay *
                    (self.backoff_factor ** attempt))
        # Add random jitter
        max_jitter = delay * self.jitter
        delay += random.uniform(-max_jitter, max_jitter)
        return max(0.1, delay)  # Minimum delay 100ms


@dataclass
class ClientConfig(BaseConfig):
    """TCP client configuration"""
    # Basic settings
    host: str = "localhost"
    port: int = 8080
    timeout: float = 10.0

    # Buffer settings
    read_buffer_size: int = 65536       # Read buffer size (64KB)
    write_buffer_size: int = 65536      # Write buffer size (64KB)
    write_batch_size: int = 8192        # Batch write size (8KB)

    # Connection settings
    max_connections: int = 5            # Connection pool size
    connection_timeout: float = 5.0     # Connection timeout

    # Retry settings
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    # Socket options
    socket_options: TCPSocketOptions = field(default_factory=TCPSocketOptions)

    # Heartbeat settings
    heartbeat_interval: float = 30.0    # Heartbeat interval (seconds)
    heartbeat_timeout: float = 5.0      # Heartbeat timeout

    # Advanced settings
    compression_enabled: bool = False   # Enable compression
    compression_level: int = 6          # Compression level (0-9)
    use_tls: bool = False               # Use TLS encryption
    tls_verify: bool = True             # Verify TLS certificates
    
    # Performance monitoring
    enable_metrics: bool = True         # Enable performance metrics collection
    metrics_interval: float = 60.0      # Metrics collection interval (seconds)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        base_dict = {
            "host": self.host,
            "port": self.port,
            "timeout": self.timeout,
            "read_buffer_size": self.read_buffer_size,
            "write_buffer_size": self.write_buffer_size,
            "write_batch_size": self.write_batch_size,
            "max_connections": self.max_connections,
            "connection_timeout": self.connection_timeout,
            "heartbeat_interval": self.heartbeat_interval,
            "heartbeat_timeout": self.heartbeat_timeout,
            "compression_enabled": self.compression_enabled,
            "compression_level": self.compression_level,
            "use_tls": self.use_tls,
            "tls_verify": self.tls_verify,
            "enable_metrics": self.enable_metrics,
            "metrics_interval": self.metrics_interval,
            "socket_options": self.socket_options.to_dict(),
            "retry_policy": {
                "max_retries": self.retry_policy.max_retries,
                "base_delay": self.retry_policy.base_delay,
                "max_delay": self.retry_policy.max_delay,
                "jitter": self.retry_policy.jitter,
                "backoff_factor": self.retry_policy.backoff_factor
            }
        }
        return base_dict
    
    def validate(self) -> bool:
        """Validate configuration
        
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"Invalid port: {self.port}")
            
        if self.timeout <= 0:
            raise ValueError(f"Invalid timeout: {self.timeout}")
            
        if self.heartbeat_interval <= 0:
            raise ValueError(f"Invalid heartbeat interval: {self.heartbeat_interval}")
            
        if self.compression_level < 0 or self.compression_level > 9:
            raise ValueError(f"Invalid compression level: {self.compression_level}")
            
        return True
