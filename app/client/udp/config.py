from dataclasses import dataclass, field
from typing import Dict, Any, List  # Removed Optional, Union
from ..base import BaseConfig


@dataclass
class UDPSocketOptions:
    """UDP socket options configuration"""
    recvbuf_size: int = 212992     # Receive buffer size
    sendbuf_size: int = 212992     # Send buffer size
    broadcast: bool = False        # Allow broadcast
    reuseaddr: bool = True         # Allow address reuse
    reuseport: bool = False        # Allow port reuse
    ip_multicast_ttl: int = 1      # Multicast TTL
    ip_multicast_loop: bool = True  # Loopback multicast
    ip_add_membership: List[str] = field(
        default_factory=list)  # Multicast groups

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "recvbuf_size": self.recvbuf_size,
            "sendbuf_size": self.sendbuf_size,
            "broadcast": self.broadcast,
            "reuseaddr": self.reuseaddr,
            "reuseport": self.reuseport,
            "ip_multicast_ttl": self.ip_multicast_ttl,
            "ip_multicast_loop": self.ip_multicast_loop,
            "ip_add_membership": self.ip_add_membership,
        }


@dataclass
class RetryPolicy:
    """Retry policy configuration"""
    max_retries: int = 3           # Maximum number of retries
    base_delay: float = 0.1        # Base delay (seconds)
    max_delay: float = 30.0        # Maximum delay (seconds)
    jitter: float = 0.1            # Jitter factor
    backoff_factor: float = 2.0    # Backoff factor

    def get_retry_delay(self, retry_count: int) -> float:
        """Calculate retry delay

        Args:
            retry_count: Number of retries

        Returns:
            Retry delay (seconds)
        """
        # Calculate exponential backoff delay
        delay = min(
            self.max_delay,
            self.base_delay * (self.backoff_factor ** retry_count)
        )

        # Add jitter
        if self.jitter > 0:
            import random
            delay = delay * (1 + random.uniform(-self.jitter, self.jitter))

        return delay


@dataclass
class ClientConfig(BaseConfig):
    """UDP client configuration"""
    # Basic settings
    host: str = "localhost"
    port: int = 8080
    timeout: float = 10.0

    # Buffer settings
    read_buffer_size: int = 65536       # Read buffer size (64KB)
    write_buffer_size: int = 65536      # Write buffer size (64KB)
    max_packet_size: int = 8192         # Maximum packet size (8KB)

    # Retry settings
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    # Socket options
    socket_options: UDPSocketOptions = field(default_factory=UDPSocketOptions)

    # UDP specific settings
    allow_broadcast: bool = False       # Allow broadcast
    multicast_support: bool = False     # Support multicast
    multicast_groups: List[str] = field(
        default_factory=list)  # Multicast group list
    packet_validation: bool = True      # Enable packet validation
    packet_sequencing: bool = True      # Enable packet sequencing
    reliable_delivery: bool = False     # Enable reliable delivery
    ordered_delivery: bool = False      # Enable ordered delivery
    ack_timeout: float = 0.2            # Acknowledgment timeout
    # Maximum UDP datagram size (1472 bytes)
    max_datagram_size: int = 1472

    # Heartbeat settings
    heartbeat_enabled: bool = True      # Enable heartbeat
    heartbeat_interval: float = 5.0     # Heartbeat interval (seconds)
    heartbeat_timeout: float = 15.0     # Heartbeat timeout (seconds)

    # Advanced settings
    compression_enabled: bool = False   # Enable compression
    compression_level: int = 6          # Compression level (0-9)
    enable_metrics: bool = True         # Enable performance metrics collection
    metrics_interval: float = 60.0      # Metrics collection interval (seconds)

    # Multiprocessing/Multithreading settings
    async_send: bool = True             # Asynchronous send
    async_receive: bool = True          # Asynchronous receive

    # Connection related settings
    auto_reconnect: bool = True         # Automatic reconnect

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        base_dict: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "timeout": self.timeout,
            "read_buffer_size": self.read_buffer_size,
            "write_buffer_size": self.write_buffer_size,
            "max_packet_size": self.max_packet_size,
            "allow_broadcast": self.allow_broadcast,
            "multicast_support": self.multicast_support,
            "multicast_groups": self.multicast_groups,
            "packet_validation": self.packet_validation,
            "packet_sequencing": self.packet_sequencing,
            "reliable_delivery": self.reliable_delivery,
            "ordered_delivery": self.ordered_delivery,
            "ack_timeout": self.ack_timeout,
            "max_datagram_size": self.max_datagram_size,
            "heartbeat_enabled": self.heartbeat_enabled,
            "heartbeat_interval": self.heartbeat_interval,
            "heartbeat_timeout": self.heartbeat_timeout,
            "compression_enabled": self.compression_enabled,
            "compression_level": self.compression_level,
            "enable_metrics": self.enable_metrics,
            "metrics_interval": self.metrics_interval,
            "async_send": self.async_send,
            "async_receive": self.async_receive,
            "auto_reconnect": self.auto_reconnect,
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
            True if the configuration is valid

        Raises:
            ValueError: If the configuration is invalid
        """
        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"Invalid port: {self.port}")

        if self.timeout <= 0:
            raise ValueError(f"Invalid timeout: {self.timeout}")

        if self.heartbeat_enabled and self.heartbeat_interval <= 0:
            raise ValueError(
                f"Invalid heartbeat interval: {self.heartbeat_interval}")

        if self.compression_enabled and (self.compression_level < 0 or self.compression_level > 9):
            raise ValueError(
                f"Invalid compression level: {self.compression_level}")

        if self.max_datagram_size > 65507:  # UDP theoretical maximum payload
            raise ValueError(
                f"UDP datagram size exceeds limit: {self.max_datagram_size}")

        return True
