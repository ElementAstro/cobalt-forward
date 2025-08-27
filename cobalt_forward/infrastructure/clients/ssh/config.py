"""
SSH client configuration for the Cobalt Forward application.

This module provides configuration classes for SSH client settings.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

from ..base import ClientConfig


@dataclass
class SSHClientConfig(ClientConfig):
    """SSH client configuration."""

    # Authentication
    username: str = ""
    password: Optional[str] = None
    key_file: Optional[str] = None
    key_passphrase: Optional[str] = None
    client_keys: Optional[List[str]] = None

    # Connection settings
    compression: bool = True
    compression_level: int = 6
    client_version: str = "Cobalt_SSH_Client_1.0"

    # SSH-specific settings
    known_hosts_file: Optional[str] = None
    host_key_checking: bool = True
    forward_agent: bool = False

    # SFTP settings
    sftp_window_size: int = 2097152  # 2MB
    sftp_packet_size: int = 32768    # 32KB

    # Connection pooling
    pool_size: int = 5
    pool_recycle: int = 3600  # 1 hour

    # Advanced settings
    banner_timeout: float = 30.0
    auth_timeout: float = 30.0
    channel_timeout: float = 30.0

    def __post_init__(self) -> None:
        """Post-initialization validation."""
        if not self.username:
            raise ValueError("Username is required for SSH client")

        if not self.password and not self.key_file and not self.client_keys:
            raise ValueError(
                "Either password, key_file, or client_keys must be provided")

        if self.pool_size < 1:
            raise ValueError("Pool size must be at least 1")

        if self.compression_level < 1 or self.compression_level > 9:
            raise ValueError("Compression level must be between 1 and 9")

    def to_asyncssh_kwargs(self) -> Dict[str, Any]:
        """Convert to asyncssh connection kwargs."""
        kwargs = {
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'client_version': self.client_version,
            'keepalive_interval': self.keepalive_interval if self.keepalive else None,
            'connect_timeout': self.timeout,
            'login_timeout': self.auth_timeout,
        }

        # Authentication
        if self.password:
            kwargs['password'] = self.password

        if self.key_file:
            kwargs['client_keys'] = [self.key_file]
        elif self.client_keys:
            kwargs['client_keys'] = self.client_keys

        if self.key_passphrase:
            kwargs['passphrase'] = self.key_passphrase

        # Compression
        if self.compression:
            kwargs['compression_algs'] = ['zlib@openssh.com', 'zlib']
        else:
            kwargs['compression_algs'] = None

        # Known hosts
        if self.known_hosts_file:
            kwargs['known_hosts'] = self.known_hosts_file
        elif not self.host_key_checking:
            kwargs['known_hosts'] = None

        # Agent forwarding
        kwargs['agent_forwarding'] = self.forward_agent

        return kwargs
