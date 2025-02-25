from .client import TCPClient, ConnectionPool
from .config import ClientConfig, TCPSocketOptions, RetryPolicy
from .state import ClientState, ConnectionStats, ClientStats
from .protocol import PacketProtocol
from .buffer import PacketBuffer

__all__ = [
    'TCPClient',
    'ConnectionPool',
    'ClientConfig',
    'TCPSocketOptions',
    'RetryPolicy',
    'ClientState',
    'ConnectionStats',
    'ClientStats',
    'PacketProtocol',
    'PacketBuffer',
]

# 版本信息
__version__ = '1.0.0'
