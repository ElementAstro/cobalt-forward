from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Union
from ..base import BaseConfig


@dataclass
class TCPSocketOptions:
    """TCP套接字选项配置"""
    nodelay: bool = True                # 禁用Nagle算法
    keepalive: bool = True              # 开启TCP保活机制
    keepalive_idle: int = 60            # 空闲多久后发送保活探测
    keepalive_interval: int = 10        # 保活探测间隔
    keepalive_count: int = 5            # 无响应重试次数
    reuseaddr: bool = True              # 允许端口复用
    recvbuf_size: int = 262144          # 接收缓冲区大小（256KB）
    sendbuf_size: int = 262144          # 发送缓冲区大小（256KB）
    linger: Optional[int] = None        # 延迟关闭
    defer_accept: bool = True           # 延迟接收连接

    def to_dict(self) -> Dict[str, Any]:
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
    """重试策略配置"""
    max_retries: int = 5                # 最大重试次数
    base_delay: float = 1.0             # 基础延迟（秒）
    max_delay: float = 30.0             # 最大延迟（秒）
    jitter: float = 0.1                 # 抖动因子（0-1）
    backoff_factor: float = 2.0         # 退避因子

    def get_retry_delay(self, attempt: int) -> float:
        """计算第n次尝试的延迟时间，使用指数退避算法"""
        import random
        # 计算指数退避延迟
        delay = min(self.max_delay, self.base_delay *
                    (self.backoff_factor ** attempt))
        # 添加随机抖动
        max_jitter = delay * self.jitter
        delay += random.uniform(-max_jitter, max_jitter)
        return max(0.1, delay)  # 最小延迟100ms


@dataclass
class ClientConfig(BaseConfig):
    """TCP客户端配置"""
    # 基本设置
    host: str = "localhost"
    port: int = 8080
    timeout: float = 10.0

    # 缓冲区设置
    read_buffer_size: int = 65536       # 读缓冲区大小（64KB）
    write_buffer_size: int = 65536      # 写缓冲区大小（64KB）
    write_batch_size: int = 8192        # 批量写入大小（8KB）

    # 连接设置
    max_connections: int = 5            # 连接池大小
    connection_timeout: float = 5.0     # 连接超时时间

    # 重试设置
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    # 套接字选项
    socket_options: TCPSocketOptions = field(default_factory=TCPSocketOptions)

    # 心跳设置
    heartbeat_interval: float = 30.0    # 心跳间隔（秒）
    heartbeat_timeout: float = 5.0      # 心跳超时时间

    # 高级设置
    compression_enabled: bool = False   # 是否启用压缩
    compression_level: int = 6          # 压缩级别 (0-9)
    use_tls: bool = False               # 是否使用TLS加密
    tls_verify: bool = True             # 是否验证TLS证书

    def to_dict(self) -> Dict[str, Any]:
        """将配置转换为字典"""
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
            "socket_options": self.socket_options.to_dict()
        }
        return base_dict
