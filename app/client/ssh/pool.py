import paramiko
from threading import Lock
import time
from typing import List, Optional
from .config import SSHConfig

class SSHConnectionPool:
    """SSH连接池管理"""

    def __init__(self, config: SSHConfig):
        self.config = config
        self.pool = []
        self.lock = Lock()
        self.pool_config = config.pool
        self._last_check = time.time()
        self._check_interval = 60  # 60秒检查一次
        self._idle_timeout = 300   # 空闲5分钟后关闭
        self._active_connections: List[tuple[paramiko.SSHClient, float]] = []
        
    def get_client(self) -> paramiko.SSHClient:
        """获取一个连接"""
        with self.lock:
            self._check_connections()
            if self._active_connections:
                client, _ = self._active_connections.pop(0)
                return client
            return self._create_client()

    def release_client(self, client: paramiko.SSHClient):
        """释放连接回池"""
        with self.lock:
            if len(self._active_connections) < self.pool_config.max_size:
                self._active_connections.append((client, time.time()))
            else:
                client.close()

    def _create_client(self) -> paramiko.SSHClient:
        """创建新的SSH客户端"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    def _check_connections(self):
        """检查连接状态并清理失效连接"""
        now = time.time()
        if now - self._last_check < self._check_interval:
            return

        self._active_connections = [
            (client, timestamp) for client, timestamp in self._active_connections
            if now - timestamp < self._idle_timeout and self._is_connection_alive(client)
        ]
        self._last_check = now

    def _is_connection_alive(self, client: paramiko.SSHClient) -> bool:
        try:
            transport = client.get_transport()
            if transport is None:
                return False
            transport.send_ignore()
            return True
        except Exception:
            return False
