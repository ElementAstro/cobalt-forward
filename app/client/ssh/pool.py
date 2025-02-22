import paramiko
from threading import Lock
from .config import SSHConfig

class SSHConnectionPool:
    """SSH连接池管理"""

    def __init__(self, config: SSHConfig):
        self.config = config
        self.pool = []
        self.lock = Lock()
        self.pool_config = config.pool

    def get_client(self) -> paramiko.SSHClient:
        """获取一个连接"""
        with self.lock:
            if not self.pool:
                if len(self.pool) < self.pool_config.max_size:
                    client = self._create_client()
                    return client
                else:
                    raise Exception("连接池已满")
            return self.pool.pop()

    def release_client(self, client: paramiko.SSHClient):
        """释放连接回池"""
        with self.lock:
            if len(self.pool) < self.pool_config.max_size:
                self.pool.append(client)
            else:
                client.close()

    def _create_client(self) -> paramiko.SSHClient:
        """创建新的SSH客户端"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client
