import time
import logging
import paramiko
from threading import Lock
from typing import List, Optional, Tuple, Dict, Any
from .config import SSHConfig
from .exceptions import SSHException


class SSHConnectionPool:
    """SSH连接池管理"""

    def __init__(self, config: SSHConfig):
        self.config = config
        self.lock = Lock()
        self.pool_config = config.pool
        self._last_check = time.time()
        self._check_interval = 60  # 60秒检查一次
        self._idle_timeout = 300   # 空闲5分钟后关闭
        # (client, last_used_time)
        self._active_connections: List[Tuple[paramiko.SSHClient, float]] = []
        self._available_connections: List[paramiko.SSHClient] = []
        self._logger = logging.getLogger("SSHConnectionPool")

        # 初始化连接池
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """初始化连接池，创建初始连接"""
        if not self.pool_config:
            return

        with self.lock:
            for _ in range(self.pool_config.min_size):
                try:
                    client = self._create_client()
                    self._connect_client(client)
                    self._available_connections.append(client)
                except Exception as e:
                    self._logger.error(f"初始化连接失败: {str(e)}")

    def get_client(self) -> paramiko.SSHClient:
        """获取一个连接"""
        with self.lock:
            # 检查连接状态
            self._check_connections()

            # 如果有可用连接，直接返回第一个
            if self._available_connections:
                client = self._available_connections.pop(0)
                # 验证连接是否有效，无效则创建新连接
                if not self._is_connection_alive(client):
                    try:
                        client.close()
                    except:
                        pass
                    client = self._create_and_connect_client()
                self._active_connections.append((client, time.time()))
                return client

            # 如果活跃连接数小于最大值，创建新连接
            if len(self._active_connections) < self.pool_config.max_size:
                client = self._create_and_connect_client()
                self._active_connections.append((client, time.time()))
                return client

            # 连接池已满，等待并重试
            raise SSHException(
                f"连接池已满，活跃连接数: {len(self._active_connections)}, 最大连接数: {self.pool_config.max_size}")

    def release_client(self, client: paramiko.SSHClient) -> None:
        """释放连接回池"""
        with self.lock:
            # 从活跃连接中移除
            self._active_connections = [
                (c, t) for c, t in self._active_connections if c != client]

            # 如果连接仍然有效，加入可用列表
            if self._is_connection_alive(client):
                self._available_connections.append(client)
            else:
                # 否则关闭无效连接
                try:
                    client.close()
                except:
                    pass

    def _create_client(self) -> paramiko.SSHClient:
        """创建新的SSH客户端"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    def _create_and_connect_client(self) -> paramiko.SSHClient:
        """创建并连接客户端"""
        client = self._create_client()
        self._connect_client(client)
        return client

    def _connect_client(self, client: paramiko.SSHClient) -> None:
        """连接SSH客户端"""
        try:
            connect_kwargs = {
                'hostname': self.config.hostname,
                'port': self.config.port,
                'username': self.config.username,
                'timeout': self.config.timeout,
                'compress': self.config.compress,
                'banner_timeout': self.config.banner_timeout,
                'auth_timeout': self.config.auth_timeout,
            }

            if self.config.password:
                connect_kwargs['password'] = self.config.password
            elif self.config.private_key_path:
                connect_kwargs['key_filename'] = self.config.private_key_path

            client.connect(**connect_kwargs)

            # 设置keepalive
            if self.config.keep_alive:
                transport = client.get_transport()
                if transport:
                    transport.set_keepalive(self.config.keep_alive_interval)

        except Exception as e:
            self._logger.error(f"SSH连接失败: {str(e)}")
            raise SSHException(f"创建SSH连接失败: {str(e)}") from e

    def _check_connections(self) -> None:
        """检查连接状态并清理失效连接"""
        now = time.time()
        if now - self._last_check < self._check_interval:
            return

        self._last_check = now

        with self.lock:
            # 检查活跃连接
            active_to_keep = []
            for client, last_used in self._active_connections:
                if now - last_used > self._idle_timeout:
                    # 超时关闭
                    try:
                        client.close()
                    except:
                        pass
                elif self._is_connection_alive(client):
                    active_to_keep.append((client, last_used))
                else:
                    try:
                        client.close()
                    except:
                        pass

            self._active_connections = active_to_keep

            # 检查可用连接
            available_to_keep = []
            for client in self._available_connections:
                if self._is_connection_alive(client):
                    available_to_keep.append(client)
                else:
                    try:
                        client.close()
                    except:
                        pass

            self._available_connections = available_to_keep

            # 确保池中有最小连接数
            while len(self._available_connections) < self.pool_config.min_size:
                try:
                    client = self._create_and_connect_client()
                    self._available_connections.append(client)
                except Exception as e:
                    self._logger.error(f"补充连接池失败: {str(e)}")
                    break

    def _is_connection_alive(self, client: paramiko.SSHClient) -> bool:
        """检查连接是否存活"""
        try:
            transport = client.get_transport()
            if transport is None or not transport.is_active():
                return False

            # 发送空指令以验证连接
            transport.send_ignore()
            return True
        except Exception:
            return False

    def close_all(self) -> None:
        """关闭所有连接"""
        with self.lock:
            # 关闭所有活跃连接
            for client, _ in self._active_connections:
                try:
                    client.close()
                except:
                    pass

            # 关闭所有可用连接
            for client in self._available_connections:
                try:
                    client.close()
                except:
                    pass

            self._active_connections = []
            self._available_connections = []

    def __del__(self):
        """析构函数，确保所有连接都被关闭"""
        self.close_all()
