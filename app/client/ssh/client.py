import gzip
import hashlib
import os
import shutil
import paramiko
import logging
import asyncio
import time
import threading
import select
import zlib
import stat
from typing import Dict, List, Union, Optional, Generator, Callable, Any
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from .config import SSHConfig
from .exceptions import SSHException
from .pool import SSHConnectionPool
from .stream import SSHStreamHandler
from .utils import get_local_files, get_file_hash

class SSHClient:
    """增强的SSH客户端类"""

    def __init__(self, config: SSHConfig):
        self.config = config
        self.pool = SSHConnectionPool(config) if config.pool else None
        self.stream_handler = SSHStreamHandler()
        self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._cache = {}
        self._cache_lock = Lock()
        self.connected = False

    def __enter__(self):
        """上下文管理器支持"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """离开上下文时关闭连接"""
        self.close()

    def connect(self):
        """建立SSH连接"""
        try:
            if not hasattr(self, 'ssh'):
                self.ssh = paramiko.SSHClient()
                self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs = {
                'hostname': self.config.hostname,
                'port': self.config.port,
                'username': self.config.username,
                'timeout': self.config.timeout,
                'banner_timeout': self.config.banner_timeout,
                'auth_timeout': self.config.auth_timeout,
            }

            if self.config.password:
                connect_kwargs['password'] = self.config.password
            if self.config.private_key_path:
                connect_kwargs['key_filename'] = self.config.private_key_path

            self.ssh.connect(**connect_kwargs)
            self.connected = True

            if self.config.keep_alive:
                transport = self.ssh.get_transport()
                transport.set_keepalive(self.config.keep_alive_interval)

        except Exception as e:
            self.logger.error(f"连接失败: {str(e)}")
            raise

    async def execute_command_async(self, command: str) -> Dict[str, Any]:
        """异步执行命令"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.execute_command,
            command
        )

    def execute_command(self, command: str) -> Dict[str, str]:
        """执行命令"""
        if not self.connected:
            raise SSHException("SSH未连接")

        stdin, stdout, stderr = self.ssh.exec_command(command)
        return {
            'stdout': stdout.read().decode(),
            'stderr': stderr.read().decode(),
            'exit_code': stdout.channel.recv_exit_status()
        }

    async def bulk_execute(self, commands: List[str]) -> List[Dict[str, Any]]:
        """批量执行命令"""
        results = await asyncio.gather(
            *[self.execute_command_async(command) for command in commands]
        )
        return results

    def upload_file(self, local_path: str, remote_path: str,
                    callback: Optional[Callable] = None):
        """上传文件"""
        sftp = self.ssh.open_sftp()
        sftp.put(local_path, remote_path, callback=callback)
        sftp.close()

    def _upload_compressed(self, local_path: str, remote_path: str,
                           callback: Optional[Callable] = None):
        """上传压缩文件"""
        compressed_path = f"{local_path}.gz"
        with open(local_path, 'rb') as f_in, gzip.open(compressed_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        self.upload_file(compressed_path, remote_path, callback)
        os.remove(compressed_path)

    def get_remote_file_size(self, path: str) -> int:
        """获取远程文件大小"""
        sftp = self.ssh.open_sftp()
        file_size = sftp.stat(path).st_size
        sftp.close()
        return file_size

    @property
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self.connected

    def close(self):
        """关闭SSH连接"""
        if hasattr(self, 'ssh'):
            self.ssh.close()
        self.connected = False

    def get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        return self.execute_command("uname -a")

    def _setup_logging(self):
        """设置日志"""
        self.logger = logging.getLogger('SSHClient')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def connect_with_retry(self) -> None:
        """重试连接"""
        for _ in range(self.config.retry_attempts):
            try:
                self.connect()
                return
            except Exception as e:
                self.logger.error(f"连接失败，重试中: {str(e)}")
                time.sleep(self.config.retry_interval)
        raise SSHException("重试连接失败")

    def execute_command_stream(
        self,
        command: str,
        timeout: int = 60,
        environment: Dict = None
    ) -> Generator[Dict[str, str], None, None]:
        """流式执行命令"""
        if not self.connected:
            raise SSHException("SSH未连接")

        transport = self.ssh.get_transport()
        channel = transport.open_session()
        channel.exec_command(command)

        while True:
            if channel.exit_status_ready():
                break
            rl, wl, xl = select.select([channel], [], [], timeout)
            if len(rl) > 0:
                yield {
                    'stdout': channel.recv(1024).decode(),
                    'stderr': channel.recv_stderr(1024).decode()
                }

    def execute_long_running_command(
        self,
        command: str,
        callback: Callable[[str], None],
        timeout: int = None
    ) -> None:
        """执行长时间运行的命令"""
        if not self.connected:
            raise SSHException("SSH未连接")

        transport = self.ssh.get_transport()
        channel = transport.open_session()
        channel.exec_command(command)

        while True:
            if channel.exit_status_ready():
                break
            rl, wl, xl = select.select([channel], [], [], timeout)
            if len(rl) > 0:
                callback(channel.recv(1024).decode())

    def execute_sudo_command(
        self,
        command: str,
        sudo_password: str,
        stream: bool = False
    ) -> Union[Dict[str, str], Generator[Dict[str, str], None, None]]:
        """执行sudo命令"""
        if not self.connected:
            raise SSHException("SSH未连接")

        sudo_command = f"echo {sudo_password} | sudo -S {command}"
        if stream:
            return self.execute_command_stream(sudo_command)
        return self.execute_command(sudo_command)

    def _collect_channel_output(self, channel: paramiko.Channel) -> Dict[str, str]:
        """收集通道输出"""
        stdout = []
        stderr = []
        while True:
            if channel.exit_status_ready():
                break
            rl, wl, xl = select.select([channel], [], [], 0.0)
            if len(rl) > 0:
                stdout.append(channel.recv(1024).decode())
                stderr.append(channel.recv_stderr(1024).decode())
        return {
            'stdout': ''.join(stdout),
            'stderr': ''.join(stderr),
            'exit_code': channel.recv_exit_status()
        }

    def sync_directory(
        self,
        local_dir: str,
        remote_dir: str,
        delete: bool = False,
        exclude: List[str] = None
    ) -> Dict[str, List[str]]:
        """同步目录"""
        local_files = self._get_local_files(local_dir, exclude)
        remote_files = self._get_remote_files(remote_dir)

        uploaded_files = []
        deleted_files = []

        for local_file in local_files:
            remote_file = os.path.join(remote_dir, os.path.relpath(local_file, local_dir))
            if self._files_different(local_file, remote_file):
                self.upload_file(local_file, remote_file)
                uploaded_files.append(remote_file)

        if delete:
            for remote_file in remote_files:
                local_file = os.path.join(local_dir, os.path.relpath(remote_file, remote_dir))
                if local_file not in local_files:
                    self.ssh.exec_command(f"rm -f {remote_file}")
                    deleted_files.append(remote_file)

        return {
            'uploaded': uploaded_files,
            'deleted': deleted_files
        }

    def _get_local_files(self, local_dir: str, exclude: List[str] = None) -> List[str]:
        """获取本地文件"""
        return get_local_files(local_dir, exclude)

    def _get_remote_files(self, remote_dir: str) -> List[str]:
        """获取远程文件"""
        sftp = self.ssh.open_sftp()
        remote_files = sftp.listdir(remote_dir)
        sftp.close()
        return remote_files

    def _files_different(self, local_path: str, remote_path: str) -> bool:
        """比较文件是否不同"""
        local_hash = self._get_local_file_hash(local_path)
        remote_hash = self._get_remote_file_hash(remote_path)
        return local_hash != remote_hash

    def _get_local_file_hash(self, path: str) -> str:
        """获取本地文件哈希"""
        return get_file_hash(path)

    def _get_remote_file_hash(self, path: str) -> str:
        """获取远程文件哈希"""
        sftp = self.ssh.open_sftp()
        with sftp.file(path, 'rb') as f:
            remote_hash = hashlib.md5(f.read()).hexdigest()
        sftp.close()
        return remote_hash

    def forward_remote_port(
        self,
        remote_host: str,
        remote_port: int,
        local_port: int,
        local_host: str = '127.0.0.1'
    ) -> None:
        """转发远程端口"""
        transport = self.ssh.get_transport()
        transport.request_port_forward(local_host, local_port, remote_host, remote_port)

    def create_remote_tunnel(
        self,
        remote_port: int,
        local_port: int,
        local_host: str = '127.0.0.1'
    ) -> None:
        """创建远程隧道"""
        transport = self.ssh.get_transport()
        transport.request_port_forward(local_host, local_port, 'localhost', remote_port)

    def monitor_file(
        self,
        remote_path: str,
        callback: Callable[[str], None],
        interval: int = 1
    ) -> None:
        """监控文件"""
        while True:
            sftp = self.ssh.open_sftp()
            with sftp.file(remote_path, 'r') as f:
                callback(f.read().decode())
            sftp.close()
            time.sleep(interval)
