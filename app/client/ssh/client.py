import gzip
import hashlib
import os
import shutil
from loguru import logger
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
from ..base import BaseClient, BaseConfig
from dataclasses import dataclass


@dataclass
class SSHConfig(BaseConfig):
    username: str
    password: str = None
    private_key_path: str = None
    banner_timeout: float = 60
    auth_timeout: float = 60
    pool: bool = False
    keep_alive_interval: int = 30


class SSHClient(BaseClient):
    """增强的SSH客户端实现"""

    def __init__(self, config: SSHConfig):
        super().__init__(config)
        self.ssh = None
        self.pool = SSHConnectionPool(config) if config.pool else None
        self.stream_handler = SSHStreamHandler()
        self._setup_logging()
        self._file_cache = {}
        self._cache_timeout = 300  # 5分钟缓存
        self._executor = ThreadPoolExecutor(max_workers=5)
        self._sftp_client = None
        self._lock = Lock()

    async def connect(self) -> bool:
        """实现SSH连接"""
        if self.connected:
            return True

        try:
            if self.pool:
                # 从连接池获取连接
                self.ssh = self.pool.get_client()
                self.connected = True
                return True

            # 直接连接
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs = {
                'hostname': self.config.host,
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

            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self.ssh.connect(**connect_kwargs)
            )

            # 设置keepalive
            if self.config.keep_alive_interval > 0:
                transport = self.ssh.get_transport()
                if transport:
                    transport.set_keepalive(self.config.keep_alive_interval)

            self.connected = True
            return True

        except Exception as e:
            logger.error(f"SSH连接失败: {str(e)}")
            self.connected = False
            return False

    async def disconnect(self) -> None:
        """实现SSH断开连接"""
        if not self.connected:
            return

        try:
            if self.pool and self.ssh:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: self.pool.release_client(self.ssh)
                )
            elif self.ssh:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self.ssh.close
                )

            if self._sftp_client:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self._sftp_client.close
                )
                self._sftp_client = None
        finally:
            self.connected = False
            self.ssh = None

    async def send(self, data: Any) -> bool:
        """实现SSH数据发送"""
        return await self.execute_with_retry(self._send_data, data)

    async def receive(self) -> Optional[Any]:
        """实现SSH数据接收"""
        return await self.execute_with_retry(self._receive_data)

    # 仅保留必要的内部方法
    async def _send_data(self, data: Any) -> bool:
        if not self.connected:
            raise ConnectionError("SSH未连接")
        try:
            stdin, stdout, stderr = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                self.ssh.exec_command,
                data if isinstance(data, str) else str(data)
            )
            return True
        except Exception as e:
            logger.error(f"发送数据失败: {str(e)}")
            return False

    async def _receive_data(self) -> Optional[Any]:
        if not self.connected:
            return None
        # 实现具体的接收逻辑
        return None

    def __enter__(self):
        """上下文管理器支持"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """离开上下文时关闭连接"""
        self.close()

    async def execute_command_async(self, command: str) -> Dict[str, Any]:
        """异步执行命令"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.execute_command,
            command
        )

    async def execute_command(self, command: str) -> Dict[str, str]:
        """优化的命令执行实现"""
        if not self.connected:
            raise SSHException("SSH未连接")

        try:
            stdin, stdout, stderr = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self.ssh.exec_command(
                    command,
                    timeout=self.config.timeout,
                    get_pty=False
                )
            )

            # 并行读取stdout和stderr
            stdout_data, stderr_data = await asyncio.gather(
                self._read_stream(stdout),
                self._read_stream(stderr)
            )

            # 获取退出状态
            exit_code = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: stdout.channel.recv_exit_status()
            )

            return {
                'stdout': stdout_data,
                'stderr': stderr_data,
                'exit_code': exit_code
            }
        except Exception as e:
            logger.error(f"执行命令失败: {command}, 错误: {str(e)}")
            # 检查连接是否需要重连
            if isinstance(e, (paramiko.SSHException, EOFError, ConnectionError)):
                self.connected = False
            raise SSHException(f"执行命令失败: {str(e)}") from e

    @staticmethod
    async def _read_stream(stream) -> str:
        """异步高效读取流数据"""
        output = []

        async def read_all():
            data = await asyncio.get_event_loop().run_in_executor(None, stream.read)
            return data.decode('utf-8', errors='replace')

        try:
            data = await asyncio.wait_for(read_all(), timeout=30)
            output.append(data)
        except asyncio.TimeoutError:
            output.append("[读取超时]")

        return ''.join(output)

    async def bulk_execute(self, commands: List[str]) -> List[Dict[str, Any]]:
        """批量执行命令"""
        results = await asyncio.gather(
            *[self.execute_command_async(command) for command in commands]
        )
        return results

    async def upload_file(self, local_path: str, remote_path: str,
                          callback: Optional[Callable] = None):
        """优化的文件上传实现"""
        if not self.connected:
            raise SSHException("SSH未连接")

        # 增加文件检查
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"本地文件不存在: {local_path}")

        file_size = os.path.getsize(local_path)

        # 大文件使用压缩
        if file_size > 10 * 1024 * 1024:  # 10MB以上使用压缩
            await self._upload_compressed(local_path, remote_path, callback)
            return

        # 使用异步IO
        sftp = await self._get_sftp()
        try:
            # 确保远程目录存在
            remote_dir = os.path.dirname(remote_path)
            await self._ensure_remote_dir(sftp, remote_dir)

            # 计算上传进度的回调包装器
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(local_path, remote_path, percentage)
                progress_callback = progress_wrapper

            # 上传文件
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.put(local_path, remote_path,
                                 callback=progress_callback)
            )
        except Exception as e:
            logger.error(
                f"文件上传失败: {local_path} -> {remote_path}, 错误: {str(e)}")
            raise SSHException(f"文件上传失败: {str(e)}") from e

    async def _get_sftp(self):
        """获取SFTP客户端，带缓存"""
        if not self.connected:
            raise SSHException("SSH未连接")

        async with asyncio.Lock():
            if not self._sftp_client:
                self._sftp_client = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self.ssh.open_sftp
                )
            return self._sftp_client

    async def _ensure_remote_dir(self, sftp, remote_dir: str):
        """确保远程目录存在，不存在则创建"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.stat(remote_dir)
            )
        except FileNotFoundError:
            # 递归创建目录
            parent_dir = os.path.dirname(remote_dir)
            if parent_dir and parent_dir != '/':
                await self._ensure_remote_dir(sftp, parent_dir)

            try:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: sftp.mkdir(remote_dir)
                )
            except Exception as e:
                # 忽略目录已存在错误，可能是并发创建
                if "exists" not in str(e).lower():
                    raise

    async def _upload_compressed(self, local_path: str, remote_path: str,
                                 callback: Optional[Callable] = None):
        """上传压缩文件"""
        compressed_path = f"{local_path}.gz"
        try:
            # 压缩文件
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self._compress_file(local_path, compressed_path)
            )

            # 上传压缩文件
            sftp = await self._get_sftp()
            compressed_remote_path = f"{remote_path}.gz"

            # 确保远程目录存在
            remote_dir = os.path.dirname(remote_path)
            await self._ensure_remote_dir(sftp, remote_dir)

            # 计算上传进度的回调包装器
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(local_path, remote_path, percentage)
                progress_callback = progress_wrapper

            # 上传压缩文件
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.put(
                    compressed_path, compressed_remote_path, callback=progress_callback)
            )

            # 在远程解压文件
            decompress_cmd = f"gunzip -c {compressed_remote_path} > {remote_path} && rm {compressed_remote_path}"
            await self.execute_command(decompress_cmd)

        finally:
            # 清理临时压缩文件
            if os.path.exists(compressed_path):
                os.remove(compressed_path)

    @staticmethod
    def _compress_file(source_path: str, dest_path: str):
        """压缩文件"""
        with open(source_path, 'rb') as f_in, gzip.open(dest_path, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)

    async def download_file(self, remote_path: str, local_path: str,
                            callback: Optional[Callable] = None):
        """优化的文件下载实现"""
        if not self.connected:
            raise SSHException("SSH未连接")

        # 确保本地目录存在
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        sftp = await self._get_sftp()
        try:
            # 获取远程文件大小
            file_attrs = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.stat(remote_path)
            )
            file_size = file_attrs.st_size

            # 大文件使用压缩
            if file_size > 10 * 1024 * 1024:  # 10MB以上使用压缩
                await self._download_compressed(remote_path, local_path, callback)
                return

            # 计算下载进度的回调包装器
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(remote_path, local_path, percentage)
                progress_callback = progress_wrapper

            # 下载文件
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.get(remote_path, local_path,
                                 callback=progress_callback)
            )
        except Exception as e:
            logger.error(
                f"文件下载失败: {remote_path} -> {local_path}, 错误: {str(e)}")
            # 清理不完整的下载文件
            if os.path.exists(local_path):
                os.remove(local_path)
            raise SSHException(f"文件下载失败: {str(e)}") from e

    async def _download_compressed(self, remote_path: str, local_path: str,
                                   callback: Optional[Callable] = None):
        """下载压缩文件"""
        # 压缩临时文件名
        compressed_remote_path = f"{remote_path}.gz"
        compressed_local_path = f"{local_path}.gz"

        try:
            # 在远程压缩文件
            compress_cmd = f"gzip -c {remote_path} > {compressed_remote_path}"
            result = await self.execute_command(compress_cmd)
            if result['exit_code'] != 0:
                raise SSHException(f"远程压缩失败: {result['stderr']}")

            # 下载压缩文件
            sftp = await self._get_sftp()

            # 计算下载进度的回调包装器
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(remote_path, local_path, percentage)
                progress_callback = progress_wrapper

            # 下载压缩文件
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.get(
                    compressed_remote_path, compressed_local_path, callback=progress_callback)
            )

            # 本地解压
            with gzip.open(compressed_local_path, 'rb') as f_in, open(local_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            # 删除远程临时文件
            await self.execute_command(f"rm -f {compressed_remote_path}")

        finally:
            # 清理本地临时文件
            if os.path.exists(compressed_local_path):
                os.remove(compressed_local_path)

    def exec_command(self, command: str, timeout: float = None, get_pty: bool = False) -> Dict[str, Any]:
        """
        同步执行命令（为兼容性保留）
        """
        if not self.connected:
            raise SSHException("SSH未连接")

        actual_timeout = timeout or self.config.timeout
        try:
            stdin, stdout, stderr = self.ssh.exec_command(
                command, timeout=actual_timeout, get_pty=get_pty)

            exit_status = stdout.channel.recv_exit_status()
            stdout_data = stdout.read().decode('utf-8', errors='replace')
            stderr_data = stderr.read().decode('utf-8', errors='replace')

            return {
                'stdout': stdout_data,
                'stderr': stderr_data,
                'exit_code': exit_status
            }
        except Exception as e:
            logger.error(f"执行命令失败: {command}, 错误: {str(e)}")
            if isinstance(e, (paramiko.SSHException, EOFError, ConnectionError)):
                self.connected = False
            raise SSHException(f"执行命令失败: {str(e)}") from e

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
            remote_file = os.path.join(
                remote_dir, os.path.relpath(local_file, local_dir))
            if self._files_different(local_file, remote_file):
                self.upload_file(local_file, remote_file)
                uploaded_files.append(remote_file)

        if delete:
            for remote_file in remote_files:
                local_file = os.path.join(
                    local_dir, os.path.relpath(remote_file, remote_dir))
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
        transport.request_port_forward(
            local_host, local_port, remote_host, remote_port)

    def create_remote_tunnel(
        self,
        remote_port: int,
        local_port: int,
        local_host: str = '127.0.0.1'
    ) -> None:
        """创建远程隧道"""
        transport = self.ssh.get_transport()
        transport.request_port_forward(
            local_host, local_port, 'localhost', remote_port)

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
