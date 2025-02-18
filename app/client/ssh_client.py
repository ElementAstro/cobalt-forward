import paramiko
import logging
import os
import time
import threading
import queue
import select
import asyncio
from typing import List, Dict, Union, Optional, Generator, Callable, Any
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import hashlib
import stat
import re
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import zlib
from threading import Lock


@dataclass
class PoolConfig:
    min_size: int = 2
    max_size: int = 10
    timeout: float = 30.0


class SSHException(Exception):
    """SSH异常类"""
    pass


@dataclass
class SSHConfig:
    """增强的SSH配置数据类"""
    hostname: str
    username: str
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    port: int = 22
    timeout: int = 10
    compress: bool = True
    keep_alive: bool = True
    keep_alive_interval: int = 30
    banner_timeout: int = 60
    auth_timeout: int = 30
    channel_timeout: int = 30
    max_retries: int = 3
    retry_interval: int = 5
    pool: Optional[PoolConfig] = None


class SSHConnectionPool:
    """SSH连接池管理"""

    def __init__(self, config: SSHConfig):
        self.config = config
        self.pool: List[paramiko.SSHClient] = []
        self.lock = Lock()
        self.pool_config = config.pool or PoolConfig()

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


class SSHStreamHandler:
    """流式输出处理器"""

    def __init__(self):
        self.stdout_queue = queue.Queue()
        self.stderr_queue = queue.Queue()
        self.running = False

    def handle_output(self, channel: paramiko.Channel) -> Generator[Dict[str, str], None, None]:
        """处理流式输出"""
        self.running = True
        while self.running and not channel.exit_status_ready():
            if channel.recv_ready():
                data = channel.recv(4096).decode()
                if data:
                    yield {'type': 'stdout', 'data': data}

            if channel.recv_stderr_ready():
                data = channel.recv_stderr(4096).decode()
                if data:
                    yield {'type': 'stderr', 'data': data}

            time.sleep(0.1)

        # 获取最终的退出状态
        exit_status = channel.recv_exit_status()
        yield {'type': 'exit_status', 'data': exit_status}


class SSHClient:
    """增强的SSH客户端类"""

    def __init__(self, config: SSHConfig):
        self.config = config
        self.pool = SSHConnectionPool(config) if config.pool else None
        self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._cache = {}
        self._cache_lock = Lock()

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
        """执行命令并返回结果"""
        if not self.connected:
            raise Exception("未连接到服务器")

        try:
            stdin, stdout, stderr = self.ssh.exec_command(command)
            return {
                'stdout': stdout.read().decode(),
                'stderr': stderr.read().decode(),
                'status': stdout.channel.recv_exit_status()
            }
        except Exception as e:
            self.logger.error(f"命令执行失败: {str(e)}")
            raise

    async def bulk_execute(self, commands: List[str]) -> List[Dict[str, Any]]:
        """并行执行多个命令"""
        tasks = [
            self.execute_command_async(cmd)
            for cmd in commands
        ]
        return await asyncio.gather(*tasks)

    def upload_file(self, local_path: str, remote_path: str,
                    callback: Optional[Callable] = None):
        """上传文件带进度回调"""
        if not hasattr(self, 'sftp'):
            self.sftp = self.ssh.open_sftp()

        try:
            if self.config.compress:
                self._upload_compressed(local_path, remote_path, callback)
            else:
                self.sftp.put(local_path, remote_path, callback=callback)
        except Exception as e:
            self.logger.error(f"文件上传失败: {str(e)}")
            raise

    def _upload_compressed(self, local_path: str, remote_path: str,
                           callback: Optional[Callable] = None):
        """使用压缩上传文件"""
        with open(local_path, 'rb') as f:
            data = f.read()
        compressed = zlib.compress(data)

        with self.sftp.open(remote_path, 'wb') as f:
            f.write(compressed)

        if callback:
            callback(len(compressed), len(compressed))

    def get_remote_file_size(self, path: str) -> int:
        """获取远程文件大小"""
        try:
            return self.sftp.stat(path).st_size
        except Exception as e:
            self.logger.error(f"获取文件大小失败: {str(e)}")
            return 0

    @property
    def is_connected(self) -> bool:
        """检查连接状态"""
        if not hasattr(self, 'ssh'):
            return False
        transport = self.ssh.get_transport()
        return transport and transport.is_active()

    def close(self):
        """关闭连接"""
        if hasattr(self, 'sftp'):
            self.sftp.close()
        if hasattr(self, 'ssh'):
            self.ssh.close()
        self.connected = False
        self.executor.shutdown(wait=False)

    def get_system_info(self) -> Dict[str, Any]:
        """获取远程系统信息"""
        commands = {
            'hostname': 'hostname',
            'os': 'cat /etc/os-release',
            'cpu': 'cat /proc/cpuinfo',
            'memory': 'free -m',
            'disk': 'df -h'
        }

        result = {}
        for key, cmd in commands.items():
            output = self.execute_command(cmd)
            result[key] = output['stdout']

        return result

    def _setup_logging(self):
        """设置日志"""
        self.logger = logging.getLogger(f'SSHClient-{self.config.hostname}')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def connect_with_retry(self) -> None:
        """带重试的连接方法"""
        for attempt in range(self.config.max_retries):
            try:
                self.connect()
                return
            except Exception as e:
                self.logger.warning(f"连接尝试 {attempt + 1} 失败: {str(e)}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_interval)
                else:
                    raise

    def execute_command_stream(
        self,
        command: str,
        timeout: int = 60,
        environment: Dict = None
    ) -> Generator[Dict[str, str], None, None]:
        """执行命令并流式返回输出"""
        if not self.connected:
            raise SSHException("未连接到服务器")

        try:
            self.logger.debug(f"执行流式命令: {command}")

            # 获取交互式shell
            channel = self.ssh.get_transport().open_session()
            channel.get_pty()
            channel.settimeout(timeout)

            # 设置环境变量
            if environment:
                for key, value in environment.items():
                    channel.set_environment_variable(key, value)

            # 执行命令
            channel.exec_command(command)

            # 返回流式输出
            yield from self.stream_handler.handle_output(channel)

        except Exception as e:
            self.logger.error(f"流式命令执行失败: {str(e)}")
            raise SSHException(f"流式命令执行失败: {str(e)}")
        finally:
            channel.close()

    def execute_long_running_command(
        self,
        command: str,
        callback: Callable[[str], None],
        timeout: int = None
    ) -> None:
        """执行长时间运行的命令，通过回调处理输出"""
        for output in self.execute_command_stream(command, timeout):
            if output['type'] in ['stdout', 'stderr']:
                callback(output['data'])

    def execute_sudo_command(
        self,
        command: str,
        sudo_password: str,
        stream: bool = False
    ) -> Union[Dict[str, str], Generator[Dict[str, str], None, None]]:
        """执行sudo命令"""
        sudo_command = f"sudo -S {command}"
        channel = self.ssh.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(sudo_command)

        # 输入sudo密码
        channel.send(f"{sudo_password}\n")

        if stream:
            return self.stream_handler.handle_output(channel)
        else:
            return self._collect_channel_output(channel)

    def _collect_channel_output(self, channel: paramiko.Channel) -> Dict[str, str]:
        """收集通道输出"""
        stdout_data = ""
        stderr_data = ""

        while True:
            if channel.exit_status_ready():
                break

            if channel.recv_ready():
                stdout_data += channel.recv(4096).decode()
            if channel.recv_stderr_ready():
                stderr_data += channel.recv_stderr(4096).decode()

        return {
            'stdout': stdout_data,
            'stderr': stderr_data,
            'status': channel.recv_exit_status()
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

        to_upload = []
        to_delete = []

        # 比较文件
        for local_file in local_files:
            relative_path = os.path.relpath(local_file, local_dir)
            remote_path = os.path.join(remote_dir, relative_path)

            if remote_path not in remote_files or \
               self._files_different(local_file, remote_path):
                to_upload.append((local_file, remote_path))

        # 处理需要删除的文件
        if delete:
            for remote_file in remote_files:
                relative_path = os.path.relpath(remote_file, remote_dir)
                local_path = os.path.join(local_dir, relative_path)
                if local_path not in local_files:
                    to_delete.append(remote_file)

        # 执行同步
        uploaded = []
        deleted = []

        for local_file, remote_path in to_upload:
            self.upload_file(local_file, remote_path)
            uploaded.append(remote_path)

        for remote_file in to_delete:
            self.execute_command(f"rm -f {remote_file}")
            deleted.append(remote_file)

        return {
            'uploaded': uploaded,
            'deleted': deleted
        }

    def _get_local_files(self, local_dir: str, exclude: List[str] = None) -> List[str]:
        """获取本地文件列表"""
        files = []
        exclude_patterns = [re.compile(pattern) for pattern in (exclude or [])]

        for root, _, filenames in os.walk(local_dir):
            for filename in filenames:
                path = os.path.join(root, filename)
                if not any(pattern.match(path) for pattern in exclude_patterns):
                    files.append(path)
        return files

    def _get_remote_files(self, remote_dir: str) -> List[str]:
        """获取远程文件列表"""
        self.init_sftp()
        files = []

        def _recurse(remote_path):
            for entry in self.sftp.listdir_attr(remote_path):
                path = os.path.join(remote_path, entry.filename)
                if stat.S_ISREG(entry.st_mode):
                    files.append(path)
                elif stat.S_ISDIR(entry.st_mode):
                    _recurse(path)

        _recurse(remote_dir)
        return files

    def _files_different(self, local_path: str, remote_path: str) -> bool:
        """比较本地和远程文件是否不同"""
        try:
            local_hash = self._get_local_file_hash(local_path)
            remote_hash = self._get_remote_file_hash(remote_path)
            return local_hash != remote_hash
        except:
            return True

    def _get_local_file_hash(self, path: str) -> str:
        """获取本地文件的MD5哈希值"""
        hasher = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _get_remote_file_hash(self, path: str) -> str:
        """获取远程文件的MD5哈希值"""
        result = self.execute_command(f"md5sum {path}")
        if result['status'] == 0:
            return result['stdout'].split()[0]
        return None

    def forward_remote_port(
        self,
        remote_host: str,
        remote_port: int,
        local_port: int,
        local_host: str = '127.0.0.1'
    ) -> None:
        """设置远程端口转发"""
        transport = self.ssh.get_transport()
        transport.request_port_forward(
            local_host,
            local_port,
            remote_host,
            remote_port
        )

    def create_remote_tunnel(
        self,
        remote_port: int,
        local_port: int,
        local_host: str = '127.0.0.1'
    ) -> None:
        """创建远程隧道"""
        def handler(channel, remote_host, remote_port):
            import socket
            sock = socket.socket()
            try:
                sock.connect((local_host, local_port))
            except Exception as e:
                self.logger.error(f"隧道连接失败: {e}")
                return

            while True:
                r, w, x = select.select([sock, channel], [], [])
                if sock in r:
                    data = sock.recv(1024)
                    if len(data) == 0:
                        break
                    channel.send(data)
                if channel in r:
                    data = channel.recv(1024)
                    if len(data) == 0:
                        break
                    sock.send(data)

            sock.close()
            channel.close()

        transport = self.ssh.get_transport()
        transport.request_port_forward('', remote_port)
        while True:
            channel = transport.accept()
            if channel is None:
                continue
            thread = threading.Thread(target=handler, args=(
                channel, local_host, local_port))
            thread.daemon = True
            thread.start()

    def monitor_file(
        self,
        remote_path: str,
        callback: Callable[[str], None],
        interval: int = 1
    ) -> None:
        """监控远程文件变化"""
        last_size = 0
        try:
            while True:
                current_size = self.get_remote_file_size(remote_path)
                if current_size > last_size:
                    result = self.execute_command(
                        f"tail -c +{last_size + 1} {remote_path}")
                    if result['stdout']:
                        callback(result['stdout'])
                    last_size = current_size
                time.sleep(interval)
        except KeyboardInterrupt:
            self.logger.info("文件监控已停止")

# 使用示例


def main():
    # 配置
    config = SSHConfig(
        hostname="example.com",
        username="user",
        password="password",
        keep_alive=True
    )

    # 流式输出示例
    def handle_output(output):
        print(f"收到输出: {output}")

    with SSHClient(config) as ssh:
        # 流式执行命令
        for output in ssh.execute_command_stream("long_running_command"):
            print(output)

        # 执行sudo命令
        ssh.execute_sudo_command("apt-get update", "sudo_password")

        # 同步目录
        result = ssh.sync_directory(
            "/local/path",
            "/remote/path",
            delete=True,
            exclude=["*.tmp", "*.log"]
        )
        print(f"同步结果: {result}")

        # 监控文件
        ssh.monitor_file("/var/log/syslog", handle_output)

        # 端口转发
        ssh.forward_remote_port(
            "database.internal",
            5432,
            5432
        )


if __name__ == "__main__":
    main()
