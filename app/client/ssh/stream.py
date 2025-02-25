from typing import Dict, Generator, Callable, Optional, List, Any, Tuple
import time
import select
import paramiko
from io import StringIO


class SSHStreamHandler:
    """流式输出处理器"""

    def __init__(self):
        self._buffer_size = 4096
        self._encoding = 'utf-8'
        self._timeout = 0.1

    def stream_output(self, channel: paramiko.Channel) -> Generator[Dict[str, str], None, None]:
        """
        从SSH通道流式获取输出

        Args:
            channel: Paramiko Channel对象

        Yields:
            Dict[str, str]: 包含stdout和stderr数据的字典
        """
        stdout_buffer = StringIO()
        stderr_buffer = StringIO()

        while not channel.exit_status_ready() or channel.recv_ready() or channel.recv_stderr_ready():
            if channel.recv_ready():
                data = channel.recv(self._buffer_size)
                if not data:
                    break
                stdout_buffer.write(data.decode(
                    self._encoding, errors='replace'))
                stdout_content = stdout_buffer.getvalue()
                stdout_buffer = StringIO()
                yield {"stdout": stdout_content, "stderr": ""}

            if channel.recv_stderr_ready():
                data = channel.recv_stderr(self._buffer_size)
                if not data:
                    break
                stderr_buffer.write(data.decode(
                    self._encoding, errors='replace'))
                stderr_content = stderr_buffer.getvalue()
                stderr_buffer = StringIO()
                yield {"stdout": "", "stderr": stderr_content}

            if not (channel.recv_ready() or channel.recv_stderr_ready()) and not channel.exit_status_ready():
                time.sleep(self._timeout)

        # 处理退出码
        exit_status = channel.recv_exit_status()
        yield {"stdout": "", "stderr": "", "exit_status": exit_status}

    def process_stream(self, channel: paramiko.Channel, callback: Callable[[Dict[str, Any]], None]) -> int:
        """
        处理SSH通道的输出并通过回调函数处理

        Args:
            channel: Paramiko Channel对象
            callback: 处理输出的回调函数

        Returns:
            int: 命令退出状态
        """
        for output in self.stream_output(channel):
            callback(output)
            if "exit_status" in output:
                return output["exit_status"]
        return -1

    def collect_output(self, channel: paramiko.Channel, timeout: Optional[float] = None) -> Tuple[str, str, int]:
        """
        收集SSH通道的完整输出

        Args:
            channel: Paramiko Channel对象
            timeout: 超时时间（秒）

        Returns:
            Tuple[str, str, int]: (stdout, stderr, exit_status)
        """
        stdout_chunks = []
        stderr_chunks = []
        exit_status = None

        start_time = time.time()

        for output in self.stream_output(channel):
            if output["stdout"]:
                stdout_chunks.append(output["stdout"])
            if output["stderr"]:
                stderr_chunks.append(output["stderr"])
            if "exit_status" in output:
                exit_status = output["exit_status"]

            if timeout and (time.time() - start_time) > timeout:
                if not exit_status:
                    exit_status = -1  # 表示超时
                break

        return ''.join(stdout_chunks), ''.join(stderr_chunks), exit_status or -1

    def set_buffer_size(self, size: int) -> None:
        """设置缓冲区大小"""
        self._buffer_size = size

    def set_encoding(self, encoding: str) -> None:
        """设置字符编码"""
        self._encoding = encoding

    def set_timeout(self, timeout: float) -> None:
        """设置轮询超时"""
        self._timeout = timeout
