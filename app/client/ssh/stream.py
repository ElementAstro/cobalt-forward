import paramiko
import queue
import time
from typing import Dict, Generator

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

        exit_status = channel.recv_exit_status()
        yield {'type': 'exit_status', 'data': exit_status}
