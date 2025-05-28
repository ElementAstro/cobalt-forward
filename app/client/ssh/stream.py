from typing import Dict, Generator, Callable, Optional, Any, Tuple, Union
import time
import paramiko
from io import StringIO


class SSHStreamHandler:
    """Handles streaming output from SSH connections"""

    def __init__(self):
        self._buffer_size = 4096
        self._encoding = 'utf-8'
        self._timeout = 0.1

    def stream_output(self, channel: paramiko.Channel) -> Generator[Dict[str, Union[str, int]], None, None]:
        """
        Stream output from an SSH channel

        Args:
            channel: Paramiko Channel object

        Yields:
            Dict[str, Union[str, int]]: Dictionary containing stdout, stderr, or exit_status data
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
                if stdout_content:  # Only yield if there's content
                    stdout_buffer = StringIO()
                    yield {"stdout": stdout_content, "stderr": ""}

            if channel.recv_stderr_ready():
                data = channel.recv_stderr(self._buffer_size)
                if not data:
                    break
                stderr_buffer.write(data.decode(
                    self._encoding, errors='replace'))
                stderr_content = stderr_buffer.getvalue()
                if stderr_content:  # Only yield if there's content
                    stderr_buffer = StringIO()
                    yield {"stdout": "", "stderr": stderr_content}

            if not (channel.recv_ready() or channel.recv_stderr_ready()) and not channel.exit_status_ready():
                # Minimal sleep if no immediate data or exit status
                time.sleep(self._timeout)

        # Process any remaining buffered output before exit status
        remaining_stdout = stdout_buffer.getvalue()
        if remaining_stdout:
            yield {"stdout": remaining_stdout, "stderr": ""}

        remaining_stderr = stderr_buffer.getvalue()
        if remaining_stderr:
            yield {"stdout": "", "stderr": remaining_stderr}

        # Process exit code
        exit_status = channel.recv_exit_status()
        yield {"stdout": "", "stderr": "", "exit_status": exit_status}

    def process_stream(self, channel: paramiko.Channel, callback: Callable[[Dict[str, Any]], None]) -> int:
        """
        Process SSH channel output using a callback function

        Args:
            channel: Paramiko Channel object
            callback: Callback function to process output

        Returns:
            int: Command exit status
        """
        exit_code: Optional[int] = None
        for output in self.stream_output(channel):
            callback(output)
            if "exit_status" in output:
                status = output["exit_status"]
                if isinstance(status, int):
                    exit_code = status
                    # Once exit status is received, we can break if it's the last piece of info expected
                    # However, stream_output is designed to yield all data then exit_status
        return exit_code if exit_code is not None else -1

    def collect_output(self, channel: paramiko.Channel, timeout: Optional[float] = None) -> Tuple[str, str, int]:
        """
        Collect complete output from an SSH channel

        Args:
            channel: Paramiko Channel object
            timeout: Timeout in seconds

        Returns:
            Tuple[str, str, int]: (stdout, stderr, exit_status)
        """
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        exit_status: Optional[int] = None

        start_time = time.time()

        for output in self.stream_output(channel):
            # Ensure keys exist and values are strings before appending
            stdout_data = output.get("stdout")
            if isinstance(stdout_data, str) and stdout_data:
                stdout_chunks.append(stdout_data)

            stderr_data = output.get("stderr")
            if isinstance(stderr_data, str) and stderr_data:
                stderr_chunks.append(stderr_data)

            exit_status_data = output.get("exit_status")
            if isinstance(exit_status_data, int):
                exit_status = exit_status_data

            if timeout and (time.time() - start_time) > timeout:
                if exit_status is None:  # Check if exit_status was set before timeout
                    exit_status = -2  # Indicates timeout, distinct from normal -1
                break

        final_exit_status = -1  # Default if not set
        if exit_status is not None:
            final_exit_status = exit_status

        return ''.join(stdout_chunks), ''.join(stderr_chunks), final_exit_status

    def set_buffer_size(self, size: int) -> None:
        """Set buffer size for reading stream data"""
        self._buffer_size = size

    def set_encoding(self, encoding: str) -> None:
        """Set character encoding for stream data"""
        self._encoding = encoding

    def set_timeout(self, timeout: float) -> None:
        """Set polling timeout interval"""
        self._timeout = timeout
