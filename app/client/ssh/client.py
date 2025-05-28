import gzip
# import hashlib # Unused import
import os
import shutil
# from loguru import logger # Unused import, using standard logging
import paramiko
import logging
import asyncio
import time
# import threading # Unused import
# import select # Unused import
from typing import Dict, List, Optional, Callable, Any, Coroutine
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from .config import SSHConfig
from .exceptions import (
    SSHException, SSHConnectionError, SSHCommandError,
    SSHTimeoutError, SSHFileTransferError
)
from .pool import SSHConnectionPool
from .stream import SSHStreamHandler
# from .utils import get_local_files, get_file_hash # Unused imports
from ..base import BaseClient


class SSHClient(BaseClient):
    """
    Enhanced SSH client implementation with robust error handling,
    connection pooling, and efficient file transfer
    """

    def __init__(self, config: SSHConfig):
        """
        Initialize SSH client with provided configuration

        Args:
            config: SSH configuration object
        """
        super().__init__(config)
        self.config: SSHConfig = config  # Explicitly type self.config for SSHClient
        self.ssh: Optional[paramiko.SSHClient] = None
        self.pool: Optional[SSHConnectionPool] = SSHConnectionPool(config) if config.pool else None
        self.stream_handler: SSHStreamHandler = SSHStreamHandler()
        self._setup_logging()
        self._file_cache: Dict[str, Any] = {}
        self._cache_timeout: int = 300  # 5 minutes cache timeout
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=5)
        self._sftp_client: Optional[paramiko.SFTPClient] = None
        self._lock: Lock = Lock()
        # Ensure 'connected' attribute is initialized from BaseClient or here
        if not hasattr(self, 'connected'):
            self.connected: bool = False


    def _setup_logging(self):
        """Configure logging for SSH client"""
        self._logger = logging.getLogger("SSHClient")
        # Ensure level is set on the logger instance
        # Check if config has a log_level attribute, otherwise default
        log_level_str = getattr(self.config, 'log_level', 'INFO').upper()
        level = getattr(logging, log_level_str, logging.INFO)
        self._logger.setLevel(level)

        # Only add handler if none exists for this specific logger
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    async def connect(self) -> bool:
        """
        Establish SSH connection

        Returns:
            bool: True if connection successful, False otherwise
        """
        if self.connected and self.ssh:
            return True

        try:
            if self.pool:
                # Get connection from pool
                self.ssh = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self.pool.get_client
                )
                self.connected = True
                self._logger.info(f"Using pooled connection to {self.config.hostname}:{self.config.port}")
                return True

            # Direct connection
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs: Dict[str, Any] = {
                'hostname': self.config.hostname,
                'port': self.config.port,
                'username': self.config.username,
                'timeout': self.config.timeout,
                'banner_timeout': self.config.banner_timeout,
                'auth_timeout': self.config.auth_timeout,
                'compress': self.config.compress, # Added compress from SSHConfig
            }

            if self.config.password:
                connect_kwargs['password'] = self.config.password
            elif self.config.private_key_path:
                connect_kwargs['key_filename'] = self.config.private_key_path
            
            if hasattr(self.config, 'private_key_passphrase') and self.config.private_key_passphrase:
                connect_kwargs['passphrase'] = self.config.private_key_passphrase
            elif hasattr(self.config, 'key_passphrase') and self.config.key_passphrase:
                connect_kwargs['passphrase'] = self.config.key_passphrase


            self._logger.info(f"Connecting to {self.config.hostname}:{self.config.port}")
            
            # Ensure self.ssh is not None before calling connect on it
            if self.ssh is None: # Should not happen due to above initialization
                self.ssh = paramiko.SSHClient()
                self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            current_ssh_client = self.ssh # Capture for lambda
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: current_ssh_client.connect(**connect_kwargs)
            )

            # Set keepalive
            if self.config.keep_alive and self.ssh: # Check self.ssh again
                transport = self.ssh.get_transport()
                if transport and hasattr(self.config, 'keep_alive_interval'):
                    transport.set_keepalive(self.config.keep_alive_interval)
                    self._logger.debug(f"Keepalive set to {self.config.keep_alive_interval} seconds")

            self.connected = True
            self._logger.info(f"Successfully connected to {self.config.hostname}:{self.config.port}")
            return True

        except Exception as e:
            self._logger.error(f"SSH connection failed: {str(e)}")
            self.connected = False
            self.ssh = None # Ensure ssh is None on failure
            raise SSHConnectionError(f"Failed to connect to {self.config.hostname}:{self.config.port}: {str(e)}") from e

    async def disconnect(self) -> None:
        """
        Disconnect from SSH server and cleanup resources
        """
        if not self.connected and not self.ssh: # Check both
            return

        try:
            if self.pool:
                ssh_client_to_release = self.ssh
                if ssh_client_to_release:  # Ensure ssh is not None
                    self._logger.debug("Returning connection to pool")
                    await asyncio.get_event_loop().run_in_executor(
                        self._executor,
                        lambda: self.pool.release_client(ssh_client_to_release) # type: ignore
                    )
            elif self.ssh:
                ssh_client_to_close = self.ssh
                self._logger.debug("Closing SSH connection")
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    ssh_client_to_close.close
                )

            if self._sftp_client:
                sftp_client_to_close = self._sftp_client
                self._logger.debug("Closing SFTP client")
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    sftp_client_to_close.close
                )
                self._sftp_client = None
        except Exception as e:
            self._logger.error(f"Error during disconnect: {str(e)}")
        finally:
            self.connected = False
            self.ssh = None
            self._logger.info("SSH connection closed")

    async def execute_with_retry(self, func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any) -> Any:
        """
        Execute a function with retry logic

        Args:
            func: Function to execute
            *args: Arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            Any: Result of the function call

        Raises:
            SSHException: If all retries fail
        """
        max_retries = self.config.max_retries
        retry_interval = self.config.retry_interval
        attempt = 0

        while attempt <= max_retries:
            try:
                if not self.connected and attempt > 0: # Also check self.ssh?
                    self._logger.info(f"Reconnecting (attempt {attempt}/{max_retries})")
                    await self.connect()

                return await func(*args, **kwargs)

            except Exception as e:
                attempt += 1
                if attempt > max_retries:
                    self._logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                    if isinstance(e, SSHException):
                        raise
                    raise SSHException(f"Operation failed after {max_retries} attempts: {str(e)}") from e

                self._logger.warning(f"Attempt {attempt}/{max_retries} failed: {str(e)}, retrying in {retry_interval}s")
                await asyncio.sleep(retry_interval)

                # If connection was lost, reset the flag
                if isinstance(e, (paramiko.SSHException, ConnectionError, SSHConnectionError, EOFError)):
                    self.connected = False
                    self.ssh = None # Also reset ssh object

    async def execute_command(self, command: str, timeout: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a command on the remote server

        Args:
            command: Command to execute
            timeout: Command timeout in seconds (uses config timeout if None)

        Returns:
            Dict[str, Any]: Dictionary with stdout, stderr, and exit_code keys

        Raises:
            SSHCommandError: If command execution fails
        """
        if not self.connected or not self.ssh:
            raise SSHConnectionError("SSH not connected or client is None")

        actual_timeout = timeout if timeout is not None else self.config.timeout
        self._logger.debug(f"Executing command: {command} with timeout {actual_timeout}")
        
        current_ssh_client = self.ssh # Capture for lambda

        try:
            _stdin, stdout, stderr = await asyncio.get_event_loop().run_in_executor( # stdin assigned to _
                self._executor,
                lambda: current_ssh_client.exec_command(
                    command,
                    timeout=float(actual_timeout) if actual_timeout is not None else None, # paramiko timeout is float
                    get_pty=False
                )
            )

            # Read stdout and stderr in parallel
            stdout_data, stderr_data = await asyncio.gather(
                self._read_stream(stdout),
                self._read_stream(stderr)
            )

            # Get exit status
            exit_code = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: stdout.channel.recv_exit_status()
            )

            self._logger.debug(f"Command executed with exit code: {exit_code}")
            if exit_code != 0:
                self._logger.warning(f"Command failed (exit code {exit_code}): {command}")
                self._logger.debug(f"stderr: {stderr_data}")

            return {
                'stdout': stdout_data,
                'stderr': stderr_data,
                'exit_code': exit_code
            }
        except asyncio.TimeoutError: # This might not be directly caught if paramiko's timeout hits first
            self._logger.error(f"Command timed out after {actual_timeout}s: {command}")
            raise SSHTimeoutError(f"Command timed out after {actual_timeout} seconds: {command}")
        except paramiko.SSHException as e: # Catch paramiko specific timeout or other SSH errors
            if "Timeout" in str(e):
                 self._logger.error(f"Command timed out (paramiko SSHException) after {actual_timeout}s: {command}")
                 raise SSHTimeoutError(f"Command timed out after {actual_timeout} seconds: {command}") from e
            self._logger.error(f"Command execution failed (paramiko SSHException): {command}, error: {str(e)}")
            self.connected = False
            self.ssh = None
            raise SSHCommandError(message=f"Command execution failed: {str(e)}", command=command) from e
        except Exception as e:
            self._logger.error(f"Command execution failed: {command}, error: {str(e)}")
            if isinstance(e, (EOFError, ConnectionError)): # paramiko.SSHException already handled
                self.connected = False
                self.ssh = None

            raise SSHCommandError(
                message=f"Command execution failed: {str(e)}",
                command=command
            ) from e

    @staticmethod
    async def _read_stream(stream: paramiko.channel.ChannelFile) -> str:
        """
        Asynchronously read stream data

        Args:
            stream: Stream to read from

        Returns:
            str: Data read from stream
        """
        output_chunks: List[str] = []

        async def read_all_from_stream() -> bytes: # Explicitly bytes
            # paramiko's stream.read() is blocking, so run_in_executor is correct
            read_data = await asyncio.get_event_loop().run_in_executor(None, stream.read)
            return read_data

        try:
            # Timeout for the read operation itself
            data_bytes = await asyncio.wait_for(read_all_from_stream(), timeout=30.0)
            output_chunks.append(data_bytes.decode('utf-8', errors='replace'))
        except asyncio.TimeoutError:
            output_chunks.append("[READ TIMEOUT]")
        except Exception as e:
            logging.getLogger("SSHClient._read_stream").error(f"Error reading stream: {e}")
            output_chunks.append("[READ ERROR]")


        return ''.join(output_chunks)

    async def bulk_execute(self, commands: List[str], timeout: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Execute multiple commands in parallel

        Args:
            commands: List of commands to execute
            timeout: Command timeout in seconds

        Returns:
            List[Dict[str, Any]]: List of command execution results
        """
        # Note: execute_with_retry itself is async, so this is correct
        tasks = [
            self.execute_with_retry(self.execute_command, command, timeout)
            for command in commands
        ]
        return await asyncio.gather(*tasks)


    async def _get_sftp(self) -> paramiko.SFTPClient:
        """
        Get or create SFTP client

        Returns:
            paramiko.SFTPClient: SFTP client

        Raises:
            SSHConnectionError: If SSH not connected
        """
        if not self.connected or not self.ssh:
            raise SSHConnectionError("SSH not connected or client is None")

        # Use the client's lock for thread safety if _sftp_client is shared
        async with asyncio.Lock(): # An asyncio Lock for async context
            if self._sftp_client is None or self._sftp_client.sock is None or self._sftp_client.sock.closed:
                self._logger.debug("Opening new SFTP client or replacing closed one")
                current_ssh_client = self.ssh # Capture for lambda
                self._sftp_client = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    current_ssh_client.open_sftp
                )
            return self._sftp_client

    async def _ensure_remote_dir(self, sftp: paramiko.SFTPClient, remote_dir: str):
        """
        Ensure remote directory exists, create if necessary

        Args:
            sftp: SFTP client
            remote_dir: Remote directory path
        """
        try:
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.stat(remote_dir)
            )
            self._logger.debug(f"Remote directory exists: {remote_dir}")
        except FileNotFoundError:
            self._logger.debug(f"Remote directory not found, attempting to create: {remote_dir}")
            parent_dir = os.path.dirname(remote_dir)
            if parent_dir and parent_dir != '/' and parent_dir != remote_dir : # Avoid infinite recursion for root or same dir
                await self._ensure_remote_dir(sftp, parent_dir)

            try:
                self._logger.debug(f"Creating remote directory: {remote_dir}")
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: sftp.mkdir(remote_dir)
                )
            except Exception as e: # Catch generic paramiko exception for "exists"
                # Check if error indicates directory already exists (can happen with concurrency)
                # Paramiko might raise IOError or SFTPError with specific codes/messages
                if not (hasattr(e, 'errno') and e.errno == paramiko.sftp_si.SFTP_OP_UNSUPPORTED or \
                        "exists" in str(e).lower() or "Failure" in str(e)): # Be more specific if possible
                    self._logger.error(f"Failed to create remote directory: {remote_dir}, error: {str(e)}")
                    raise SSHFileTransferError(f"Failed to create remote directory {remote_dir}: {e}") from e
                else:
                    self._logger.debug(f"Remote directory {remote_dir} likely created concurrently or already exists.")


    async def upload_file(self, local_path: str, remote_path: str,
                         callback: Optional[Callable[[str, str, float], None]] = None):
        """
        Upload file to remote server with optimizations for large files

        Args:
            local_path: Local file path
            remote_path: Remote file path
            callback: Progress callback function (local_path, remote_path, percentage)

        Raises:
            FileNotFoundError: If local file does not exist
            SSHFileTransferError: If file transfer fails
        """
        if not self.connected or not self.ssh: # Check self.ssh as well
            raise SSHConnectionError("SSH not connected or client is None")

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        file_size = os.path.getsize(local_path)
        self._logger.info(f"Uploading file: {local_path} to {remote_path} ({file_size} bytes)")

        if self.config.compress_large_files and file_size > self.config.compression_threshold_mb * 1024 * 1024:
            self._logger.info(f"Using compression for large file: {local_path}")
            await self._upload_compressed(local_path, remote_path, callback)
            return

        sftp = await self._get_sftp()
        try:
            remote_dir = os.path.dirname(remote_path)
            if remote_dir: # Ensure remote_dir is not empty (e.g. for root uploads)
                await self._ensure_remote_dir(sftp, remote_dir)

            progress_callback_wrapper = None
            if callback:
                def progress_wrapper_func(transferred: int, total: int):
                    percentage = (transferred / total) * 100.0 if total > 0 else 0.0
                    callback(local_path, remote_path, percentage)
                progress_callback_wrapper = progress_wrapper_func

            start_time = time.time()
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.put(local_path, remote_path, callback=progress_callback_wrapper)
            )
            elapsed = time.time() - start_time
            transfer_rate = (file_size / (1024 * 1024)) / elapsed if elapsed > 0 else 0 # MB/s
            self._logger.info(f"Upload completed in {elapsed:.2f}s ({transfer_rate:.2f} MB/s)")

        except Exception as e:
            self._logger.error(f"File upload failed: {local_path} -> {remote_path}, error: {str(e)}")
            raise SSHFileTransferError(
                message=f"File upload failed: {str(e)}",
                source_path=local_path,
                destination_path=remote_path
            ) from e

    async def _upload_compressed(self, local_path: str, remote_path: str,
                              callback: Optional[Callable[[str, str, float], None]] = None):
        """
        Upload file with compression
        """
        compressed_path = f"{local_path}.gz"
        try:
            self._logger.debug(f"Compressing file: {local_path} to {compressed_path}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self._compress_file(local_path, compressed_path)
            )

            sftp = await self._get_sftp()
            compressed_remote_path = f"{remote_path}.gz"
            remote_dir = os.path.dirname(compressed_remote_path)
            if remote_dir:
                await self._ensure_remote_dir(sftp, remote_dir)

            progress_callback_wrapper = None
            if callback:
                # Estimate progress based on compressed size if possible, or original for simplicity
                # For now, pass original local_path and remote_path to callback
                def progress_wrapper_func(transferred: int, total: int):
                    percentage = (transferred / total) * 100.0 if total > 0 else 0.0
                    callback(local_path, remote_path, percentage) # Reports progress for the compressed file transfer
                progress_callback_wrapper = progress_wrapper_func
            
            self._logger.debug(f"Uploading compressed file: {compressed_path} to {compressed_remote_path}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.put(compressed_path, compressed_remote_path, callback=progress_callback_wrapper)
            )

            self._logger.debug(f"Decompressing file on remote server: {compressed_remote_path} to {remote_path}")
            decompress_cmd = f"gunzip -c {compressed_remote_path} > \"{remote_path}\" && rm -f {compressed_remote_path}"
            result = await self.execute_command(decompress_cmd) # execute_command is already wrapped with retry

            if result['exit_code'] != 0:
                raise SSHFileTransferError(
                    message=f"Remote decompression failed: {result['stderr']}",
                    source_path=local_path,
                    destination_path=remote_path
                )

            self._logger.info(f"Compression upload completed: {local_path} -> {remote_path}")

        finally:
            if os.path.exists(compressed_path):
                try:
                    os.remove(compressed_path)
                    self._logger.debug(f"Removed temporary compressed file: {compressed_path}")
                except OSError as e:
                    self._logger.warning(f"Could not remove temporary compressed file {compressed_path}: {e}")


    @staticmethod
    def _compress_file(source_path: str, dest_path: str):
        """Compress file using gzip"""
        with open(source_path, 'rb') as f_in, gzip.open(dest_path, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)

    async def download_file(self, remote_path: str, local_path: str,
                        callback: Optional[Callable[[str, str, float], None]] = None):
        """
        Download file from remote server with optimizations for large files
        """
        if not self.connected or not self.ssh:
            raise SSHConnectionError("SSH not connected or client is None")

        local_dir = os.path.dirname(local_path)
        if local_dir: # Ensure local_dir is not empty
            os.makedirs(local_dir, exist_ok=True)

        sftp = await self._get_sftp()
        try:
            file_attrs = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.stat(remote_path)
            )
            file_size = file_attrs.st_size if file_attrs and hasattr(file_attrs, 'st_size') else 0
            self._logger.info(f"Downloading file: {remote_path} to {local_path} ({file_size} bytes)")

            if self.config.compress_large_files and file_size and file_size > self.config.compression_threshold_mb * 1024 * 1024:
                self._logger.info(f"Using compression for large file download: {remote_path}")
                await self._download_compressed(remote_path, local_path, callback)
                return

            progress_callback_wrapper = None
            if callback:
                def progress_wrapper_func(transferred: int, total: int):
                    percentage = (transferred / total) * 100.0 if total > 0 else 0.0
                    callback(remote_path, local_path, percentage)
                progress_callback_wrapper = progress_wrapper_func

            start_time = time.time()
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.get(remote_path, local_path, callback=progress_callback_wrapper)
            )
            elapsed = time.time() - start_time
            transfer_rate = (file_size / (1024 * 1024)) / elapsed if file_size and elapsed > 0 else 0 # MB/s
            self._logger.info(f"Download completed in {elapsed:.2f}s ({transfer_rate:.2f} MB/s)")

        except Exception as e:
            self._logger.error(f"File download failed: {remote_path} -> {local_path}, error: {str(e)}")
            if os.path.exists(local_path): # Cleanup incomplete download
                try:
                    os.remove(local_path)
                    self._logger.debug(f"Removed incomplete download: {local_path}")
                except OSError as remove_err:
                    self._logger.warning(f"Could not remove incomplete download {local_path}: {remove_err}")

            raise SSHFileTransferError(
                message=f"File download failed: {str(e)}",
                source_path=remote_path,
                destination_path=local_path
            ) from e

    async def _download_compressed(self, remote_path: str, local_path: str,
                              callback: Optional[Callable[[str, str, float], None]] = None):
        """
        Download file with compression
        """
        compressed_remote_path = f"{remote_path}.gz"
        compressed_local_path = f"{local_path}.gz"

        try:
            self._logger.debug(f"Compressing file on remote server: {remote_path} to {compressed_remote_path}")
            compress_cmd = f"gzip -c \"{remote_path}\" > {compressed_remote_path}" # Quote remote_path
            result = await self.execute_command(compress_cmd)
            if result['exit_code'] != 0:
                raise SSHFileTransferError(
                    message=f"Remote compression failed: {result['stderr']}",
                    source_path=remote_path,
                    destination_path=local_path
                )

            sftp = await self._get_sftp()
            progress_callback_wrapper = None
            if callback:
                def progress_wrapper_func(transferred: int, total: int):
                    percentage = (transferred / total) * 100.0 if total > 0 else 0.0
                    callback(remote_path, local_path, percentage)
                progress_callback_wrapper = progress_wrapper_func

            self._logger.debug(f"Downloading compressed file: {compressed_remote_path} to {compressed_local_path}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.get(compressed_remote_path, compressed_local_path, callback=progress_callback_wrapper)
            )

            self._logger.debug(f"Decompressing file locally: {compressed_local_path} to {local_path}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self._decompress_file(compressed_local_path, local_path)
            )


            self._logger.debug(f"Removing remote temporary file: {compressed_remote_path}")
            await self.execute_command(f"rm -f {compressed_remote_path}")
            self._logger.info(f"Compression download completed: {remote_path} -> {local_path}")

        finally:
            if os.path.exists(compressed_local_path):
                try:
                    os.remove(compressed_local_path)
                    self._logger.debug(f"Removed local temporary file: {compressed_local_path}")
                except OSError as e:
                     self._logger.warning(f"Could not remove local temporary compressed file {compressed_local_path}: {e}")


    @staticmethod
    def _decompress_file(source_path: str, dest_path: str):
        """Decompress gzip file"""
        with gzip.open(source_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


    # Method for backward compatibility - Synchronous
    def exec_command(self, command: str, timeout: Optional[float] = None, get_pty: bool = False) -> Dict[str, Any]:
        """
        Synchronous command execution (kept for backward compatibility)
        """
        if not self.connected or not self.ssh:
            raise SSHConnectionError("SSH not connected or client is None")

        actual_timeout = timeout if timeout is not None else self.config.timeout
        self._logger.debug(f"Executing command synchronously: {command} with timeout {actual_timeout}")

        try:
            # stdin is not used, assign to _
            _stdin, stdout, stderr = self.ssh.exec_command(
                command, 
                timeout=float(actual_timeout) if actual_timeout is not None else None, # paramiko timeout is float
                get_pty=get_pty
            )

            exit_status = stdout.channel.recv_exit_status() # Blocking call
            stdout_data = stdout.read().decode('utf-8', errors='replace') # Blocking call
            stderr_data = stderr.read().decode('utf-8', errors='replace') # Blocking call

            if exit_status != 0:
                self._logger.warning(f"Command failed (exit code {exit_status}): {command}")
                self._logger.debug(f"stderr (sync): {stderr_data}")


            return {
                'stdout': stdout_data,
                'stderr': stderr_data,
                'exit_code': exit_status
            }
        except Exception as e:
            self._logger.error(f"Synchronous command execution failed: {command}, error: {str(e)}")
            if isinstance(e, (paramiko.SSHException, EOFError, ConnectionError)):
                self.connected = False
                self.ssh = None

            raise SSHCommandError(
                message=f"Synchronous command execution failed: {str(e)}",
                command=command
            ) from e

    def close(self):
        """Close the SSH connection (synchronous wrapper for disconnect)"""
        # This synchronous close method might be problematic in an async environment
        # if not handled carefully. For true async cleanup, rely on await self.disconnect().
        # If this is called from a non-async context, it tries to run disconnect.
        self._logger.debug("Synchronous close called.")
        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError: # No event loop in current thread
            self._logger.warning("No event loop in current thread for synchronous close. Performing direct close.")
            loop = None

        if loop and loop.is_running():
            # If there's a running loop, schedule disconnect and wait (can lead to deadlocks if not careful)
            # This is generally not a good pattern. Better to manage async resources from async code.
            # For simplicity, we'll just call the blocking parts if no loop or not running.
            # Consider deprecating this synchronous close or making it truly synchronous.
            future = asyncio.run_coroutine_threadsafe(self.disconnect(), loop)
            try:
                future.result(timeout=self.config.timeout + 5) # Wait for disconnect to complete
            except TimeoutError:
                self._logger.error("Timeout waiting for async disconnect in synchronous close.")
            except Exception as e:
                self._logger.error(f"Error during async disconnect in synchronous close: {e}")
        else: # No running loop, or no loop at all, perform direct blocking operations
            if self.pool:
                ssh_client_to_release = self.ssh
                if ssh_client_to_release:
                    self.pool.release_client(ssh_client_to_release)
            elif self.ssh:
                self.ssh.close()

            if self._sftp_client:
                self._sftp_client.close()
                self._sftp_client = None

            self.connected = False
            self.ssh = None
            self._logger.info("SSH connection closed via synchronous method.")
