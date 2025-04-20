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
from typing import Dict, List, Union, Optional, Generator, Callable, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from .config import SSHConfig
from .exceptions import (
    SSHException, SSHConnectionError, SSHCommandError, 
    SSHTimeoutError, SSHFileTransferError
)
from .pool import SSHConnectionPool
from .stream import SSHStreamHandler
from .utils import get_local_files, get_file_hash
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
        self.ssh = None
        self.pool = SSHConnectionPool(config) if config.pool else None
        self.stream_handler = SSHStreamHandler()
        self._setup_logging()
        self._file_cache = {}
        self._cache_timeout = 300  # 5 minutes cache timeout
        self._executor = ThreadPoolExecutor(max_workers=5)
        self._sftp_client = None
        self._lock = Lock()

    def _setup_logging(self):
        """Configure logging for SSH client"""
        self._logger = logging.getLogger("SSHClient")
        self._logger.setLevel(logging.INFO)
        
        # Only add handler if none exists
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
        if self.connected:
            return True

        try:
            if self.pool:
                # Get connection from pool
                self.ssh = self.pool.get_client()
                self.connected = True
                self._logger.info(f"Using pooled connection to {self.config.hostname}:{self.config.port}")
                return True

            # Direct connection
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
            elif self.config.private_key_path:
                connect_kwargs['key_filename'] = self.config.private_key_path

            self._logger.info(f"Connecting to {self.config.hostname}:{self.config.port}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self.ssh.connect(**connect_kwargs)
            )

            # Set keepalive
            if self.config.keep_alive:
                transport = self.ssh.get_transport()
                if transport:
                    transport.set_keepalive(self.config.keep_alive_interval)
                    self._logger.debug(f"Keepalive set to {self.config.keep_alive_interval} seconds")

            self.connected = True
            self._logger.info(f"Successfully connected to {self.config.hostname}:{self.config.port}")
            return True

        except Exception as e:
            self._logger.error(f"SSH connection failed: {str(e)}")
            self.connected = False
            raise SSHConnectionError(f"Failed to connect to {self.config.hostname}:{self.config.port}: {str(e)}") from e

    async def disconnect(self) -> None:
        """
        Disconnect from SSH server and cleanup resources
        """
        if not self.connected:
            return

        try:
            if self.pool and self.ssh:
                self._logger.debug("Returning connection to pool")
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: self.pool.release_client(self.ssh)
                )
            elif self.ssh:
                self._logger.debug("Closing SSH connection")
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self.ssh.close
                )

            if self._sftp_client:
                self._logger.debug("Closing SFTP client")
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self._sftp_client.close
                )
                self._sftp_client = None
        except Exception as e:
            self._logger.error(f"Error during disconnect: {str(e)}")
        finally:
            self.connected = False
            self.ssh = None
            self._logger.info("SSH connection closed")

    async def execute_with_retry(self, func, *args, **kwargs) -> Any:
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
                if not self.connected and attempt > 0:
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
                if isinstance(e, (paramiko.SSHException, ConnectionError, SSHConnectionError)):
                    self.connected = False

    async def execute_command(self, command: str, timeout: int = None) -> Dict[str, str]:
        """
        Execute a command on the remote server
        
        Args:
            command: Command to execute
            timeout: Command timeout in seconds (uses config timeout if None)
            
        Returns:
            Dict[str, str]: Dictionary with stdout, stderr, and exit_code keys
            
        Raises:
            SSHCommandError: If command execution fails
        """
        if not self.connected:
            raise SSHConnectionError("SSH not connected")

        actual_timeout = timeout or self.config.timeout
        self._logger.debug(f"Executing command: {command}")

        try:
            stdin, stdout, stderr = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self.ssh.exec_command(
                    command,
                    timeout=actual_timeout,
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
        except asyncio.TimeoutError:
            self._logger.error(f"Command timed out after {actual_timeout}s: {command}")
            raise SSHTimeoutError(f"Command timed out after {actual_timeout} seconds: {command}")
        except Exception as e:
            self._logger.error(f"Command execution failed: {command}, error: {str(e)}")
            # Check if connection needs to be re-established
            if isinstance(e, (paramiko.SSHException, EOFError, ConnectionError)):
                self.connected = False
                
            raise SSHCommandError(
                message=f"Command execution failed: {str(e)}",
                command=command
            ) from e

    @staticmethod
    async def _read_stream(stream) -> str:
        """
        Asynchronously read stream data
        
        Args:
            stream: Stream to read from
            
        Returns:
            str: Data read from stream
        """
        output = []

        async def read_all():
            data = await asyncio.get_event_loop().run_in_executor(None, stream.read)
            return data.decode('utf-8', errors='replace')

        try:
            data = await asyncio.wait_for(read_all(), timeout=30)
            output.append(data)
        except asyncio.TimeoutError:
            output.append("[READ TIMEOUT]")

        return ''.join(output)

    async def bulk_execute(self, commands: List[str], timeout: int = None) -> List[Dict[str, Any]]:
        """
        Execute multiple commands in parallel
        
        Args:
            commands: List of commands to execute
            timeout: Command timeout in seconds
            
        Returns:
            List[Dict[str, Any]]: List of command execution results
        """
        return await asyncio.gather(
            *[self.execute_with_retry(self.execute_command, command, timeout) for command in commands]
        )

    async def _get_sftp(self):
        """
        Get or create SFTP client
        
        Returns:
            paramiko.SFTPClient: SFTP client
            
        Raises:
            SSHConnectionError: If SSH not connected
        """
        if not self.connected:
            raise SSHConnectionError("SSH not connected")

        async with asyncio.Lock():
            if not self._sftp_client:
                self._logger.debug("Opening new SFTP client")
                self._sftp_client = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self.ssh.open_sftp
                )
            return self._sftp_client

    async def _ensure_remote_dir(self, sftp, remote_dir: str):
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
            # Recursively create directory
            parent_dir = os.path.dirname(remote_dir)
            if parent_dir and parent_dir != '/':
                await self._ensure_remote_dir(sftp, parent_dir)

            try:
                self._logger.debug(f"Creating remote directory: {remote_dir}")
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: sftp.mkdir(remote_dir)
                )
            except Exception as e:
                # Ignore directory exists error, might be from concurrent creation
                if "exists" not in str(e).lower():
                    self._logger.error(f"Failed to create remote directory: {remote_dir}, error: {str(e)}")
                    raise

    async def upload_file(self, local_path: str, remote_path: str,
                         callback: Optional[Callable] = None):
        """
        Upload file to remote server with optimizations for large files
        
        Args:
            local_path: Local file path
            remote_path: Remote file path
            callback: Progress callback function
            
        Raises:
            FileNotFoundError: If local file does not exist
            SSHFileTransferError: If file transfer fails
        """
        if not self.connected:
            raise SSHConnectionError("SSH not connected")

        # Check local file
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        file_size = os.path.getsize(local_path)
        self._logger.info(f"Uploading file: {local_path} to {remote_path} ({file_size} bytes)")

        # Use compression for large files
        if file_size > 10 * 1024 * 1024:  # 10MB threshold for compression
            self._logger.info(f"Using compression for large file: {local_path}")
            await self._upload_compressed(local_path, remote_path, callback)
            return

        # Use async IO for standard upload
        sftp = await self._get_sftp()
        try:
            # Ensure remote directory exists
            remote_dir = os.path.dirname(remote_path)
            await self._ensure_remote_dir(sftp, remote_dir)

            # Progress callback wrapper
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(local_path, remote_path, percentage)
                progress_callback = progress_wrapper

            # Upload file
            start_time = time.time()
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.put(local_path, remote_path,
                                 callback=progress_callback)
            )
            elapsed = time.time() - start_time
            transfer_rate = file_size / (elapsed * 1024 * 1024) if elapsed > 0 else 0
            self._logger.info(f"Upload completed in {elapsed:.2f}s ({transfer_rate:.2f} MB/s)")
            
        except Exception as e:
            self._logger.error(f"File upload failed: {local_path} -> {remote_path}, error: {str(e)}")
            raise SSHFileTransferError(
                message=f"File upload failed: {str(e)}",
                source_path=local_path,
                destination_path=remote_path
            ) from e

    async def _upload_compressed(self, local_path: str, remote_path: str,
                              callback: Optional[Callable] = None):
        """
        Upload file with compression
        
        Args:
            local_path: Local file path
            remote_path: Remote file path
            callback: Progress callback function
        """
        compressed_path = f"{local_path}.gz"
        try:
            # Compress file
            self._logger.debug(f"Compressing file: {local_path}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: self._compress_file(local_path, compressed_path)
            )

            # Upload compressed file
            sftp = await self._get_sftp()
            compressed_remote_path = f"{remote_path}.gz"

            # Ensure remote directory exists
            remote_dir = os.path.dirname(remote_path)
            await self._ensure_remote_dir(sftp, remote_dir)

            # Progress callback wrapper
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(local_path, remote_path, percentage)
                progress_callback = progress_wrapper

            # Upload compressed file
            self._logger.debug(f"Uploading compressed file: {compressed_path}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.put(
                    compressed_path, compressed_remote_path, callback=progress_callback)
            )

            # Decompress file on remote server
            self._logger.debug(f"Decompressing file on remote server: {compressed_remote_path}")
            decompress_cmd = f"gunzip -c {compressed_remote_path} > {remote_path} && rm {compressed_remote_path}"
            result = await self.execute_command(decompress_cmd)
            
            if result['exit_code'] != 0:
                raise SSHFileTransferError(
                    message=f"Remote decompression failed: {result['stderr']}",
                    source_path=local_path,
                    destination_path=remote_path
                )
                
            self._logger.info(f"Compression upload completed: {local_path} -> {remote_path}")

        finally:
            # Cleanup temporary compressed file
            if os.path.exists(compressed_path):
                os.remove(compressed_path)
                self._logger.debug(f"Removed temporary compressed file: {compressed_path}")

    @staticmethod
    def _compress_file(source_path: str, dest_path: str):
        """
        Compress file using gzip
        
        Args:
            source_path: Source file path
            dest_path: Destination compressed file path
        """
        with open(source_path, 'rb') as f_in, gzip.open(dest_path, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)

    async def download_file(self, remote_path: str, local_path: str,
                        callback: Optional[Callable] = None):
        """
        Download file from remote server with optimizations for large files
        
        Args:
            remote_path: Remote file path
            local_path: Local file path
            callback: Progress callback function
            
        Raises:
            SSHFileTransferError: If file transfer fails
        """
        if not self.connected:
            raise SSHConnectionError("SSH not connected")

        # Ensure local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        sftp = await self._get_sftp()
        try:
            # Get remote file size
            file_attrs = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.stat(remote_path)
            )
            file_size = file_attrs.st_size
            self._logger.info(f"Downloading file: {remote_path} to {local_path} ({file_size} bytes)")

            # Use compression for large files
            if file_size > 10 * 1024 * 1024:  # 10MB threshold for compression
                self._logger.info(f"Using compression for large file: {remote_path}")
                await self._download_compressed(remote_path, local_path, callback)
                return

            # Progress callback wrapper
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(remote_path, local_path, percentage)
                progress_callback = progress_wrapper

            # Download file
            start_time = time.time()
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.get(remote_path, local_path,
                                 callback=progress_callback)
            )
            elapsed = time.time() - start_time
            transfer_rate = file_size / (elapsed * 1024 * 1024) if elapsed > 0 else 0
            self._logger.info(f"Download completed in {elapsed:.2f}s ({transfer_rate:.2f} MB/s)")
            
        except Exception as e:
            self._logger.error(f"File download failed: {remote_path} -> {local_path}, error: {str(e)}")
            # Cleanup incomplete download
            if os.path.exists(local_path):
                os.remove(local_path)
                self._logger.debug(f"Removed incomplete download: {local_path}")
                
            raise SSHFileTransferError(
                message=f"File download failed: {str(e)}",
                source_path=remote_path,
                destination_path=local_path
            ) from e

    async def _download_compressed(self, remote_path: str, local_path: str,
                              callback: Optional[Callable] = None):
        """
        Download file with compression
        
        Args:
            remote_path: Remote file path
            local_path: Local file path
            callback: Progress callback function
        """
        # Compressed file paths
        compressed_remote_path = f"{remote_path}.gz"
        compressed_local_path = f"{local_path}.gz"

        try:
            # Compress file on remote server
            self._logger.debug(f"Compressing file on remote server: {remote_path}")
            compress_cmd = f"gzip -c {remote_path} > {compressed_remote_path}"
            result = await self.execute_command(compress_cmd)
            if result['exit_code'] != 0:
                raise SSHFileTransferError(
                    message=f"Remote compression failed: {result['stderr']}",
                    source_path=remote_path,
                    destination_path=local_path
                )

            # Download compressed file
            sftp = await self._get_sftp()

            # Progress callback wrapper
            progress_callback = None
            if callback:
                def progress_wrapper(transferred, total):
                    percentage = (transferred / total) * 100
                    callback(remote_path, local_path, percentage)
                progress_callback = progress_wrapper

            # Download compressed file
            self._logger.debug(f"Downloading compressed file: {compressed_remote_path}")
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: sftp.get(
                    compressed_remote_path, compressed_local_path, callback=progress_callback)
            )

            # Decompress locally
            self._logger.debug(f"Decompressing file locally: {compressed_local_path}")
            with gzip.open(compressed_local_path, 'rb') as f_in, open(local_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            # Delete remote temporary file
            self._logger.debug(f"Removing remote temporary file: {compressed_remote_path}")
            await self.execute_command(f"rm -f {compressed_remote_path}")
            self._logger.info(f"Compression download completed: {remote_path} -> {local_path}")

        finally:
            # Cleanup local temporary file
            if os.path.exists(compressed_local_path):
                os.remove(compressed_local_path)
                self._logger.debug(f"Removed local temporary file: {compressed_local_path}")

    # Method for backward compatibility
    def exec_command(self, command: str, timeout: float = None, get_pty: bool = False) -> Dict[str, Any]:
        """
        Synchronous command execution (kept for backward compatibility)
        
        Args:
            command: Command to execute
            timeout: Command timeout in seconds
            get_pty: Whether to allocate a pseudo-terminal
            
        Returns:
            Dict[str, Any]: Command execution result
            
        Raises:
            SSHCommandError: If command execution fails
        """
        if not self.connected:
            raise SSHConnectionError("SSH not connected")

        actual_timeout = timeout or self.config.timeout
        self._logger.debug(f"Executing command synchronously: {command}")
        
        try:
            stdin, stdout, stderr = self.ssh.exec_command(
                command, timeout=actual_timeout, get_pty=get_pty)

            exit_status = stdout.channel.recv_exit_status()
            stdout_data = stdout.read().decode('utf-8', errors='replace')
            stderr_data = stderr.read().decode('utf-8', errors='replace')

            if exit_status != 0:
                self._logger.warning(f"Command failed (exit code {exit_status}): {command}")

            return {
                'stdout': stdout_data,
                'stderr': stderr_data,
                'exit_code': exit_status
            }
        except Exception as e:
            self._logger.error(f"Command execution failed: {command}, error: {str(e)}")
            if isinstance(e, (paramiko.SSHException, EOFError, ConnectionError)):
                self.connected = False
                
            raise SSHCommandError(
                message=f"Command execution failed: {str(e)}",
                command=command
            ) from e

    def close(self):
        """Close the SSH connection (synchronous)"""
        if hasattr(self, 'loop') and self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self.disconnect(), self.loop)
        else:
            if self.pool and self.ssh:
                self.pool.release_client(self.ssh)
            elif self.ssh:
                self.ssh.close()
                
            if self._sftp_client:
                self._sftp_client.close()
                self._sftp_client = None
                
            self.connected = False
            self.ssh = None
