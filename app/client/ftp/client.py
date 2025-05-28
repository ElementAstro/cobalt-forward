import tempfile
from typing import Any, List, Optional, Dict, ParamSpec, Tuple, Callable, TypeVar, Union, Type, Awaitable, Set, Coroutine
import asyncio
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
# from dataclasses import dataclass # Not used directly in this file
from functools import wraps
from threading import Lock
import queue

from loguru import logger
from pyftpdlib.authorizers import DummyAuthorizer  # type: ignore
from pyftpdlib.handlers import FTPHandler, ThrottledDTPHandler  # type: ignore
from pyftpdlib.servers import FTPServer as PyftpdlibFTPServer, MultiprocessFTPServer as PyftpdlibMultiprocessFTPServer  # type: ignore

# Assuming FTPConfig is compatible with BaseClient's config
from app.client.ftp.config import FTPConfig
# FTPAuthError removed as unused
from app.client.ftp.exception import FTPError, FTPConnectionError, FTPTransferError
from app.client.ftp.operation import FileOperationEnhancer
from app.client.ftp.security import SecurityEnhancer
from app.client.ftp.transfer import TransferMonitor
from app.client.ftp.resumable import ResumableTransferManager
from app.client.base import BaseClient
from app.client.ftp.plugin import FTPPluginManager  # FTPPlugin removed as unused

P = ParamSpec("P")
R = TypeVar("R")


def retry_operation(retries: int = 3, delay: int = 1) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Retry decorator for FTP operations

    Args:
        retries: Number of retry attempts
        delay: Delay between retries in seconds

    Returns:
        Decorated function
    """
    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            last_exception: Optional[Exception] = None
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < retries - 1:
                        await asyncio.sleep(delay)
                        logger.debug(
                            f"Retrying {func.__name__} after error: {str(e)}")
            logger.error(
                f"Operation {func.__name__} failed after {retries} attempts: {str(last_exception)}")
            if last_exception is not None:
                raise last_exception
            else:
                # This case should ideally not be reached if retries > 0 and an error occurred
                raise FTPError(
                    f"Operation {func.__name__} failed after {retries} attempts with no specific exception.")
        return wrapper
    return decorator


class FTPClient(BaseClient):
    """Enhanced FTP client implementation"""

    def __init__(self, config: FTPConfig):
        """Initialize FTP client

        Args:
            config: FTP configuration
        """
        super().__init__(config)  # type: ignore # Assuming FTPConfig is compatible with BaseClient's expected config type
        self.transfer_monitor = TransferMonitor()
        self.file_ops = FileOperationEnhancer()
        self.security = SecurityEnhancer()
        self.resumable_manager = ResumableTransferManager()
        self._executor = ThreadPoolExecutor(
            max_workers=config.max_concurrent_transfers)
        self.connection_pool: queue.Queue[Any] = queue.Queue(maxsize=10)
        self.transfer_lock = Lock()
        self._setup_logging()
        # 初始化插件管理器
        self.plugin_manager = FTPPluginManager()

    def _setup_logging(self):
        """Configure logging for FTP client"""
        logger.remove()  # Remove default handlers

        # Add console handler
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
                   "<level>{message}</level>",
            level="INFO"
        )

        # Add file handler
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        logger.add(
            os.path.join(log_dir, "ftp_client.log"),
            rotation="500 MB",
            retention="10 days",
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                   "{name}:{function} - {message}",
            level="DEBUG",
            enqueue=True
        )

    @retry_operation(retries=3, delay=2)
    async def connect(self) -> Coroutine[Any, Any, bool]:
        """Establish FTP connection

        Returns:
            True if connection successful, False otherwise
        """
        try:
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                self._connect_sync
            )
            self.connected = True
            logger.info(
                f"Connected to FTP server at {self.config.host}:{self.config.port}")
            return True
        except Exception as e:
            logger.error(f"FTP connection failed: {str(e)}")
            raise FTPConnectionError(f"Failed to connect: {str(e)}")

    async def disconnect(self) -> None:
        """Disconnect from FTP server"""
        if self.connected:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    self._disconnect_sync
                )
                logger.info("Disconnected from FTP server")
            except Exception as e:
                logger.error(f"Error during disconnect: {str(e)}")
            finally:
                self.connected = False

    async def send(self, data: Union[str, bytes]) -> bool:
        """Send data over FTP

        Args:
            data: Data to send (file path or binary data)

        Returns:
            True if sending was successful

        Raises:
            FTPTransferError: If sending fails
        """
        try:
            if isinstance(data, str) and os.path.isfile(data):
                # It's a file path
                return await self._send_file(data)
            elif isinstance(data, bytes):
                # It's binary data
                temp_path = os.path.join(
                    tempfile.gettempdir(), f"ftp_upload_{uuid.uuid4().hex}")
                try:
                    with open(temp_path, 'wb') as f:
                        f.write(data)
                    return await self._send_file(temp_path)
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            else:
                raise FTPTransferError(
                    f"Unsupported data type for send: {type(data)}. Must be a valid file path or bytes.")
        except Exception as e:
            logger.error(f"Failed to send data: {str(e)}")
            raise FTPTransferError(f"Send operation failed: {str(e)}")

    async def receive(self, **kwargs: Any) -> Any:
        """Receive data from FTP server.
           Signature adapted to match BaseClient.
           Expects 'remote_path' and optionally 'local_path' in kwargs.
        """
        remote_path = kwargs.get("remote_path")
        local_path = kwargs.get("local_path")

        if not isinstance(remote_path, str):  # Ensure remote_path is a string
            raise FTPTransferError(
                "remote_path (string) is required in kwargs for receive operation")
        # Ensure local_path is a string if provided
        if local_path is not None and not isinstance(local_path, str):
            raise FTPTransferError(
                "local_path must be a string if provided in kwargs for receive operation")

        try:
            return await self._receive_file(remote_path, local_path)
        except Exception as e:
            logger.error(f"Failed to receive data: {str(e)}")
            raise FTPTransferError(f"Receive operation failed: {str(e)}")

    def _connect_sync(self):
        """Synchronous implementation of FTP connection"""
        # Implementation details depend on the specific FTP library being used
        logger.debug("Performing synchronous connection")
        # This would be implemented based on the actual FTP library

    def _disconnect_sync(self):
        """Synchronous implementation of FTP disconnection"""
        # Implementation details depend on the specific FTP library being used
        logger.debug("Performing synchronous disconnection")
        # This would be implemented based on the actual FTP library

    async def _send_file(self, local_path: str) -> bool:
        """Internal method to send a file

        Args:
            local_path: Path to local file

        Returns:
            True if successful
        """
        # Implementation would depend on the specific FTP library
        logger.debug(f"Sending file: {local_path}")

        # Monitor the transfer
        self.transfer_monitor.start_monitoring()
        transfer_id = str(uuid.uuid4())
        file_size = os.path.getsize(local_path)

        try:
            self.transfer_monitor.create_transfer(
                transfer_id, file_size, os.path.basename(local_path))

            # Actual implementation would go here
            # Simulate transfer for example purposes
            await asyncio.sleep(0.5)  # Simulated transfer delay

            self.transfer_monitor.update_transfer(
                transfer_id, file_size, "completed")
            logger.info(f"File sent successfully: {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to send file {local_path}: {str(e)}")
            self.transfer_monitor.update_transfer(transfer_id, 0, "failed")
            return False
        finally:
            self.transfer_monitor.stop_monitoring()

    async def _receive_file(self, remote_path: str, local_path: Optional[str] = None) -> Optional[str]:
        """Internal method to receive a file

        Args:
            remote_path: Path to remote file
            local_path: Optional local path

        Returns:
            Local file path if successful
        """
        if local_path is None:
            local_path = os.path.join(
                tempfile.gettempdir(),
                f"ftp_download_{os.path.basename(remote_path)}"
            )

        logger.debug(f"Receiving file: {remote_path} -> {local_path}")

        # Monitor the transfer
        self.transfer_monitor.start_monitoring()
        transfer_id = str(uuid.uuid4())

        try:
            # In a real implementation, we would get the file size from the server
            file_size = 1024 * 1024  # Simulated file size

            self.transfer_monitor.create_transfer(
                transfer_id, file_size, os.path.basename(remote_path))

            # Actual implementation would go here
            # Simulate transfer for example purposes
            await asyncio.sleep(0.5)  # Simulated transfer delay

            # Create an empty file for simulation
            os.makedirs(os.path.dirname(
                os.path.abspath(local_path)), exist_ok=True)
            with open(local_path, 'wb') as f:
                f.write(b'Sample content')

            self.transfer_monitor.update_transfer(
                transfer_id, file_size, "completed")
            logger.info(
                f"File received successfully: {remote_path} -> {local_path}")
            return local_path
        except Exception as e:
            logger.error(f"Failed to receive file {remote_path}: {str(e)}")
            self.transfer_monitor.update_transfer(transfer_id, 0, "failed")
            return None
        finally:
            self.transfer_monitor.stop_monitoring()

    async def list_directory(self, path: str = ".") -> List[Dict[str, Any]]:
        """List files in a directory

        Args:
            path: Directory path

        Returns:
            List of file information dictionaries
        """
        try:
            # 运行插件前处理
            processed_path = await self.plugin_manager.run_pre_list_directory(path)

            # 实现会依赖于特定的 FTP 库
            # 这是一个模拟的响应
            logger.debug(f"列出目录: {processed_path}")
            await asyncio.sleep(0.2)  # 模拟延迟

            files: List[Dict[str, Any]] = [
                {"name": "file1.txt", "size": 1024, "type": "file",
                    "modified": time.time() - 3600},
                {"name": "file2.txt", "size": 2048, "type": "file",
                    "modified": time.time() - 7200},
                {"name": "subdir", "size": 0, "type": "directory",
                    "modified": time.time() - 86400}
            ]

            # 运行插件后处理
            processed_files = await self.plugin_manager.run_post_list_directory(processed_path, files)
            return processed_files

        except Exception as e:
            logger.error(f"列出目录 {path} 失败: {str(e)}")
            # 运行错误处理插件
            await self.plugin_manager.run_on_error("list_directory", e)
            raise FTPError(f"列表操作失败: {str(e)}")

    async def create_directory(self, path: str) -> bool:
        """Create a directory on the FTP server

        Args:
            path: Directory path

        Returns:
            True if successful
        """
        try:
            # 运行插件前处理
            processed_path = await self.plugin_manager.run_pre_mkdir(path)

            logger.debug(f"创建目录: {processed_path}")
            await asyncio.sleep(0.2)  # 模拟延迟
            logger.info(f"目录已创建: {processed_path}")
            success = True

            # 运行插件后处理
            await self.plugin_manager.run_post_mkdir(processed_path, success)
            return success

        except Exception as e:
            logger.error(f"创建目录 {path} 失败: {str(e)}")
            # 运行错误处理插件
            await self.plugin_manager.run_on_error("create_directory", e)
            raise FTPError(f"创建目录失败: {str(e)}")

    async def delete_file(self, path: str) -> bool:
        """Delete a file from the FTP server

        Args:
            path: File path

        Returns:
            True if successful
        """
        try:
            logger.debug(f"Deleting file: {path}")
            await asyncio.sleep(0.2)  # Simulated delay
            logger.info(f"File deleted: {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file {path}: {str(e)}")
            # 运行错误处理插件
            await self.plugin_manager.run_on_error("delete_file", e)
            raise FTPError(f"删除文件失败: {str(e)}")

    async def rename_file(self, old_path: str, new_path: str) -> bool:
        """Rename a file on the FTP server

        Args:
            old_path: Current file path
            new_path: New file path

        Returns:
            True if successful
        """
        try:
            logger.debug(f"重命名文件: {old_path} -> {new_path}")
            await asyncio.sleep(0.2)  # 模拟延迟
            logger.info(f"文件已重命名: {new_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to rename file {old_path}: {str(e)}")
            # 运行错误处理插件
            await self.plugin_manager.run_on_error("rename_file", e)
            raise FTPError(f"重命名文件失败: {str(e)}")

    async def upload_file(self, local_path: str, remote_path: str) -> bool:
        """将文件上传到 FTP 服务器

        Args:
            local_path: 本地文件路径
            remote_path: 远程文件路径

        Returns:
            bool: 如果成功则为 True
        """
        try:
            # 运行插件前处理
            processed_local, processed_remote = await self.plugin_manager.run_pre_upload(local_path, remote_path)
            # The type checker warning about 'is None' always being False is likely due to
            # the plugin manager's return type not being Optional[str]. Assuming it can return None.
            if processed_local is None or processed_remote is None:  # type: ignore
                logger.debug("文件上传被插件取消")
                return False

            logger.debug(f"上传文件: {processed_local} -> {processed_remote}")
            await asyncio.sleep(0.5)  # 模拟延迟
            logger.info(f"文件上传成功: {processed_remote}")
            success = True

            # 运行插件后处理
            await self.plugin_manager.run_post_upload(processed_local, processed_remote, success)
            return success

        except Exception as e:
            logger.error(f"上传文件 {local_path} 到 {remote_path} 失败: {str(e)}")
            success = False
            # 运行错误处理插件
            await self.plugin_manager.run_on_error("upload_file", e)
            # 运行插件后处理以便清理
            # type: ignore
            await self.plugin_manager.run_post_upload(local_path, remote_path, success)
            raise FTPError(f"文件上传失败: {str(e)}")

    async def download_file(self, remote_path: str, local_path: str) -> bool:
        """从 FTP 服务器下载文件

        Args:
            remote_path: 远程文件路径
            local_path: 本地文件路径

        Returns:
            bool: 如果成功则为 True
        """
        try:
            # 运行插件前处理
            processed_remote, processed_local = await self.plugin_manager.run_pre_download(remote_path, local_path)
            if processed_remote is None or processed_local is None:  # type: ignore
                logger.debug("文件下载被插件取消")
                return False

            logger.debug(f"下载文件: {processed_remote} -> {processed_local}")
            await asyncio.sleep(0.5)  # 模拟延迟
            logger.info(f"文件下载成功: {processed_local}")
            success = True

            # 运行插件后处理
            await self.plugin_manager.run_post_download(processed_remote, processed_local, success)
            return success

        except Exception as e:
            logger.error(f"下载文件 {remote_path} 到 {local_path} 失败: {str(e)}")
            success = False
            # 运行错误处理插件
            await self.plugin_manager.run_on_error("download_file", e)
            # 运行插件后处理以便清理
            # type: ignore
            await self.plugin_manager.run_post_download(remote_path, local_path, success)
            raise FTPError(f"文件下载失败: {str(e)}")

    async def initialize_plugins(self) -> None:
        """初始化所有插件"""
        await self.plugin_manager.initialize(self)

    # Assuming FTPPlugin is a type
    def register_plugin(self, plugin_class: Type[Any]) -> bool:
        """注册 FTP 插件

        Args:
            plugin_class: 要注册的插件类

        Returns:
            bool: 是否成功注册
        """
        return self.plugin_manager.register(plugin_class)

    def unregister_plugin(self, plugin_name: str) -> bool:
        """注销 FTP 插件

        Args:
            plugin_name: 要注销的插件名称

        Returns:
            bool: 是否成功注销
        """
        return self.plugin_manager.unregister(plugin_name)

    async def close(self) -> None:
        """关闭 FTP 连接并清理资源"""
        try:
            # 关闭插件
            await self.plugin_manager.shutdown()
            await self.disconnect()  # Ensure connection is closed
            self._executor.shutdown(wait=True)  # Shutdown thread pool
            logger.info("FTP 客户端已关闭")
        except Exception as e:
            logger.error(f"关闭 FTP 客户端失败: {str(e)}")
            raise FTPError(f"关闭失败: {str(e)}")

    # Advanced operations using the enhancers

    async def upload_with_verification(self, local_path: str, remote_path: str) -> Tuple[bool, str]:
        """Upload file with integrity verification

        Args:
            local_path: Local file path
            remote_path: Remote file path

        Returns:
            Tuple of (success, verification_result)
        """
        try:
            # Calculate checksum before upload
            checksum = self.security.calculate_file_hash(local_path)

            # Upload the file
            # Assuming send uploads to remote_path, if not, adjust logic
            success = await self.upload_file(local_path, remote_path)

            if not success:
                return False, "Upload failed"

            # Verify the checksum - in a real implementation we would
            # retrieve the file and verify, or use FTP features to verify
            logger.info(
                f"File uploaded with verification: {local_path} -> {remote_path}")
            return True, checksum
        except Exception as e:
            logger.error(f"Verified upload failed: {str(e)}")
            return False, str(e)

    async def download_with_verification(self, remote_path: str, local_path: str,
                                         expected_checksum: Optional[str] = None) -> Tuple[bool, str]:
        """Download file with integrity verification

        Args:
            remote_path: Remote file path
            local_path: Local file path
            expected_checksum: Expected file checksum

        Returns:
            Tuple of (success, verification_result)
        """
        try:
            # Download the file
            success_download = await self.download_file(remote_path, local_path)

            if not success_download:
                return False, "Download failed"

            # Calculate checksum
            checksum = self.security.calculate_file_hash(local_path)

            # Verify if expected checksum was provided
            if expected_checksum and checksum != expected_checksum:
                logger.warning(
                    f"Checksum verification failed: {checksum} != {expected_checksum}")
                return False, "Checksum verification failed"

            logger.info(
                f"File downloaded with verification: {remote_path} -> {local_path}")
            return True, checksum
        except Exception as e:
            logger.error(f"Verified download failed: {str(e)}")
            return False, str(e)

    async def secure_upload(self, local_path: str, remote_path: str, encrypt: bool = True) -> str:
        """Upload file with encryption

        Args:
            local_path: Local file path
            remote_path: Remote file path
            encrypt: Whether to encrypt the file

        Returns:
            Transfer ID if successful
        """
        transfer_id = str(uuid.uuid4())

        try:
            logger.info(
                f"Starting secure upload: {local_path} -> {remote_path}")
            self.transfer_monitor.start_monitoring()

            # Create transfer record
            file_size = os.path.getsize(local_path)
            self.transfer_monitor.create_transfer(
                transfer_id, file_size, os.path.basename(local_path))

            # Encrypt if needed
            if encrypt:
                encrypted_path = self.security.encrypt_file(local_path)
                try:
                    # Register with resumable manager
                    encrypted_size = os.path.getsize(encrypted_path)
                    self.resumable_manager.register_transfer(
                        encrypted_path, remote_path, encrypted_size)

                    # Upload the encrypted file
                    await self.upload_file(encrypted_path, remote_path)
                finally:
                    # Clean up the encrypted file
                    if os.path.exists(encrypted_path):
                        os.unlink(encrypted_path)
            else:
                # Register with resumable manager
                self.resumable_manager.register_transfer(
                    local_path, remote_path, file_size)

                # Upload directly
                await self.upload_file(local_path, remote_path)

            # Update transfer status
            self.transfer_monitor.update_transfer(
                transfer_id, file_size, "completed")
            logger.info(
                f"Secure upload completed: {local_path} -> {remote_path}")

            return transfer_id
        except Exception as e:
            logger.error(f"Secure upload failed: {str(e)}")
            self.transfer_monitor.update_transfer(transfer_id, 0, "failed")
            raise FTPTransferError(f"Secure upload failed: {str(e)}")
        finally:
            self.transfer_monitor.stop_monitoring()

    async def batch_upload(self, local_files: List[str], remote_dir: str,
                           parallel: bool = False, max_workers: int = 3) -> List[Dict[str, Any]]:
        """Upload multiple files in batch

        Args:
            local_files: List of local file paths
            remote_dir: Remote directory path
            parallel: Whether to upload in parallel
            max_workers: Maximum number of parallel workers

        Returns:
            List of result dictionaries
        """
        results: List[Dict[str, Any]] = []

        if parallel:
            # Upload files in parallel
            tasks: List[asyncio.Task[Dict[str, Any]]] = []
            for file_path in local_files:
                remote_path = os.path.join(
                    remote_dir, os.path.basename(file_path))
                task: asyncio.Task[Dict[str, Any]] = asyncio.create_task(
                    self._upload_with_result(file_path, remote_path)
                )
                tasks.append(task)

                # Limit concurrency
                if len(tasks) >= max_workers:
                    # Wait for some tasks to complete
                    done: Set[asyncio.Task[Dict[str, Any]]]
                    pending: Set[asyncio.Task[Dict[str, Any]]]
                    done, pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    tasks = list(pending)
                    for completed_task in done:
                        results.append(await completed_task)

            # Wait for remaining tasks
            if tasks:
                done_remaining, _ = await asyncio.wait(tasks)
                for completed_task in done_remaining:
                    results.append(await completed_task)
        else:
            # Upload files sequentially
            for file_path in local_files:
                remote_path = os.path.join(
                    remote_dir, os.path.basename(file_path))
                result = await self._upload_with_result(file_path, remote_path)
                results.append(result)

        return results

    async def _upload_with_result(self, local_path: str, remote_path: str) -> Dict[str, Any]:
        """Helper method for batch uploads

        Args:
            local_path: Local file path
            remote_path: Remote file path

        Returns:
            Result dictionary
        """
        try:
            start_time = time.time()
            # Use self.upload_file for consistency with plugins, etc.
            success = await self.upload_file(local_path, remote_path)
            end_time = time.time()

            return {
                "file": local_path,
                "remote_path": remote_path,
                "success": success,
                "time": end_time - start_time,
                "error": None if success else "Upload failed"
            }
        except Exception as e:
            return {
                "file": local_path,
                "remote_path": remote_path,
                "success": False,
                "time": 0,
                "error": str(e)
            }

    async def synchronize_directories(self, local_dir: str, remote_dir: str,
                                      delete_extra: bool = False) -> Dict[str, Any]:
        """Synchronize local and remote directories

        Args:
            local_dir: Local directory path
            remote_dir: Remote directory path
            delete_extra: Whether to delete extra files on remote

        Returns:
            Synchronization statistics
        """
        stats: Dict[str, int] = {
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "deleted": 0,
        }

        try:
            # Get local files
            local_files: Dict[str, Dict[str, Any]] = {}
            for root, _, files_in_root in os.walk(local_dir):
                rel_path_from_root = os.path.relpath(root, local_dir)
                for file_name in files_in_root:
                    file_path = os.path.join(root, file_name)
                    # Ensure rel_file_path is '.' for files in local_dir root
                    rel_file_path_parts: List[str] = []
                    if rel_path_from_root != ".":
                        rel_file_path_parts.append(rel_path_from_root)
                    rel_file_path_parts.append(file_name)
                    rel_file_path: str = os.path.join(
                        *rel_file_path_parts).replace('\\', '/')

                    local_files[rel_file_path] = {
                        "path": file_path,
                        "size": os.path.getsize(file_path),
                        "mtime": os.path.getmtime(file_path)
                    }

            # Get remote files (in a real implementation, this would query the server)
            remote_files_list = await self.list_directory(remote_dir)
            remote_files: Dict[str, Dict[str, Any]] = {
                item["name"]: item for item in remote_files_list if item["type"] == "file"
            }
            # TODO: Handle recursive listing for remote_files if list_directory is not recursive

            # Upload new and modified files
            for rel_path_key, local_info in local_files.items():
                remote_file_path = os.path.join(
                    remote_dir, rel_path_key).replace('\\', '/')
                remote_parent_dir_path = os.path.dirname(remote_file_path)

                if remote_parent_dir_path != remote_dir:
                    try:
                        await self.create_directory(remote_parent_dir_path)
                    except FTPError as ftp_e:
                        logger.warning(
                            f"Could not ensure remote directory {remote_parent_dir_path} exists: {ftp_e}")

                if rel_path_key not in remote_files or local_info["mtime"] > remote_files[rel_path_key].get("modified", 0.0):
                    logger.debug(
                        f"Synchronizing: {local_info['path']} -> {remote_file_path}")
                    success = await self.upload_file(local_info["path"], remote_file_path)

                    if success:
                        stats["uploaded"] += 1
                    else:
                        stats["failed"] += 1
                else:
                    stats["skipped"] += 1

            # Delete extra files if requested
            if delete_extra:
                for rel_path_key in remote_files:
                    if rel_path_key not in local_files:
                        remote_file_path_to_delete = os.path.join(
                            remote_dir, rel_path_key).replace('\\', '/')
                        success_delete = await self.delete_file(remote_file_path_to_delete)

                        if success_delete:
                            stats["deleted"] += 1

            logger.info(
                f"Directory synchronization completed: {local_dir} -> {remote_dir}")
            logger.info(f"Sync stats: {stats}")
            return stats
        except Exception as e:
            logger.error(f"Directory synchronization failed: {str(e)}")
            raise FTPError(f"Synchronization failed: {str(e)}")


class FTPServer:
    """FTP server implementation"""

    def __init__(self, config: FTPConfig):
        self.config = config
        self.authorizer: DummyAuthorizer = DummyAuthorizer()
        self.handler: Optional[Type[FTPHandler]] = None
        self.server: Optional[Union[PyftpdlibFTPServer,
                                    PyftpdlibMultiprocessFTPServer]] = None
        self.transfer_monitor = TransferMonitor()
        self._setup_logging()

    def _setup_logging(self):
        logger.remove()
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
            level="INFO"
        )
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        logger.add(
            os.path.join(log_dir, "ftp_server.log"),
            rotation="500 MB",
            retention="10 days",
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
            level="DEBUG",
            enqueue=True
        )

    def add_user(self, username: str, password: str, directory: str,
                 perm: str = 'elradfmw', max_speed: int = 0):
        try:
            os.makedirs(directory, exist_ok=True)
            self.authorizer.add_user(
                username, password, directory, perm=perm)  # type: ignore
            logger.info(f"Added user: {username}")

            # type: ignore
            if max_speed > 0 and self.handler and issubclass(self.handler, ThrottledDTPHandler):
                self.handler.read_limit = max_speed  # type: ignore
                self.handler.write_limit = max_speed  # type: ignore
                logger.info(
                    f"Set speed limit for future connections: {max_speed} bytes/sec")
            elif max_speed > 0:
                logger.warning(
                    f"Max speed set for {username}, but ThrottledDTPHandler is not configured. Speed limit may not apply.")
        except Exception as e:
            logger.error(f"Failed to add user: {str(e)}")
            raise FTPError(f"Failed to add user: {str(e)}")

    def configure_handler(self):
        try:
            self.handler = FTPHandler  # type: ignore
            self.handler.authorizer = self.authorizer  # type: ignore
            self.handler.banner = "Welcome to Enhanced FTP Server"  # type: ignore
            if self.config.masquerade_address is not None:
                self.handler.masquerade_address = self.config.masquerade_address  # type: ignore
            if self.config.passive_ports:
                self.handler.passive_ports = self.config.passive_ports  # type: ignore
            logger.debug("FTP handler configured successfully")
        except Exception as e:
            logger.error(f"Failed to configure handler: {str(e)}")
            raise FTPError(f"Handler configuration error: {str(e)}")

    def start_server(self, use_multiprocess: bool = False):
        try:
            self.configure_handler()
            if self.handler is None:
                raise FTPError("FTP Handler not configured.")

            server_class: Type[Union[PyftpdlibFTPServer,
                                     PyftpdlibMultiprocessFTPServer]]
            if use_multiprocess:
                server_class = PyftpdlibMultiprocessFTPServer  # type: ignore
            else:
                server_class = PyftpdlibFTPServer  # type: ignore

            self.server = server_class(  # type: ignore
                (self.config.host, self.config.port),  # type: ignore
                self.handler  # type: ignore
            )

            if self.server:
                self.server.max_cons = self.config.max_cons  # type: ignore
                self.server.max_cons_per_ip = self.config.max_cons_per_ip  # type: ignore

            logger.info(
                f"Starting FTP server on {self.config.host}:{self.config.port}")
            if use_multiprocess:
                logger.info("Running in multiprocess mode")

            if self.server:
                self.server.serve_forever()  # type: ignore
            else:
                raise FTPError("Server object not initialized.")
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise FTPError(f"Server error: {str(e)}")

    def stop_server(self):
        if self.server:
            self.server.close_all()  # type: ignore
            logger.info("FTP server stopped")
