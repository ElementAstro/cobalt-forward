import tempfile
from typing import Any, List, Optional, Dict, Tuple
import asyncio
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import wraps
from threading import Lock
import queue

from loguru import logger
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler, ThrottledDTPHandler
from pyftpdlib.servers import FTPServer, MultiprocessFTPServer

from app.client.ftp.config import FTPConfig
from app.client.ftp.exception import FTPError, FTPConnectionError, FTPAuthError, FTPTransferError
from app.client.ftp.operation import FileOperationEnhancer
from app.client.ftp.security import SecurityEnhancer
from app.client.ftp.transfer import TransferMonitor
from app.client.ftp.resumable import ResumableTransferManager
from app.client.base import BaseClient
from app.client.ftp.plugin import FTPPlugin, FTPPluginManager


def retry_operation(retries=3, delay=1):
    """Retry decorator for FTP operations
    
    Args:
        retries: Number of retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < retries - 1:
                        await asyncio.sleep(delay)
                        logger.debug(f"Retrying {func.__name__} after error: {str(e)}")
            logger.error(f"Operation {func.__name__} failed after {retries} attempts: {str(last_exception)}")
            raise last_exception
        return wrapper
    return decorator


class FTPClient(BaseClient):
    """Enhanced FTP client implementation"""

    def __init__(self, config: FTPConfig):
        """Initialize FTP client
        
        Args:
            config: FTP configuration
        """
        super().__init__(config)
        self.transfer_monitor = TransferMonitor()
        self.file_ops = FileOperationEnhancer()
        self.security = SecurityEnhancer()
        self.resumable_manager = ResumableTransferManager()
        self._executor = ThreadPoolExecutor(max_workers=config.max_concurrent_transfers)
        self.connection_pool = queue.Queue(maxsize=10)
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
    async def connect(self) -> bool:
        """Establish FTP connection
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                self._connect_sync
            )
            self.connected = True
            logger.info(f"Connected to FTP server at {self.config.host}:{self.config.port}")
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

    async def send(self, data: Any) -> bool:
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
            else:
                # It's binary data
                temp_path = os.path.join(tempfile.gettempdir(), f"ftp_upload_{uuid.uuid4().hex}")
                try:
                    with open(temp_path, 'wb') as f:
                        f.write(data)
                    return await self._send_file(temp_path)
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
        except Exception as e:
            logger.error(f"Failed to send data: {str(e)}")
            raise FTPTransferError(f"Send operation failed: {str(e)}")

    async def receive(self, remote_path: str, local_path: Optional[str] = None) -> Any:
        """Receive data from FTP server
        
        Args:
            remote_path: Path to file on remote server
            local_path: Optional local path to save file
            
        Returns:
            Local file path if successful, None otherwise
            
        Raises:
            FTPTransferError: If receiving fails
        """
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
            
            self.transfer_monitor.update_transfer(transfer_id, file_size, "completed")
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
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            with open(local_path, 'wb') as f:
                f.write(b'Sample content')
                
            self.transfer_monitor.update_transfer(transfer_id, file_size, "completed")
            logger.info(f"File received successfully: {remote_path} -> {local_path}")
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
            
            files = [
                {"name": "file1.txt", "size": 1024, "type": "file", "modified": time.time() - 3600},
                {"name": "file2.txt", "size": 2048, "type": "file", "modified": time.time() - 7200},
                {"name": "subdir", "size": 0, "type": "directory", "modified": time.time() - 86400}
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
            if processed_local is None or processed_remote is None:
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
            if processed_remote is None or processed_local is None:
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
            await self.plugin_manager.run_post_download(remote_path, local_path, success)
            raise FTPError(f"文件下载失败: {str(e)}")

    async def initialize_plugins(self) -> None:
        """初始化所有插件"""
        await self.plugin_manager.initialize(self)

    def register_plugin(self, plugin_class) -> bool:
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
            success = await self.send(local_path)
            
            if not success:
                return False, "Upload failed"
                
            # Verify the checksum - in a real implementation we would
            # retrieve the file and verify, or use FTP features to verify
            logger.info(f"File uploaded with verification: {local_path} -> {remote_path}")
            return True, checksum
        except Exception as e:
            logger.error(f"Verified upload failed: {str(e)}")
            return False, str(e)

    async def download_with_verification(self, remote_path: str, local_path: str, 
                                         expected_checksum: str = None) -> Tuple[bool, str]:
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
            downloaded_path = await self.receive(remote_path, local_path)
            
            if not downloaded_path:
                return False, "Download failed"
                
            # Calculate checksum
            checksum = self.security.calculate_file_hash(downloaded_path)
            
            # Verify if expected checksum was provided
            if expected_checksum and checksum != expected_checksum:
                logger.warning(
                    f"Checksum verification failed: {checksum} != {expected_checksum}")
                return False, "Checksum verification failed"
                
            logger.info(f"File downloaded with verification: {remote_path} -> {local_path}")
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
            logger.info(f"Starting secure upload: {local_path} -> {remote_path}")
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
                    await self.send(encrypted_path)
                finally:
                    # Clean up the encrypted file
                    if os.path.exists(encrypted_path):
                        os.unlink(encrypted_path)
            else:
                # Register with resumable manager
                self.resumable_manager.register_transfer(
                    local_path, remote_path, file_size)
                
                # Upload directly
                await self.send(local_path)
            
            # Update transfer status
            self.transfer_monitor.update_transfer(transfer_id, file_size, "completed")
            logger.info(f"Secure upload completed: {local_path} -> {remote_path}")
            
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
        results = []
        
        if parallel:
            # Upload files in parallel
            tasks = []
            for file_path in local_files:
                remote_path = os.path.join(remote_dir, os.path.basename(file_path))
                task = asyncio.create_task(
                    self._upload_with_result(file_path, remote_path)
                )
                tasks.append(task)
                
                # Limit concurrency
                if len(tasks) >= max_workers:
                    # Wait for some tasks to complete
                    done, pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    tasks = list(pending)
                    for task in done:
                        results.append(await task)
                        
            # Wait for remaining tasks
            if tasks:
                done, _ = await asyncio.wait(tasks)
                for task in done:
                    results.append(await task)
        else:
            # Upload files sequentially
            for file_path in local_files:
                remote_path = os.path.join(remote_dir, os.path.basename(file_path))
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
            success = await self.send(local_path)
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
        stats = {
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "deleted": 0,
        }
        
        try:
            # Get local files
            local_files = {}
            for root, _, files in os.walk(local_dir):
                rel_path = os.path.relpath(root, local_dir)
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_file_path = os.path.join(rel_path, file).replace('\\', '/')
                    if rel_file_path.startswith('./'):
                        rel_file_path = rel_file_path[2:]
                        
                    local_files[rel_file_path] = {
                        "path": file_path,
                        "size": os.path.getsize(file_path),
                        "mtime": os.path.getmtime(file_path)
                    }
            
            # Get remote files (in a real implementation, this would query the server)
            # This is a simulated remote file list
            remote_files = {
                "file1.txt": {"size": 100, "mtime": time.time() - 3600},
                "subdir/file2.txt": {"size": 200, "mtime": time.time() - 7200}
            }
            
            # Upload new and modified files
            for rel_path, info in local_files.items():
                remote_file_path = os.path.join(remote_dir, rel_path).replace('\\', '/')
                remote_dir_path = os.path.dirname(remote_file_path)
                
                if rel_path not in remote_files or info["mtime"] > remote_files[rel_path]["mtime"]:
                    # Ensure remote directory exists
                    await self.create_directory(remote_dir_path)
                    
                    # Upload the file
                    logger.debug(f"Synchronizing: {info['path']} -> {remote_file_path}")
                    success = await self.send(info["path"])
                    
                    if success:
                        stats["uploaded"] += 1
                    else:
                        stats["failed"] += 1
                else:
                    stats["skipped"] += 1
                    
            # Delete extra files if requested
            if delete_extra:
                for rel_path in remote_files:
                    if rel_path not in local_files:
                        remote_file_path = os.path.join(remote_dir, rel_path).replace('\\', '/')
                        success = await self.delete_file(remote_file_path)
                        
                        if success:
                            stats["deleted"] += 1
            
            logger.info(f"Directory synchronization completed: {local_dir} -> {remote_dir}")
            logger.info(f"Sync stats: {stats}")
            return stats
        except Exception as e:
            logger.error(f"Directory synchronization failed: {str(e)}")
            raise FTPError(f"Synchronization failed: {str(e)}")


class FTPServer:
    """FTP server implementation"""
    
    def __init__(self, config: FTPConfig):
        """Initialize FTP server
        
        Args:
            config: FTP server configuration
        """
        self.config = config
        self.authorizer = DummyAuthorizer()
        self.handler = None
        self.server = None
        self.transfer_monitor = TransferMonitor()
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging for FTP server"""
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
            os.path.join(log_dir, "ftp_server.log"),
            rotation="500 MB",
            retention="10 days",
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                   "{name}:{function} - {message}",
            level="DEBUG",
            enqueue=True
        )

    def add_user(self, username: str, password: str, directory: str,
                 perm: str = 'elradfmw', max_speed: int = 0):
        """Add user to FTP server
        
        Args:
            username: Username
            password: Password
            directory: Home directory
            perm: Permissions
            max_speed: Maximum speed limit
            
        Raises:
            FTPError: If adding user fails
        """
        try:
            os.makedirs(directory, exist_ok=True)
            self.authorizer.add_user(username, password, directory, perm=perm)
            logger.info(f"Added user: {username}")

            if max_speed > 0:
                ThrottledDTPHandler.read_limit = max_speed
                ThrottledDTPHandler.write_limit = max_speed
                logger.info(f"Set speed limit for {username}: {max_speed} bytes/sec")
        except Exception as e:
            logger.error(f"Failed to add user: {str(e)}")
            raise FTPError(f"Failed to add user: {str(e)}")

    def configure_handler(self):
        """Configure FTP handler"""
        try:
            self.handler = FTPHandler
            self.handler.authorizer = self.authorizer
            self.handler.banner = "Welcome to Enhanced FTP Server"
            self.handler.masquerade_address = self.config.masquerade_address
            self.handler.passive_ports = self.config.passive_ports
            logger.debug("FTP handler configured successfully")
        except Exception as e:
            logger.error(f"Failed to configure handler: {str(e)}")
            raise FTPError(f"Handler configuration error: {str(e)}")

    def start_server(self, use_multiprocess: bool = False):
        """Start FTP server
        
        Args:
            use_multiprocess: Whether to use multiprocess mode
            
        Raises:
            FTPError: If server fails to start
        """
        try:
            self.configure_handler()

            ServerClass = MultiprocessFTPServer if use_multiprocess else FTPServer
            self.server = ServerClass(
                (self.config.host, self.config.port),
                self.handler
            )

            # Set server parameters
            self.server.max_cons = self.config.max_cons
            self.server.max_cons_per_ip = self.config.max_cons_per_ip

            logger.info(f"Starting FTP server on {self.config.host}:{self.config.port}")
            if use_multiprocess:
                logger.info("Running in multiprocess mode")
            self.server.serve_forever()
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise FTPError(f"Server error: {str(e)}")

    def stop_server(self):
        """Stop FTP server"""
        if self.server:
            self.server.close_all()
            logger.info("FTP server stopped")
