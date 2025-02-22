from typing import List
from app.client.ftp.config import FTPConfig
from app.client.ftp.exception import FTPError
from app.client.ftp.operation import FileOperationEnhancer
from app.client.ftp.security import SecurityEnhancer
from app.client.ftp.transfer import TransferMonitor
from app.client.ftp.resumable import ResumableTransferManager
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler, ThrottledDTPHandler
from pyftpdlib.servers import FTPServer, MultiprocessFTPServer
from loguru import logger
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import queue


class EnhancedFTPServer:
    def __init__(self, config: FTPConfig):
        self.config = config
        self.authorizer = DummyAuthorizer()
        self._setup_logging()
        self.handler = None
        self.server = None
        self.transfer_monitor = TransferMonitor()
        self.resumable_manager = ResumableTransferManager()
        self.security = SecurityEnhancer()
        self.file_ops = FileOperationEnhancer()
        self.connection_pool = queue.Queue(maxsize=10)
        self.transfer_lock = Lock()
        self._init_connection_pool()

    def _init_connection_pool(self):
        """初始化连接池"""
        for _ in range(self.connection_pool.maxsize):
            conn = self._create_connection()
            self.connection_pool.put(conn)

    def get_connection(self):
        """从连接池获取连接"""
        return self.connection_pool.get()

    def return_connection(self, conn):
        """归还连接到连接池"""
        self.connection_pool.put(conn)

    def _setup_logging(self):
        """配置loguru日志"""
        logger.remove()  # 移除默认处理器

        # 添加控制台处理器
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
                   "<level>{message}</level>",
            level="INFO"
        )

        # 添加文件处理器
        logger.add(
            "logs/ftp_server.log",
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
        """添加用户，支持速度限制"""
        try:
            os.makedirs(directory, exist_ok=True)
            self.authorizer.add_user(username, password, directory, perm=perm)
            logger.success(f"Added user: {username}")

            if max_speed > 0:
                ThrottledDTPHandler.read_limit = max_speed
                ThrottledDTPHandler.write_limit = max_speed
                logger.info(
                    f"Set speed limit for {username}: {max_speed} bytes/sec")
        except Exception as e:
            logger.error(f"Failed to add user: {str(e)}")
            raise FTPError(f"Failed to add user: {str(e)}")

    def configure_handler(self):
        """配置FTP处理器"""
        try:
            self.handler = FTPHandler
            self.handler.authorizer = self.authorizer
            self.handler.banner = "Welcome to Enhanced FTP Server"
            self.handler.masquerade_address = self.config.host
            self.handler.passive_ports = range(60000, 65535)
            logger.debug("FTP handler configured successfully")
        except Exception as e:
            logger.error(f"Failed to configure handler: {str(e)}")
            raise FTPError(f"Handler configuration error: {str(e)}")

    def start_server(self, use_multiprocess: bool = False):
        """启动服务器，支持多进程模式"""
        try:
            self.configure_handler()

            ServerClass = MultiprocessFTPServer if use_multiprocess else FTPServer
            self.server = ServerClass(
                (self.config.host, self.config.port),
                self.handler
            )

            # 设置服务器参数
            self.server.max_cons = 256
            self.server.max_cons_per_ip = 5

            logger.success(
                f"Starting FTP server on {self.config.host}:{self.config.port}")
            if use_multiprocess:
                logger.info("Running in multiprocess mode")
            self.server.serve_forever()
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise FTPError(f"Server error: {str(e)}")

    def upload_directory_with_compression(self, local_dir: str, remote_dir: str):
        """压缩上传目录"""
        file_enhancer = FileOperationEnhancer()
        temp_zip = f"{local_dir}.zip"
        try:
            logger.info(f"Compressing directory: {local_dir}")
            file_enhancer.compress_files([local_dir], temp_zip)
            logger.info(f"Uploading compressed file to {remote_dir}.zip")
            self.upload_file(temp_zip, f"{remote_dir}.zip")
        except Exception as e:
            logger.error(f"Failed to upload compressed directory: {str(e)}")
            raise
        finally:
            if os.path.exists(temp_zip):
                logger.debug(f"Cleaning up temporary file: {temp_zip}")
                os.remove(temp_zip)

    def download_with_verification(self, remote_path: str, local_path: str):
        """带验证的下载"""
        security = SecurityEnhancer()
        self.download_file(remote_path, local_path)
        checksum = self.ftp.voidcmd(f"XSHA256 {remote_path}").split()[1]
        if not security.verify_file_integrity(local_path, checksum):
            raise FTPError("File integrity check failed")

    def synchronize_directories(self, local_dir: str, remote_dir: str,
                                delete_extra: bool = False):
        """高级目录同步"""
        local_files = set(os.listdir(local_dir))
        remote_files = set(item['name']
                           for item in self.list_directory(remote_dir))

        # 上传新文件
        for file in local_files - remote_files:
            local_path = os.path.join(local_dir, file)
            remote_path = os.path.join(remote_dir, file)
            self.upload_file(local_path, remote_path)

        # 更新已存在的文件
        for file in local_files & remote_files:
            local_path = os.path.join(local_dir, file)
            remote_path = os.path.join(remote_dir, file)
            if os.path.getmtime(local_path) > self.ftp.sendcmd(f"MDTM {remote_path}"):
                self.upload_file(local_path, remote_path)

        # 删除多余的远程文件
        if delete_extra:
            for file in remote_files - local_files:
                self.ftp.delete(os.path.join(remote_dir, file))

    def secure_upload(self, local_path: str, remote_path: str):
        """加密上传文件"""
        try:
            logger.info(f"Starting secure upload: {local_path}")
            self.transfer_monitor.start_monitoring()
            
            # 加密文件
            encrypted_path = self.security.encrypt_file(local_path)
            
            # 注册可恢复传输
            transfer_id = self.resumable_manager.register_transfer(
                encrypted_path, remote_path, os.path.getsize(encrypted_path)
            )

            # 执行上传
            self.upload_file(encrypted_path, remote_path)
            
            # 记录传输完成
            self.transfer_monitor.stop_monitoring()
            stats = self.transfer_monitor.get_statistics()
            logger.info(f"Upload statistics: {stats}")
            
            return transfer_id
        finally:
            if 'encrypted_path' in locals():
                os.remove(encrypted_path)

    def batch_process(self, action: str, file_list: List[str], **kwargs):
        """批量处理文件操作"""
        results = []
        for file_path in file_list:
            try:
                if action == "upload":
                    result = self.secure_upload(file_path, kwargs.get("remote_dir"))
                elif action == "download":
                    result = self.download_with_verification(file_path, kwargs.get("local_dir"))
                elif action == "sync":
                    result = self.synchronize_directories(file_path, kwargs.get("remote_dir"))
                results.append({"file": file_path, "status": "success", "result": result})
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {str(e)}")
                results.append({"file": file_path, "status": "failed", "error": str(e)})
        return results

    def parallel_upload(self, file_list: List[str], remote_dir: str, max_workers: int = 5):
        """并行上传文件"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for file_path in file_list:
                future = executor.submit(self.secure_upload, file_path, remote_dir)
                futures.append(future)
            return [f.result() for f in futures]

    def resume_upload(self, transfer_id: str):
        """恢复上传"""
        transfer_info = self.resumable_manager.get_transfer_info(transfer_id)
        if not transfer_info:
            raise FTPError("Transfer not found")

        resume_point = self.resumable_manager.get_resume_point(transfer_id)
        with self.transfer_lock:
            conn = self.get_connection()
            try:
                conn.transfercmd(f"REST {resume_point}")
                self.secure_upload(
                    transfer_info['file_path'],
                    transfer_info['remote_path'],
                    resume_point=resume_point
                )
            finally:
                self.return_connection(conn)
