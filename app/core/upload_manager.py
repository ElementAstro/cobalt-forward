import asyncio
import os
import shutil
import uuid
import time
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Dict, List, Optional, Set, Union, BinaryIO, Callable, AsyncGenerator
from pathlib import Path
import aiofiles
import aiofiles.os
import tempfile

from app.core.base import BaseComponent
from app.utils.error_handler import error_boundary

logger = logging.getLogger(__name__)


class UploadStatus(Enum):
    """上传状态枚举"""
    PENDING = auto()        # 等待开始
    UPLOADING = auto()      # 上传中
    PAUSED = auto()         # 已暂停
    COMPLETED = auto()      # 已完成
    FAILED = auto()         # 失败
    CANCELLED = auto()      # 已取消


@dataclass
class UploadChunk:
    """上传分块信息"""
    chunk_id: str
    start_offset: int
    end_offset: int
    size: int
    checksum: str
    is_uploaded: bool = False
    attempt_count: int = 0
    upload_time: Optional[float] = None
    temp_path: Optional[str] = None


@dataclass
class UploadInfo:
    """上传信息"""
    upload_id: str
    filename: str
    file_size: int
    mime_type: str
    destination_path: str
    chunks: List[UploadChunk] = field(default_factory=list)
    status: UploadStatus = UploadStatus.PENDING
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    user_id: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    checksum_algorithm: str = "sha256"
    file_checksum: Optional[str] = None
    upload_rate: float = 0.0  # 上传速率，字节/秒
    total_uploaded: int = 0   # 已上传的总字节数
    
    def update_progress(self, bytes_uploaded: int, elapsed_time: float) -> None:
        """更新上传进度"""
        self.total_uploaded += bytes_uploaded
        if elapsed_time > 0:
            self.upload_rate = bytes_uploaded / elapsed_time
        self.updated_at = time.time()
    
    def get_progress_percentage(self) -> float:
        """获取上传进度百分比"""
        if self.file_size == 0:
            return 100.0
        return (self.total_uploaded / self.file_size) * 100
    
    def is_complete(self) -> bool:
        """检查上传是否完成"""
        return all(chunk.is_uploaded for chunk in self.chunks)


class ChunkUploadStrategy(Enum):
    """分块上传策略"""
    SEQUENTIAL = auto()     # 顺序上传
    PARALLEL = auto()       # 并行上传
    ADAPTIVE = auto()       # 自适应


class UploadManager(BaseComponent):
    """
    上传管理器，处理文件上传、恢复和管理
    
    功能：
    1. 支持大文件分块上传和断点续传
    2. 支持多种上传策略（顺序、并行、自适应）
    3. 提供上传进度和状态监控
    4. 支持文件完整性校验
    5. 自动清理临时文件
    """
    
    def __init__(self, upload_dir: str = None, temp_dir: str = None, 
                 chunk_size: int = 1024 * 1024, max_concurrent_uploads: int = 5,
                 name: str = "upload_manager"):
        """
        初始化上传管理器
        
        Args:
            upload_dir: 上传文件保存目录
            temp_dir: 临时文件目录
            chunk_size: 默认分块大小（字节）
            max_concurrent_uploads: 最大并发上传数
            name: 组件名称
        """
        super().__init__(name=name)
        self._upload_dir = upload_dir or os.path.join(tempfile.gettempdir(), "uploads")
        self._temp_dir = temp_dir or os.path.join(tempfile.gettempdir(), "temp_uploads")
        self._chunk_size = chunk_size
        self._max_concurrent_uploads = max_concurrent_uploads
        self._uploads: Dict[str, UploadInfo] = {}
        self._active_uploads: Set[str] = set()
        self._upload_queue = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max_concurrent_uploads)
        self._worker_task = None
        self._event_callbacks: Dict[str, List[Callable]] = {
            'upload_started': [],
            'upload_completed': [],
            'upload_failed': [],
            'upload_progress': [],
            'chunk_uploaded': [],
        }
        self._message_bus = None
        
        # 确保上传目录和临时目录存在
        os.makedirs(self._upload_dir, exist_ok=True)
        os.makedirs(self._temp_dir, exist_ok=True)
    
    async def _start_impl(self) -> None:
        """启动上传管理器"""
        self._worker_task = asyncio.create_task(self._upload_worker())
        logger.info(f"上传管理器启动，上传目录: {self._upload_dir}, 临时目录: {self._temp_dir}")
    
    async def _stop_impl(self) -> None:
        """停止上传管理器"""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
        
        # 暂停所有活跃上传
        for upload_id in list(self._active_uploads):
            await self.pause_upload(upload_id)
        
        logger.info("上传管理器已停止")
    
    async def _upload_worker(self) -> None:
        """上传工作线程，处理上传队列"""
        while True:
            try:
                upload_id = await self._upload_queue.get()
                
                # 检查上传是否存在且状态为待上传
                if upload_id not in self._uploads:
                    self._upload_queue.task_done()
                    continue
                
                upload_info = self._uploads[upload_id]
                if upload_info.status != UploadStatus.PENDING:
                    self._upload_queue.task_done()
                    continue
                
                # 处理上传
                async with self._semaphore:
                    self._active_uploads.add(upload_id)
                    try:
                        await self._process_upload(upload_info)
                    except Exception as e:
                        logger.error(f"处理上传 {upload_id} 时出错: {e}")
                        upload_info.status = UploadStatus.FAILED
                        await self._trigger_event('upload_failed', upload_info, error=str(e))
                    finally:
                        self._active_uploads.remove(upload_id)
                
                self._upload_queue.task_done()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"上传工作线程出错: {e}")
    
    async def _process_upload(self, upload_info: UploadInfo) -> None:
        """处理单个上传任务"""
        logger.info(f"开始处理上传: {upload_info.upload_id}, 文件: {upload_info.filename}")
        
        # 更新上传状态
        upload_info.status = UploadStatus.UPLOADING
        await self._trigger_event('upload_started', upload_info)
        
        # 根据上传策略处理分块
        if not upload_info.chunks:
            await self._prepare_chunks(upload_info)
        
        # 使用的上传策略
        strategy = ChunkUploadStrategy.PARALLEL
        
        if strategy == ChunkUploadStrategy.SEQUENTIAL:
            # 顺序上传分块
            for chunk in upload_info.chunks:
                if not chunk.is_uploaded:
                    await self._upload_chunk(upload_info, chunk)
                    if upload_info.status != UploadStatus.UPLOADING:
                        # 上传已暂停、取消或失败
                        break
        
        elif strategy == ChunkUploadStrategy.PARALLEL:
            # 并行上传分块
            pending_chunks = [chunk for chunk in upload_info.chunks if not chunk.is_uploaded]
            if pending_chunks:
                tasks = [self._upload_chunk(upload_info, chunk) for chunk in pending_chunks]
                try:
                    await asyncio.gather(*tasks)
                except Exception as e:
                    logger.error(f"并行上传分块失败: {e}")
                    upload_info.status = UploadStatus.FAILED
                    await self._trigger_event('upload_failed', upload_info, error=str(e))
        
        # 检查上传状态
        if upload_info.status == UploadStatus.UPLOADING:
            if upload_info.is_complete():
                # 所有分块已上传，合并文件
                await self._finalize_upload(upload_info)
            else:
                # 还有分块未上传
                upload_info.status = UploadStatus.PAUSED
                logger.info(f"上传未完成: {upload_info.upload_id}, 已上传: {upload_info.get_progress_percentage():.2f}%")
    
    async def _prepare_chunks(self, upload_info: UploadInfo) -> None:
        """准备上传分块"""
        total_chunks = (upload_info.file_size + self._chunk_size - 1) // self._chunk_size
        
        for i in range(total_chunks):
            start_offset = i * self._chunk_size
            end_offset = min(start_offset + self._chunk_size, upload_info.file_size)
            chunk_size = end_offset - start_offset
            
            # 创建分块信息
            chunk = UploadChunk(
                chunk_id=f"{upload_info.upload_id}_{i}",
                start_offset=start_offset,
                end_offset=end_offset,
                size=chunk_size,
                checksum="",
                temp_path=os.path.join(self._temp_dir, f"{upload_info.upload_id}_{i}.part")
            )
            
            upload_info.chunks.append(chunk)
        
        logger.debug(f"为上传 {upload_info.upload_id} 准备了 {len(upload_info.chunks)} 个分块")
    
    @error_boundary()
    async def _upload_chunk(self, upload_info: UploadInfo, chunk: UploadChunk) -> None:
        """
        上传单个分块
        
        这个方法在实际应用中需要根据具体的存储策略实现，这里仅作示例。
        """
        if chunk.is_uploaded:
            return
        
        logger.debug(f"上传分块: {chunk.chunk_id}, 大小: {chunk.size} 字节")
        
        # 模拟分块上传过程
        chunk.attempt_count += 1
        start_time = time.time()
        
        # 创建临时目录（如果不存在）
        os.makedirs(os.path.dirname(chunk.temp_path), exist_ok=True)
        
        # 这里应该是实际的分块上传逻辑
        # 例如，从源数据流读取数据并写入临时文件
        try:
            # 模拟数据写入
            async with aiofiles.open(chunk.temp_path, 'wb') as f:
                # 实际情况下，这里应该从源数据中读取对应范围的数据
                # 这里仅作示例，写入随机数据
                await f.write(os.urandom(chunk.size))
            
            # 计算校验和
            chunk.checksum = await self._calculate_checksum(chunk.temp_path)
            
            # 更新分块状态
            chunk.is_uploaded = True
            chunk.upload_time = time.time()
            
            # 更新上传进度
            elapsed_time = time.time() - start_time
            upload_info.update_progress(chunk.size, elapsed_time)
            
            # 触发分块上传完成事件
            await self._trigger_event('chunk_uploaded', upload_info, chunk=chunk)
            await self._trigger_event('upload_progress', upload_info)
            
            logger.debug(f"分块 {chunk.chunk_id} 上传完成，校验和: {chunk.checksum}")
            
        except Exception as e:
            logger.error(f"上传分块 {chunk.chunk_id} 失败: {e}")
            raise
    
    async def _finalize_upload(self, upload_info: UploadInfo) -> None:
        """完成上传，合并分块文件"""
        logger.info(f"完成上传: {upload_info.upload_id}, 文件: {upload_info.filename}")
        
        # 创建目标目录（如果不存在）
        os.makedirs(os.path.dirname(upload_info.destination_path), exist_ok=True)
        
        try:
            # 合并分块文件
            async with aiofiles.open(upload_info.destination_path, 'wb') as output_file:
                for chunk in sorted(upload_info.chunks, key=lambda c: c.start_offset):
                    if os.path.exists(chunk.temp_path):
                        async with aiofiles.open(chunk.temp_path, 'rb') as chunk_file:
                            while True:
                                data = await chunk_file.read(8192)
                                if not data:
                                    break
                                await output_file.write(data)
            
            # 计算文件完整性校验和
            upload_info.file_checksum = await self._calculate_checksum(upload_info.destination_path)
            
            # 更新上传状态
            upload_info.status = UploadStatus.COMPLETED
            upload_info.completed_at = time.time()
            
            # 清理临时文件
            await self._cleanup_temp_files(upload_info)
            
            # 触发上传完成事件
            await self._trigger_event('upload_completed', upload_info)
            
            logger.info(f"上传 {upload_info.upload_id} 成功完成，保存为: {upload_info.destination_path}")
            
        except Exception as e:
            logger.error(f"完成上传 {upload_info.upload_id} 失败: {e}")
            upload_info.status = UploadStatus.FAILED
            await self._trigger_event('upload_failed', upload_info, error=str(e))
    
    async def _calculate_checksum(self, file_path: str) -> str:
        """计算文件校验和"""
        hash_obj = hashlib.sha256()
        
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                data = await f.read(8192)
                if not data:
                    break
                hash_obj.update(data)
                
        return hash_obj.hexdigest()
    
    async def _cleanup_temp_files(self, upload_info: UploadInfo) -> None:
        """清理上传的临时文件"""
        for chunk in upload_info.chunks:
            if chunk.temp_path and os.path.exists(chunk.temp_path):
                try:
                    await aiofiles.os.remove(chunk.temp_path)
                except Exception as e:
                    logger.warning(f"清理临时文件 {chunk.temp_path} 失败: {e}")
    
    async def _trigger_event(self, event_type: str, upload_info: UploadInfo, **kwargs) -> None:
        """触发上传事件"""
        # 调用注册的回调函数
        for callback in self._event_callbacks.get(event_type, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(upload_info, **kwargs)
                else:
                    callback(upload_info, **kwargs)
            except Exception as e:
                logger.error(f"执行 {event_type} 回调时出错: {e}")
        
        # 如果存在消息总线，也发布事件
        if self._message_bus:
            try:
                event_data = {
                    "upload_id": upload_info.upload_id,
                    "filename": upload_info.filename,
                    "status": upload_info.status.name,
                    "progress": upload_info.get_progress_percentage(),
                    "timestamp": time.time(),
                    **kwargs
                }
                await self._message_bus.publish(f"upload.{event_type}", event_data)
            except Exception as e:
                logger.error(f"通过消息总线发布上传事件失败: {e}")
    
    async def create_upload(self, filename: str, file_size: int, mime_type: str, 
                          destination: str = None, user_id: str = None,
                          metadata: Dict[str, str] = None) -> str:
        """
        创建新的上传任务
        
        Args:
            filename: 文件名
            file_size: 文件大小（字节）
            mime_type: MIME类型
            destination: 目标路径，None表示使用默认路径
            user_id: 用户ID
            metadata: 元数据
            
        Returns:
            上传任务ID
        """
        # 生成上传ID
        upload_id = str(uuid.uuid4())
        
        # 确定目标路径
        if not destination:
            # 使用默认路径格式
            today = datetime.now().strftime('%Y%m%d')
            destination = os.path.join(self._upload_dir, today, filename)
        
        # 创建上传信息
        upload_info = UploadInfo(
            upload_id=upload_id,
            filename=filename,
            file_size=file_size,
            mime_type=mime_type,
            destination_path=destination,
            user_id=user_id,
            metadata=metadata or {}
        )
        
        # 存储上传信息
        self._uploads[upload_id] = upload_info
        
        logger.info(f"创建上传任务: {upload_id}, 文件: {filename}, 大小: {file_size} 字节")
        
        return upload_id
    
    async def start_upload(self, upload_id: str) -> bool:
        """
        开始上传任务
        
        Args:
            upload_id: 上传任务ID
            
        Returns:
            是否成功启动上传
        """
        if upload_id not in self._uploads:
            logger.warning(f"上传任务不存在: {upload_id}")
            return False
        
        upload_info = self._uploads[upload_id]
        
        if upload_info.status in (UploadStatus.UPLOADING, UploadStatus.COMPLETED):
            logger.warning(f"上传任务已经在进行或已完成: {upload_id}")
            return False
        
        # 将上传任务重置为待处理状态
        upload_info.status = UploadStatus.PENDING
        
        # 添加到上传队列
        await self._upload_queue.put(upload_id)
        
        logger.info(f"开始上传任务: {upload_id}")
        
        return True
    
    async def pause_upload(self, upload_id: str) -> bool:
        """
        暂停上传任务
        
        Args:
            upload_id: 上传任务ID
            
        Returns:
            是否成功暂停上传
        """
        if upload_id not in self._uploads:
            logger.warning(f"上传任务不存在: {upload_id}")
            return False
        
        upload_info = self._uploads[upload_id]
        
        if upload_info.status != UploadStatus.UPLOADING:
            logger.warning(f"上传任务未在进行中，无法暂停: {upload_id}")
            return False
        
        # 更新上传状态
        upload_info.status = UploadStatus.PAUSED
        
        logger.info(f"暂停上传任务: {upload_id}")
        
        return True
    
    async def resume_upload(self, upload_id: str) -> bool:
        """
        恢复上传任务
        
        Args:
            upload_id: 上传任务ID
            
        Returns:
            是否成功恢复上传
        """
        if upload_id not in self._uploads:
            logger.warning(f"上传任务不存在: {upload_id}")
            return False
        
        upload_info = self._uploads[upload_id]
        
        if upload_info.status != UploadStatus.PAUSED:
            logger.warning(f"上传任务未暂停，无法恢复: {upload_id}")
            return False
        
        # 通过重新开始上传来恢复
        return await self.start_upload(upload_id)
    
    async def cancel_upload(self, upload_id: str) -> bool:
        """
        取消上传任务
        
        Args:
            upload_id: 上传任务ID
            
        Returns:
            是否成功取消上传
        """
        if upload_id not in self._uploads:
            logger.warning(f"上传任务不存在: {upload_id}")
            return False
        
        upload_info = self._uploads[upload_id]
        
        if upload_info.status == UploadStatus.COMPLETED:
            logger.warning(f"上传任务已完成，无法取消: {upload_id}")
            return False
        
        # 更新上传状态
        prev_status = upload_info.status
        upload_info.status = UploadStatus.CANCELLED
        
        # 清理临时文件
        await self._cleanup_temp_files(upload_info)
        
        logger.info(f"取消上传任务: {upload_id}, 之前状态: {prev_status.name}")
        
        return True
    
    async def get_upload_info(self, upload_id: str) -> Optional[UploadInfo]:
        """
        获取上传任务信息
        
        Args:
            upload_id: 上传任务ID
            
        Returns:
            上传任务信息，不存在则返回None
        """
        return self._uploads.get(upload_id)
    
    async def get_upload_status(self, upload_id: str) -> Optional[UploadStatus]:
        """
        获取上传任务状态
        
        Args:
            upload_id: 上传任务ID
            
        Returns:
            上传任务状态，不存在则返回None
        """
        upload_info = await self.get_upload_info(upload_id)
        return upload_info.status if upload_info else None
    
    async def get_upload_progress(self, upload_id: str) -> Optional[float]:
        """
        获取上传任务进度百分比
        
        Args:
            upload_id: 上传任务ID
            
        Returns:
            上传进度百分比(0-100)，不存在则返回None
        """
        upload_info = await self.get_upload_info(upload_id)
        return upload_info.get_progress_percentage() if upload_info else None
    
    async def list_uploads(self, user_id: Optional[str] = None, 
                         status: Optional[UploadStatus] = None) -> List[UploadInfo]:
        """
        列出上传任务
        
        Args:
            user_id: 过滤特定用户的上传，None表示不过滤
            status: 过滤特定状态的上传，None表示不过滤
            
        Returns:
            上传任务信息列表
        """
        result = []
        
        for upload_info in self._uploads.values():
            # 应用过滤条件
            if user_id and upload_info.user_id != user_id:
                continue
            if status and upload_info.status != status:
                continue
            
            result.append(upload_info)
        
        return result
    
    async def cleanup_uploads(self, max_age_days: int = 7) -> int:
        """
        清理旧的上传任务
        
        Args:
            max_age_days: 最大保留天数
            
        Returns:
            清理的上传任务数量
        """
        now = time.time()
        max_age_seconds = max_age_days * 24 * 60 * 60
        to_delete = []
        
        for upload_id, upload_info in self._uploads.items():
            # 已经完成/失败/取消的上传，且超过最大保留时间
            if (upload_info.status in (UploadStatus.COMPLETED, UploadStatus.FAILED, UploadStatus.CANCELLED) and
                now - upload_info.updated_at > max_age_seconds):
                to_delete.append(upload_id)
        
        # 清理上传
        for upload_id in to_delete:
            if upload_id in self._uploads:
                await self._cleanup_temp_files(self._uploads[upload_id])
                del self._uploads[upload_id]
        
        logger.info(f"清理了 {len(to_delete)} 个旧的上传任务")
        
        return len(to_delete)
    
    def add_event_listener(self, event_type: str, callback: Callable) -> None:
        """
        添加上传事件监听器
        
        Args:
            event_type: 事件类型 ('upload_started', 'upload_completed', 'upload_failed', 'upload_progress', 'chunk_uploaded')
            callback: 回调函数，接收上传信息和其他相关参数
        """
        if event_type not in self._event_callbacks:
            self._event_callbacks[event_type] = []
        
        self._event_callbacks[event_type].append(callback)
        logger.debug(f"添加 {event_type} 事件监听器")
    
    def remove_event_listener(self, event_type: str, callback: Callable) -> bool:
        """
        移除上传事件监听器
        
        Args:
            event_type: 事件类型
            callback: 回调函数
            
        Returns:
            是否成功移除
        """
        if event_type not in self._event_callbacks:
            return False
        
        try:
            self._event_callbacks[event_type].remove(callback)
            logger.debug(f"移除 {event_type} 事件监听器")
            return True
        except ValueError:
            return False
    
    async def set_message_bus(self, message_bus) -> None:
        """设置消息总线引用"""
        self._message_bus = message_bus
        logger.info("消息总线已与上传管理器集成")
    
    async def handle_message(self, topic: str, data: Any) -> None:
        """处理接收到的消息"""
        if topic.startswith("upload.command."):
            command = topic.split(".")[-1]
            
            if command == "create":
                # 创建上传任务
                if isinstance(data, dict) and all(k in data for k in ["filename", "size", "mime_type"]):
                    upload_id = await self.create_upload(
                        filename=data["filename"],
                        file_size=data["size"],
                        mime_type=data["mime_type"],
                        destination=data.get("destination"),
                        user_id=data.get("user_id"),
                        metadata=data.get("metadata")
                    )
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("upload.response.created", {
                            "upload_id": upload_id,
                            "success": True
                        })
            
            elif command == "start":
                # 开始上传
                if isinstance(data, dict) and "upload_id" in data:
                    success = await self.start_upload(data["upload_id"])
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("upload.response.started", {
                            "upload_id": data["upload_id"],
                            "success": success
                        })
            
            elif command == "pause":
                # 暂停上传
                if isinstance(data, dict) and "upload_id" in data:
                    success = await self.pause_upload(data["upload_id"])
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("upload.response.paused", {
                            "upload_id": data["upload_id"],
                            "success": success
                        })
            
            elif command == "resume":
                # 恢复上传
                if isinstance(data, dict) and "upload_id" in data:
                    success = await self.resume_upload(data["upload_id"])
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("upload.response.resumed", {
                            "upload_id": data["upload_id"],
                            "success": success
                        })
            
            elif command == "cancel":
                # 取消上传
                if isinstance(data, dict) and "upload_id" in data:
                    success = await self.cancel_upload(data["upload_id"])
                    
                    # 发送响应
                    if self._message_bus:
                        await self._message_bus.publish("upload.response.cancelled", {
                            "upload_id": data["upload_id"],
                            "success": success
                        })
            
            elif command == "status":
                # 查询上传状态
                if isinstance(data, dict) and "upload_id" in data:
                    upload_info = await self.get_upload_info(data["upload_id"])
                    
                    # 发送响应
                    if self._message_bus:
                        if upload_info:
                            await self._message_bus.publish("upload.response.status", {
                                "upload_id": data["upload_id"],
                                "status": upload_info.status.name,
                                "progress": upload_info.get_progress_percentage(),
                                "success": True
                            })
                        else:
                            await self._message_bus.publish("upload.response.status", {
                                "upload_id": data["upload_id"],
                                "success": False,
                                "error": "Upload not found"
                            })
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取上传管理器指标"""
        metrics = super().metrics
        
        # 添加上传相关指标
        uploads_count = {status.name: 0 for status in UploadStatus}
        total_bytes = 0
        active_count = len(self._active_uploads)
        
        for upload_info in self._uploads.values():
            uploads_count[upload_info.status.name] += 1
            total_bytes += upload_info.total_uploaded
        
        metrics.update({
            'uploads_count': uploads_count,
            'total_uploads': len(self._uploads),
            'active_uploads': active_count,
            'queue_size': self._upload_queue.qsize(),
            'total_bytes_uploaded': total_bytes,
            'upload_dir': self._upload_dir,
            'temp_dir': self._temp_dir,
            'chunk_size': self._chunk_size,
            'max_concurrent_uploads': self._max_concurrent_uploads
        })
        
        return metrics
