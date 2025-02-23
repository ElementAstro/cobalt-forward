from pathlib import Path
import shutil
import asyncio
import time
from typing import Dict, Optional
from datetime import datetime, timedelta
import uuid
from loguru import logger


class UploadManager:
    """
    UploadManager class handles asynchronous file uploads by managing upload sessions,
    tracking progress, and cleaning up expired or cancelled uploads. It supports
    concurrent uploads with rate limiting, appending file chunks, and finalizing the upload.
    Attributes:
        base_dir (Path): The directory where upload files and temporary chunks are stored.
        active_uploads (Dict[str, Dict]): Dictionary mapping upload IDs to their respective upload session details,
            including file paths, received sizes, and activity timestamps.
        cleanup_task (Optional[asyncio.Task]): Background task responsible for cleaning up expired uploads.
        _max_concurrent_uploads (int): Maximum number of uploads that can be processed concurrently.
        _upload_semaphore (asyncio.Semaphore): Semaphore to manage concurrent access to the upload process.
        _upload_progress (Dict): Dictionary mapping upload IDs to their progress, including filename, total size,
            received bytes, set of processed chunks, and the upload start time.
    Methods:
        start() -> Coroutine:
            Asynchronously starts the cleanup loop for periodically removing expired uploads.
        stop() -> Coroutine:
            Asynchronously stops the cleanup loop and cancels the associated task.
        create_upload(filename: str, total_size: int) -> Coroutine[str]:
            Creates a new upload session with a unique upload ID, setting up initial progress tracking.
            Uses a semaphore to limit the number of concurrent upload sessions.
        _setup_upload_tracking(upload_id: str, filename: str, total_size: int):
            Initializes tracking information for an upload session including the filename,
            total size, received bytes, chunk information, and the start time.
        append_chunk(upload_id: str, chunk: bytes) -> Coroutine[Dict]:
            Appends a data chunk to an active upload session, writes the chunk to disk, updates the received size,
            and calculates the current progress and upload speed. Raises a ValueError if the upload session is not found.
        complete_upload(upload_id: str) -> Coroutine[Path]:
            Merges all chunk files of an upload session into a final file, cleans up temporary files,
            and removes the session from active uploads. Returns the path to the final merged file.
        get_upload_status(upload_id: str) -> Coroutine[Dict]:
            Retrieves the current status of an upload session including filename, total size, bytes received,
            progress percentage, upload speed, and the start time. Returns None if the session is not tracked.
        cancel_upload(upload_id: str) -> Coroutine:
            Cancels an active upload session by removing its temporary directory and clearing tracking
            information. Raises a ValueError if the upload session is not found.
        _calculate_upload_speed(upload_id: str) -> float:
            Calculates and returns the upload speed in bytes per second based on the elapsed time from
            the session start time.
        _cleanup_loop() -> Coroutine:
            Runs an infinite asynchronous loop that periodically checks for and cleans up upload sessions
            that have exceeded the inactivity threshold (1 hour). Logs information about any deleted sessions.
    Raises:
        ValueError: If operations are attempted on non-existent or cancelled upload sessions.
    """

    def __init__(self, base_dir: str = "/tmp/ssh_uploads"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.active_uploads: Dict[str, Dict] = {}
        self.cleanup_task = None
        self._max_concurrent_uploads = 5
        self._upload_semaphore = asyncio.Semaphore(
            self._max_concurrent_uploads)
        self._upload_progress = {}

    async def start(self):
        """启动上传管理器"""
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self):
        """停止上传管理器"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass

    async def create_upload(self, filename: str, total_size: int) -> str:
        async with self._upload_semaphore:
            upload_id = str(uuid.uuid4())
            self._setup_upload_tracking(upload_id, filename, total_size)
            return upload_id

    def _setup_upload_tracking(self, upload_id: str, filename: str, total_size: int):
        self._upload_progress[upload_id] = {
            'filename': filename,
            'total_size': total_size,
            'received': 0,
            'chunks': set(),
            'start_time': time.time()
        }

    async def append_chunk(self, upload_id: str, chunk: bytes) -> Dict:
        """追加文件块"""
        if upload_id not in self.active_uploads:
            raise ValueError("Upload session not found")

        upload = self.active_uploads[upload_id]
        chunk_path = upload["path"] / str(upload["received_size"])

        with open(chunk_path, "wb") as f:
            f.write(chunk)

        upload["received_size"] += len(chunk)
        upload["last_activity"] = datetime.now()

        progress = self._upload_progress[upload_id]
        progress['received'] += len(chunk)
        progress['chunks'].add(progress['received'])

        return {
            "received": upload["received_size"],
            "total": upload["total_size"],
            "progress": upload["received_size"] / upload["total_size"],
            'speed': self._calculate_upload_speed(upload_id)
        }

    async def complete_upload(self, upload_id: str) -> Path:
        """完成上传并合并文件"""
        if upload_id not in self.active_uploads:
            raise ValueError("Upload session not found")

        upload = self.active_uploads[upload_id]
        final_path = self.base_dir / upload["filename"]

        with open(final_path, "wb") as dest:
            for i in range(0, upload["received_size"]):
                chunk_path = upload["path"] / str(i)
                if chunk_path.exists():
                    with open(chunk_path, "rb") as src:
                        shutil.copyfileobj(src, dest)

        # 清理临时文件
        shutil.rmtree(upload["path"])
        del self.active_uploads[upload_id]

        return final_path

    async def get_upload_status(self, upload_id: str) -> Dict:
        """获取上传状态"""
        if upload_id not in self._upload_progress:
            return None

        progress = self._upload_progress[upload_id]
        return {
            "filename": progress["filename"],
            "total_size": progress["total_size"],
            "received": progress["received"],
            "progress": progress["received"] / progress["total_size"],
            "speed": self._calculate_upload_speed(upload_id),
            "start_time": progress["start_time"]
        }

    async def cancel_upload(self, upload_id: str):
        """取消上传任务"""
        if upload_id not in self.active_uploads:
            raise ValueError("Upload session not found")

        upload = self.active_uploads[upload_id]
        if upload["path"].exists():
            shutil.rmtree(upload["path"])

        del self.active_uploads[upload_id]
        if upload_id in self._upload_progress:
            del self._upload_progress[upload_id]

        logger.info(f"Upload cancelled: {upload_id}")

    def _calculate_upload_speed(self, upload_id: str) -> float:
        """计算上传速度(bytes/s)"""
        progress = self._upload_progress[upload_id]
        duration = time.time() - progress["start_time"]
        if duration == 0:
            return 0
        return progress["received"] / duration

    async def _cleanup_loop(self):
        """清理过期的上传"""
        while True:
            try:
                now = datetime.now()
                expired_ids = [
                    upload_id for upload_id, upload in self.active_uploads.items()
                    if now - upload["last_activity"] > timedelta(hours=1)
                ]

                for upload_id in expired_ids:
                    upload = self.active_uploads[upload_id]
                    shutil.rmtree(upload["path"])
                    del self.active_uploads[upload_id]
                    logger.info(f"已清理过期上传: {upload_id}")

                await asyncio.sleep(300)  # 每5分钟检查一次

            except Exception as e:
                logger.error(f"清理过期上传失败: {str(e)}")
                await asyncio.sleep(60)
