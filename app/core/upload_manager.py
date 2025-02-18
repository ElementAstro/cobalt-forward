from pathlib import Path
import shutil
import asyncio
import time
from typing import Dict, Optional
from datetime import datetime, timedelta
import uuid
from loguru import logger

class UploadManager:
    def __init__(self, base_dir: str = "/tmp/ssh_uploads"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.active_uploads: Dict[str, Dict] = {}
        self.cleanup_task = None
        self._max_concurrent_uploads = 5
        self._upload_semaphore = asyncio.Semaphore(self._max_concurrent_uploads)
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
