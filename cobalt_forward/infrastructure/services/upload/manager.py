"""
Upload Manager implementation for the Cobalt Forward application.

This module provides upload management with support for chunked uploads,
resume capabilities, and progress monitoring.
"""

import asyncio
import os
import uuid
import time
import logging
import hashlib
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Set, Callable, Any, AsyncGenerator

try:
    import aiofiles
    import aiofiles.os
except ImportError:
    aiofiles = None

from ....core.interfaces.upload import (
    IUploadManager, IUploadSession, UploadStatus, UploadStrategy,
    UploadInfo, UploadChunk
)
from ....core.interfaces.lifecycle import IComponent
from ....core.interfaces.messaging import IEventBus

logger = logging.getLogger(__name__)


class UploadSession(IUploadSession):
    """Upload session implementation."""
    
    def __init__(
        self,
        upload_info: UploadInfo,
        upload_dir: str,
        temp_dir: str,
        event_bus: Optional[IEventBus] = None
    ):
        self._info = upload_info
        self._upload_dir = upload_dir
        self._temp_dir = temp_dir
        self._event_bus = event_bus
        self._chunks: List[UploadChunk] = []
        self._active_chunks: Set[int] = set()
        self._lock = asyncio.Lock()
    
    async def start(self) -> bool:
        """Start the upload session."""
        async with self._lock:
            if self._info.status != UploadStatus.PENDING:
                logger.warning(f"Upload {self._info.upload_id} is not in pending state")
                return False
            
            self._info.status = UploadStatus.UPLOADING
            self._info.updated_at = time.time()
            
            # Prepare chunks
            await self._prepare_chunks()
            
            if self._event_bus:
                await self._event_bus.publish("upload.started", {
                    "upload_id": self._info.upload_id,
                    "filename": self._info.filename,
                    "file_size": self._info.file_size
                })
            
            logger.info(f"Started upload session: {self._info.upload_id}")
            return True
    
    async def pause(self) -> bool:
        """Pause the upload session."""
        async with self._lock:
            if self._info.status != UploadStatus.UPLOADING:
                return False
            
            self._info.status = UploadStatus.PAUSED
            self._info.updated_at = time.time()
            
            if self._event_bus:
                await self._event_bus.publish("upload.paused", {
                    "upload_id": self._info.upload_id
                })
            
            logger.info(f"Paused upload session: {self._info.upload_id}")
            return True
    
    async def resume(self) -> bool:
        """Resume the upload session."""
        async with self._lock:
            if self._info.status != UploadStatus.PAUSED:
                return False
            
            self._info.status = UploadStatus.UPLOADING
            self._info.updated_at = time.time()
            
            if self._event_bus:
                await self._event_bus.publish("upload.resumed", {
                    "upload_id": self._info.upload_id
                })
            
            logger.info(f"Resumed upload session: {self._info.upload_id}")
            return True
    
    async def cancel(self) -> bool:
        """Cancel the upload session."""
        async with self._lock:
            if self._info.status in (UploadStatus.COMPLETED, UploadStatus.CANCELLED):
                return False
            
            self._info.status = UploadStatus.CANCELLED
            self._info.updated_at = time.time()
            
            # Clean up temporary files
            await self._cleanup_temp_files()
            
            if self._event_bus:
                await self._event_bus.publish("upload.cancelled", {
                    "upload_id": self._info.upload_id
                })
            
            logger.info(f"Cancelled upload session: {self._info.upload_id}")
            return True
    
    async def upload_chunk(self, chunk_data: bytes, chunk_index: int) -> bool:
        """Upload a specific chunk."""
        if chunk_index >= len(self._chunks):
            logger.error(f"Invalid chunk index: {chunk_index}")
            return False
        
        chunk = self._chunks[chunk_index]
        
        try:
            # Write chunk to temporary file
            temp_path = os.path.join(self._temp_dir, f"{self._info.upload_id}_chunk_{chunk_index}")
            
            if aiofiles:
                async with aiofiles.open(temp_path, 'wb') as f:
                    await f.write(chunk_data)
            else:
                with open(temp_path, 'wb') as f:
                    f.write(chunk_data)
            
            # Update chunk info
            chunk.uploaded = True
            chunk.upload_time = time.time()
            chunk.checksum = hashlib.sha256(chunk_data).hexdigest()
            
            # Update upload progress
            self._info.bytes_uploaded += len(chunk_data)
            self._info.chunks_completed += 1
            self._info.updated_at = time.time()
            
            # Check if upload is complete
            if self._info.chunks_completed >= self._info.chunks_total:
                await self._finalize_upload()
            
            if self._event_bus:
                await self._event_bus.publish("upload.chunk_completed", {
                    "upload_id": self._info.upload_id,
                    "chunk_index": chunk_index,
                    "progress": self._info.progress_percentage
                })
            
            return True
            
        except Exception as e:
            chunk.retry_count += 1
            self._info.error_count += 1
            self._info.last_error = str(e)
            
            logger.error(f"Failed to upload chunk {chunk_index}: {e}")
            
            if self._event_bus:
                await self._event_bus.publish("upload.chunk_error", {
                    "upload_id": self._info.upload_id,
                    "chunk_index": chunk_index,
                    "error": str(e)
                })
            
            return False
    
    def get_info(self) -> UploadInfo:
        """Get upload session information."""
        return self._info
    
    def get_progress(self) -> Dict[str, Any]:
        """Get detailed progress information."""
        return {
            "upload_id": self._info.upload_id,
            "status": self._info.status.value,
            "progress_percentage": self._info.progress_percentage,
            "bytes_uploaded": self._info.bytes_uploaded,
            "file_size": self._info.file_size,
            "chunks_completed": self._info.chunks_completed,
            "chunks_total": self._info.chunks_total,
            "upload_speed": self._info.upload_speed,
            "estimated_time_remaining": self._info.estimated_time_remaining,
            "created_at": self._info.created_at,
            "updated_at": self._info.updated_at
        }
    
    async def _prepare_chunks(self) -> None:
        """Prepare upload chunks."""
        chunk_size = self._info.chunk_size
        file_size = self._info.file_size
        
        chunks_total = (file_size + chunk_size - 1) // chunk_size
        self._info.chunks_total = chunks_total
        
        for i in range(chunks_total):
            start_offset = i * chunk_size
            end_offset = min(start_offset + chunk_size, file_size)
            
            chunk = UploadChunk(
                chunk_id=f"{self._info.upload_id}_chunk_{i}",
                upload_id=self._info.upload_id,
                chunk_index=i,
                start_offset=start_offset,
                end_offset=end_offset,
                size=end_offset - start_offset
            )
            
            self._chunks.append(chunk)
    
    async def _finalize_upload(self) -> None:
        """Finalize the upload by combining chunks."""
        try:
            # Combine all chunks into final file
            final_path = os.path.join(self._upload_dir, self._info.filename)
            
            if aiofiles:
                async with aiofiles.open(final_path, 'wb') as final_file:
                    for i in range(len(self._chunks)):
                        chunk_path = os.path.join(self._temp_dir, f"{self._info.upload_id}_chunk_{i}")
                        if os.path.exists(chunk_path):
                            async with aiofiles.open(chunk_path, 'rb') as chunk_file:
                                chunk_data = await chunk_file.read()
                                await final_file.write(chunk_data)
            else:
                with open(final_path, 'wb') as final_file:
                    for i in range(len(self._chunks)):
                        chunk_path = os.path.join(self._temp_dir, f"{self._info.upload_id}_chunk_{i}")
                        if os.path.exists(chunk_path):
                            with open(chunk_path, 'rb') as chunk_file:
                                chunk_data = chunk_file.read()
                                final_file.write(chunk_data)
            
            # Update status
            self._info.status = UploadStatus.COMPLETED
            self._info.completed_at = time.time()
            self._info.updated_at = time.time()
            
            # Clean up temporary files
            await self._cleanup_temp_files()
            
            if self._event_bus:
                await self._event_bus.publish("upload.completed", {
                    "upload_id": self._info.upload_id,
                    "filename": self._info.filename,
                    "final_path": final_path
                })
            
            logger.info(f"Upload completed: {self._info.upload_id}")
            
        except Exception as e:
            self._info.status = UploadStatus.FAILED
            self._info.last_error = str(e)
            self._info.error_count += 1
            
            logger.error(f"Failed to finalize upload {self._info.upload_id}: {e}")
            
            if self._event_bus:
                await self._event_bus.publish("upload.failed", {
                    "upload_id": self._info.upload_id,
                    "error": str(e)
                })
    
    async def _cleanup_temp_files(self) -> None:
        """Clean up temporary chunk files."""
        for i in range(len(self._chunks)):
            chunk_path = os.path.join(self._temp_dir, f"{self._info.upload_id}_chunk_{i}")
            try:
                if os.path.exists(chunk_path):
                    if aiofiles:
                        await aiofiles.os.remove(chunk_path)
                    else:
                        os.remove(chunk_path)
            except Exception as e:
                logger.warning(f"Failed to remove temp file {chunk_path}: {e}")


class UploadManager(IUploadManager, IComponent):
    """
    Upload manager service implementation.

    Provides file upload management with support for chunked uploads,
    resume capabilities, and progress monitoring.
    """

    def __init__(
        self,
        event_bus: Optional[IEventBus] = None,
        upload_dir: Optional[str] = None,
        temp_dir: Optional[str] = None,
        chunk_size: int = 1024 * 1024,  # 1MB default
        max_concurrent_uploads: int = 5
    ):
        """
        Initialize upload manager.

        Args:
            event_bus: Event bus for publishing events
            upload_dir: Directory for storing uploaded files
            temp_dir: Directory for temporary files
            chunk_size: Default chunk size in bytes
            max_concurrent_uploads: Maximum concurrent uploads
        """
        self._event_bus = event_bus
        self._upload_dir = upload_dir or os.path.join(tempfile.gettempdir(), "uploads")
        self._temp_dir = temp_dir or os.path.join(tempfile.gettempdir(), "temp_uploads")
        self._chunk_size = chunk_size
        self._max_concurrent_uploads = max_concurrent_uploads

        self._uploads: Dict[str, UploadSession] = {}
        self._active_uploads: Set[str] = set()
        self._upload_queue: asyncio.Queue[str] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max_concurrent_uploads)
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._running = False

        # Statistics
        self._stats = {
            "total_uploads": 0,
            "completed_uploads": 0,
            "failed_uploads": 0,
            "cancelled_uploads": 0,
            "total_bytes_uploaded": 0,
            "average_upload_speed": 0.0
        }

    async def start(self) -> None:
        """Start the upload manager service."""
        if self._running:
            return

        self._running = True

        # Create directories if they don't exist
        os.makedirs(self._upload_dir, exist_ok=True)
        os.makedirs(self._temp_dir, exist_ok=True)

        # Start worker task
        self._worker_task = asyncio.create_task(self._upload_worker())

        logger.info("Upload manager service started")

        if self._event_bus:
            await self._event_bus.publish("upload_manager.started", {})

    async def stop(self) -> None:
        """Stop the upload manager service."""
        if not self._running:
            return

        self._running = False

        # Cancel all active uploads
        for upload_id in list(self._active_uploads):
            await self.cancel_upload(upload_id)

        # Stop worker task
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        self._uploads.clear()
        self._active_uploads.clear()

        logger.info("Upload manager service stopped")

        if self._event_bus:
            await self._event_bus.publish("upload_manager.stopped", {})

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        active_count = len(self._active_uploads)
        total_count = len(self._uploads)

        return {
            "healthy": self._running,
            "uploads_total": total_count,
            "uploads_active": active_count,
            "uploads_queued": self._upload_queue.qsize(),
            "details": {
                "running": self._running,
                "upload_directory": self._upload_dir,
                "temp_directory": self._temp_dir,
                "max_concurrent": self._max_concurrent_uploads,
                "statistics": self._stats
            }
        }

    async def create_upload(
        self,
        filename: str,
        file_size: int,
        mime_type: str,
        destination: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        chunk_size: Optional[int] = None,
        strategy: UploadStrategy = UploadStrategy.SEQUENTIAL
    ) -> str:
        """Create a new upload session."""
        upload_id = str(uuid.uuid4())

        # Determine destination path
        if destination is None:
            today = datetime.now().strftime('%Y%m%d')
            destination = os.path.join(self._upload_dir, today, filename)

        # Create upload info
        upload_info = UploadInfo(
            upload_id=upload_id,
            filename=filename,
            file_size=file_size,
            mime_type=mime_type,
            destination=destination,
            status=UploadStatus.PENDING,
            created_at=time.time(),
            updated_at=time.time(),
            user_id=user_id,
            metadata=metadata or {},
            chunk_size=chunk_size or self._chunk_size,
            strategy=strategy
        )

        # Create upload session
        session = UploadSession(
            upload_info=upload_info,
            upload_dir=self._upload_dir,
            temp_dir=self._temp_dir,
            event_bus=self._event_bus
        )

        self._uploads[upload_id] = session
        self._stats["total_uploads"] += 1

        logger.info(f"Created upload session: {upload_id} ({filename})")

        if self._event_bus:
            await self._event_bus.publish("upload.created", {
                "upload_id": upload_id,
                "filename": filename,
                "file_size": file_size,
                "user_id": user_id
            })

        return upload_id

    async def start_upload(self, upload_id: str) -> bool:
        """Start an upload session."""
        if upload_id not in self._uploads:
            logger.warning(f"Upload not found: {upload_id}")
            return False

        session = self._uploads[upload_id]

        # Add to queue for processing
        await self._upload_queue.put(upload_id)

        logger.info(f"Queued upload for processing: {upload_id}")
        return True

    async def pause_upload(self, upload_id: str) -> bool:
        """Pause an upload session."""
        if upload_id not in self._uploads:
            logger.warning(f"Upload not found: {upload_id}")
            return False

        session = self._uploads[upload_id]
        return await session.pause()

    async def resume_upload(self, upload_id: str) -> bool:
        """Resume a paused upload session."""
        if upload_id not in self._uploads:
            logger.warning(f"Upload not found: {upload_id}")
            return False

        session = self._uploads[upload_id]
        result = await session.resume()

        if result:
            # Add back to queue
            await self._upload_queue.put(upload_id)

        return result

    async def cancel_upload(self, upload_id: str) -> bool:
        """Cancel an upload session."""
        if upload_id not in self._uploads:
            logger.warning(f"Upload not found: {upload_id}")
            return False

        session = self._uploads[upload_id]
        result = await session.cancel()

        if result:
            self._active_uploads.discard(upload_id)
            self._stats["cancelled_uploads"] += 1

        return result

    async def upload_chunk(
        self,
        upload_id: str,
        chunk_data: bytes,
        chunk_index: int,
        checksum: Optional[str] = None
    ) -> bool:
        """Upload a file chunk."""
        if upload_id not in self._uploads:
            logger.warning(f"Upload not found: {upload_id}")
            return False

        session = self._uploads[upload_id]
        return await session.upload_chunk(chunk_data, chunk_index)

    async def upload_file_stream(
        self,
        upload_id: str,
        file_stream: AsyncGenerator[bytes, None],
        progress_callback: Optional[Callable[[str, float], None]] = None
    ) -> bool:
        """Upload a file from an async stream."""
        if upload_id not in self._uploads:
            logger.warning(f"Upload not found: {upload_id}")
            return False

        session = self._uploads[upload_id]
        upload_info = session.get_info()

        try:
            chunk_index = 0
            bytes_processed = 0

            async for chunk_data in file_stream:
                if not chunk_data:
                    break

                success = await session.upload_chunk(chunk_data, chunk_index)
                if not success:
                    return False

                bytes_processed += len(chunk_data)
                chunk_index += 1

                # Call progress callback
                if progress_callback:
                    progress = (bytes_processed / upload_info.file_size) * 100
                    progress_callback(upload_id, progress)

            return True

        except Exception as e:
            logger.error(f"Stream upload failed for {upload_id}: {e}")
            return False

    def get_upload_info(self, upload_id: str) -> Optional[UploadInfo]:
        """Get information about an upload session."""
        if upload_id not in self._uploads:
            return None

        session = self._uploads[upload_id]
        return session.get_info()

    def list_uploads(
        self,
        user_id: Optional[str] = None,
        status: Optional[UploadStatus] = None
    ) -> List[UploadInfo]:
        """List upload sessions with optional filtering."""
        uploads = []

        for session in self._uploads.values():
            upload_info = session.get_info()

            # Filter by user_id if specified
            if user_id and upload_info.user_id != user_id:
                continue

            # Filter by status if specified
            if status and upload_info.status != status:
                continue

            uploads.append(upload_info)

        return uploads

    async def cleanup_completed_uploads(self, older_than_hours: int = 24) -> int:
        """Clean up completed uploads older than specified hours."""
        cutoff_time = time.time() - (older_than_hours * 3600)
        cleaned_count = 0

        upload_ids_to_remove = []

        for upload_id, session in self._uploads.items():
            upload_info = session.get_info()

            if (upload_info.status == UploadStatus.COMPLETED and
                upload_info.completed_at and
                upload_info.completed_at < cutoff_time):

                upload_ids_to_remove.append(upload_id)

        for upload_id in upload_ids_to_remove:
            del self._uploads[upload_id]
            cleaned_count += 1

        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} completed uploads")

        return cleaned_count

    async def cleanup_failed_uploads(self, older_than_hours: int = 1) -> int:
        """Clean up failed uploads older than specified hours."""
        cutoff_time = time.time() - (older_than_hours * 3600)
        cleaned_count = 0

        upload_ids_to_remove = []

        for upload_id, session in self._uploads.items():
            upload_info = session.get_info()

            if (upload_info.status == UploadStatus.FAILED and
                upload_info.updated_at < cutoff_time):

                # Clean up any temp files
                await session.cancel()
                upload_ids_to_remove.append(upload_id)

        for upload_id in upload_ids_to_remove:
            del self._uploads[upload_id]
            cleaned_count += 1

        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} failed uploads")

        return cleaned_count

    def get_upload_statistics(self) -> Dict[str, Any]:
        """Get upload statistics and metrics."""
        active_uploads = [
            session.get_info() for session in self._uploads.values()
            if session.get_info().is_active
        ]

        total_active_size = sum(info.file_size for info in active_uploads)
        total_uploaded_size = sum(info.bytes_uploaded for info in active_uploads)

        return {
            **self._stats,
            "active_uploads": len(active_uploads),
            "total_active_size": total_active_size,
            "total_uploaded_size": total_uploaded_size,
            "queue_size": self._upload_queue.qsize()
        }

    async def verify_upload_integrity(self, upload_id: str) -> Dict[str, Any]:
        """Verify the integrity of an uploaded file."""
        if upload_id not in self._uploads:
            return {"success": False, "error": "Upload not found"}

        session = self._uploads[upload_id]
        upload_info = session.get_info()

        if upload_info.status != UploadStatus.COMPLETED:
            return {"success": False, "error": "Upload not completed"}

        try:
            file_path = upload_info.destination
            if not os.path.exists(file_path):
                return {"success": False, "error": "File not found"}

            # Calculate file checksum
            hasher = hashlib.sha256()

            if aiofiles:
                async with aiofiles.open(file_path, 'rb') as f:
                    while chunk := await f.read(8192):
                        hasher.update(chunk)
            else:
                with open(file_path, 'rb') as f:
                    while chunk := f.read(8192):
                        hasher.update(chunk)

            file_checksum = hasher.hexdigest()
            file_size = os.path.getsize(file_path)

            # Verify size
            size_match = file_size == upload_info.file_size

            return {
                "success": True,
                "upload_id": upload_id,
                "file_size": file_size,
                "expected_size": upload_info.file_size,
                "size_match": size_match,
                "file_checksum": file_checksum,
                "file_path": file_path
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _upload_worker(self) -> None:
        """Worker task to process upload queue."""
        while self._running:
            try:
                # Wait for upload to process
                upload_id = await asyncio.wait_for(
                    self._upload_queue.get(),
                    timeout=1.0
                )

                if upload_id not in self._uploads:
                    continue

                # Acquire semaphore for concurrent upload limit
                async with self._semaphore:
                    await self._process_upload(upload_id)

            except asyncio.TimeoutError:
                # Timeout is normal, continue loop
                continue
            except Exception as e:
                logger.error(f"Upload worker error: {e}")

    async def _process_upload(self, upload_id: str) -> None:
        """Process an individual upload."""
        if upload_id not in self._uploads:
            return

        session = self._uploads[upload_id]
        upload_info = session.get_info()

        try:
            self._active_uploads.add(upload_id)

            # Start the upload session
            success = await session.start()

            if success:
                # Update statistics
                if upload_info.status == UploadStatus.COMPLETED:
                    self._stats["completed_uploads"] += 1
                    self._stats["total_bytes_uploaded"] += upload_info.file_size
                elif upload_info.status == UploadStatus.FAILED:
                    self._stats["failed_uploads"] += 1

        except Exception as e:
            logger.error(f"Error processing upload {upload_id}: {e}")
            upload_info.status = UploadStatus.FAILED
            upload_info.last_error = str(e)
            self._stats["failed_uploads"] += 1

        finally:
            self._active_uploads.discard(upload_id)
