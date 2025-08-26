"""
Upload service interfaces for the Cobalt Forward application.

This module defines the contracts for upload management services
including file upload, resume capabilities, and progress monitoring.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator
from enum import Enum
from dataclasses import dataclass
import time

from .lifecycle import IStartable, IStoppable, IHealthCheckable


class UploadStatus(Enum):
    """Upload status enumeration."""
    PENDING = "pending"
    UPLOADING = "uploading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class UploadStrategy(Enum):
    """Upload strategy enumeration."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ADAPTIVE = "adaptive"


@dataclass
class UploadChunk:
    """Upload chunk information."""
    chunk_id: str
    upload_id: str
    chunk_index: int
    start_offset: int
    end_offset: int
    size: int
    checksum: Optional[str] = None
    uploaded: bool = False
    upload_time: Optional[float] = None
    retry_count: int = 0


@dataclass
class UploadInfo:
    """Upload session information."""
    upload_id: str
    filename: str
    file_size: int
    mime_type: str
    destination: str
    status: UploadStatus
    created_at: float
    updated_at: float
    completed_at: Optional[float] = None
    user_id: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    # Progress tracking
    bytes_uploaded: int = 0
    chunks_total: int = 0
    chunks_completed: int = 0
    upload_speed: float = 0.0  # bytes per second
    estimated_time_remaining: Optional[float] = None
    
    # Configuration
    chunk_size: int = 1024 * 1024  # 1MB default
    strategy: UploadStrategy = UploadStrategy.SEQUENTIAL
    max_retries: int = 3
    
    # Error tracking
    error_count: int = 0
    last_error: Optional[str] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def progress_percentage(self) -> float:
        """Calculate upload progress percentage."""
        if self.file_size == 0:
            return 100.0
        return (self.bytes_uploaded / self.file_size) * 100.0
    
    @property
    def is_complete(self) -> bool:
        """Check if upload is complete."""
        return self.status == UploadStatus.COMPLETED
    
    @property
    def is_active(self) -> bool:
        """Check if upload is actively running."""
        return self.status in (UploadStatus.UPLOADING, UploadStatus.PENDING)


class IUploadSession(ABC):
    """Interface for individual upload session management."""
    
    @abstractmethod
    async def start(self) -> bool:
        """Start the upload session."""
        pass
    
    @abstractmethod
    async def pause(self) -> bool:
        """Pause the upload session."""
        pass
    
    @abstractmethod
    async def resume(self) -> bool:
        """Resume the upload session."""
        pass
    
    @abstractmethod
    async def cancel(self) -> bool:
        """Cancel the upload session."""
        pass
    
    @abstractmethod
    async def upload_chunk(self, chunk_data: bytes, chunk_index: int) -> bool:
        """Upload a specific chunk."""
        pass
    
    @abstractmethod
    def get_info(self) -> UploadInfo:
        """Get upload session information."""
        pass
    
    @abstractmethod
    def get_progress(self) -> Dict[str, Any]:
        """Get detailed progress information."""
        pass


class IUploadManager(IStartable, IStoppable, IHealthCheckable):
    """
    Interface for upload management service.
    
    Provides file upload management with support for chunked uploads,
    resume capabilities, and progress monitoring.
    """
    
    @abstractmethod
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
        """
        Create a new upload session.
        
        Args:
            filename: Name of the file to upload
            file_size: Size of the file in bytes
            mime_type: MIME type of the file
            destination: Destination path (optional)
            user_id: User ID for the upload (optional)
            metadata: Additional metadata (optional)
            chunk_size: Size of each chunk in bytes (optional)
            strategy: Upload strategy to use
            
        Returns:
            Upload session ID
        """
        pass
    
    @abstractmethod
    async def start_upload(self, upload_id: str) -> bool:
        """Start an upload session."""
        pass
    
    @abstractmethod
    async def pause_upload(self, upload_id: str) -> bool:
        """Pause an upload session."""
        pass
    
    @abstractmethod
    async def resume_upload(self, upload_id: str) -> bool:
        """Resume a paused upload session."""
        pass
    
    @abstractmethod
    async def cancel_upload(self, upload_id: str) -> bool:
        """Cancel an upload session."""
        pass
    
    @abstractmethod
    async def upload_chunk(
        self,
        upload_id: str,
        chunk_data: bytes,
        chunk_index: int,
        checksum: Optional[str] = None
    ) -> bool:
        """Upload a file chunk."""
        pass
    
    @abstractmethod
    async def upload_file_stream(
        self,
        upload_id: str,
        file_stream: AsyncGenerator[bytes, None],
        progress_callback: Optional[Callable[[str, float], None]] = None
    ) -> bool:
        """Upload a file from an async stream."""
        pass
    
    @abstractmethod
    def get_upload_info(self, upload_id: str) -> Optional[UploadInfo]:
        """Get information about an upload session."""
        pass
    
    @abstractmethod
    def list_uploads(
        self,
        user_id: Optional[str] = None,
        status: Optional[UploadStatus] = None
    ) -> List[UploadInfo]:
        """List upload sessions with optional filtering."""
        pass
    
    @abstractmethod
    async def cleanup_completed_uploads(self, older_than_hours: int = 24) -> int:
        """Clean up completed uploads older than specified hours."""
        pass
    
    @abstractmethod
    async def cleanup_failed_uploads(self, older_than_hours: int = 1) -> int:
        """Clean up failed uploads older than specified hours."""
        pass
    
    @abstractmethod
    def get_upload_statistics(self) -> Dict[str, Any]:
        """Get upload statistics and metrics."""
        pass
    
    @abstractmethod
    async def verify_upload_integrity(self, upload_id: str) -> Dict[str, Any]:
        """Verify the integrity of an uploaded file."""
        pass
