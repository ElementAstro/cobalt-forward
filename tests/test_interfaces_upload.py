"""
Tests for upload interfaces.

This module tests the upload interfaces including IUploadManager, IUploadSession,
and related data classes and enums.
"""

import pytest
import time
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator
from unittest.mock import AsyncMock, Mock

from cobalt_forward.core.interfaces.upload import (
    IUploadManager, IUploadSession, UploadStatus, UploadStrategy,
    UploadInfo, UploadChunk
)
from cobalt_forward.core.interfaces.lifecycle import IStartable, IStoppable, IHealthCheckable


class TestUploadStatus:
    """Test cases for UploadStatus enum."""
    
    def test_upload_statuses_exist(self) -> None:
        """Test that all expected upload statuses exist."""
        expected_statuses = {
            'PENDING', 'UPLOADING', 'PAUSED', 'COMPLETED', 'FAILED', 'CANCELLED'
        }
        actual_statuses = {us.name for us in UploadStatus}
        assert actual_statuses == expected_statuses
    
    def test_upload_status_values(self) -> None:
        """Test upload status values."""
        assert UploadStatus.PENDING.value == "pending"
        assert UploadStatus.UPLOADING.value == "uploading"
        assert UploadStatus.PAUSED.value == "paused"
        assert UploadStatus.COMPLETED.value == "completed"
        assert UploadStatus.FAILED.value == "failed"
        assert UploadStatus.CANCELLED.value == "cancelled"


class TestUploadStrategy:
    """Test cases for UploadStrategy enum."""
    
    def test_upload_strategies_exist(self) -> None:
        """Test that all expected upload strategies exist."""
        expected_strategies = {'SEQUENTIAL', 'PARALLEL', 'ADAPTIVE'}
        actual_strategies = {us.name for us in UploadStrategy}
        assert actual_strategies == expected_strategies
    
    def test_upload_strategy_values(self) -> None:
        """Test upload strategy values."""
        assert UploadStrategy.SEQUENTIAL.value == "sequential"
        assert UploadStrategy.PARALLEL.value == "parallel"
        assert UploadStrategy.ADAPTIVE.value == "adaptive"


class TestUploadChunk:
    """Test cases for UploadChunk dataclass."""
    
    def test_upload_chunk_creation_minimal(self) -> None:
        """Test creating UploadChunk with minimal fields."""
        chunk = UploadChunk(
            chunk_id="chunk-123",
            upload_id="upload-456",
            chunk_index=0,
            start_offset=0,
            end_offset=1023,
            size=1024
        )
        
        assert chunk.chunk_id == "chunk-123"
        assert chunk.upload_id == "upload-456"
        assert chunk.chunk_index == 0
        assert chunk.start_offset == 0
        assert chunk.end_offset == 1023
        assert chunk.size == 1024
        assert chunk.checksum is None
        assert chunk.uploaded is False
        assert chunk.upload_time is None
        assert chunk.retry_count == 0
    
    def test_upload_chunk_creation_full(self) -> None:
        """Test creating UploadChunk with all fields."""
        upload_time = time.time()
        
        chunk = UploadChunk(
            chunk_id="chunk-789",
            upload_id="upload-abc",
            chunk_index=5,
            start_offset=5120,
            end_offset=6143,
            size=1024,
            checksum="sha256hash",
            uploaded=True,
            upload_time=upload_time,
            retry_count=2
        )
        
        assert chunk.chunk_id == "chunk-789"
        assert chunk.upload_id == "upload-abc"
        assert chunk.chunk_index == 5
        assert chunk.start_offset == 5120
        assert chunk.end_offset == 6143
        assert chunk.size == 1024
        assert chunk.checksum == "sha256hash"
        assert chunk.uploaded is True
        assert chunk.upload_time == upload_time
        assert chunk.retry_count == 2


class TestUploadInfo:
    """Test cases for UploadInfo dataclass."""
    
    def test_upload_info_creation_minimal(self) -> None:
        """Test creating UploadInfo with minimal fields."""
        created_time = time.time()
        updated_time = created_time + 10
        
        info = UploadInfo(
            upload_id="upload-123",
            filename="test.txt",
            file_size=1024,
            mime_type="text/plain",
            destination="/uploads/test.txt",
            status=UploadStatus.PENDING,
            created_at=created_time,
            updated_at=updated_time
        )
        
        assert info.upload_id == "upload-123"
        assert info.filename == "test.txt"
        assert info.file_size == 1024
        assert info.mime_type == "text/plain"
        assert info.destination == "/uploads/test.txt"
        assert info.status == UploadStatus.PENDING
        assert info.created_at == created_time
        assert info.updated_at == updated_time
        assert info.completed_at is None
        assert info.user_id is None
        assert info.metadata is None
        
        # Progress tracking defaults
        assert info.bytes_uploaded == 0
        assert info.chunks_total == 0
        assert info.chunks_completed == 0
        assert info.upload_speed == 0.0
        assert info.estimated_time_remaining is None
        
        # Configuration defaults
        assert info.chunk_size == 1024 * 1024  # 1MB
        assert info.strategy == UploadStrategy.SEQUENTIAL
        assert info.max_retries == 3
        
        # Error tracking defaults
        assert info.error_count == 0
        assert info.last_error is None
    
    def test_upload_info_creation_full(self) -> None:
        """Test creating UploadInfo with all fields."""
        created_time = time.time()
        updated_time = created_time + 10
        completed_time = created_time + 100
        metadata = {"source": "web", "category": "document"}
        
        info = UploadInfo(
            upload_id="upload-456",
            filename="document.pdf",
            file_size=5242880,  # 5MB
            mime_type="application/pdf",
            destination="/uploads/documents/document.pdf",
            status=UploadStatus.COMPLETED,
            created_at=created_time,
            updated_at=updated_time,
            completed_at=completed_time,
            user_id="user-789",
            metadata=metadata,
            bytes_uploaded=5242880,
            chunks_total=5,
            chunks_completed=5,
            upload_speed=1048576.0,  # 1MB/s
            estimated_time_remaining=0.0,
            chunk_size=1048576,  # 1MB
            strategy=UploadStrategy.PARALLEL,
            max_retries=5,
            error_count=1,
            last_error="Temporary network error"
        )
        
        assert info.upload_id == "upload-456"
        assert info.filename == "document.pdf"
        assert info.file_size == 5242880
        assert info.mime_type == "application/pdf"
        assert info.destination == "/uploads/documents/document.pdf"
        assert info.status == UploadStatus.COMPLETED
        assert info.created_at == created_time
        assert info.updated_at == updated_time
        assert info.completed_at == completed_time
        assert info.user_id == "user-789"
        assert info.metadata == metadata
        assert info.bytes_uploaded == 5242880
        assert info.chunks_total == 5
        assert info.chunks_completed == 5
        assert info.upload_speed == 1048576.0
        assert info.estimated_time_remaining == 0.0
        assert info.chunk_size == 1048576
        assert info.strategy == UploadStrategy.PARALLEL
        assert info.max_retries == 5
        assert info.error_count == 1
        assert info.last_error == "Temporary network error"
    
    def test_upload_info_progress_calculation(self) -> None:
        """Test upload progress calculation."""
        info = UploadInfo(
            upload_id="upload-progress",
            filename="test.bin",
            file_size=10240,  # 10KB
            mime_type="application/octet-stream",
            destination="/uploads/test.bin",
            status=UploadStatus.UPLOADING,
            created_at=time.time(),
            updated_at=time.time(),
            bytes_uploaded=5120,  # 50% uploaded
            chunks_total=10,
            chunks_completed=5
        )
        
        # Calculate progress percentage
        progress_percentage = (info.bytes_uploaded / info.file_size) * 100
        assert progress_percentage == 50.0
        
        # Calculate chunk progress percentage
        chunk_progress = (info.chunks_completed / info.chunks_total) * 100
        assert chunk_progress == 50.0


class MockUploadSession(IUploadSession):
    """Mock implementation of IUploadSession for testing."""
    
    def __init__(self, upload_info: UploadInfo):
        self._info = upload_info
        self.chunks: List[bytes] = []
        
        # Call counters
        self.start_called_count = 0
        self.pause_called_count = 0
        self.resume_called_count = 0
        self.cancel_called_count = 0
        self.upload_chunk_called_count = 0
        self.get_info_called_count = 0
        self.get_progress_called_count = 0
        
        # Configuration
        self.should_fail_start = False
        self.should_fail_pause = False
        self.should_fail_resume = False
        self.should_fail_cancel = False
        self.should_fail_upload_chunk = False
    
    async def start(self) -> bool:
        """Start the upload session."""
        self.start_called_count += 1
        if self.should_fail_start:
            self._info.status = UploadStatus.FAILED
            self._info.last_error = "Mock start failure"
            return False
        
        self._info.status = UploadStatus.UPLOADING
        self._info.updated_at = time.time()
        return True
    
    async def pause(self) -> bool:
        """Pause the upload session."""
        self.pause_called_count += 1
        if self.should_fail_pause:
            return False
        
        if self._info.status == UploadStatus.UPLOADING:
            self._info.status = UploadStatus.PAUSED
            self._info.updated_at = time.time()
            return True
        return False
    
    async def resume(self) -> bool:
        """Resume the upload session."""
        self.resume_called_count += 1
        if self.should_fail_resume:
            return False
        
        if self._info.status == UploadStatus.PAUSED:
            self._info.status = UploadStatus.UPLOADING
            self._info.updated_at = time.time()
            return True
        return False
    
    async def cancel(self) -> bool:
        """Cancel the upload session."""
        self.cancel_called_count += 1
        if self.should_fail_cancel:
            return False
        
        if self._info.status not in (UploadStatus.COMPLETED, UploadStatus.CANCELLED):
            self._info.status = UploadStatus.CANCELLED
            self._info.updated_at = time.time()
            return True
        return False
    
    async def upload_chunk(self, chunk_data: bytes, chunk_index: int) -> bool:
        """Upload a specific chunk."""
        self.upload_chunk_called_count += 1
        if self.should_fail_upload_chunk:
            self._info.error_count += 1
            self._info.last_error = "Mock chunk upload failure"
            return False
        
        # Store chunk data
        while len(self.chunks) <= chunk_index:
            self.chunks.append(b"")
        self.chunks[chunk_index] = chunk_data
        
        # Update progress
        self._info.bytes_uploaded += len(chunk_data)
        self._info.chunks_completed += 1
        self._info.updated_at = time.time()
        
        # Check if complete
        if self._info.chunks_completed >= self._info.chunks_total:
            self._info.status = UploadStatus.COMPLETED
            self._info.completed_at = time.time()
        
        return True
    
    def get_info(self) -> UploadInfo:
        """Get upload session information."""
        self.get_info_called_count += 1
        return self._info
    
    def get_progress(self) -> Dict[str, Any]:
        """Get detailed progress information."""
        self.get_progress_called_count += 1
        
        progress_percentage = 0.0
        if self._info.file_size > 0:
            progress_percentage = (self._info.bytes_uploaded / self._info.file_size) * 100
        
        return {
            "upload_id": self._info.upload_id,
            "status": self._info.status.value,
            "progress_percentage": progress_percentage,
            "bytes_uploaded": self._info.bytes_uploaded,
            "file_size": self._info.file_size,
            "chunks_completed": self._info.chunks_completed,
            "chunks_total": self._info.chunks_total,
            "upload_speed": self._info.upload_speed,
            "estimated_time_remaining": self._info.estimated_time_remaining,
            "error_count": self._info.error_count,
            "last_error": self._info.last_error
        }


class TestIUploadSession:
    """Test cases for IUploadSession interface."""

    @pytest.fixture
    def upload_info(self) -> UploadInfo:
        """Create sample upload info."""
        return UploadInfo(
            upload_id="test-upload-123",
            filename="test.txt",
            file_size=1024,
            mime_type="text/plain",
            destination="/uploads/test.txt",
            status=UploadStatus.PENDING,
            created_at=time.time(),
            updated_at=time.time(),
            chunks_total=1
        )

    @pytest.fixture
    def upload_session(self, upload_info: UploadInfo) -> MockUploadSession:
        """Create a mock upload session."""
        return MockUploadSession(upload_info)

    @pytest.mark.asyncio
    async def test_start_session(self, upload_session: MockUploadSession) -> None:
        """Test starting an upload session."""
        assert hasattr(upload_session, 'start')
        assert callable(upload_session.start)

        result = await upload_session.start()

        assert result is True
        assert upload_session.start_called_count == 1
        assert upload_session.get_info().status == UploadStatus.UPLOADING

    @pytest.mark.asyncio
    async def test_start_session_failure(self, upload_session: MockUploadSession) -> None:
        """Test starting an upload session failure."""
        upload_session.should_fail_start = True

        result = await upload_session.start()

        assert result is False
        assert upload_session.start_called_count == 1
        assert upload_session.get_info().status == UploadStatus.FAILED
        assert upload_session.get_info().last_error == "Mock start failure"

    @pytest.mark.asyncio
    async def test_pause_session(self, upload_session: MockUploadSession) -> None:
        """Test pausing an upload session."""
        # Start first
        await upload_session.start()

        result = await upload_session.pause()

        assert result is True
        assert upload_session.pause_called_count == 1
        assert upload_session.get_info().status == UploadStatus.PAUSED

    @pytest.mark.asyncio
    async def test_pause_session_not_uploading(self, upload_session: MockUploadSession) -> None:
        """Test pausing a session that's not uploading."""
        # Don't start the session
        result = await upload_session.pause()

        assert result is False
        assert upload_session.pause_called_count == 1
        assert upload_session.get_info().status == UploadStatus.PENDING

    @pytest.mark.asyncio
    async def test_resume_session(self, upload_session: MockUploadSession) -> None:
        """Test resuming a paused upload session."""
        # Start and pause first
        await upload_session.start()
        await upload_session.pause()

        result = await upload_session.resume()

        assert result is True
        assert upload_session.resume_called_count == 1
        assert upload_session.get_info().status == UploadStatus.UPLOADING

    @pytest.mark.asyncio
    async def test_resume_session_not_paused(self, upload_session: MockUploadSession) -> None:
        """Test resuming a session that's not paused."""
        result = await upload_session.resume()

        assert result is False
        assert upload_session.resume_called_count == 1
        assert upload_session.get_info().status == UploadStatus.PENDING

    @pytest.mark.asyncio
    async def test_cancel_session(self, upload_session: MockUploadSession) -> None:
        """Test cancelling an upload session."""
        await upload_session.start()

        result = await upload_session.cancel()

        assert result is True
        assert upload_session.cancel_called_count == 1
        assert upload_session.get_info().status == UploadStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_completed_session(self, upload_session: MockUploadSession) -> None:
        """Test cancelling a completed session."""
        # Manually set to completed
        upload_session._info.status = UploadStatus.COMPLETED

        result = await upload_session.cancel()

        assert result is False
        assert upload_session.cancel_called_count == 1
        assert upload_session.get_info().status == UploadStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_upload_chunk(self, upload_session: MockUploadSession) -> None:
        """Test uploading a chunk."""
        chunk_data = b"test chunk data"
        chunk_index = 0

        result = await upload_session.upload_chunk(chunk_data, chunk_index)

        assert result is True
        assert upload_session.upload_chunk_called_count == 1
        assert len(upload_session.chunks) == 1
        assert upload_session.chunks[0] == chunk_data

        info = upload_session.get_info()
        assert info.bytes_uploaded == len(chunk_data)
        assert info.chunks_completed == 1

    @pytest.mark.asyncio
    async def test_upload_chunk_completion(self, upload_session: MockUploadSession) -> None:
        """Test upload completion after uploading all chunks."""
        # Set up for single chunk upload
        upload_session._info.chunks_total = 1
        chunk_data = b"complete file data"

        result = await upload_session.upload_chunk(chunk_data, 0)

        assert result is True
        info = upload_session.get_info()
        assert info.status == UploadStatus.COMPLETED
        assert info.completed_at is not None

    @pytest.mark.asyncio
    async def test_upload_chunk_failure(self, upload_session: MockUploadSession) -> None:
        """Test chunk upload failure."""
        upload_session.should_fail_upload_chunk = True

        result = await upload_session.upload_chunk(b"test", 0)

        assert result is False
        assert upload_session.upload_chunk_called_count == 1

        info = upload_session.get_info()
        assert info.error_count == 1
        assert info.last_error == "Mock chunk upload failure"

    def test_get_info(self, upload_session: MockUploadSession) -> None:
        """Test getting upload session info."""
        info = upload_session.get_info()

        assert isinstance(info, UploadInfo)
        assert info.upload_id == "test-upload-123"
        assert upload_session.get_info_called_count == 1

    def test_get_progress(self, upload_session: MockUploadSession) -> None:
        """Test getting upload progress."""
        progress = upload_session.get_progress()

        assert isinstance(progress, dict)
        assert "upload_id" in progress
        assert "status" in progress
        assert "progress_percentage" in progress
        assert "bytes_uploaded" in progress
        assert "file_size" in progress
        assert upload_session.get_progress_called_count == 1

        # Test progress calculation
        assert progress["progress_percentage"] == 0.0  # No bytes uploaded yet
        assert progress["upload_id"] == "test-upload-123"
        assert progress["status"] == "pending"
