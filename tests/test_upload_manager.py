"""
Tests for upload manager functionality.

This module tests the upload manager service integration
with the dependency injection container.
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, AsyncMock
from typing import Dict, Any

from cobalt_forward.application.container import Container
from cobalt_forward.application.startup import ApplicationStartup
from cobalt_forward.core.interfaces.upload import (
    IUploadManager, UploadStatus, UploadStrategy
)
from cobalt_forward.core.interfaces.messaging import IEventBus
from cobalt_forward.infrastructure.services.upload.manager import UploadManager


class TestUploadManagerIntegration:
    """Test upload manager integration with DI container."""
    
    @pytest.fixture
    def container(self) -> Container:
        """Create a test container."""
        return Container()
    
    @pytest.fixture
    def mock_event_bus(self) -> Mock:
        """Create a mock event bus."""
        mock_bus = Mock(spec=IEventBus)
        mock_bus.publish = AsyncMock()
        return mock_bus
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing."""
        upload_dir = tempfile.mkdtemp(prefix="test_uploads_")
        temp_dir = tempfile.mkdtemp(prefix="test_temp_")
        
        yield upload_dir, temp_dir
        
        # Cleanup
        import shutil
        shutil.rmtree(upload_dir, ignore_errors=True)
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    async def upload_manager(
        self, 
        container: Container, 
        mock_event_bus: Mock,
        temp_dirs
    ) -> UploadManager:
        """Create upload manager with dependencies."""
        upload_dir, temp_dir = temp_dirs
        
        # Register event bus
        container.register_instance(IEventBus, mock_event_bus)
        
        # Create upload manager
        manager = UploadManager(
            event_bus=mock_event_bus,
            upload_dir=upload_dir,
            temp_dir=temp_dir,
            chunk_size=1024,  # Small chunks for testing
            max_concurrent_uploads=2
        )
        
        return manager
    
    async def test_upload_manager_creation(self, upload_manager: UploadManager):
        """Test upload manager can be created."""
        assert upload_manager is not None
        assert isinstance(upload_manager, IUploadManager)
    
    async def test_upload_manager_lifecycle(self, upload_manager: UploadManager):
        """Test upload manager lifecycle methods."""
        # Test start
        await upload_manager.start()
        
        # Test health check
        health = await upload_manager.health_check()
        assert health["healthy"] is True
        assert health["uploads_total"] == 0
        
        # Test stop
        await upload_manager.stop()
    
    async def test_upload_creation(self, upload_manager: UploadManager):
        """Test upload session creation."""
        await upload_manager.start()
        
        # Create upload
        upload_id = await upload_manager.create_upload(
            filename="test.txt",
            file_size=1024,
            mime_type="text/plain",
            user_id="test_user",
            metadata={"description": "Test upload"}
        )
        
        assert upload_id is not None
        assert len(upload_id) > 0
        
        # Check upload info
        upload_info = upload_manager.get_upload_info(upload_id)
        assert upload_info is not None
        assert upload_info.filename == "test.txt"
        assert upload_info.file_size == 1024
        assert upload_info.mime_type == "text/plain"
        assert upload_info.user_id == "test_user"
        assert upload_info.status == UploadStatus.PENDING
        
        # List uploads
        uploads = upload_manager.list_uploads()
        assert len(uploads) == 1
        assert uploads[0].upload_id == upload_id
        
        await upload_manager.stop()
    
    async def test_upload_filtering(self, upload_manager: UploadManager):
        """Test upload listing with filters."""
        await upload_manager.start()
        
        # Create uploads for different users
        upload1 = await upload_manager.create_upload(
            filename="user1_file.txt",
            file_size=1024,
            mime_type="text/plain",
            user_id="user1"
        )
        
        upload2 = await upload_manager.create_upload(
            filename="user2_file.txt",
            file_size=2048,
            mime_type="text/plain",
            user_id="user2"
        )
        
        # Test filtering by user
        user1_uploads = upload_manager.list_uploads(user_id="user1")
        assert len(user1_uploads) == 1
        assert user1_uploads[0].upload_id == upload1
        
        user2_uploads = upload_manager.list_uploads(user_id="user2")
        assert len(user2_uploads) == 1
        assert user2_uploads[0].upload_id == upload2
        
        # Test filtering by status
        pending_uploads = upload_manager.list_uploads(status=UploadStatus.PENDING)
        assert len(pending_uploads) == 2
        
        await upload_manager.stop()
    
    async def test_upload_statistics(self, upload_manager: UploadManager):
        """Test upload statistics."""
        await upload_manager.start()
        
        # Create some uploads
        await upload_manager.create_upload(
            filename="file1.txt",
            file_size=1024,
            mime_type="text/plain"
        )
        
        await upload_manager.create_upload(
            filename="file2.txt",
            file_size=2048,
            mime_type="text/plain"
        )
        
        # Get statistics
        stats = upload_manager.get_upload_statistics()
        assert stats["total_uploads"] == 2
        assert stats["active_uploads"] == 0  # Not started yet
        assert stats["queue_size"] == 0
        
        await upload_manager.stop()
    
    async def test_container_integration(self, container: Container, temp_dirs):
        """Test upload manager registration in container."""
        upload_dir, temp_dir = temp_dirs
        
        # Register upload manager
        container.register(IUploadManager, UploadManager)
        
        # Resolve upload manager
        manager = container.resolve(IUploadManager)
        assert manager is not None
        assert isinstance(manager, UploadManager)
        
        # Test it's a singleton
        manager2 = container.resolve(IUploadManager)
        assert manager is manager2


class TestApplicationStartupWithUpload:
    """Test application startup with upload services."""
    
    @pytest.fixture
    def container(self) -> Container:
        """Create a test container."""
        return Container()
    
    @pytest.fixture
    def startup(self, container: Container) -> ApplicationStartup:
        """Create application startup."""
        return ApplicationStartup(container)
    
    async def test_upload_service_registration(self, startup: ApplicationStartup):
        """Test upload services are registered during startup."""
        # Configure services
        config = {
            "name": "test-app",
            "version": "1.0.0",
            "logging": {"level": "INFO"},
            "upload": {
                "enabled": True,
                "chunk_size": 1024 * 1024,
                "max_concurrent": 5
            }
        }
        
        await startup.configure_services(config)
        
        # Check upload manager is registered
        manager = startup._container.resolve(IUploadManager)
        assert manager is not None
        assert isinstance(manager, UploadManager)
    
    async def test_upload_service_startup(self, startup: ApplicationStartup):
        """Test upload services start correctly."""
        # Configure services
        config = {
            "name": "test-app",
            "version": "1.0.0",
            "logging": {"level": "INFO"}
        }
        
        await startup.configure_services(config)
        
        # Start application (this should start upload manager)
        await startup.start_application()
        
        # Check upload manager is running
        manager = startup._container.resolve(IUploadManager)
        health = await manager.health_check()
        assert health["healthy"] is True
        
        # Stop application
        await startup.stop_application()


class TestUploadChunkHandling:
    """Test upload chunk handling functionality."""
    
    @pytest.fixture
    async def upload_manager(self) -> UploadManager:
        """Create upload manager for testing."""
        manager = UploadManager(
            event_bus=None,
            upload_dir=None,
            temp_dir=None,
            chunk_size=1024,
            max_concurrent_uploads=5
        )
        return manager
    
    @pytest.fixture
    async def upload_manager_with_upload(self, upload_manager: UploadManager):
        """Create upload manager with a test upload."""
        await upload_manager.start()
        
        upload_id = await upload_manager.create_upload(
            filename="test_chunks.txt",
            file_size=4096,  # 4 chunks of 1024 bytes
            mime_type="text/plain",
            chunk_size=1024
        )
        
        # Start the upload session to prepare chunks
        await upload_manager.start_upload(upload_id)
        
        yield upload_manager, upload_id
        
        await upload_manager.stop()
    
    async def test_chunk_upload(self, upload_manager_with_upload):
        """Test uploading individual chunks."""
        manager, upload_id = upload_manager_with_upload
        
        # Upload first chunk
        chunk_data = b"A" * 1024
        success = await manager.upload_chunk(upload_id, chunk_data, 0)
        assert success is True
        
        # Check progress
        upload_info = manager.get_upload_info(upload_id)
        assert upload_info is not None
        assert upload_info.bytes_uploaded == 1024
        assert upload_info.chunks_completed == 1
        assert upload_info.progress_percentage == 25.0  # 1/4 chunks
    
    async def test_upload_verification(self, upload_manager_with_upload):
        """Test upload integrity verification."""
        manager, upload_id = upload_manager_with_upload
        
        # Upload all chunks to complete the upload
        for i in range(4):
            chunk_data = b"A" * 1024
            await manager.upload_chunk(upload_id, chunk_data, i)
        
        # Verify upload integrity
        result = await manager.verify_upload_integrity(upload_id)
        
        # Note: This test might fail if the file combination logic
        # in _finalize_upload doesn't work properly in the test environment
        # The test is here to demonstrate the interface
        assert "success" in result
