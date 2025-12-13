"""
Tests for download mode functionality.

Tests the include_audio and include_transcript parameters,
including different mode combinations and fallback logic.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from pydantic import ValidationError

from src.api.schemas import CreateTaskRequest, TaskResponse
from src.config import Settings
from src.core.downloader import DownloadResult, TranscriptOnlyResult
from src.core.worker import DownloadWorker
from src.db.database import Database
from src.db.models import FileType, Task, TaskStatus, VideoInfo
from src.services.callback_service import CallbackService
from src.services.file_service import FileService
from src.services.notify import NotificationService
from src.services.task_service import TaskService


# ==================== Request Validation Tests ====================


class TestCreateTaskRequestValidation:
    """Test CreateTaskRequest validation for download modes."""

    def test_default_mode_both_true(self):
        """Default mode should have both include_audio and include_transcript as True."""
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        )
        assert request.include_audio is True
        assert request.include_transcript is True

    def test_audio_only_mode(self):
        """Audio-only mode should be valid."""
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            include_audio=True,
            include_transcript=False,
        )
        assert request.include_audio is True
        assert request.include_transcript is False

    def test_transcript_only_mode(self):
        """Transcript-only mode should be valid."""
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            include_audio=False,
            include_transcript=True,
        )
        assert request.include_audio is False
        assert request.include_transcript is True

    def test_both_false_invalid(self):
        """Both false should raise validation error."""
        with pytest.raises(ValidationError) as exc_info:
            CreateTaskRequest(
                video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                include_audio=False,
                include_transcript=False,
            )
        assert "at least one" in str(exc_info.value).lower()

    def test_invalid_url_still_checked(self):
        """Invalid URL should still be validated regardless of mode."""
        with pytest.raises(ValidationError):
            CreateTaskRequest(
                video_url="https://not-youtube.com/video",
                include_audio=True,
                include_transcript=True,
            )


# ==================== Task Service Tests ====================


class TestTaskServiceModes:
    """Test TaskService handling of download modes."""

    @pytest_asyncio.fixture
    async def task_service(self, test_db: Database, test_settings: Settings):
        """Create task service for testing."""
        return TaskService(test_db, test_settings)

    @pytest.mark.asyncio
    async def test_create_task_default_mode(self, task_service: TaskService):
        """Create task with default mode."""
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        )
        response = await task_service.create_task(request)

        assert response.status == TaskStatus.PENDING
        assert response.request is not None
        assert response.request.include_audio is True
        assert response.request.include_transcript is True

    @pytest.mark.asyncio
    async def test_create_task_transcript_only_mode(self, task_service: TaskService):
        """Create task with transcript-only mode."""
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=test123456",
            include_audio=False,
            include_transcript=True,
        )
        response = await task_service.create_task(request)

        assert response.status == TaskStatus.PENDING
        assert response.request is not None
        assert response.request.include_audio is False
        assert response.request.include_transcript is True

    @pytest.mark.asyncio
    async def test_create_task_audio_only_mode(self, task_service: TaskService):
        """Create task with audio-only mode."""
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=audio12345",
            include_audio=True,
            include_transcript=False,
        )
        response = await task_service.create_task(request)

        assert response.status == TaskStatus.PENDING
        assert response.request is not None
        assert response.request.include_audio is True
        assert response.request.include_transcript is False


# ==================== Worker Execution Tests ====================


class TestWorkerExecutionModes:
    """Test DownloadWorker execution for different modes."""

    @pytest_asyncio.fixture
    async def worker_deps(self, test_db: Database, test_settings: Settings):
        """Create worker dependencies."""
        task_service = TaskService(test_db, test_settings)
        file_service = FileService(test_db, test_settings)
        callback_service = CallbackService(test_db, base_url="http://localhost:8000")
        notify_service = NotificationService(test_settings)

        return {
            "db": test_db,
            "settings": test_settings,
            "task_service": task_service,
            "file_service": file_service,
            "callback_service": callback_service,
            "notify_service": notify_service,
        }

    @pytest.mark.asyncio
    async def test_execute_full_mode_with_transcript(
        self, worker_deps: dict, mock_downloader: AsyncMock, temp_dir: Path
    ):
        """Test full mode execution when transcript is available."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader

        # Create actual temp files for mock to return
        audio_file = temp_dir / "test.m4a"
        transcript_file = temp_dir / "test.en.srt"
        audio_file.write_text("mock audio content")
        transcript_file.write_text("mock transcript content")

        # Update mock to return actual file paths
        mock_downloader.download.return_value = DownloadResult(
            video_info=VideoInfo(
                title="Test Video",
                author="Test Author",
                duration=60,
                channel_id="UC123456",
            ),
            audio_path=audio_file,
            transcript_path=transcript_file,
        )

        # Create task with full mode
        task = Task(
            id="test-task-001",
            video_id="dQw4w9WgXcQ",
            video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            status=TaskStatus.PENDING,
            include_audio=True,
            include_transcript=True,
        )

        # Mock file service to avoid actual file operations
        with patch.object(
            worker.file_service,
            "create_file_record",
            new_callable=AsyncMock,
        ) as mock_create_file:
            mock_create_file.return_value = MagicMock(id="file-001")

            result = await worker._execute_task(task)

        assert result["video_info"] is not None
        assert result["audio_file_id"] is not None
        assert result["has_transcript"] is True
        assert result["audio_fallback"] is False
        mock_downloader.download.assert_called_once()
        mock_downloader.extract_transcript_only.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_audio_only_mode(
        self, worker_deps: dict, mock_downloader: AsyncMock, temp_dir: Path
    ):
        """Test audio-only mode execution."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader

        # Create actual temp file for mock to return
        audio_file = temp_dir / "test2.m4a"
        audio_file.write_text("mock audio content")

        # Update mock to return actual file path (no transcript)
        mock_downloader.download.return_value = DownloadResult(
            video_info=VideoInfo(
                title="Test Video",
                author="Test Author",
                duration=60,
                channel_id="UC123456",
            ),
            audio_path=audio_file,
            transcript_path=None,
        )

        task = Task(
            id="test-task-002",
            video_id="audio123456",
            video_url="https://www.youtube.com/watch?v=audio123456",
            status=TaskStatus.PENDING,
            include_audio=True,
            include_transcript=False,
        )

        with patch.object(
            worker.file_service,
            "create_file_record",
            new_callable=AsyncMock,
        ) as mock_create_file:
            mock_create_file.return_value = MagicMock(id="file-002")

            result = await worker._execute_task(task)

        assert result["video_info"] is not None
        assert result["audio_file_id"] is not None
        assert result["has_transcript"] is False  # Not requested
        assert result["audio_fallback"] is False
        mock_downloader.download.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_transcript_only_with_available_transcript(
        self, worker_deps: dict, mock_downloader: AsyncMock, temp_dir: Path
    ):
        """Test transcript-only mode when transcript is available."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader

        task = Task(
            id="test-task-003",
            video_id="subs123456",
            video_url="https://www.youtube.com/watch?v=subs123456",
            status=TaskStatus.PENDING,
            include_audio=False,
            include_transcript=True,
        )

        with patch.object(
            worker.file_service,
            "create_file_record",
            new_callable=AsyncMock,
        ) as mock_create_file:
            mock_create_file.return_value = MagicMock(id="file-003")

            result = await worker._execute_task(task)

        assert result["video_info"] is not None
        assert result["audio_file_id"] is None  # No audio downloaded
        assert result["transcript_file_id"] is not None
        assert result["has_transcript"] is True
        assert result["audio_fallback"] is False
        # Should call extract_transcript_only, not download
        mock_downloader.extract_transcript_only.assert_called_once()
        mock_downloader.download.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_transcript_only_fallback_to_audio(
        self, worker_deps: dict, mock_downloader_no_transcript: AsyncMock, temp_dir: Path
    ):
        """Test transcript-only mode fallback to audio when no transcript available."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader_no_transcript

        # Create actual temp file for fallback audio download
        audio_file = temp_dir / "test4.m4a"
        audio_file.write_text("mock audio content")

        # Update mock download to return actual file path
        mock_downloader_no_transcript.download.return_value = DownloadResult(
            video_info=VideoInfo(
                title="Test Video No Subs",
                author="Test Author",
                duration=60,
                channel_id="UC123456",
            ),
            audio_path=audio_file,
            transcript_path=None,
        )

        task = Task(
            id="test-task-004",
            video_id="nosubs12345",
            video_url="https://www.youtube.com/watch?v=nosubs12345",
            status=TaskStatus.PENDING,
            include_audio=False,
            include_transcript=True,
        )

        with patch.object(
            worker.file_service,
            "create_file_record",
            new_callable=AsyncMock,
        ) as mock_create_file:
            mock_create_file.return_value = MagicMock(id="file-004")

            result = await worker._execute_task(task)

        assert result["video_info"] is not None
        assert result["audio_file_id"] is not None  # Audio downloaded as fallback
        assert result["has_transcript"] is False
        assert result["audio_fallback"] is True  # Fallback flag set
        # Should call extract_transcript_only first, then download as fallback
        mock_downloader_no_transcript.extract_transcript_only.assert_called_once()
        mock_downloader_no_transcript.download.assert_called_once()


# ==================== Database Persistence Tests ====================


class TestDatabaseModePersistence:
    """Test database persistence of download mode settings."""

    @pytest.mark.asyncio
    async def test_task_mode_saved_to_db(self, test_db: Database):
        """Test that task mode settings are saved to database."""
        task = Task(
            id="db-test-001",
            video_id="dbtest12345",
            video_url="https://www.youtube.com/watch?v=dbtest12345",
            status=TaskStatus.PENDING,
            include_audio=False,
            include_transcript=True,
        )

        await test_db.create_task(task)
        retrieved = await test_db.get_task(task.id)

        assert retrieved is not None
        assert retrieved.include_audio is False
        assert retrieved.include_transcript is True

    @pytest.mark.asyncio
    async def test_task_result_saved_to_db(self, test_db: Database):
        """Test that task result info is saved to database."""
        task = Task(
            id="db-test-002",
            video_id="dbtest67890",
            video_url="https://www.youtube.com/watch?v=dbtest67890",
            status=TaskStatus.PENDING,
            include_audio=False,
            include_transcript=True,
        )

        await test_db.create_task(task)

        # Simulate task completion with fallback
        from datetime import datetime, timezone

        await test_db.update_task_completed(
            task_id=task.id,
            video_info=VideoInfo(title="Test", author="Author", duration=60),
            audio_file_id="audio-file-id",
            transcript_file_id=None,
            expires_at=datetime.now(timezone.utc),
            has_transcript=False,
            audio_fallback=True,
        )

        retrieved = await test_db.get_task(task.id)

        assert retrieved is not None
        assert retrieved.status == TaskStatus.COMPLETED
        assert retrieved.has_transcript is False
        assert retrieved.audio_fallback is True


# ==================== Response Format Tests ====================


class TestResponseFormat:
    """Test response format for different modes."""

    @pytest_asyncio.fixture
    async def task_service(self, test_db: Database, test_settings: Settings):
        """Create task service for testing."""
        return TaskService(test_db, test_settings)

    @pytest.mark.asyncio
    async def test_pending_task_response_includes_request_mode(
        self, task_service: TaskService
    ):
        """Pending task response should include request mode."""
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=resp123456",
            include_audio=False,
            include_transcript=True,
        )
        response = await task_service.create_task(request)

        assert response.request is not None
        assert response.request.include_audio is False
        assert response.request.include_transcript is True
        # Result should be None for pending tasks
        assert response.result is None

    @pytest.mark.asyncio
    async def test_completed_task_response_includes_result(
        self, test_db: Database, task_service: TaskService
    ):
        """Completed task response should include result info."""
        # Create task
        request = CreateTaskRequest(
            video_url="https://www.youtube.com/watch?v=comp123456",
            include_audio=False,
            include_transcript=True,
        )
        create_response = await task_service.create_task(request)

        # Simulate completion with fallback
        from datetime import datetime, timezone

        await test_db.update_task_completed(
            task_id=create_response.task_id,
            video_info=VideoInfo(title="Test", author="Author", duration=60),
            audio_file_id="audio-id",
            transcript_file_id=None,
            expires_at=datetime.now(timezone.utc),
            has_transcript=False,
            audio_fallback=True,
        )

        # Get updated task
        response = await task_service.get_task(create_response.task_id)

        assert response is not None
        assert response.status == TaskStatus.COMPLETED
        assert response.request is not None
        assert response.request.include_audio is False
        assert response.request.include_transcript is True
        assert response.result is not None
        assert response.result.has_transcript is False
        assert response.result.audio_fallback is True
