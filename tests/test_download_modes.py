"""
Tests for download mode functionality and resource caching.

Tests the include_audio and include_transcript parameters,
resource reuse logic, and file-level caching.
"""

import tempfile
from datetime import datetime, timezone
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
from src.db.models import FileRecord, FileType, Task, TaskStatus, VideoInfo
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
    async def task_service(
        self, test_db: Database, test_settings: Settings, file_service: FileService
    ):
        """Create task service for testing."""
        return TaskService(test_db, test_settings, file_service)

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


# ==================== Resource Caching Tests ====================


class TestResourceCaching:
    """Test resource caching and reuse logic."""

    @pytest_asyncio.fixture
    async def task_service(
        self, test_db: Database, test_settings: Settings, file_service: FileService
    ):
        """Create task service for testing."""
        return TaskService(test_db, test_settings, file_service)

    @pytest.mark.asyncio
    async def test_cache_hit_when_all_resources_exist(
        self,
        test_db: Database,
        test_settings: Settings,
        file_service: FileService,
        task_service: TaskService,
        temp_dir: Path,
    ):
        """Test cache hit when all requested resources already exist."""
        video_id = "cachetest123"

        # Create video resource
        await test_db.get_or_create_video_resource(video_id)

        # Create mock audio file on disk and in database
        audio_dir = test_settings.audio_dir
        audio_dir.mkdir(parents=True, exist_ok=True)
        audio_path = audio_dir / "test-audio.m4a"
        audio_path.write_text("mock audio")

        audio_record = FileRecord(
            id="audio-file-001",
            video_id=video_id,
            file_type=FileType.AUDIO,
            filename="test-audio.m4a",
            filepath=str(audio_path.relative_to(test_settings.data_dir)),
            size=100,
            format="m4a",
            created_at=datetime.now(timezone.utc),
            last_accessed_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )
        await test_db.create_file(audio_record)

        # Create mock transcript file on disk and in database
        transcript_dir = test_settings.transcript_dir
        transcript_dir.mkdir(parents=True, exist_ok=True)
        transcript_path = transcript_dir / "test-transcript.json"
        transcript_path.write_text("mock transcript")

        transcript_record = FileRecord(
            id="transcript-file-001",
            video_id=video_id,
            file_type=FileType.TRANSCRIPT,
            filename="test-transcript.json",
            filepath=str(transcript_path.relative_to(test_settings.data_dir)),
            size=50,
            format="json",
            language="en",
            created_at=datetime.now(timezone.utc),
            last_accessed_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )
        await test_db.create_file(transcript_record)

        # Request for same video should hit cache
        request = CreateTaskRequest(
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            include_audio=True,
            include_transcript=True,
        )
        response = await task_service.create_task(request)

        # Should return immediately with cached status
        assert response.status == TaskStatus.COMPLETED
        assert response.task_id is None  # No task created for cache hits
        assert response.cache_hit is True
        assert response.message == "Resources retrieved from cache"
        assert response.result is not None
        assert response.result.reused_audio is True
        assert response.result.reused_transcript is True

    @pytest.mark.asyncio
    async def test_partial_cache_hit_audio_only(
        self,
        test_db: Database,
        test_settings: Settings,
        file_service: FileService,
        task_service: TaskService,
        temp_dir: Path,
    ):
        """Test partial cache hit when only audio exists."""
        video_id = "partialtest1"

        # Create video resource
        await test_db.get_or_create_video_resource(video_id)

        # Create mock audio file on disk and in database
        audio_dir = test_settings.audio_dir
        audio_dir.mkdir(parents=True, exist_ok=True)
        audio_path = audio_dir / "partial-audio.m4a"
        audio_path.write_text("mock audio")

        audio_record = FileRecord(
            id="audio-partial-001",
            video_id=video_id,
            file_type=FileType.AUDIO,
            filename="partial-audio.m4a",
            filepath=str(audio_path.relative_to(test_settings.data_dir)),
            size=100,
            format="m4a",
            created_at=datetime.now(timezone.utc),
            last_accessed_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )
        await test_db.create_file(audio_record)

        # Request for audio+transcript should create task (transcript missing)
        request = CreateTaskRequest(
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            include_audio=True,
            include_transcript=True,
        )
        response = await task_service.create_task(request)

        # Should create new task (transcript missing)
        assert response.status == TaskStatus.PENDING
        assert response.task_id is not None  # Real task created
        assert response.cache_hit is False

    @pytest.mark.asyncio
    async def test_cache_hit_audio_only_request(
        self,
        test_db: Database,
        test_settings: Settings,
        file_service: FileService,
        task_service: TaskService,
        temp_dir: Path,
    ):
        """Test cache hit when requesting only audio and audio exists."""
        video_id = "audioonlytest"

        # Create video resource
        await test_db.get_or_create_video_resource(video_id)

        # Create mock audio file on disk and in database
        audio_dir = test_settings.audio_dir
        audio_dir.mkdir(parents=True, exist_ok=True)
        audio_path = audio_dir / "audioonly.m4a"
        audio_path.write_text("mock audio")

        audio_record = FileRecord(
            id="audio-only-001",
            video_id=video_id,
            file_type=FileType.AUDIO,
            filename="audioonly.m4a",
            filepath=str(audio_path.relative_to(test_settings.data_dir)),
            size=100,
            format="m4a",
            created_at=datetime.now(timezone.utc),
            last_accessed_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )
        await test_db.create_file(audio_record)

        # Request for audio only should hit cache
        request = CreateTaskRequest(
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            include_audio=True,
            include_transcript=False,
        )
        response = await task_service.create_task(request)

        # Should return immediately with cached status
        assert response.status == TaskStatus.COMPLETED
        assert response.task_id is None  # No task created for cache hits
        assert response.cache_hit is True
        assert response.result.reused_audio is True

    @pytest.mark.asyncio
    async def test_stale_file_record_cleanup(
        self,
        test_db: Database,
        test_settings: Settings,
        file_service: FileService,
        task_service: TaskService,
        temp_dir: Path,
    ):
        """Test that stale file records (file deleted) are cleaned up."""
        video_id = "stalefile123"

        # Create video resource
        await test_db.get_or_create_video_resource(video_id)

        # Create file record WITHOUT actual file on disk
        audio_record = FileRecord(
            id="stale-audio-001",
            video_id=video_id,
            file_type=FileType.AUDIO,
            filename="nonexistent.m4a",
            filepath="audio/nonexistent.m4a",  # File doesn't exist
            size=100,
            format="m4a",
            created_at=datetime.now(timezone.utc),
            last_accessed_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )
        await test_db.create_file(audio_record)

        # Request should NOT hit cache (file doesn't exist on disk)
        request = CreateTaskRequest(
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            include_audio=True,
            include_transcript=False,
        )
        response = await task_service.create_task(request)

        # Should create new task (stale record should be cleaned up)
        assert response.status == TaskStatus.PENDING
        assert not response.task_id.startswith("cached-")

        # Verify stale record was deleted
        file_record = await test_db.get_file("stale-audio-001")
        assert file_record is None


# ==================== Worker Execution Tests ====================


class TestWorkerExecutionModes:
    """Test DownloadWorker execution for different modes."""

    @pytest_asyncio.fixture
    async def worker_deps(
        self, test_db: Database, test_settings: Settings, file_service: FileService
    ):
        """Create worker dependencies."""
        task_service = TaskService(test_db, test_settings, file_service)
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
        self, worker_deps: dict, mock_downloader: AsyncMock, temp_dir: Path, test_db: Database
    ):
        """Test full mode execution when transcript is available."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader

        video_id = "fullmode123"
        await test_db.get_or_create_video_resource(video_id)

        # Create actual temp files for mock to return
        audio_file = temp_dir / "test.m4a"
        transcript_file = temp_dir / "test.en.json"
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
            video_id=video_id,
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            status=TaskStatus.PENDING,
            include_audio=True,
            include_transcript=True,
        )

        result = await worker._execute_task(task)

        assert result["audio_file_id"] is not None
        assert result["transcript_file_id"] is not None
        assert result["reused_audio"] is False
        assert result["reused_transcript"] is False
        mock_downloader.download.assert_called_once()
        mock_downloader.extract_transcript_only.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_audio_only_mode(
        self, worker_deps: dict, mock_downloader: AsyncMock, temp_dir: Path, test_db: Database
    ):
        """Test audio-only mode execution."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader

        video_id = "audioonly123"
        await test_db.get_or_create_video_resource(video_id)

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
            video_id=video_id,
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            status=TaskStatus.PENDING,
            include_audio=True,
            include_transcript=False,
        )

        result = await worker._execute_task(task)

        assert result["audio_file_id"] is not None
        assert result["transcript_file_id"] is None
        assert result["reused_audio"] is False
        assert result["reused_transcript"] is False
        mock_downloader.download.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_transcript_only_with_available_transcript(
        self, worker_deps: dict, mock_downloader: AsyncMock, temp_dir: Path, test_db: Database
    ):
        """Test transcript-only mode when transcript is available."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader

        video_id = "subsonly123"
        await test_db.get_or_create_video_resource(video_id)

        # Create actual temp file for mock to return
        transcript_file = temp_dir / "test3.en.json"
        transcript_file.write_text("mock transcript content")

        # Update mock for transcript-only extraction
        mock_downloader.extract_transcript_only.return_value = TranscriptOnlyResult(
            video_info=VideoInfo(
                title="Test Video",
                author="Test Author",
                duration=60,
                channel_id="UC123456",
            ),
            has_transcript=True,
            transcript_path=transcript_file,
        )

        task = Task(
            id="test-task-003",
            video_id=video_id,
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            status=TaskStatus.PENDING,
            include_audio=False,
            include_transcript=True,
        )

        result = await worker._execute_task(task)

        assert result["audio_file_id"] is None
        assert result["transcript_file_id"] is not None
        assert result["reused_audio"] is False
        assert result["reused_transcript"] is False
        # Should call extract_transcript_only, not download
        mock_downloader.extract_transcript_only.assert_called_once()
        mock_downloader.download.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_transcript_only_fallback_to_audio(
        self, worker_deps: dict, mock_downloader_no_transcript: AsyncMock, temp_dir: Path, test_db: Database
    ):
        """Test transcript-only mode fallback to audio when no transcript available."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader_no_transcript

        video_id = "nosubs12345"
        await test_db.get_or_create_video_resource(video_id)

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
            video_id=video_id,
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            status=TaskStatus.PENDING,
            include_audio=False,
            include_transcript=True,
        )

        result = await worker._execute_task(task)

        assert result["audio_file_id"] is not None  # Audio downloaded as fallback
        assert result["transcript_file_id"] is None
        # Should call extract_transcript_only first, then download as fallback
        mock_downloader_no_transcript.extract_transcript_only.assert_called_once()
        mock_downloader_no_transcript.download.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_with_existing_resources_reuse(
        self, worker_deps: dict, mock_downloader: AsyncMock, temp_dir: Path, test_db: Database, test_settings: Settings
    ):
        """Test that worker reuses existing resources and only downloads missing ones."""
        worker = DownloadWorker(**worker_deps)
        worker.downloader = mock_downloader

        video_id = "reuse123456"
        await test_db.get_or_create_video_resource(video_id)

        # Create existing audio file
        audio_dir = test_settings.audio_dir
        audio_dir.mkdir(parents=True, exist_ok=True)
        existing_audio = audio_dir / "existing-audio.m4a"
        existing_audio.write_text("existing audio")

        audio_record = FileRecord(
            id="existing-audio-001",
            video_id=video_id,
            file_type=FileType.AUDIO,
            filename="existing-audio.m4a",
            filepath=str(existing_audio.relative_to(test_settings.data_dir)),
            size=100,
            format="m4a",
            created_at=datetime.now(timezone.utc),
            last_accessed_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )
        await test_db.create_file(audio_record)

        # Create temp transcript file for mock
        transcript_file = temp_dir / "new-transcript.en.json"
        transcript_file.write_text("new transcript")

        # Since audio exists, worker will call extract_transcript_only (not download)
        mock_downloader.extract_transcript_only.return_value = TranscriptOnlyResult(
            video_info=VideoInfo(
                title="Test Video",
                author="Test Author",
                duration=60,
                channel_id="UC123456",
            ),
            has_transcript=True,
            transcript_path=transcript_file,
        )

        # Task requests both but audio already exists
        task = Task(
            id="test-task-005",
            video_id=video_id,
            video_url=f"https://www.youtube.com/watch?v={video_id}",
            status=TaskStatus.PENDING,
            include_audio=True,
            include_transcript=True,
        )

        result = await worker._execute_task(task)

        # Should reuse existing audio and download new transcript
        assert result["audio_file_id"] == "existing-audio-001"
        assert result["transcript_file_id"] is not None
        assert result["reused_audio"] is True
        assert result["reused_transcript"] is False


# ==================== Database Persistence Tests ====================


class TestDatabaseModePersistence:
    """Test database persistence of download mode settings."""

    @pytest.mark.asyncio
    async def test_task_mode_saved_to_db(self, test_db: Database):
        """Test that task mode settings are saved to database."""
        await test_db.get_or_create_video_resource("dbtest12345")

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
    async def test_task_completion_with_reuse_flags(self, test_db: Database):
        """Test that task completion saves reuse flags."""
        await test_db.get_or_create_video_resource("dbtest67890")

        task = Task(
            id="db-test-002",
            video_id="dbtest67890",
            video_url="https://www.youtube.com/watch?v=dbtest67890",
            status=TaskStatus.PENDING,
            include_audio=True,
            include_transcript=True,
        )

        await test_db.create_task(task)

        # Simulate task completion with reuse
        await test_db.update_task_completed(
            task_id=task.id,
            audio_file_id="audio-id",
            transcript_file_id="transcript-id",
            reused_audio=True,
            reused_transcript=False,
        )

        retrieved = await test_db.get_task(task.id)

        assert retrieved is not None
        assert retrieved.status == TaskStatus.COMPLETED
        assert retrieved.reused_audio is True
        assert retrieved.reused_transcript is False


# ==================== Response Format Tests ====================


class TestResponseFormat:
    """Test response format for different modes."""

    @pytest_asyncio.fixture
    async def task_service(
        self, test_db: Database, test_settings: Settings, file_service: FileService
    ):
        """Create task service for testing."""
        return TaskService(test_db, test_settings, file_service)

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
            include_audio=True,
            include_transcript=True,
        )
        create_response = await task_service.create_task(request)

        # Simulate completion with reuse flags
        await test_db.update_task_completed(
            task_id=create_response.task_id,
            audio_file_id="audio-id",
            transcript_file_id="transcript-id",
            reused_audio=True,
            reused_transcript=False,
        )

        # Get updated task
        response = await task_service.get_task(create_response.task_id)

        assert response is not None
        assert response.status == TaskStatus.COMPLETED
        assert response.request is not None
        assert response.request.include_audio is True
        assert response.request.include_transcript is True
        assert response.result is not None
        assert response.result.reused_audio is True
        assert response.result.reused_transcript is False
