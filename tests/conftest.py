"""
Pytest fixtures and configuration.
"""

import asyncio
import tempfile
from pathlib import Path
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from src.config import Settings
from src.db.database import Database
from src.db.models import Task, TaskStatus, VideoInfo
from src.services.file_service import FileService


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_settings(temp_dir: Path) -> Settings:
    """Create test settings."""
    return Settings(
        api_key="test-api-key-12345",
        wecom_webhook_url="",
        debug=True,
        pot_server_url="http://localhost:4416",
        data_dir=temp_dir,
        file_retention_days=1,
        task_interval_min=5,
        task_interval_max=10,
        dry_run=True,
    )


@pytest_asyncio.fixture
async def test_db(temp_dir: Path) -> AsyncGenerator[Database, None]:
    """Create test database."""
    db_path = temp_dir / "test.sqlite"
    db = Database(db_path)
    await db.connect()
    yield db
    await db.disconnect()


@pytest_asyncio.fixture
async def file_service(
    test_db: Database, test_settings: Settings
) -> FileService:
    """Create file service for testing."""
    return FileService(test_db, test_settings)


@pytest.fixture
def mock_downloader() -> AsyncMock:
    """Mock yt-dlp downloader."""
    from src.core.downloader import DownloadResult, TranscriptOnlyResult

    downloader = AsyncMock()
    downloader.download.return_value = DownloadResult(
        video_info=VideoInfo(
            title="Test Video",
            author="Test Author",
            duration=60,
            channel_id="UC123456",
        ),
        audio_path=Path("/tmp/test.m4a"),
        transcript_path=Path("/tmp/test.en.srt"),
    )
    # 默认模拟有字幕的情况
    downloader.extract_transcript_only.return_value = TranscriptOnlyResult(
        video_info=VideoInfo(
            title="Test Video",
            author="Test Author",
            duration=60,
            channel_id="UC123456",
        ),
        has_transcript=True,
        transcript_path=Path("/tmp/test.en.srt"),
    )
    return downloader


@pytest.fixture
def mock_downloader_no_transcript() -> AsyncMock:
    """Mock downloader for videos without transcript."""
    from src.core.downloader import DownloadResult, TranscriptOnlyResult

    downloader = AsyncMock()
    # 下载时没有字幕
    downloader.download.return_value = DownloadResult(
        video_info=VideoInfo(
            title="Test Video No Subs",
            author="Test Author",
            duration=60,
            channel_id="UC123456",
        ),
        audio_path=Path("/tmp/test.m4a"),
        transcript_path=None,
    )
    # 提取字幕时发现没有字幕
    downloader.extract_transcript_only.return_value = TranscriptOnlyResult(
        video_info=VideoInfo(
            title="Test Video No Subs",
            author="Test Author",
            duration=60,
            channel_id="UC123456",
        ),
        has_transcript=False,
        transcript_path=None,
    )
    return downloader


@pytest.fixture
def mock_notifier() -> MagicMock:
    """Mock WeCom notifier."""
    notifier = MagicMock()
    notifier.send_markdown.return_value = MagicMock(is_success=lambda: True)
    return notifier


@pytest.fixture
def sample_task() -> Task:
    """Create sample task for testing."""
    return Task(
        id="550e8400-e29b-41d4-a716-446655440000",
        video_id="dQw4w9WgXcQ",
        video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        status=TaskStatus.PENDING,
    )


@pytest.fixture
def sample_video_info() -> VideoInfo:
    """Create sample video info for testing."""
    return VideoInfo(
        title="Rick Astley - Never Gonna Give You Up",
        author="Rick Astley",
        channel_id="UCuAXFkgsw1L7xaCfnd5JJOw",
        duration=213,
        description="The official video for Never Gonna Give You Up",
        upload_date="20091025",
        view_count=1500000000,
        thumbnail="https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg",
    )
