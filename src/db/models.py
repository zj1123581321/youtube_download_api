"""
Database models and enums.

Defines data structures for video resources, files, and tasks.
Architecture: Video -> Files <- Task (video owns files, tasks reference files)
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class TaskStatus(str, Enum):
    """Task status enumeration."""

    PENDING = "pending"  # Waiting for download
    DOWNLOADING = "downloading"  # Currently downloading
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"  # Failed after all retries
    CANCELLED = "cancelled"  # Cancelled by user


class ErrorCode(str, Enum):
    """Error code enumeration."""

    # Video issues
    VIDEO_UNAVAILABLE = "VIDEO_UNAVAILABLE"  # Video doesn't exist / deleted
    VIDEO_PRIVATE = "VIDEO_PRIVATE"  # Private video
    VIDEO_REGION_BLOCKED = "VIDEO_REGION_BLOCKED"  # Region restricted
    VIDEO_AGE_RESTRICTED = "VIDEO_AGE_RESTRICTED"  # Age restricted
    VIDEO_LIVE_STREAM = "VIDEO_LIVE_STREAM"  # Live stream not supported

    # Download issues
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"  # General download failure
    RATE_LIMITED = "RATE_LIMITED"  # Rate limited by YouTube
    NETWORK_ERROR = "NETWORK_ERROR"  # Network error

    # System issues
    POT_TOKEN_FAILED = "POT_TOKEN_FAILED"  # PO Token acquisition failed
    INTERNAL_ERROR = "INTERNAL_ERROR"  # Internal error


class CallbackStatus(str, Enum):
    """Callback status enumeration."""

    PENDING = "pending"  # Not yet sent
    SUCCESS = "success"  # Successfully delivered
    FAILED = "failed"  # Failed after all retries


class FileType(str, Enum):
    """File type enumeration."""

    AUDIO = "audio"
    TRANSCRIPT = "transcript"


@dataclass
class VideoInfo:
    """Video information extracted from YouTube."""

    title: Optional[str] = None
    author: Optional[str] = None
    channel_id: Optional[str] = None
    duration: Optional[int] = None
    description: Optional[str] = None
    upload_date: Optional[str] = None
    view_count: Optional[int] = None
    thumbnail: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "author": self.author,
            "channel_id": self.channel_id,
            "duration": self.duration,
            "description": self.description,
            "upload_date": self.upload_date,
            "view_count": self.view_count,
            "thumbnail": self.thumbnail,
        }

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> Optional["VideoInfo"]:
        """Create from dictionary."""
        if not data:
            return None
        return cls(
            title=data.get("title"),
            author=data.get("author"),
            channel_id=data.get("channel_id"),
            duration=data.get("duration"),
            description=data.get("description"),
            upload_date=data.get("upload_date"),
            view_count=data.get("view_count"),
            thumbnail=data.get("thumbnail"),
        )


@dataclass
class VideoResource:
    """
    Video resource entity - the core entity.

    One video_id corresponds to one record, storing video metadata.
    Files are associated with video_id, not task_id.
    """

    video_id: str  # YouTube video ID (primary key)
    video_info: Optional[VideoInfo] = None  # Video metadata
    has_native_transcript: Optional[bool] = None  # Whether video has native subtitles (cached)

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "video_id": self.video_id,
            "video_info": self.video_info.to_dict() if self.video_info else None,
            "has_native_transcript": self.has_native_transcript,
        }


@dataclass
class FileRecord:
    """
    File record entity - resource entity.

    Files are indexed by video_id, supporting multiple files per video.
    Unique constraint: (video_id, file_type, quality, language)
    """

    id: str  # UUID for download URL
    video_id: str  # Associated video (not task_id anymore)
    file_type: FileType  # audio / transcript

    # File attributes
    filename: str  # Actual filename
    filepath: str  # Relative path from data_dir
    size: Optional[int] = None  # File size in bytes
    format: Optional[str] = None  # m4a / srt

    # Extended attributes (for future multi-version support)
    quality: Optional[str] = None  # Audio: 128 / 320
    language: Optional[str] = None  # Transcript: en / zh

    # Lifecycle
    created_at: Optional[datetime] = None
    last_accessed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None


@dataclass
class Task:
    """
    Task record entity - request entity.

    Tasks are triggers that reference existing resources or trigger new downloads.
    Tasks don't own files; they reference files via file_ids.
    """

    id: str  # UUID
    video_id: str  # YouTube video ID
    video_url: str  # Original URL
    status: TaskStatus = TaskStatus.PENDING

    # Request parameters (what client wants)
    include_audio: bool = True  # Whether to download audio
    include_transcript: bool = True  # Whether to fetch transcript

    # File references (pointing to files table, may reuse existing files)
    audio_file_id: Optional[str] = None
    transcript_file_id: Optional[str] = None

    # Reuse flags (for statistics/debugging)
    reused_audio: bool = False  # Whether audio file was reused
    reused_transcript: bool = False  # Whether transcript file was reused

    # Callback configuration
    callback_url: Optional[str] = None
    callback_secret: Optional[str] = None
    callback_status: Optional[CallbackStatus] = None
    callback_attempts: int = 0

    # Error information
    error_code: Optional[ErrorCode] = None
    error_message: Optional[str] = None
    retry_count: int = 0

    # Timestamps
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Progress tracking (not persisted)
    progress: int = field(default=0, compare=False)


# Retry configuration for different error types
RETRY_CONFIG: dict[ErrorCode, dict[str, Any]] = {
    # Retryable errors
    ErrorCode.NETWORK_ERROR: {
        "max_retries": 3,
        "backoff": [120, 240, 480],  # Exponential backoff (seconds)
        "jitter": 30,  # Random jitter range (seconds)
    },
    ErrorCode.RATE_LIMITED: {
        "max_retries": 3,
        "backoff": [120, 240, 480],
        "jitter": 60,
    },
    ErrorCode.POT_TOKEN_FAILED: {
        "max_retries": 3,
        "backoff": [120, 240, 480],
        "jitter": 30,
    },
    ErrorCode.DOWNLOAD_FAILED: {
        "max_retries": 3,
        "backoff": [120, 240, 480],
        "jitter": 30,
    },
    # Non-retryable errors (fail immediately)
    ErrorCode.VIDEO_UNAVAILABLE: {"max_retries": 0},
    ErrorCode.VIDEO_PRIVATE: {"max_retries": 0},
    ErrorCode.VIDEO_REGION_BLOCKED: {"max_retries": 0},
    ErrorCode.VIDEO_AGE_RESTRICTED: {"max_retries": 0},
    ErrorCode.VIDEO_LIVE_STREAM: {"max_retries": 0},
    ErrorCode.INTERNAL_ERROR: {"max_retries": 0},
}


def is_retryable_error(error_code: ErrorCode) -> bool:
    """
    Check if an error code is retryable.

    Args:
        error_code: The error code to check.

    Returns:
        True if the error is retryable.
    """
    config = RETRY_CONFIG.get(error_code, {})
    return config.get("max_retries", 0) > 0
