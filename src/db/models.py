"""
Database models and enums.

Defines data structures for tasks and files stored in SQLite.
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
class FileRecord:
    """File record in database."""

    id: str  # UUID for URL
    task_id: str
    type: FileType
    filename: str  # Actual filename
    filepath: str  # Relative path
    size: Optional[int] = None  # File size in bytes
    format: Optional[str] = None  # m4a / json
    metadata: Optional[dict[str, Any]] = None  # Additional metadata

    created_at: Optional[datetime] = None
    last_accessed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None


@dataclass
class Task:
    """Task record in database."""

    id: str  # UUID
    video_id: str  # YouTube video ID
    video_url: str  # Original URL
    status: TaskStatus = TaskStatus.PENDING

    # Request mode configuration
    include_audio: bool = True  # Whether to download audio
    include_transcript: bool = True  # Whether to fetch transcript

    # Result info (populated after execution)
    has_transcript: Optional[bool] = None  # Whether video has available transcript
    audio_fallback: bool = False  # Whether audio was downloaded as fallback

    # Video info (populated after download)
    video_info: Optional[VideoInfo] = None

    # File references
    audio_file_id: Optional[str] = None
    transcript_file_id: Optional[str] = None

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
    expires_at: Optional[datetime] = None

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
