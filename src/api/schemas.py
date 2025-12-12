"""
Pydantic models for API request/response schemas.

Provides type-safe validation for all API endpoints.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator

from src.db.models import CallbackStatus, ErrorCode, TaskStatus
from src.utils.helpers import extract_video_id


# ==================== Request Schemas ====================


class CreateTaskRequest(BaseModel):
    """Request schema for creating a download task."""

    video_url: str = Field(
        ...,
        description="YouTube video URL",
        examples=["https://www.youtube.com/watch?v=dQw4w9WgXcQ"],
    )
    callback_url: Optional[HttpUrl] = Field(
        default=None,
        description="Webhook URL for completion callback",
    )
    callback_secret: Optional[str] = Field(
        default=None,
        description="HMAC-SHA256 secret for callback signature verification",
        min_length=8,
        max_length=256,
    )

    @field_validator("video_url")
    @classmethod
    def validate_video_url(cls, v: str) -> str:
        """Validate that the URL is a valid YouTube video URL."""
        video_id = extract_video_id(v)
        if not video_id:
            raise ValueError("Invalid YouTube video URL")
        return v


# ==================== Response Schemas ====================


class VideoInfoResponse(BaseModel):
    """Video information in response."""

    title: Optional[str] = None
    author: Optional[str] = None
    channel_id: Optional[str] = None
    duration: Optional[int] = Field(None, description="Duration in seconds")
    description: Optional[str] = None
    upload_date: Optional[str] = None
    view_count: Optional[int] = None
    thumbnail: Optional[str] = None


class FileInfoResponse(BaseModel):
    """File information in response."""

    url: str = Field(..., description="Download URL")
    size: Optional[int] = Field(None, description="File size in bytes")
    format: Optional[str] = Field(None, description="File format (m4a/json)")
    bitrate: Optional[int] = Field(None, description="Audio bitrate (for audio files)")
    language: Optional[str] = Field(
        None, description="Language code (for transcript files)"
    )


class FilesResponse(BaseModel):
    """Files information in response."""

    audio: FileInfoResponse
    transcript: Optional[FileInfoResponse] = None


class ErrorInfoResponse(BaseModel):
    """Error information in response."""

    code: ErrorCode
    message: str
    retry_count: int = 0


class TaskResponse(BaseModel):
    """Response schema for task details."""

    task_id: str
    status: TaskStatus
    video_id: str
    video_url: Optional[str] = None
    video_info: Optional[VideoInfoResponse] = None
    files: Optional[FilesResponse] = None
    error: Optional[ErrorInfoResponse] = None

    # Queue information (for pending tasks)
    position: Optional[int] = Field(None, description="Position in queue")
    estimated_wait: Optional[int] = Field(
        None, description="Estimated wait time in seconds"
    )

    # Progress (for downloading tasks)
    progress: Optional[int] = Field(None, ge=0, le=100)

    # Timestamps
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    # Additional message
    message: Optional[str] = None

    model_config = {"from_attributes": True}


class CreateTaskResponse(TaskResponse):
    """Response schema for task creation."""

    pass


class TaskListResponse(BaseModel):
    """Response schema for task list."""

    tasks: list[TaskResponse]
    total: int
    limit: int
    offset: int


class CancelTaskResponse(BaseModel):
    """Response schema for task cancellation."""

    task_id: str
    status: TaskStatus
    message: str


# ==================== Health Check Schemas ====================


class ComponentStatus(BaseModel):
    """Individual component status."""

    database: str = "ok"
    pot_provider: str = "ok"
    disk_space: str = "ok"


class QueueStatus(BaseModel):
    """Queue statistics."""

    pending: int = 0
    downloading: int = 0


class HealthResponse(BaseModel):
    """Response schema for health check."""

    status: str = "healthy"
    version: str
    components: ComponentStatus
    queue: QueueStatus
    uptime: int = Field(..., description="Uptime in seconds")


# ==================== Callback Schemas ====================


class CallbackPayload(BaseModel):
    """Webhook callback payload."""

    task_id: str
    status: TaskStatus
    video_id: str
    video_info: Optional[VideoInfoResponse] = None
    files: Optional[FilesResponse] = None
    error: Optional[ErrorInfoResponse] = None
    expires_at: Optional[datetime] = None


# ==================== Error Response Schemas ====================


class ErrorResponse(BaseModel):
    """Standard error response."""

    detail: str
    error_code: Optional[str] = None


class ValidationErrorDetail(BaseModel):
    """Validation error detail."""

    loc: list[Any]
    msg: str
    type: str


class ValidationErrorResponse(BaseModel):
    """Validation error response."""

    detail: list[ValidationErrorDetail]
