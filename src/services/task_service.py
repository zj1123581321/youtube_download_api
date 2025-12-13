"""
Task service module.

Handles task creation, querying, and resource-based deduplication.
Implements file-level caching: if requested resources already exist,
returns them without creating a new download task.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from src.api.schemas import (
    CreateTaskRequest,
    ErrorInfoResponse,
    FileInfoResponse,
    FilesResponse,
    RequestModeResponse,
    ResultInfoResponse,
    TaskListResponse,
    TaskResponse,
    VideoInfoResponse,
)
from src.config import Settings
from src.db.database import Database
from src.db.models import CallbackStatus, FileType, Task, TaskStatus
from src.services.file_service import FileService
from src.utils.helpers import extract_video_id
from src.utils.logger import logger


class TaskService:
    """
    Service for managing download tasks.

    Implements resource-based caching: checks if requested files already exist
    before creating download tasks. Files are shared across tasks for the same video.
    """

    def __init__(self, db: Database, settings: Settings, file_service: FileService):
        """
        Initialize task service.

        Args:
            db: Database instance.
            settings: Application settings.
            file_service: File service for resource management.
        """
        self.db = db
        self.settings = settings
        self.file_service = file_service
        # Task queue for worker to consume
        self._task_queue: asyncio.Queue[str] = asyncio.Queue()

    @property
    def task_queue(self) -> asyncio.Queue[str]:
        """Get the task queue."""
        return self._task_queue

    async def create_task(self, request: CreateTaskRequest) -> TaskResponse:
        """
        Create a new download task or return cached resources.

        Resource-based caching logic:
        1. Check if requested files already exist for this video
        2. If all needed files exist -> return immediately with cached resources
        3. If some files missing -> create task to download missing files
        4. If active task exists for same video -> return existing task

        Args:
            request: Task creation request.

        Returns:
            TaskResponse with task details or cached resources.
        """
        video_id = extract_video_id(request.video_url)
        if not video_id:
            raise ValueError("Invalid YouTube URL")

        # Check existing resources for this video
        existing_files = await self.file_service.get_all_files_for_video(video_id)
        existing_audio = existing_files.get("audio")
        existing_transcript = existing_files.get("transcript")

        # Check if video is known to have no native transcript
        video_resource = await self.db.get_video_resource(video_id)
        video_has_no_transcript = (
            video_resource is not None
            and video_resource.has_native_transcript is False
        )

        # Determine what we need
        need_audio = request.include_audio and existing_audio is None
        need_transcript = request.include_transcript and existing_transcript is None

        # Special case: user requests transcript, but video is known to have none
        # If audio exists, return it as fallback (no need to create new task)
        if (
            need_transcript
            and video_has_no_transcript
            and existing_audio is not None
        ):
            logger.info(
                f"Cache hit for video {video_id}: no native transcript, "
                f"returning existing audio as fallback"
            )
            return await self._build_cached_response(
                video_id=video_id,
                video_url=request.video_url,
                request=request,
                audio_file=existing_audio,
                transcript_file=None,
                audio_fallback=True,
            )

        # If all requested resources exist, return immediately (cache hit)
        if not need_audio and not need_transcript:
            logger.info(f"Cache hit for video {video_id}: all requested resources exist")
            return await self._build_cached_response(
                video_id=video_id,
                video_url=request.video_url,
                request=request,
                audio_file=existing_audio,
                transcript_file=existing_transcript,
            )

        # Check for active (pending/downloading) task for same video
        active_task = await self.db.get_active_task_by_video(video_id)
        if active_task:
            logger.info(f"Found active task for video {video_id}: {active_task.id}")
            response = await self._build_task_response(active_task)
            response.message = "Task already in progress"
            return response

        # Ensure video resource exists
        await self.db.get_or_create_video_resource(video_id)

        # Create new task with pre-filled existing resources
        task = Task(
            id=str(uuid4()),
            video_id=video_id,
            video_url=request.video_url,
            status=TaskStatus.PENDING,
            include_audio=request.include_audio,
            include_transcript=request.include_transcript,
            # Pre-fill existing resources
            audio_file_id=existing_audio.id if existing_audio else None,
            transcript_file_id=existing_transcript.id if existing_transcript else None,
            reused_audio=existing_audio is not None,
            reused_transcript=existing_transcript is not None,
            callback_url=str(request.callback_url) if request.callback_url else None,
            callback_secret=request.callback_secret,
            callback_status=CallbackStatus.PENDING if request.callback_url else None,
            created_at=datetime.now(timezone.utc),
        )

        await self.db.create_task(task)
        logger.info(
            f"Created task {task.id} for video {video_id} "
            f"(need_audio={need_audio}, need_transcript={need_transcript}, "
            f"reused_audio={task.reused_audio}, reused_transcript={task.reused_transcript})"
        )

        # Add to queue for worker
        await self._task_queue.put(task.id)

        # Build response
        response = await self._build_task_response(task)
        return response

    async def _build_cached_response(
        self,
        video_id: str,
        video_url: str,
        request: CreateTaskRequest,
        audio_file,
        transcript_file,
        audio_fallback: bool = False,
    ) -> TaskResponse:
        """
        Build response for cache hit (all resources exist).

        Args:
            video_id: YouTube video ID.
            video_url: Original video URL.
            request: Original request.
            audio_file: Existing audio FileRecord (or None).
            transcript_file: Existing transcript FileRecord (or None).
            audio_fallback: Whether audio is returned as fallback for missing transcript.

        Returns:
            TaskResponse with cached resources.
        """
        # Get video resource for metadata
        video_resource = await self.db.get_video_resource(video_id)

        # Build response
        now = datetime.now(timezone.utc)
        response = TaskResponse(
            task_id=None,  # No task created for cache hits
            cache_hit=True,  # Indicate this is a cache hit
            status=TaskStatus.COMPLETED,
            video_id=video_id,
            video_url=video_url,
            created_at=now,
            completed_at=now,
            message="Resources retrieved from cache",
            request=RequestModeResponse(
                include_audio=request.include_audio,
                include_transcript=request.include_transcript,
            ),
            result=ResultInfoResponse(
                has_transcript=transcript_file is not None,
                audio_fallback=audio_fallback,
                reused_audio=audio_file is not None if request.include_audio or audio_fallback else False,
                reused_transcript=transcript_file is not None if request.include_transcript else False,
            ),
        )

        # Add video info if available
        if video_resource and video_resource.video_info:
            info = video_resource.video_info
            response.video_info = VideoInfoResponse(
                title=info.title,
                author=info.author,
                channel_id=info.channel_id,
                duration=info.duration,
                description=info.description,
                upload_date=info.upload_date,
                view_count=info.view_count,
                thumbnail=info.thumbnail,
            )

        # Build files response
        audio_info = None
        # Include audio if requested OR if it's a fallback for missing transcript
        if audio_file and (request.include_audio or audio_fallback):
            audio_info = FileInfoResponse(
                url=f"/api/v1/files/{audio_file.id}",
                size=audio_file.size,
                format=audio_file.format,
                bitrate=audio_file.quality,
            )

        transcript_info = None
        if transcript_file and request.include_transcript:
            transcript_info = FileInfoResponse(
                url=f"/api/v1/files/{transcript_file.id}",
                size=transcript_file.size,
                format=transcript_file.format,
                language=transcript_file.language,
            )

        if audio_info or transcript_info:
            response.files = FilesResponse(
                audio=audio_info,
                transcript=transcript_info,
            )

        # Set expiry based on file expiry
        if audio_file:
            response.expires_at = audio_file.expires_at
        elif transcript_file:
            response.expires_at = transcript_file.expires_at

        return response

    async def get_task(self, task_id: str) -> Optional[TaskResponse]:
        """
        Get task by ID.

        Args:
            task_id: Task UUID.

        Returns:
            TaskResponse or None if not found.
        """
        task = await self.db.get_task(task_id)
        if not task:
            return None

        return await self._build_task_response(task)

    async def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> TaskListResponse:
        """
        List tasks with pagination and optional filtering.

        Args:
            status: Filter by task status.
            limit: Maximum number of results (max 100).
            offset: Number of results to skip.

        Returns:
            TaskListResponse with paginated results.
        """
        # Enforce limit
        limit = min(limit, 100)

        tasks, total = await self.db.list_tasks(status=status, limit=limit, offset=offset)

        task_responses = []
        for task in tasks:
            response = await self._build_task_response(task)
            task_responses.append(response)

        return TaskListResponse(
            tasks=task_responses,
            total=total,
            limit=limit,
            offset=offset,
        )

    async def cancel_task(self, task_id: str) -> Optional[TaskResponse]:
        """
        Cancel a pending task.

        Only pending tasks can be cancelled. Tasks that are already
        downloading or completed cannot be cancelled.

        Args:
            task_id: Task UUID.

        Returns:
            TaskResponse or None if task not found.

        Raises:
            ValueError: If task cannot be cancelled.
        """
        task = await self.db.get_task(task_id)
        if not task:
            return None

        if task.status != TaskStatus.PENDING:
            raise ValueError(f"Cannot cancel task with status: {task.status.value}")

        await self.db.update_task_status(task_id, TaskStatus.CANCELLED)
        logger.info(f"Task cancelled: {task_id}")

        task.status = TaskStatus.CANCELLED
        response = await self._build_task_response(task)
        response.message = "Task cancelled successfully"
        return response

    async def get_next_task(self) -> Optional[Task]:
        """
        Get next task from queue.

        Returns:
            Task object or None if queue is empty.
        """
        try:
            task_id = await asyncio.wait_for(self._task_queue.get(), timeout=1.0)
            task = await self.db.get_task(task_id)

            # Skip if task was cancelled while waiting
            if task and task.status != TaskStatus.PENDING:
                logger.debug(f"Skipping task {task_id} with status {task.status}")
                return None

            return task
        except asyncio.TimeoutError:
            return None

    async def restore_pending_tasks(self) -> int:
        """
        Restore pending tasks to queue after restart.

        Returns:
            Number of tasks restored.
        """
        tasks = await self.db.get_pending_tasks(limit=100)
        count = 0

        for task in tasks:
            await self._task_queue.put(task.id)
            count += 1

        if count > 0:
            logger.info(f"Restored {count} pending tasks to queue")

        return count

    async def _build_task_response(self, task: Task) -> TaskResponse:
        """
        Build TaskResponse from Task model.

        Args:
            task: Task database model.

        Returns:
            TaskResponse API model.
        """
        # Base response
        response = TaskResponse(
            task_id=task.id,
            status=task.status,
            video_id=task.video_id,
            video_url=task.video_url,
            created_at=task.created_at or datetime.now(timezone.utc),
            started_at=task.started_at,
            completed_at=task.completed_at,
            request=RequestModeResponse(
                include_audio=task.include_audio,
                include_transcript=task.include_transcript,
            ),
        )

        # Add queue position for pending tasks
        if task.status == TaskStatus.PENDING:
            position = await self.db.get_queue_position(task.id)
            response.position = position
            # Estimate wait time based on average task interval
            avg_interval = (
                self.settings.task_interval_min + self.settings.task_interval_max
            ) / 2
            response.estimated_wait = int(position * avg_interval)

        # Add progress for downloading tasks
        elif task.status == TaskStatus.DOWNLOADING:
            response.progress = task.progress

        # Add video info and files for completed tasks
        elif task.status == TaskStatus.COMPLETED:
            # Get video resource for metadata
            video_resource = await self.db.get_video_resource(task.video_id)
            if video_resource and video_resource.video_info:
                info = video_resource.video_info
                response.video_info = VideoInfoResponse(
                    title=info.title,
                    author=info.author,
                    channel_id=info.channel_id,
                    duration=info.duration,
                    description=info.description,
                    upload_date=info.upload_date,
                    view_count=info.view_count,
                    thumbnail=info.thumbnail,
                )

            # Build result info
            # Determine has_transcript based on whether transcript file exists
            has_transcript = task.transcript_file_id is not None
            # audio_fallback: requested transcript only but got audio instead
            audio_fallback = (
                not task.include_audio
                and task.include_transcript
                and task.audio_file_id is not None
                and task.transcript_file_id is None
            )

            response.result = ResultInfoResponse(
                has_transcript=has_transcript,
                audio_fallback=audio_fallback,
                reused_audio=task.reused_audio,
                reused_transcript=task.reused_transcript,
            )

            # Get file info
            audio_file = None
            transcript_file = None

            if task.audio_file_id:
                audio_file = await self.db.get_file(task.audio_file_id)
            if task.transcript_file_id:
                transcript_file = await self.db.get_file(task.transcript_file_id)

            # Build files response
            if audio_file or transcript_file:
                audio_info = None
                if audio_file:
                    audio_info = FileInfoResponse(
                        url=f"/api/v1/files/{audio_file.id}",
                        size=audio_file.size,
                        format=audio_file.format,
                        bitrate=audio_file.quality,
                    )

                transcript_info = None
                if transcript_file:
                    transcript_info = FileInfoResponse(
                        url=f"/api/v1/files/{transcript_file.id}",
                        size=transcript_file.size,
                        format=transcript_file.format,
                        language=transcript_file.language,
                    )

                response.files = FilesResponse(
                    audio=audio_info,
                    transcript=transcript_info,
                )

            # Set expiry
            if audio_file:
                response.expires_at = audio_file.expires_at
            elif transcript_file:
                response.expires_at = transcript_file.expires_at

        # Add error info for failed tasks
        elif task.status == TaskStatus.FAILED:
            if task.error_code:
                response.error = ErrorInfoResponse(
                    code=task.error_code,
                    message=task.error_message or "Unknown error",
                    retry_count=task.retry_count,
                )

        return response
