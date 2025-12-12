"""
Task service module.

Handles task creation, querying, and management logic.
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
    TaskListResponse,
    TaskResponse,
    VideoInfoResponse,
)
from src.config import Settings
from src.db.database import Database
from src.db.models import CallbackStatus, Task, TaskStatus
from src.utils.helpers import extract_video_id, get_expiry_time
from src.utils.logger import logger


class TaskService:
    """
    Service for managing download tasks.

    Handles task creation, deduplication, status queries, and lifecycle management.
    """

    def __init__(self, db: Database, settings: Settings):
        """
        Initialize task service.

        Args:
            db: Database instance.
            settings: Application settings.
        """
        self.db = db
        self.settings = settings
        # Task queue for worker to consume
        self._task_queue: asyncio.Queue[str] = asyncio.Queue()

    @property
    def task_queue(self) -> asyncio.Queue[str]:
        """Get the task queue."""
        return self._task_queue

    async def create_task(self, request: CreateTaskRequest) -> TaskResponse:
        """
        Create a new download task or return existing one.

        Implements deduplication: if a task for the same video already exists
        and is not failed/cancelled, returns the existing task.

        Args:
            request: Task creation request.

        Returns:
            TaskResponse with task details.
        """
        video_id = extract_video_id(request.video_url)
        if not video_id:
            raise ValueError("Invalid YouTube URL")

        # Check for existing task (deduplication)
        existing = await self.db.get_task_by_video_id(video_id, active_only=True)

        if existing:
            logger.info(f"Found existing task for video {video_id}: {existing.id}")
            response = await self._build_task_response(existing)
            response.message = "Task already exists"
            return response

        # Create new task
        task = Task(
            id=str(uuid4()),
            video_id=video_id,
            video_url=request.video_url,
            status=TaskStatus.PENDING,
            callback_url=str(request.callback_url) if request.callback_url else None,
            callback_secret=request.callback_secret,
            callback_status=CallbackStatus.PENDING if request.callback_url else None,
            created_at=datetime.now(timezone.utc),
        )

        await self.db.create_task(task)
        logger.info(f"Created new task: {task.id} for video {video_id}")

        # Add to queue for worker
        await self._task_queue.put(task.id)

        # Build response with queue position
        response = await self._build_task_response(task)
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
            expires_at=task.expires_at,
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
            if task.video_info:
                response.video_info = VideoInfoResponse(
                    title=task.video_info.title,
                    author=task.video_info.author,
                    channel_id=task.video_info.channel_id,
                    duration=task.video_info.duration,
                    description=task.video_info.description,
                    upload_date=task.video_info.upload_date,
                    view_count=task.video_info.view_count,
                    thumbnail=task.video_info.thumbnail,
                )

            # Get file info
            if task.audio_file_id:
                files = await self.db.get_files_by_task(task.id)
                audio_file = next(
                    (f for f in files if f.id == task.audio_file_id), None
                )
                transcript_file = (
                    next((f for f in files if f.id == task.transcript_file_id), None)
                    if task.transcript_file_id
                    else None
                )

                if audio_file:
                    audio_info = FileInfoResponse(
                        url=f"/api/v1/files/{audio_file.id}",
                        size=audio_file.size,
                        format=audio_file.format,
                        bitrate=audio_file.metadata.get("bitrate")
                        if audio_file.metadata
                        else None,
                    )

                    transcript_info = None
                    if transcript_file:
                        transcript_info = FileInfoResponse(
                            url=f"/api/v1/files/{transcript_file.id}",
                            size=transcript_file.size,
                            format=transcript_file.format,
                            language=transcript_file.metadata.get("language")
                            if transcript_file.metadata
                            else None,
                        )

                    response.files = FilesResponse(
                        audio=audio_info,
                        transcript=transcript_info,
                    )

        # Add error info for failed tasks
        elif task.status == TaskStatus.FAILED:
            if task.error_code:
                response.error = ErrorInfoResponse(
                    code=task.error_code,
                    message=task.error_message or "Unknown error",
                    retry_count=task.retry_count,
                )

        return response
