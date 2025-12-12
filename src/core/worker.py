"""
Download worker module.

Background worker that processes download tasks from the queue.
"""

import asyncio
import random
import tempfile
from pathlib import Path
from typing import Optional

from src.config import Settings
from src.core.downloader import DownloadError, DownloadResult, YouTubeDownloader
from src.db.database import Database
from src.db.models import (
    ErrorCode,
    FileType,
    FileRecord,
    RETRY_CONFIG,
    Task,
    TaskStatus,
    is_retryable_error,
)
from src.services.callback_service import CallbackService
from src.services.file_service import FileService
from src.services.notify import NotificationService
from src.services.task_service import TaskService
from src.utils.helpers import get_expiry_time
from src.utils.logger import logger


class DownloadWorker:
    """
    Background worker for processing download tasks.

    Handles task execution, retry logic, and notifications.
    """

    def __init__(
        self,
        db: Database,
        settings: Settings,
        task_service: TaskService,
        file_service: FileService,
        callback_service: CallbackService,
        notify_service: NotificationService,
    ):
        """
        Initialize download worker.

        Args:
            db: Database instance.
            settings: Application settings.
            task_service: Task service.
            file_service: File service.
            callback_service: Callback service.
            notify_service: Notification service.
        """
        self.db = db
        self.settings = settings
        self.task_service = task_service
        self.file_service = file_service
        self.callback_service = callback_service
        self.notify_service = notify_service

        self.downloader = YouTubeDownloader(settings)
        self._running = False
        self._current_task: Optional[Task] = None

    async def start(self) -> None:
        """Start the worker loop."""
        self._running = True
        logger.info("Download worker started")

        while self._running:
            try:
                await self._process_next_task()
            except asyncio.CancelledError:
                logger.info("Worker cancelled")
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(5)

        logger.info("Download worker stopped")

    async def stop(self) -> None:
        """Stop the worker loop."""
        self._running = False
        logger.info("Stopping download worker...")

    async def _process_next_task(self) -> None:
        """Process the next task from the queue."""
        # Get next task from queue
        task = await self.task_service.get_next_task()

        if not task:
            # No tasks, wait briefly
            await asyncio.sleep(1)
            return

        self._current_task = task
        logger.info(f"Processing task: {task.id} ({task.video_id})")

        try:
            # Update status to downloading
            await self.db.update_task_status(task.id, TaskStatus.DOWNLOADING)

            # Execute download
            download_result, audio_file_id, transcript_file_id = await self._download_task(task)

            # Save task completion
            await self.db.update_task_completed(
                task_id=task.id,
                video_info=download_result.video_info,
                audio_file_id=audio_file_id,
                transcript_file_id=transcript_file_id,
                expires_at=get_expiry_time(self.settings.file_retention_days),
            )

            logger.info(f"Task {task.id} completed successfully")

            # Send notifications
            task_updated = await self.db.get_task(task.id)
            if task_updated:
                await self.notify_service.notify_completed(task_updated)

                # Send callback if configured
                if task_updated.callback_url:
                    await self.callback_service.send_callback(task_updated)

        except DownloadError as e:
            await self._handle_download_error(task, e)

        except Exception as e:
            logger.error(f"Unexpected error processing task {task.id}: {e}")
            await self._handle_download_error(
                task, DownloadError(ErrorCode.INTERNAL_ERROR, str(e))
            )

        finally:
            self._current_task = None

            # Random wait between tasks
            wait_time = random.uniform(
                self.settings.task_interval_min,
                self.settings.task_interval_max,
            )
            logger.debug(f"Waiting {wait_time:.1f}s before next task")
            await asyncio.sleep(wait_time)

    async def _download_task(
        self, task: Task
    ) -> tuple[DownloadResult, str, Optional[str]]:
        """
        Execute download for a task.

        Args:
            task: Task to download.

        Returns:
            Tuple of (DownloadResult, audio_file_id, transcript_file_id).

        Raises:
            DownloadError: If download fails.
        """
        # Create temporary directory for download
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Download with progress callback
            def progress_callback(progress: int) -> None:
                task.progress = progress
                logger.debug(f"Task {task.id} progress: {progress}%")

            result = await self.downloader.download(
                video_url=task.video_url,
                output_dir=output_dir,
                progress_callback=progress_callback,
            )

            # Move files to permanent storage
            audio_file = await self.file_service.create_file_record(
                task_id=task.id,
                file_type=FileType.AUDIO,
                source_path=result.audio_path,
                metadata={"bitrate": self.settings.audio_quality},
            )

            transcript_file: Optional[FileRecord] = None
            if result.transcript_path and result.transcript_path.exists():
                # Extract language from filename
                lang = self._extract_language(result.transcript_path)
                transcript_file = await self.file_service.create_file_record(
                    task_id=task.id,
                    file_type=FileType.TRANSCRIPT,
                    source_path=result.transcript_path,
                    metadata={"language": lang},
                )

            return (
                result,
                audio_file.id,
                transcript_file.id if transcript_file else None,
            )

    async def _handle_download_error(self, task: Task, error: DownloadError) -> None:
        """
        Handle download error with retry logic.

        Args:
            task: Failed task.
            error: Download error.
        """
        logger.error(f"Task {task.id} failed: {error.error_code.value} - {error.message}")

        # Check if error is retryable
        if is_retryable_error(error.error_code):
            config = RETRY_CONFIG.get(error.error_code, {})
            max_retries = config.get("max_retries", 0)

            if task.retry_count < max_retries:
                # Schedule retry
                new_count = await self.db.increment_retry_count(task.id)

                # Calculate retry delay
                backoff = config.get("backoff", [60])
                delay_idx = min(new_count - 1, len(backoff) - 1)
                base_delay = backoff[delay_idx]
                jitter = random.uniform(0, config.get("jitter", 0))
                retry_delay = base_delay + jitter

                logger.warning(
                    f"Task {task.id} will retry ({new_count}/{max_retries}) "
                    f"in {retry_delay:.0f}s"
                )

                # Re-add to queue after delay
                await asyncio.sleep(retry_delay)
                await self.task_service.task_queue.put(task.id)
                return

        # Mark as failed
        await self.db.update_task_status(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error_code=error.error_code,
            error_message=error.message,
        )

        # Send failure notification
        task_updated = await self.db.get_task(task.id)
        if task_updated:
            await self.notify_service.notify_failed(task_updated, error.message)

            # Send callback for failure
            if task_updated.callback_url:
                await self.callback_service.send_callback(task_updated)

    def _extract_language(self, filepath: Path) -> str:
        """
        Extract language code from transcript filename.

        Args:
            filepath: Transcript file path.

        Returns:
            Language code.
        """
        # Filename format: {video_id}.{lang}.json3
        parts = filepath.stem.split(".")
        if len(parts) >= 2:
            return parts[-1]
        return "unknown"
