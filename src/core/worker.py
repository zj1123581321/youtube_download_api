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
from src.core.downloader import (
    DownloadError,
    DownloadResult,
    TranscriptOnlyResult,
    YouTubeDownloader,
)
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

            # Execute download based on task mode
            result = await self._execute_task(task)

            # Save task completion
            await self.db.update_task_completed(
                task_id=task.id,
                video_info=result["video_info"],
                audio_file_id=result["audio_file_id"],
                transcript_file_id=result["transcript_file_id"],
                expires_at=get_expiry_time(self.settings.file_retention_days),
                has_transcript=result["has_transcript"],
                audio_fallback=result["audio_fallback"],
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

    async def _execute_task(self, task: Task) -> dict:
        """
        Execute task based on its mode configuration.

        Handles different modes:
        - Full mode (include_audio=True, include_transcript=True): Download audio + fetch transcript
        - Audio only (include_audio=True, include_transcript=False): Only download audio
        - Transcript only (include_audio=False, include_transcript=True): Try to fetch transcript,
          fallback to audio download if no transcript available

        Args:
            task: Task to execute.

        Returns:
            Dict with keys: video_info, audio_file_id, transcript_file_id, has_transcript, audio_fallback

        Raises:
            DownloadError: If task execution fails.
        """
        # Determine execution mode
        if not task.include_audio and task.include_transcript:
            # Transcript-only mode: try to get transcript first
            return await self._execute_transcript_only(task)
        else:
            # Full or audio-only mode
            return await self._execute_download(task)

    async def _execute_transcript_only(self, task: Task) -> dict:
        """
        Execute transcript-only mode.

        First checks if transcript is available. If yes, only fetch transcript.
        If no transcript available, fallback to downloading audio.

        Args:
            task: Task to execute.

        Returns:
            Dict with execution result.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Try to extract transcript only
            logger.info(f"Task {task.id}: Attempting transcript-only extraction")
            transcript_result = await self.downloader.extract_transcript_only(
                video_url=task.video_url,
                output_dir=output_dir,
            )

            if transcript_result.has_transcript and transcript_result.transcript_path:
                # Success: transcript available and fetched
                logger.info(f"Task {task.id}: Transcript available, no audio download needed")

                # Save transcript file
                lang = self._extract_language(transcript_result.transcript_path)
                transcript_file = await self.file_service.create_file_record(
                    task_id=task.id,
                    file_type=FileType.TRANSCRIPT,
                    source_path=transcript_result.transcript_path,
                    metadata={"language": lang},
                )

                return {
                    "video_info": transcript_result.video_info,
                    "audio_file_id": None,
                    "transcript_file_id": transcript_file.id,
                    "has_transcript": True,
                    "audio_fallback": False,
                }
            else:
                # No transcript available, fallback to audio download
                logger.info(
                    f"Task {task.id}: No transcript available, falling back to audio download"
                )
                return await self._execute_download(task, audio_fallback=True)

    async def _execute_download(self, task: Task, audio_fallback: bool = False) -> dict:
        """
        Execute audio download (with optional transcript fetch).

        Args:
            task: Task to execute.
            audio_fallback: Whether this is a fallback from transcript-only mode.

        Returns:
            Dict with execution result.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Download with progress callback
            def progress_callback(progress: int) -> None:
                task.progress = progress
                logger.debug(f"Task {task.id} progress: {progress}%")

            # Determine if we should fetch transcript
            fetch_transcript = task.include_transcript

            result = await self.downloader.download(
                video_url=task.video_url,
                output_dir=output_dir,
                progress_callback=progress_callback,
            )

            # Move audio file to permanent storage
            audio_file: Optional[FileRecord] = None
            if result.audio_path and result.audio_path.exists():
                audio_file = await self.file_service.create_file_record(
                    task_id=task.id,
                    file_type=FileType.AUDIO,
                    source_path=result.audio_path,
                    metadata={"bitrate": self.settings.audio_quality},
                )

            # Move transcript file if exists and transcript is requested
            transcript_file: Optional[FileRecord] = None
            has_transcript = False
            if fetch_transcript and result.transcript_path and result.transcript_path.exists():
                has_transcript = True
                lang = self._extract_language(result.transcript_path)
                transcript_file = await self.file_service.create_file_record(
                    task_id=task.id,
                    file_type=FileType.TRANSCRIPT,
                    source_path=result.transcript_path,
                    metadata={"language": lang},
                )

            return {
                "video_info": result.video_info,
                "audio_file_id": audio_file.id if audio_file else None,
                "transcript_file_id": transcript_file.id if transcript_file else None,
                "has_transcript": has_transcript,
                "audio_fallback": audio_fallback,
            }

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
