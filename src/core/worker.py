"""
Download worker module.

Background worker that processes download tasks from the queue.
Only downloads what's needed - reuses existing files when available.
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
    VideoInfo,
    is_retryable_error,
)
from src.services.callback_service import CallbackService
from src.services.file_service import FileService
from src.services.notify import NotificationService
from src.services.task_service import TaskService
from src.utils.logger import logger


class DownloadWorker:
    """
    Background worker for processing download tasks.

    Smart downloading: only downloads what's missing, reuses existing files.
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
        task = await self.task_service.get_next_task()

        if not task:
            await asyncio.sleep(1)
            return

        self._current_task = task
        logger.info(f"Processing task: {task.id} ({task.video_id})")

        try:
            await self.db.update_task_status(task.id, TaskStatus.DOWNLOADING)

            # Execute task (smart download - only what's needed)
            result = await self._execute_task(task)

            # Update task completion
            await self.db.update_task_completed(
                task_id=task.id,
                audio_file_id=result["audio_file_id"],
                transcript_file_id=result["transcript_file_id"],
                reused_audio=result["reused_audio"],
                reused_transcript=result["reused_transcript"],
            )

            logger.info(f"Task {task.id} completed successfully")

            # Send notifications
            task_updated = await self.db.get_task(task.id)
            if task_updated:
                await self.notify_service.notify_completed(task_updated)

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

            wait_time = random.uniform(
                self.settings.task_interval_min,
                self.settings.task_interval_max,
            )
            logger.debug(f"Waiting {wait_time:.1f}s before next task")
            await asyncio.sleep(wait_time)

    async def _execute_task(self, task: Task) -> dict:
        """
        Execute task with smart downloading.

        Only downloads what's missing. Reuses existing files when available.
        Updates video_resource with metadata.

        Args:
            task: Task to execute.

        Returns:
            Dict with: audio_file_id, transcript_file_id, reused_audio, reused_transcript
        """
        # Check what's already available (double-check, may have changed)
        existing_files = await self.file_service.get_all_files_for_video(task.video_id)
        existing_audio = existing_files.get("audio")
        existing_transcript = existing_files.get("transcript")

        # Determine what we actually need to download
        need_audio = task.include_audio and existing_audio is None
        need_transcript = task.include_transcript and existing_transcript is None

        # If nothing to download, just return existing files
        if not need_audio and not need_transcript:
            logger.info(f"Task {task.id}: All resources already exist, nothing to download")
            return {
                "audio_file_id": existing_audio.id if existing_audio else None,
                "transcript_file_id": existing_transcript.id if existing_transcript else None,
                "reused_audio": existing_audio is not None,
                "reused_transcript": existing_transcript is not None,
            }

        logger.info(
            f"Task {task.id}: need_audio={need_audio}, need_transcript={need_transcript}"
        )

        # Determine execution mode
        if need_transcript and not need_audio:
            # Only need transcript, try transcript-only first
            return await self._execute_transcript_only(
                task, existing_audio, existing_transcript
            )
        else:
            # Need audio (and maybe transcript)
            return await self._execute_download(
                task, existing_audio, existing_transcript, need_audio, need_transcript
            )

    async def _execute_transcript_only(
        self,
        task: Task,
        existing_audio: Optional[FileRecord],
        existing_transcript: Optional[FileRecord],
    ) -> dict:
        """
        Execute transcript-only mode.

        Args:
            task: Task to execute.
            existing_audio: Existing audio file (if any).
            existing_transcript: Existing transcript file (if any).

        Returns:
            Dict with execution result.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            logger.info(f"Task {task.id}: Attempting transcript-only extraction")
            transcript_result = await self.downloader.extract_transcript_only(
                video_url=task.video_url,
                output_dir=output_dir,
            )

            # Update video resource with metadata
            await self._update_video_resource(
                task.video_id,
                transcript_result.video_info,
                transcript_result.has_transcript,
            )

            if transcript_result.has_transcript and transcript_result.transcript_path:
                logger.info(f"Task {task.id}: Transcript available")

                lang = self._extract_language(transcript_result.transcript_path)
                transcript_file = await self.file_service.create_file_record(
                    video_id=task.video_id,
                    file_type=FileType.TRANSCRIPT,
                    source_path=transcript_result.transcript_path,
                    language=lang,
                )

                return {
                    "audio_file_id": existing_audio.id if existing_audio else None,
                    "transcript_file_id": transcript_file.id,
                    "reused_audio": existing_audio is not None,
                    "reused_transcript": False,
                }
            else:
                # No transcript, fallback to audio download
                logger.info(f"Task {task.id}: No transcript, falling back to audio")
                return await self._execute_download(
                    task, existing_audio, existing_transcript,
                    need_audio=True, need_transcript=False,
                    audio_fallback=True,
                )

    async def _execute_download(
        self,
        task: Task,
        existing_audio: Optional[FileRecord],
        existing_transcript: Optional[FileRecord],
        need_audio: bool,
        need_transcript: bool,
        audio_fallback: bool = False,
    ) -> dict:
        """
        Execute audio download (with optional transcript).

        Args:
            task: Task to execute.
            existing_audio: Existing audio file (if any).
            existing_transcript: Existing transcript file (if any).
            need_audio: Whether to download audio.
            need_transcript: Whether to fetch transcript.
            audio_fallback: Whether this is a fallback from transcript-only mode.

        Returns:
            Dict with execution result.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            def progress_callback(progress: int) -> None:
                task.progress = progress
                logger.debug(f"Task {task.id} progress: {progress}%")

            result = await self.downloader.download(
                video_url=task.video_url,
                output_dir=output_dir,
                progress_callback=progress_callback,
            )

            # Update video resource with metadata
            has_transcript = bool(result.transcript_path and result.transcript_path.exists())
            await self._update_video_resource(
                task.video_id,
                result.video_info,
                has_transcript,
            )

            # Process audio file
            audio_file_id = existing_audio.id if existing_audio else None
            reused_audio = existing_audio is not None

            if need_audio and result.audio_path and result.audio_path.exists():
                audio_file = await self.file_service.create_file_record(
                    video_id=task.video_id,
                    file_type=FileType.AUDIO,
                    source_path=result.audio_path,
                    quality=str(self.settings.audio_quality),
                )
                audio_file_id = audio_file.id
                reused_audio = False

            # Process transcript file
            transcript_file_id = existing_transcript.id if existing_transcript else None
            reused_transcript = existing_transcript is not None

            if need_transcript and result.transcript_path and result.transcript_path.exists():
                lang = self._extract_language(result.transcript_path)
                transcript_file = await self.file_service.create_file_record(
                    video_id=task.video_id,
                    file_type=FileType.TRANSCRIPT,
                    source_path=result.transcript_path,
                    language=lang,
                )
                transcript_file_id = transcript_file.id
                reused_transcript = False

            return {
                "audio_file_id": audio_file_id,
                "transcript_file_id": transcript_file_id,
                "reused_audio": reused_audio,
                "reused_transcript": reused_transcript,
            }

    async def _update_video_resource(
        self,
        video_id: str,
        video_info: VideoInfo,
        has_native_transcript: bool,
    ) -> None:
        """
        Update video resource with metadata.

        Args:
            video_id: YouTube video ID.
            video_info: Video metadata.
            has_native_transcript: Whether video has native subtitles.
        """
        await self.db.update_video_resource(
            video_id=video_id,
            video_info=video_info,
            has_native_transcript=has_native_transcript,
        )
        logger.debug(f"Updated video resource: {video_id}")

    async def _handle_download_error(self, task: Task, error: DownloadError) -> None:
        """
        Handle download error with retry logic.

        Args:
            task: Failed task.
            error: Download error.
        """
        logger.error(f"Task {task.id} failed: {error.error_code.value} - {error.message}")

        if is_retryable_error(error.error_code):
            config = RETRY_CONFIG.get(error.error_code, {})
            max_retries = config.get("max_retries", 0)

            if task.retry_count < max_retries:
                new_count = await self.db.increment_retry_count(task.id)

                backoff = config.get("backoff", [60])
                delay_idx = min(new_count - 1, len(backoff) - 1)
                base_delay = backoff[delay_idx]
                jitter = random.uniform(0, config.get("jitter", 0))
                retry_delay = base_delay + jitter

                logger.warning(
                    f"Task {task.id} will retry ({new_count}/{max_retries}) "
                    f"in {retry_delay:.0f}s"
                )

                await asyncio.sleep(retry_delay)
                await self.task_service.task_queue.put(task.id)
                return

        await self.db.update_task_status(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error_code=error.error_code,
            error_message=error.message,
        )

        task_updated = await self.db.get_task(task.id)
        if task_updated:
            await self.notify_service.notify_failed(task_updated, error.message)

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
        parts = filepath.stem.split(".")
        if len(parts) >= 2:
            return parts[-1]
        return "unknown"
