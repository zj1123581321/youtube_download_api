"""
File service module.

Handles file storage, retrieval, validation, and cleanup operations.
Files are indexed by video_id for resource sharing across tasks.
"""

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from src.config import Settings
from src.db.database import Database
from src.db.models import FileRecord, FileType
from src.utils.helpers import get_expiry_time, sanitize_filename
from src.utils.logger import logger


class FileService:
    """
    Service for managing downloaded files.

    Files are associated with video_id (not task_id), enabling resource
    sharing across multiple tasks requesting the same video.
    """

    def __init__(self, db: Database, settings: Settings):
        """
        Initialize file service.

        Args:
            db: Database instance.
            settings: Application settings.
        """
        self.db = db
        self.settings = settings
        self.data_dir = settings.data_dir

    async def create_file_record(
        self,
        video_id: str,
        file_type: FileType,
        source_path: Path,
        quality: Optional[str] = None,
        language: Optional[str] = None,
        video_title: Optional[str] = None,
    ) -> FileRecord:
        """
        Create a file record and move file to storage.

        Args:
            video_id: YouTube video ID (files are indexed by video).
            file_type: Type of file (audio/transcript).
            source_path: Path to source file.
            quality: Audio quality (e.g., "128").
            language: Transcript language (e.g., "en").
            video_title: Video title for friendly filename.

        Returns:
            Created FileRecord.
        """
        file_id = str(uuid4())
        file_format = source_path.suffix.lstrip(".")

        # Generate friendly filename: {video_id}_{sanitized_title}.{ext}
        # Fallback to video_id only if no title provided
        if video_title:
            # Calculate max bytes for title to stay under filesystem limit (255 bytes)
            # Fixed parts: {file_id}_ (37 bytes) + {video_id}_ (12 bytes) + .{ext} (~5 bytes)
            # Reserve ~55 bytes for fixed parts, ~200 bytes for title
            safe_title = sanitize_filename(video_title, max_bytes=180)
            filename = f"{video_id}_{safe_title}.{file_format}"
        else:
            filename = source_path.name

        # Determine target directory
        if file_type == FileType.AUDIO:
            target_dir = self.settings.audio_dir
        else:
            target_dir = self.settings.transcript_dir

        # Ensure directory exists
        target_dir.mkdir(parents=True, exist_ok=True)

        # Target path with UUID prefix for uniqueness
        target_filename = f"{file_id}_{filename}"
        target_path = target_dir / target_filename
        relative_path = target_path.relative_to(self.data_dir)

        # Move file to storage
        shutil.move(str(source_path), str(target_path))
        logger.debug(f"Moved file to: {target_path}")

        # Get file size
        file_size = target_path.stat().st_size

        # Create record
        now = datetime.now(timezone.utc)
        file_record = FileRecord(
            id=file_id,
            video_id=video_id,
            file_type=file_type,
            filename=filename,
            filepath=str(relative_path),
            size=file_size,
            format=file_format,
            quality=quality,
            language=language,
            created_at=now,
            last_accessed_at=now,
            expires_at=get_expiry_time(self.settings.file_retention_days),
        )

        await self.db.create_file(file_record)
        logger.info(f"Created file record: {file_id} ({file_type.value}) for video {video_id}")

        return file_record

    async def get_file(self, file_id: str) -> Optional[tuple[FileRecord, Path]]:
        """
        Get file record and path by file ID.

        Also updates last access time for cleanup tracking.

        Args:
            file_id: File UUID.

        Returns:
            Tuple of (FileRecord, file Path) or None if not found or file missing.
        """
        record = await self.db.get_file(file_id)
        if not record:
            return None

        file_path = self.data_dir / record.filepath
        if not file_path.exists():
            logger.warning(f"File not found on disk: {file_path}, cleaning up record")
            await self.db.delete_file(file_id)
            return None

        # Update access time
        await self.db.update_file_access_time(file_id)

        return record, file_path

    async def get_existing_file(
        self,
        video_id: str,
        file_type: FileType,
        quality: Optional[str] = None,
        language: Optional[str] = None,
    ) -> Optional[FileRecord]:
        """
        Get existing file for a video, with physical file validation.

        This is the core method for resource reuse. It checks:
        1. Database record exists
        2. Physical file exists on disk

        If database record exists but file is missing, the record is cleaned up.

        Args:
            video_id: YouTube video ID.
            file_type: Type of file (audio/transcript).
            quality: Audio quality filter.
            language: Transcript language filter.

        Returns:
            FileRecord if valid file exists, None otherwise.
        """
        record = await self.db.get_file_by_video(
            video_id=video_id,
            file_type=file_type,
            quality=quality,
            language=language,
        )

        if not record:
            return None

        # Validate physical file exists
        file_path = self.data_dir / record.filepath
        if not file_path.exists():
            logger.warning(
                f"File record exists but file missing: {record.id} ({file_path}), "
                "cleaning up stale record"
            )
            await self.db.delete_file(record.id)
            return None

        # Update access time for valid file
        await self.db.update_file_access_time(record.id)
        logger.debug(f"Found existing {file_type.value} file for video {video_id}: {record.id}")

        return record

    async def get_all_files_for_video(self, video_id: str) -> dict[str, Optional[FileRecord]]:
        """
        Get all valid files for a video.

        Returns a dict with 'audio' and 'transcript' keys, validating
        that physical files exist.

        Args:
            video_id: YouTube video ID.

        Returns:
            Dict with 'audio' and 'transcript' FileRecords (or None if not found).
        """
        result: dict[str, Optional[FileRecord]] = {
            "audio": None,
            "transcript": None,
        }

        files = await self.db.get_files_by_video(video_id)

        for record in files:
            file_path = self.data_dir / record.filepath
            if not file_path.exists():
                logger.warning(f"Cleaning up stale file record: {record.id}")
                await self.db.delete_file(record.id)
                continue

            if record.file_type == FileType.AUDIO:
                result["audio"] = record
            elif record.file_type == FileType.TRANSCRIPT:
                result["transcript"] = record

        return result

    async def cleanup_expired_files(self) -> int:
        """
        Clean up files that haven't been accessed within retention period.

        Returns:
            Number of files cleaned up.
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(
            days=self.settings.file_retention_days
        )

        expired_files = await self.db.get_expired_files(cutoff_time)
        deleted_count = 0

        for file_record in expired_files:
            try:
                file_path = self.data_dir / file_record.filepath

                # Delete physical file
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"Deleted expired file: {file_path}")

                # Delete database record
                await self.db.delete_file(file_record.id)
                deleted_count += 1

            except Exception as e:
                logger.error(f"Failed to delete file {file_record.id}: {e}")

        # Clean up empty directories
        self._cleanup_empty_dirs()

        # Clean up orphan video resources
        await self.db.delete_orphan_video_resources()

        # Clean up old completed tasks
        await self.db.delete_expired_tasks(cutoff_time)

        if deleted_count > 0:
            logger.info(f"Cleanup completed: {deleted_count} files removed")

        return deleted_count

    def _cleanup_empty_dirs(self) -> None:
        """Remove empty directories in data storage."""
        for dir_path in [self.settings.audio_dir, self.settings.transcript_dir]:
            if dir_path.exists():
                for subdir in dir_path.iterdir():
                    if subdir.is_dir() and not any(subdir.iterdir()):
                        try:
                            subdir.rmdir()
                            logger.debug(f"Removed empty directory: {subdir}")
                        except OSError:
                            pass

    def get_disk_usage(self) -> dict[str, int]:
        """
        Get disk usage statistics.

        Returns:
            Dictionary with usage statistics.
        """
        audio_size = self._get_dir_size(self.settings.audio_dir)
        transcript_size = self._get_dir_size(self.settings.transcript_dir)

        # Get disk free space
        try:
            import os
            stat = os.statvfs(self.data_dir)
            free_space = stat.f_bavail * stat.f_frsize
        except (OSError, AttributeError):
            # Windows fallback
            try:
                import ctypes
                free_bytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                    ctypes.c_wchar_p(str(self.data_dir)),
                    None,
                    None,
                    ctypes.pointer(free_bytes),
                )
                free_space = free_bytes.value
            except Exception:
                free_space = 0

        return {
            "audio_size": audio_size,
            "transcript_size": transcript_size,
            "total_size": audio_size + transcript_size,
            "free_space": free_space,
        }

    def _get_dir_size(self, dir_path: Path) -> int:
        """
        Get total size of directory.

        Args:
            dir_path: Directory path.

        Returns:
            Total size in bytes.
        """
        if not dir_path.exists():
            return 0

        total = 0
        for file in dir_path.rglob("*"):
            if file.is_file():
                try:
                    total += file.stat().st_size
                except OSError:
                    pass

        return total

    def check_disk_space(self, required_mb: int = 100) -> bool:
        """
        Check if sufficient disk space is available.

        Args:
            required_mb: Required free space in MB.

        Returns:
            True if sufficient space available.
        """
        usage = self.get_disk_usage()
        free_mb = usage["free_space"] / (1024 * 1024)
        return free_mb >= required_mb
