"""
File service module.

Handles file storage, retrieval, and cleanup operations.
"""

import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from src.config import Settings
from src.db.database import Database
from src.db.models import FileRecord, FileType
from src.utils.helpers import get_expiry_time
from src.utils.logger import logger


class FileService:
    """
    Service for managing downloaded files.

    Handles file storage, access tracking, and cleanup of expired files.
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
        task_id: str,
        file_type: FileType,
        source_path: Path,
        metadata: Optional[dict] = None,
    ) -> FileRecord:
        """
        Create a file record and move file to storage.

        Args:
            task_id: Associated task ID.
            file_type: Type of file (audio/transcript).
            source_path: Path to source file.
            metadata: Optional metadata (bitrate, language, etc.).

        Returns:
            Created FileRecord.
        """
        file_id = str(uuid4())
        filename = source_path.name
        file_format = source_path.suffix.lstrip(".")

        # Determine target directory
        if file_type == FileType.AUDIO:
            target_dir = self.settings.audio_dir
        else:
            target_dir = self.settings.transcript_dir

        # Ensure directory exists
        target_dir.mkdir(parents=True, exist_ok=True)

        # Target path with UUID prefix
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
            task_id=task_id,
            type=file_type,
            filename=filename,
            filepath=str(relative_path),
            size=file_size,
            format=file_format,
            metadata=metadata,
            created_at=now,
            last_accessed_at=now,
            expires_at=get_expiry_time(self.settings.file_retention_days),
        )

        await self.db.create_file(file_record)
        logger.info(f"Created file record: {file_id} ({file_type.value})")

        return file_record

    async def get_file(self, file_id: str) -> Optional[tuple[FileRecord, Path]]:
        """
        Get file record and path.

        Also updates last access time for cleanup tracking.

        Args:
            file_id: File UUID.

        Returns:
            Tuple of (FileRecord, file Path) or None if not found.
        """
        record = await self.db.get_file(file_id)
        if not record:
            return None

        file_path = self.data_dir / record.filepath
        if not file_path.exists():
            logger.warning(f"File not found on disk: {file_path}")
            return None

        # Update access time
        await self.db.update_file_access_time(file_id)

        return record, file_path

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

        # Clean up orphan tasks
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
