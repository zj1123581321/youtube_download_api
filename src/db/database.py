"""
SQLite database connection and operations.

Provides async database operations using aiosqlite.
Architecture: Video -> Files <- Task (video owns files, tasks reference files)
"""

import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

import aiosqlite

from src.db.models import (
    CallbackStatus,
    ErrorCode,
    FileRecord,
    FileType,
    Task,
    TaskStatus,
    VideoInfo,
    VideoResource,
)
from src.utils.logger import logger


class Database:
    """Async SQLite database manager."""

    def __init__(self, db_path: Path):
        """
        Initialize database manager.

        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Establish database connection and create tables."""
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._connection = await aiosqlite.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self._connection.row_factory = aiosqlite.Row

        await self._create_tables()
        logger.info(f"Database connected: {self.db_path}")

    async def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Database disconnected")

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """
        Context manager for database transactions.

        Yields:
            Database connection with transaction support.
        """
        if not self._connection:
            raise RuntimeError("Database not connected")

        try:
            yield self._connection
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        """
        Execute SQL statement.

        Args:
            sql: SQL statement.
            params: Query parameters.

        Returns:
            Cursor for the executed query.
        """
        if not self._connection:
            raise RuntimeError("Database not connected")
        return await self._connection.execute(sql, params)

    async def _create_tables(self) -> None:
        """Create database tables if not exist."""
        async with self.transaction():
            # Video resources table (core entity)
            await self.execute("""
                CREATE TABLE IF NOT EXISTS video_resources (
                    video_id TEXT PRIMARY KEY,
                    video_info TEXT,
                    has_native_transcript INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Files table (indexed by video_id, not task_id)
            await self.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    filepath TEXT NOT NULL,
                    size INTEGER,
                    format TEXT,
                    quality TEXT,
                    language TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_accessed_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES video_resources(video_id),
                    UNIQUE(video_id, file_type, quality, language)
                )
            """)

            # Tasks table (request entity, references files)
            await self.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    video_url TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    include_audio INTEGER DEFAULT 1,
                    include_transcript INTEGER DEFAULT 1,
                    audio_file_id TEXT,
                    transcript_file_id TEXT,
                    reused_audio INTEGER DEFAULT 0,
                    reused_transcript INTEGER DEFAULT 0,
                    callback_url TEXT,
                    callback_secret TEXT,
                    callback_status TEXT,
                    callback_attempts INTEGER DEFAULT 0,
                    error_code TEXT,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES video_resources(video_id),
                    FOREIGN KEY (audio_file_id) REFERENCES files(id),
                    FOREIGN KEY (transcript_file_id) REFERENCES files(id)
                )
            """)

            # Create indexes
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_video_resources_updated ON video_resources(updated_at)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_video_id ON files(video_id)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_expires ON files(expires_at)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_last_accessed ON files(last_accessed_at)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_video_id ON tasks(video_id)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)"
            )

    # ==================== Video Resource Operations ====================

    async def get_video_resource(self, video_id: str) -> Optional[VideoResource]:
        """
        Get video resource by video ID.

        Args:
            video_id: YouTube video ID.

        Returns:
            VideoResource or None if not found.
        """
        cursor = await self.execute(
            "SELECT * FROM video_resources WHERE video_id = ?", (video_id,)
        )
        row = await cursor.fetchone()
        return self._row_to_video_resource(row) if row else None

    async def create_video_resource(self, resource: VideoResource) -> None:
        """
        Create a new video resource record.

        Args:
            resource: VideoResource object to create.
        """
        video_info_json = (
            json.dumps(resource.video_info.to_dict()) if resource.video_info else None
        )
        now = datetime.now(timezone.utc)

        async with self.transaction():
            await self.execute(
                """
                INSERT INTO video_resources (
                    video_id, video_info, has_native_transcript, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    resource.video_id,
                    video_info_json,
                    1 if resource.has_native_transcript else (0 if resource.has_native_transcript is False else None),
                    resource.created_at or now,
                    resource.updated_at or now,
                ),
            )
        logger.debug(f"Video resource created: {resource.video_id}")

    async def update_video_resource(
        self,
        video_id: str,
        video_info: Optional[VideoInfo] = None,
        has_native_transcript: Optional[bool] = None,
    ) -> None:
        """
        Update video resource information.

        Args:
            video_id: YouTube video ID.
            video_info: Video metadata to update.
            has_native_transcript: Whether video has native subtitles.
        """
        now = datetime.now(timezone.utc)
        updates = ["updated_at = ?"]
        params: list[Any] = [now]

        if video_info is not None:
            updates.append("video_info = ?")
            params.append(json.dumps(video_info.to_dict()))

        if has_native_transcript is not None:
            updates.append("has_native_transcript = ?")
            params.append(1 if has_native_transcript else 0)

        params.append(video_id)

        async with self.transaction():
            await self.execute(
                f"UPDATE video_resources SET {', '.join(updates)} WHERE video_id = ?",
                tuple(params),
            )
        logger.debug(f"Video resource updated: {video_id}")

    async def get_or_create_video_resource(self, video_id: str) -> VideoResource:
        """
        Get existing video resource or create a new one.

        Args:
            video_id: YouTube video ID.

        Returns:
            VideoResource object.
        """
        existing = await self.get_video_resource(video_id)
        if existing:
            return existing

        now = datetime.now(timezone.utc)
        resource = VideoResource(
            video_id=video_id,
            created_at=now,
            updated_at=now,
        )
        await self.create_video_resource(resource)
        return resource

    # ==================== File Operations ====================

    async def create_file(self, file: FileRecord) -> None:
        """
        Create a new file record.

        Args:
            file: File record to create.
        """
        async with self.transaction():
            await self.execute(
                """
                INSERT INTO files (
                    id, video_id, file_type, filename, filepath, size, format,
                    quality, language, created_at, last_accessed_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file.id,
                    file.video_id,
                    file.file_type.value,
                    file.filename,
                    file.filepath,
                    file.size,
                    file.format,
                    file.quality,
                    file.language,
                    file.created_at or datetime.now(timezone.utc),
                    file.last_accessed_at,
                    file.expires_at,
                ),
            )
        logger.debug(f"File record created: {file.id} ({file.file_type.value})")

    async def get_file(self, file_id: str) -> Optional[FileRecord]:
        """
        Get file by ID.

        Args:
            file_id: File UUID.

        Returns:
            FileRecord or None if not found.
        """
        cursor = await self.execute("SELECT * FROM files WHERE id = ?", (file_id,))
        row = await cursor.fetchone()
        return self._row_to_file(row) if row else None

    async def get_file_by_video(
        self,
        video_id: str,
        file_type: FileType,
        quality: Optional[str] = None,
        language: Optional[str] = None,
    ) -> Optional[FileRecord]:
        """
        Get file by video ID and type.

        Args:
            video_id: YouTube video ID.
            file_type: Type of file (audio/transcript).
            quality: Audio quality (optional).
            language: Transcript language (optional).

        Returns:
            FileRecord or None if not found.
        """
        # Build query based on parameters
        sql = "SELECT * FROM files WHERE video_id = ? AND file_type = ?"
        params: list[Any] = [video_id, file_type.value]

        if quality is not None:
            sql += " AND quality = ?"
            params.append(quality)
        else:
            sql += " AND (quality IS NULL OR quality = '')"

        if language is not None:
            sql += " AND language = ?"
            params.append(language)

        cursor = await self.execute(sql, tuple(params))
        row = await cursor.fetchone()
        return self._row_to_file(row) if row else None

    async def get_files_by_video(self, video_id: str) -> list[FileRecord]:
        """
        Get all files for a video.

        Args:
            video_id: YouTube video ID.

        Returns:
            List of file records.
        """
        cursor = await self.execute(
            "SELECT * FROM files WHERE video_id = ?", (video_id,)
        )
        rows = await cursor.fetchall()
        return [self._row_to_file(row) for row in rows]

    async def update_file_access_time(self, file_id: str) -> None:
        """
        Update file last access time.

        Args:
            file_id: File UUID.
        """
        async with self.transaction():
            await self.execute(
                "UPDATE files SET last_accessed_at = ? WHERE id = ?",
                (datetime.now(timezone.utc), file_id),
            )

    async def get_expired_files(self, cutoff_time: datetime) -> list[FileRecord]:
        """
        Get files that haven't been accessed since cutoff time.

        Args:
            cutoff_time: Cutoff datetime for last access.

        Returns:
            List of expired file records.
        """
        cursor = await self.execute(
            """
            SELECT * FROM files
            WHERE last_accessed_at < ? OR (last_accessed_at IS NULL AND created_at < ?)
            """,
            (cutoff_time, cutoff_time),
        )
        rows = await cursor.fetchall()
        return [self._row_to_file(row) for row in rows]

    async def delete_file(self, file_id: str) -> None:
        """
        Delete file record.

        Args:
            file_id: File UUID.
        """
        async with self.transaction():
            await self.execute("DELETE FROM files WHERE id = ?", (file_id,))
        logger.debug(f"File record deleted: {file_id}")

    # ==================== Task Operations ====================

    async def create_task(self, task: Task) -> None:
        """
        Create a new task in database.

        Args:
            task: Task object to create.
        """
        async with self.transaction():
            await self.execute(
                """
                INSERT INTO tasks (
                    id, video_id, video_url, status,
                    include_audio, include_transcript,
                    audio_file_id, transcript_file_id,
                    reused_audio, reused_transcript,
                    callback_url, callback_secret, callback_status,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.id,
                    task.video_id,
                    task.video_url,
                    task.status.value,
                    1 if task.include_audio else 0,
                    1 if task.include_transcript else 0,
                    task.audio_file_id,
                    task.transcript_file_id,
                    1 if task.reused_audio else 0,
                    1 if task.reused_transcript else 0,
                    task.callback_url,
                    task.callback_secret,
                    task.callback_status.value if task.callback_status else None,
                    task.created_at or datetime.now(timezone.utc),
                ),
            )
        logger.debug(f"Task created: {task.id}")

    async def get_task(self, task_id: str) -> Optional[Task]:
        """
        Get task by ID.

        Args:
            task_id: Task UUID.

        Returns:
            Task object or None if not found.
        """
        cursor = await self.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = await cursor.fetchone()
        return self._row_to_task(row) if row else None

    async def get_active_task_by_video(self, video_id: str) -> Optional[Task]:
        """
        Find active (pending/downloading) task by video ID.

        Args:
            video_id: YouTube video ID.

        Returns:
            Task object or None if not found.
        """
        cursor = await self.execute(
            """
            SELECT * FROM tasks
            WHERE video_id = ? AND status IN ('pending', 'downloading')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (video_id,),
        )
        row = await cursor.fetchone()
        return self._row_to_task(row) if row else None

    async def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Task], int]:
        """
        List tasks with pagination.

        Args:
            status: Filter by status.
            limit: Maximum number of results.
            offset: Number of results to skip.

        Returns:
            Tuple of (tasks list, total count).
        """
        where_clause = "WHERE 1=1"
        params: list[Any] = []

        if status:
            where_clause += " AND status = ?"
            params.append(status.value)

        count_cursor = await self.execute(
            f"SELECT COUNT(*) FROM tasks {where_clause}", tuple(params)
        )
        total = (await count_cursor.fetchone())[0]

        params.extend([limit, offset])
        cursor = await self.execute(
            f"""
            SELECT * FROM tasks
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params),
        )

        rows = await cursor.fetchall()
        tasks = [self._row_to_task(row) for row in rows]

        return tasks, total

    async def get_pending_tasks(self, limit: int = 10) -> list[Task]:
        """
        Get pending tasks ordered by creation time.

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of pending tasks.
        """
        cursor = await self.execute(
            """
            SELECT * FROM tasks
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_task(row) for row in rows]

    async def update_task_status(
        self,
        task_id: str,
        status: TaskStatus,
        error_code: Optional[ErrorCode] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Update task status.

        Args:
            task_id: Task UUID.
            status: New status.
            error_code: Error code if failed.
            error_message: Error message if failed.
        """
        now = datetime.now(timezone.utc)

        async with self.transaction():
            if status == TaskStatus.DOWNLOADING:
                await self.execute(
                    "UPDATE tasks SET status = ?, started_at = ? WHERE id = ?",
                    (status.value, now, task_id),
                )
            elif status == TaskStatus.COMPLETED:
                await self.execute(
                    "UPDATE tasks SET status = ?, completed_at = ? WHERE id = ?",
                    (status.value, now, task_id),
                )
            elif status == TaskStatus.FAILED:
                await self.execute(
                    """
                    UPDATE tasks
                    SET status = ?, error_code = ?, error_message = ?, completed_at = ?
                    WHERE id = ?
                    """,
                    (
                        status.value,
                        error_code.value if error_code else None,
                        error_message,
                        now,
                        task_id,
                    ),
                )
            else:
                await self.execute(
                    "UPDATE tasks SET status = ? WHERE id = ?",
                    (status.value, task_id),
                )

        logger.debug(f"Task {task_id} status updated to {status.value}")

    async def update_task_completed(
        self,
        task_id: str,
        audio_file_id: Optional[str] = None,
        transcript_file_id: Optional[str] = None,
        reused_audio: bool = False,
        reused_transcript: bool = False,
    ) -> None:
        """
        Update task as completed with file references.

        Args:
            task_id: Task UUID.
            audio_file_id: Audio file UUID (may be None).
            transcript_file_id: Transcript file UUID (may be None).
            reused_audio: Whether audio file was reused.
            reused_transcript: Whether transcript file was reused.
        """
        now = datetime.now(timezone.utc)

        async with self.transaction():
            await self.execute(
                """
                UPDATE tasks
                SET status = ?, audio_file_id = ?, transcript_file_id = ?,
                    reused_audio = ?, reused_transcript = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    TaskStatus.COMPLETED.value,
                    audio_file_id,
                    transcript_file_id,
                    1 if reused_audio else 0,
                    1 if reused_transcript else 0,
                    now,
                    task_id,
                ),
            )

        logger.info(f"Task {task_id} completed")

    async def increment_retry_count(self, task_id: str) -> int:
        """
        Increment task retry count.

        Args:
            task_id: Task UUID.

        Returns:
            New retry count.
        """
        async with self.transaction():
            await self.execute(
                "UPDATE tasks SET retry_count = retry_count + 1, status = 'pending' WHERE id = ?",
                (task_id,),
            )
            cursor = await self.execute(
                "SELECT retry_count FROM tasks WHERE id = ?", (task_id,)
            )
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def reset_downloading_tasks(self) -> int:
        """
        Reset all downloading tasks to pending (for recovery after restart).

        Returns:
            Number of tasks reset.
        """
        async with self.transaction():
            cursor = await self.execute(
                "UPDATE tasks SET status = 'pending' WHERE status = 'downloading'"
            )
            count = cursor.rowcount
            if count > 0:
                logger.warning(f"Reset {count} downloading tasks to pending")
            return count

    async def get_queue_position(self, task_id: str) -> int:
        """
        Get task position in queue.

        Args:
            task_id: Task UUID.

        Returns:
            Position in queue (1-based), 0 if not in queue.
        """
        cursor = await self.execute(
            """
            SELECT COUNT(*) + 1 FROM tasks
            WHERE status = 'pending'
            AND created_at < (SELECT created_at FROM tasks WHERE id = ?)
            """,
            (task_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    # ==================== Callback Operations ====================

    async def update_callback_status(
        self,
        task_id: str,
        status: CallbackStatus,
        attempts: Optional[int] = None,
    ) -> None:
        """
        Update task callback status.

        Args:
            task_id: Task UUID.
            status: Callback status.
            attempts: Number of callback attempts.
        """
        async with self.transaction():
            if attempts is not None:
                await self.execute(
                    """
                    UPDATE tasks
                    SET callback_status = ?, callback_attempts = ?
                    WHERE id = ?
                    """,
                    (status.value, attempts, task_id),
                )
            else:
                await self.execute(
                    "UPDATE tasks SET callback_status = ? WHERE id = ?",
                    (status.value, task_id),
                )

    # ==================== Cleanup Operations ====================

    async def delete_expired_tasks(self, cutoff_time: datetime) -> int:
        """
        Delete expired tasks (completed tasks older than cutoff).

        Args:
            cutoff_time: Cutoff datetime.

        Returns:
            Number of tasks deleted.
        """
        async with self.transaction():
            cursor = await self.execute(
                """
                DELETE FROM tasks
                WHERE status = 'completed' AND completed_at < ?
                """,
                (cutoff_time,),
            )
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Deleted {count} expired tasks")
            return count

    async def delete_orphan_video_resources(self) -> int:
        """
        Delete video resources with no files.

        Returns:
            Number of resources deleted.
        """
        async with self.transaction():
            cursor = await self.execute(
                """
                DELETE FROM video_resources
                WHERE video_id NOT IN (SELECT DISTINCT video_id FROM files)
                """
            )
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Deleted {count} orphan video resources")
            return count

    # ==================== Statistics ====================

    async def get_queue_stats(self) -> dict[str, int]:
        """
        Get queue statistics.

        Returns:
            Dictionary with pending and downloading counts.
        """
        cursor = await self.execute(
            """
            SELECT status, COUNT(*) as count
            FROM tasks
            WHERE status IN ('pending', 'downloading')
            GROUP BY status
            """
        )
        rows = await cursor.fetchall()

        stats = {"pending": 0, "downloading": 0}
        for row in rows:
            stats[row["status"]] = row["count"]

        return stats

    async def get_resource_stats(self) -> dict[str, int]:
        """
        Get resource statistics.

        Returns:
            Dictionary with video, file, and task counts.
        """
        video_cursor = await self.execute("SELECT COUNT(*) FROM video_resources")
        video_count = (await video_cursor.fetchone())[0]

        file_cursor = await self.execute("SELECT COUNT(*) FROM files")
        file_count = (await file_cursor.fetchone())[0]

        task_cursor = await self.execute("SELECT COUNT(*) FROM tasks")
        task_count = (await task_cursor.fetchone())[0]

        return {
            "videos": video_count,
            "files": file_count,
            "tasks": task_count,
        }

    # ==================== Helper Methods ====================

    def _row_to_video_resource(self, row: aiosqlite.Row) -> VideoResource:
        """Convert database row to VideoResource object."""
        video_info = None
        if row["video_info"]:
            video_info = VideoInfo.from_dict(json.loads(row["video_info"]))

        has_native = row["has_native_transcript"]
        has_native_transcript = None
        if has_native is not None:
            has_native_transcript = bool(has_native)

        return VideoResource(
            video_id=row["video_id"],
            video_info=video_info,
            has_native_transcript=has_native_transcript,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_task(self, row: aiosqlite.Row) -> Task:
        """Convert database row to Task object."""
        return Task(
            id=row["id"],
            video_id=row["video_id"],
            video_url=row["video_url"],
            status=TaskStatus(row["status"]),
            include_audio=bool(row["include_audio"]),
            include_transcript=bool(row["include_transcript"]),
            audio_file_id=row["audio_file_id"],
            transcript_file_id=row["transcript_file_id"],
            reused_audio=bool(row["reused_audio"]),
            reused_transcript=bool(row["reused_transcript"]),
            callback_url=row["callback_url"],
            callback_secret=row["callback_secret"],
            callback_status=CallbackStatus(row["callback_status"])
            if row["callback_status"]
            else None,
            callback_attempts=row["callback_attempts"] or 0,
            error_code=ErrorCode(row["error_code"]) if row["error_code"] else None,
            error_message=row["error_message"],
            retry_count=row["retry_count"] or 0,
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
        )

    def _row_to_file(self, row: aiosqlite.Row) -> FileRecord:
        """Convert database row to FileRecord object."""
        return FileRecord(
            id=row["id"],
            video_id=row["video_id"],
            file_type=FileType(row["file_type"]),
            filename=row["filename"],
            filepath=row["filepath"],
            size=row["size"],
            format=row["format"],
            quality=row["quality"],
            language=row["language"],
            created_at=row["created_at"],
            last_accessed_at=row["last_accessed_at"],
            expires_at=row["expires_at"],
        )


# Global database instance (initialized in main.py)
db: Optional[Database] = None


async def get_database() -> Database:
    """
    Get database instance.

    Returns:
        Database instance.

    Raises:
        RuntimeError: If database not initialized.
    """
    if db is None:
        raise RuntimeError("Database not initialized")
    return db
