"""
SQLite database connection and operations.

Provides async database operations using aiosqlite.
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
            # Tasks table
            await self.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    video_url TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    include_audio INTEGER DEFAULT 1,
                    include_transcript INTEGER DEFAULT 1,
                    has_transcript INTEGER,
                    audio_fallback INTEGER DEFAULT 0,
                    video_info TEXT,
                    audio_file_id TEXT,
                    transcript_file_id TEXT,
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
                    expires_at TIMESTAMP
                )
            """)

            # Files table
            await self.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    filepath TEXT NOT NULL,
                    size INTEGER,
                    format TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_accessed_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES tasks(id)
                )
            """)

            # Create indexes
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_video_id ON tasks(video_id)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_expires_at ON tasks(expires_at)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_task_id ON files(task_id)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_expires_at ON files(expires_at)"
            )
            await self.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_last_accessed ON files(last_accessed_at)"
            )

    # ==================== Task Operations ====================

    async def create_task(self, task: Task) -> None:
        """
        Create a new task in database.

        Args:
            task: Task object to create.
        """
        video_info_json = (
            json.dumps(task.video_info.to_dict()) if task.video_info else None
        )

        async with self.transaction():
            await self.execute(
                """
                INSERT INTO tasks (
                    id, video_id, video_url, status,
                    include_audio, include_transcript,
                    video_info, callback_url, callback_secret, callback_status,
                    created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.id,
                    task.video_id,
                    task.video_url,
                    task.status.value,
                    1 if task.include_audio else 0,
                    1 if task.include_transcript else 0,
                    video_info_json,
                    task.callback_url,
                    task.callback_secret,
                    task.callback_status.value if task.callback_status else None,
                    task.created_at or datetime.now(timezone.utc),
                    task.expires_at,
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

    async def get_task_by_video_id(
        self, video_id: str, active_only: bool = True
    ) -> Optional[Task]:
        """
        Find task by video ID.

        Args:
            video_id: YouTube video ID.
            active_only: Only return active (non-failed) tasks.

        Returns:
            Task object or None if not found.
        """
        if active_only:
            cursor = await self.execute(
                """
                SELECT * FROM tasks
                WHERE video_id = ? AND status NOT IN ('failed', 'cancelled')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (video_id,),
            )
        else:
            cursor = await self.execute(
                """
                SELECT * FROM tasks
                WHERE video_id = ?
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
        # Build query
        where_clause = "WHERE 1=1"
        params: list[Any] = []

        if status:
            where_clause += " AND status = ?"
            params.append(status.value)

        # Get total count
        count_cursor = await self.execute(
            f"SELECT COUNT(*) FROM tasks {where_clause}", tuple(params)
        )
        total = (await count_cursor.fetchone())[0]

        # Get tasks
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
        video_info: VideoInfo,
        audio_file_id: Optional[str],
        transcript_file_id: Optional[str],
        expires_at: datetime,
        has_transcript: bool = True,
        audio_fallback: bool = False,
    ) -> None:
        """
        Update task as completed with file information.

        Args:
            task_id: Task UUID.
            video_info: Video information.
            audio_file_id: Audio file UUID (may be None for transcript_only mode).
            transcript_file_id: Transcript file UUID (may be None).
            expires_at: File expiry time.
            has_transcript: Whether video has available transcript.
            audio_fallback: Whether audio was downloaded as fallback.
        """
        now = datetime.now(timezone.utc)
        video_info_json = json.dumps(video_info.to_dict())

        async with self.transaction():
            await self.execute(
                """
                UPDATE tasks
                SET status = ?, video_info = ?, audio_file_id = ?,
                    transcript_file_id = ?, completed_at = ?, expires_at = ?,
                    has_transcript = ?, audio_fallback = ?
                WHERE id = ?
                """,
                (
                    TaskStatus.COMPLETED.value,
                    video_info_json,
                    audio_file_id,
                    transcript_file_id,
                    now,
                    expires_at,
                    1 if has_transcript else 0,
                    1 if audio_fallback else 0,
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

    # ==================== File Operations ====================

    async def create_file(self, file: FileRecord) -> None:
        """
        Create a new file record.

        Args:
            file: File record to create.
        """
        metadata_json = json.dumps(file.metadata) if file.metadata else None

        async with self.transaction():
            await self.execute(
                """
                INSERT INTO files (
                    id, task_id, type, filename, filepath, size, format,
                    metadata, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file.id,
                    file.task_id,
                    file.type.value,
                    file.filename,
                    file.filepath,
                    file.size,
                    file.format,
                    metadata_json,
                    file.created_at or datetime.now(timezone.utc),
                    file.expires_at,
                ),
            )

        logger.debug(f"File record created: {file.id}")

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

    async def get_files_by_task(self, task_id: str) -> list[FileRecord]:
        """
        Get all files for a task.

        Args:
            task_id: Task UUID.

        Returns:
            List of file records.
        """
        cursor = await self.execute(
            "SELECT * FROM files WHERE task_id = ?", (task_id,)
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

    async def delete_expired_tasks(self, cutoff_time: datetime) -> int:
        """
        Delete expired tasks with no remaining files.

        Args:
            cutoff_time: Cutoff datetime.

        Returns:
            Number of tasks deleted.
        """
        async with self.transaction():
            # Delete tasks that are expired and have no files
            cursor = await self.execute(
                """
                DELETE FROM tasks
                WHERE expires_at < ?
                AND id NOT IN (SELECT DISTINCT task_id FROM files)
                """,
                (cutoff_time,),
            )
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Deleted {count} expired tasks")
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

    # ==================== Helper Methods ====================

    def _row_to_task(self, row: aiosqlite.Row) -> Task:
        """Convert database row to Task object."""
        video_info = None
        if row["video_info"]:
            video_info = VideoInfo.from_dict(json.loads(row["video_info"]))

        # Handle new fields with backwards compatibility
        include_audio = row["include_audio"] if "include_audio" in row.keys() else 1
        include_transcript = (
            row["include_transcript"] if "include_transcript" in row.keys() else 1
        )
        has_transcript = row["has_transcript"] if "has_transcript" in row.keys() else None
        audio_fallback = row["audio_fallback"] if "audio_fallback" in row.keys() else 0

        return Task(
            id=row["id"],
            video_id=row["video_id"],
            video_url=row["video_url"],
            status=TaskStatus(row["status"]),
            include_audio=bool(include_audio),
            include_transcript=bool(include_transcript),
            has_transcript=bool(has_transcript) if has_transcript is not None else None,
            audio_fallback=bool(audio_fallback),
            video_info=video_info,
            audio_file_id=row["audio_file_id"],
            transcript_file_id=row["transcript_file_id"],
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
            expires_at=row["expires_at"],
        )

    def _row_to_file(self, row: aiosqlite.Row) -> FileRecord:
        """Convert database row to FileRecord object."""
        metadata = None
        if row["metadata"]:
            metadata = json.loads(row["metadata"])

        return FileRecord(
            id=row["id"],
            task_id=row["task_id"],
            type=FileType(row["type"]),
            filename=row["filename"],
            filepath=row["filepath"],
            size=row["size"],
            format=row["format"],
            metadata=metadata,
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
