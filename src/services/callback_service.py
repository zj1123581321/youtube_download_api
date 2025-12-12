"""
Callback service module.

Handles webhook callbacks to notify external services of task completion.
"""

import asyncio
import hashlib
import hmac
import json
import time
from typing import Any, Optional

import httpx

from src.api.schemas import (
    CallbackPayload,
    ErrorInfoResponse,
    FileInfoResponse,
    FilesResponse,
    VideoInfoResponse,
)
from src.db.database import Database
from src.db.models import CallbackStatus, Task, TaskStatus
from src.utils.logger import logger


class CallbackService:
    """
    Service for sending webhook callbacks.

    Implements retry logic and HMAC signature verification.
    """

    # Callback configuration
    TIMEOUT_SECONDS = 10
    MAX_RETRIES = 3
    RETRY_DELAYS = [5, 10, 20]  # seconds

    def __init__(self, db: Database, base_url: str = ""):
        """
        Initialize callback service.

        Args:
            db: Database instance.
            base_url: Base URL for file downloads.
        """
        self.db = db
        self.base_url = base_url.rstrip("/")

    async def send_callback(self, task: Task) -> bool:
        """
        Send webhook callback for a completed/failed task.

        Args:
            task: Task with callback_url configured.

        Returns:
            True if callback was successful.
        """
        if not task.callback_url:
            return True

        payload = self._build_payload(task)
        success = False
        attempts = 0

        for attempt in range(self.MAX_RETRIES):
            attempts = attempt + 1
            try:
                await self._send_request(
                    url=task.callback_url,
                    payload=payload,
                    secret=task.callback_secret,
                    task_id=task.id,
                )
                success = True
                logger.info(f"Callback sent successfully for task {task.id}")
                break

            except Exception as e:
                logger.warning(
                    f"Callback attempt {attempts} failed for task {task.id}: {e}"
                )
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAYS[attempt])

        # Update callback status
        status = CallbackStatus.SUCCESS if success else CallbackStatus.FAILED
        await self.db.update_callback_status(task.id, status, attempts)

        if not success:
            logger.error(
                f"All callback attempts failed for task {task.id} "
                f"after {attempts} attempts"
            )

        return success

    async def _send_request(
        self,
        url: str,
        payload: CallbackPayload,
        secret: Optional[str],
        task_id: str,
    ) -> None:
        """
        Send HTTP POST request to callback URL.

        Args:
            url: Callback URL.
            payload: Callback payload.
            secret: HMAC secret for signature.
            task_id: Task ID for headers.

        Raises:
            httpx.HTTPError: If request fails.
        """
        body = payload.model_dump_json()
        timestamp = str(int(time.time()))

        headers = {
            "Content-Type": "application/json",
            "X-Task-Id": task_id,
            "X-Timestamp": timestamp,
        }

        # Add signature if secret provided
        if secret:
            signature = self._generate_signature(body.encode(), secret)
            headers["X-Signature"] = signature

        async with httpx.AsyncClient(timeout=self.TIMEOUT_SECONDS) as client:
            response = await client.post(url, content=body, headers=headers)
            response.raise_for_status()

    def _generate_signature(self, body: bytes, secret: str) -> str:
        """
        Generate HMAC-SHA256 signature.

        Args:
            body: Request body bytes.
            secret: HMAC secret.

        Returns:
            Signature string prefixed with "sha256=".
        """
        signature = hmac.new(
            secret.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()
        return f"sha256={signature}"

    def _build_payload(self, task: Task) -> CallbackPayload:
        """
        Build callback payload from task.

        Args:
            task: Task to build payload for.

        Returns:
            CallbackPayload object.
        """
        payload = CallbackPayload(
            task_id=task.id,
            status=task.status,
            video_id=task.video_id,
            expires_at=task.expires_at,
        )

        # Add video info for completed tasks
        if task.status == TaskStatus.COMPLETED and task.video_info:
            payload.video_info = VideoInfoResponse(
                title=task.video_info.title,
                author=task.video_info.author,
                channel_id=task.video_info.channel_id,
                duration=task.video_info.duration,
                description=task.video_info.description,
                upload_date=task.video_info.upload_date,
                view_count=task.video_info.view_count,
                thumbnail=task.video_info.thumbnail,
            )

            # Add file URLs (with base URL for external access)
            if task.audio_file_id:
                audio_url = f"{self.base_url}/api/v1/files/{task.audio_file_id}"
                transcript_url = None
                if task.transcript_file_id:
                    transcript_url = (
                        f"{self.base_url}/api/v1/files/{task.transcript_file_id}"
                    )

                payload.files = FilesResponse(
                    audio=FileInfoResponse(url=audio_url, size=None, format="m4a"),
                    transcript=FileInfoResponse(
                        url=transcript_url, size=None, format="json"
                    )
                    if transcript_url
                    else None,
                )

        # Add error info for failed tasks
        elif task.status == TaskStatus.FAILED and task.error_code:
            payload.error = ErrorInfoResponse(
                code=task.error_code,
                message=task.error_message or "Unknown error",
                retry_count=task.retry_count,
            )

        return payload


def verify_callback_signature(body: bytes, signature: str, secret: str) -> bool:
    """
    Verify HMAC-SHA256 signature from callback.

    This function is provided for clients to verify incoming callbacks.

    Args:
        body: Request body bytes.
        signature: Signature from X-Signature header.
        secret: Shared HMAC secret.

    Returns:
        True if signature is valid.
    """
    expected = hmac.new(
        secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)
