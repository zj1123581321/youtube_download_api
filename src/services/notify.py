"""
Notification service module.

Handles sending notifications to WeCom (Enterprise WeChat) webhook.
"""

import socket
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from src.config import Settings
from src.db.models import Task, VideoInfo
from src.utils.helpers import format_duration
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.db.database import Database

# Try to import wecom_notifier, but make it optional
try:
    from wecom_notifier import WeComNotifier

    WECOM_AVAILABLE = True
except ImportError:
    WECOM_AVAILABLE = False
    logger.warning("wecom-notifier not installed, notifications will be disabled")


class NotificationService:
    """
    Service for sending WeCom webhook notifications.

    Sends notifications for startup, task completion, and failures.
    """

    def __init__(self, settings: Settings, db: Optional["Database"] = None):
        """
        Initialize notification service.

        Args:
            settings: Application settings.
            db: Database instance for fetching video info.
        """
        self.settings = settings
        self.db = db
        self.webhook_url = settings.wecom_webhook_url
        self.enabled = bool(settings.wecom_webhook_url) and WECOM_AVAILABLE

        if WECOM_AVAILABLE and self.enabled:
            self.notifier = WeComNotifier()
        else:
            self.notifier = None

        if not self.enabled:
            logger.info("WeCom notifications disabled")

    async def notify_startup(self, version: str) -> None:
        """
        Send system startup notification.

        Args:
            version: Application version.
        """
        if not self.enabled or not self.notifier:
            return

        try:
            hostname = socket.gethostname()
            ip = self._get_local_ip()

            content = f"""# 🚀 YouTube Audio API Started

🖥️ **Host**: {hostname} ({ip})
📦 **Version**: {version}
🕐 **Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

⚙️ **Configuration**:
> 📊 Concurrency: {self.settings.download_concurrency}
> ⏳ Task Interval: {self.settings.task_interval_min}-{self.settings.task_interval_max}s
> 🗂️ File Retention: {self.settings.file_retention_days} days
> 🔑 PO Token: {self.settings.pot_server_url}

✨ Service is ready to accept requests!
"""
            self.notifier.send_markdown(
                webhook_url=self.webhook_url,
                content=content,
            )
            logger.debug("Startup notification sent")

        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")

    async def _get_video_info(self, video_id: str) -> Optional[VideoInfo]:
        """
        Get video info from database.

        Args:
            video_id: YouTube video ID.

        Returns:
            VideoInfo or None.
        """
        if not self.db:
            return None
        video_resource = await self.db.get_video_resource(video_id)
        return video_resource.video_info if video_resource else None

    async def notify_completed(self, task: Task) -> None:
        """
        Send task completion notification.

        Args:
            task: Completed task.
        """
        if not self.enabled or not self.notifier:
            return

        try:
            video_info = await self._get_video_info(task.video_id)
            title = video_info.title if video_info else "Unknown"
            author = video_info.author if video_info else "Unknown"
            duration = (
                format_duration(video_info.duration)
                if video_info and video_info.duration
                else "N/A"
            )

            # 获取视频描述，截断过长内容
            description = ""
            if video_info and video_info.description:
                desc = video_info.description
                max_len = 200
                if len(desc) > max_len:
                    description = desc[:max_len] + "..."
                else:
                    description = desc

            # 构建文件下载链接（带后缀）
            base_url = self.settings.base_url.rstrip("/")
            audio_url = "N/A"
            transcript_url = "无字幕"

            if task.audio_file_id and self.db:
                audio_file = await self.db.get_file(task.audio_file_id)
                if audio_file:
                    audio_ext = audio_file.format or "m4a"
                    audio_url = f"{base_url}/api/v1/files/{task.audio_file_id}.{audio_ext}"

            if task.transcript_file_id and self.db:
                transcript_file = await self.db.get_file(task.transcript_file_id)
                if transcript_file:
                    transcript_ext = transcript_file.format or "srt"
                    transcript_url = f"{base_url}/api/v1/files/{task.transcript_file_id}.{transcript_ext}"

            content = f"""# ✅ Download Completed

🎬 **Video**: {title}
👤 **Author**: {author}
⏱️ **Duration**: {duration}

🔗 **Video URL**: {task.video_url}

📝 **Description**:
> {description if description else "无描述"}

🎵 **Audio**: {audio_url}
📄 **Transcript**: {transcript_url if task.transcript_file_id else "无字幕"}

🆔 **Task ID**: `{task.id}`
"""
            self.notifier.send_markdown(
                webhook_url=self.webhook_url,
                content=content,
            )
            logger.debug(f"Completion notification sent for task {task.id}")

        except Exception as e:
            logger.error(f"Failed to send completion notification: {e}")

    async def notify_failed(self, task: Task, error: str) -> None:
        """
        Send task failure notification.

        Args:
            task: Failed task.
            error: Error message.
        """
        if not self.enabled or not self.notifier:
            return

        try:
            # 获取视频标题（如果有）
            video_info = await self._get_video_info(task.video_id)
            title = video_info.title if video_info else "Unknown"

            # 获取错误码（如果有）
            error_code = task.error_code.value if task.error_code else "UNKNOWN"

            content = f"""# ❌ Download Failed

🎬 **Video**: {title}
🔗 **Video URL**: {task.video_url}

💥 **Error Code**: `{error_code}`
📋 **Error Message**: {error}

🔄 **Retry Count**: {task.retry_count}
🆔 **Task ID**: `{task.id}`
"""
            self.notifier.send_markdown(
                webhook_url=self.webhook_url,
                content=content,
                mention_all=True,  # @all on failure
            )
            logger.debug(f"Failure notification sent for task {task.id}")

        except Exception as e:
            logger.error(f"Failed to send failure notification: {e}")

    async def notify_cookie_expired(self) -> None:
        """Send cookie expiration warning notification."""
        if not self.enabled or not self.notifier:
            return

        try:
            content = """# ⚠️ Cookie Expired Warning

🍪 YouTube cookie has expired. Some features may be limited:

> ❌ Age-restricted videos cannot be downloaded
> ❌ Member-only content cannot be downloaded

🔧 Please update the cookie file and restart the service.
"""
            self.notifier.send_markdown(
                webhook_url=self.webhook_url,
                content=content,
                mention_all=True,
            )
            logger.warning("Cookie expiration notification sent")

        except Exception as e:
            logger.error(f"Failed to send cookie expiration notification: {e}")

    async def notify_disk_space_warning(self, free_mb: int) -> None:
        """
        Send low disk space warning notification.

        Args:
            free_mb: Free disk space in MB.
        """
        if not self.enabled or not self.notifier:
            return

        try:
            content = f"""# ⚠️ Low Disk Space Warning

💾 Available disk space is running low!

📉 **Free Space**: {free_mb} MB

🔧 Please clean up old files or expand storage.
"""
            self.notifier.send_markdown(
                webhook_url=self.webhook_url,
                content=content,
                mention_all=True,
            )
            logger.warning(f"Disk space warning notification sent: {free_mb}MB free")

        except Exception as e:
            logger.error(f"Failed to send disk space notification: {e}")

    def _get_local_ip(self) -> str:
        """
        Get local IP address.

        Returns:
            Local IP address string.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"
