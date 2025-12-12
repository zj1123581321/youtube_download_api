"""
YouTube downloader module using yt-dlp.

Handles audio downloads with error handling and retry logic.
Subtitles are fetched separately via TikHub API to avoid YouTube rate limiting.
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import yt_dlp

from src.config import Settings
from src.db.models import ErrorCode, VideoInfo
from src.services.tikhub_service import TikHubService
from src.utils.logger import logger


@dataclass
class DownloadResult:
    """Result of a download operation."""

    video_info: VideoInfo
    audio_path: Path
    transcript_path: Optional[Path] = None


@dataclass
class _AudioDownloadResult:
    """Internal result of audio download (before subtitle fetch)."""

    video_info: VideoInfo
    audio_path: Path
    video_id: str
    raw_info: dict[str, Any]  # Raw yt-dlp info for subtitle URL extraction


class DownloadError(Exception):
    """Custom exception for download errors."""

    def __init__(self, error_code: ErrorCode, message: str):
        self.error_code = error_code
        self.message = message
        super().__init__(message)


class YouTubeDownloader:
    """
    YouTube audio and transcript downloader.

    Wraps yt-dlp with configuration for audio-only downloads,
    subtitle extraction, and YouTube anti-bot measures.
    """

    def __init__(self, settings: Settings):
        """
        Initialize downloader with settings.

        Args:
            settings: Application settings.
        """
        self.settings = settings
        self._base_opts = self._build_base_opts()
        self._tikhub_service = TikHubService(settings)

    def _build_base_opts(self) -> dict[str, Any]:
        """
        Build base yt-dlp options.

        Returns:
            Dictionary of yt-dlp options.
        """
        opts: dict[str, Any] = {
            # Format selection: audio only, prefer m4a 128kbps
            "format": f"bestaudio[ext=m4a][abr<={self.settings.audio_quality}]/bestaudio[ext=m4a]/bestaudio",
            "extract_flat": False,
            # Subtitle configuration
            # 禁用 yt-dlp 字幕下载，字幕通过 TikHub API 获取（避免 429 错误）
            # 但仍然需要获取字幕信息（URL）用于 TikHub API
            "writesubtitles": False,
            "writeautomaticsub": False,
            # Network configuration
            "socket_timeout": 30,
            "retries": 3,
            "fragment_retries": 3,
            # Safety configuration
            "no_warnings": False,
            "ignoreerrors": False,
            "no_color": True,
            # Disable unnecessary features
            "writethumbnail": False,
            # Logging
            "quiet": not self.settings.debug,
            "verbose": self.settings.debug,
            # Progress hooks will be added per download
            "progress_hooks": [],
        }

        # Proxy configuration
        if self.settings.http_proxy:
            opts["proxy"] = self.settings.http_proxy

        # Cookie configuration
        if self.settings.cookie_file:
            cookie_path = Path(self.settings.cookie_file)
            if cookie_path.exists():
                opts["cookiefile"] = str(cookie_path)
                logger.info(f"Using cookie file: {cookie_path}")

        # PO Token Provider configuration
        # 配置 YouTube player client 和 PO Token provider
        # extractor_args 值必须是字符串列表格式
        opts["extractor_args"] = {
            "youtube": ["player_client=mweb"],
            "youtubepot-bgutilhttp": [f"base_url={self.settings.pot_server_url}"],
        }

        # 启用远程组件下载，用于解决 n challenge
        # 这允许 deno 下载所需的 npm 包来解决 YouTube 的 JS 挑战
        # 格式必须是 set，包含 "ejs:github" 或 "ejs:npm"
        opts["remote_components"] = {"ejs:github"}

        return opts

    async def download(
        self,
        video_url: str,
        output_dir: Path,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> DownloadResult:
        """
        Download video audio and fetch transcript via TikHub API.

        Audio is downloaded using yt-dlp, while subtitles are fetched separately
        via TikHub API to avoid YouTube's 429 rate limiting.

        Args:
            video_url: YouTube video URL.
            output_dir: Directory to save downloaded files.
            progress_callback: Optional callback for progress updates.

        Returns:
            DownloadResult with paths to downloaded files.

        Raises:
            DownloadError: If audio download fails.
            Note: Subtitle fetch failures do NOT raise errors, only log warnings.
        """
        if self.settings.dry_run:
            logger.info(f"Dry run: would download {video_url}")
            return self._create_dry_run_result(output_dir)

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build options for this download
        opts = self._build_download_opts(output_dir, progress_callback)

        try:
            # Step 1: Download audio in thread pool
            loop = asyncio.get_event_loop()
            audio_result = await loop.run_in_executor(
                None, self._do_download, video_url, opts, output_dir
            )

            # Step 2: Fetch subtitle via TikHub API (non-blocking, failure doesn't affect audio)
            transcript_path = await self._fetch_subtitle_via_tikhub(
                audio_result.raw_info,
                output_dir,
                audio_result.video_id,
            )

            logger.info(f"Download completed: {audio_result.video_id}")
            logger.info(f"Audio: {audio_result.audio_path}")
            logger.info(f"Transcript: {transcript_path}")

            return DownloadResult(
                video_info=audio_result.video_info,
                audio_path=audio_result.audio_path,
                transcript_path=transcript_path,
            )

        except yt_dlp.utils.DownloadError as e:
            error_code, message = self._map_ytdlp_error(e)
            logger.error(f"Download failed: {error_code.value} - {message}")
            raise DownloadError(error_code, message) from e

        except Exception as e:
            logger.error(f"Unexpected download error: {e}")
            raise DownloadError(ErrorCode.DOWNLOAD_FAILED, str(e)) from e

    async def _fetch_subtitle_via_tikhub(
        self,
        raw_info: dict[str, Any],
        output_dir: Path,
        video_id: str,
    ) -> Optional[Path]:
        """
        Fetch subtitle via TikHub API.

        This method is designed to be non-blocking and failure-safe.
        Subtitle fetch failures only log warnings, they do NOT affect audio download.

        Args:
            raw_info: Raw yt-dlp video info containing subtitle URLs.
            output_dir: Directory to save subtitle file.
            video_id: YouTube video ID for filename.

        Returns:
            Path to saved subtitle file, or None if fetch failed or skipped.
        """
        try:
            if not self._tikhub_service.is_available:
                logger.info("TikHub API not configured, skipping subtitle fetch")
                return None

            transcript_path = await self._tikhub_service.fetch_best_subtitle(
                info=raw_info,
                output_dir=output_dir,
                video_id=video_id,
            )

            if transcript_path:
                logger.info(f"Subtitle fetched via TikHub: {transcript_path}")
            else:
                logger.warning(f"No subtitle available for video {video_id}")

            return transcript_path

        except Exception as e:
            # Catch all exceptions to ensure subtitle failure doesn't affect audio
            logger.warning(f"Failed to fetch subtitle via TikHub: {e}")
            return None

    def _build_download_opts(
        self,
        output_dir: Path,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> dict[str, Any]:
        """
        Build download-specific options.

        Args:
            output_dir: Output directory.
            progress_callback: Progress callback function.

        Returns:
            Dictionary of yt-dlp options.
        """
        opts = {
            **self._base_opts,
            "outtmpl": {
                "default": str(output_dir / "%(id)s.%(ext)s"),
            },
            "paths": {
                "home": str(output_dir),
            },
        }

        # Add progress hook if callback provided
        if progress_callback:

            def progress_hook(d: dict[str, Any]) -> None:
                if d["status"] == "downloading":
                    total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                    downloaded = d.get("downloaded_bytes", 0)
                    if total > 0:
                        progress = int(downloaded / total * 100)
                        progress_callback(progress)
                elif d["status"] == "finished":
                    progress_callback(100)

            opts["progress_hooks"] = [progress_hook]

        return opts

    def _do_download(
        self, video_url: str, opts: dict[str, Any], output_dir: Path
    ) -> _AudioDownloadResult:
        """
        执行音频下载（在线程池中运行）。

        优化策略：只发起 1 次 YouTube 页面请求，模拟正常用户行为。
        1. extract_info(download=False) 获取视频信息和可用字幕 URL
        2. process_video_result() 复用已获取的信息下载音频

        注意：字幕通过 TikHub API 单独获取，不在此方法中处理。

        Args:
            video_url: YouTube video URL.
            opts: yt-dlp options.
            output_dir: Output directory.

        Returns:
            _AudioDownloadResult with video info, audio path, and raw info for subtitle fetch.
        """
        with yt_dlp.YoutubeDL(opts) as ydl:
            # 第一步：提取视频信息（唯一的页面请求）
            logger.debug("Extracting video info (single page request)...")
            info = ydl.extract_info(video_url, download=False)

            if not info:
                raise DownloadError(
                    ErrorCode.DOWNLOAD_FAILED, "Failed to extract video info"
                )

            video_id = info["id"]
            video_info = self._extract_video_info(info)
            logger.debug(f"Extracted video info: {video_info}")

            # 第二步：下载音频（复用 info，不再请求页面）
            logger.debug("Downloading audio (reusing info)...")
            ydl.process_video_result(info, download=True)

        # 查找下载的音频文件
        audio_path = self._find_audio_file(output_dir, video_id)

        if not audio_path:
            raise DownloadError(
                ErrorCode.DOWNLOAD_FAILED, "Audio file not found after download"
            )

        logger.info(f"Audio download completed: {video_id}")
        logger.info(f"Audio: {audio_path}")

        return _AudioDownloadResult(
            video_info=video_info,
            audio_path=audio_path,
            video_id=video_id,
            raw_info=info,
        )

    def _extract_video_info(self, info: dict[str, Any]) -> VideoInfo:
        """
        Extract video information from yt-dlp info dict.

        Args:
            info: yt-dlp info dictionary.

        Returns:
            VideoInfo object.
        """
        return VideoInfo(
            title=info.get("title"),
            author=info.get("uploader"),
            channel_id=info.get("channel_id"),
            duration=info.get("duration"),
            description=info.get("description"),
            upload_date=info.get("upload_date"),
            view_count=info.get("view_count"),
            thumbnail=info.get("thumbnail"),
        )

    def _find_audio_file(self, output_dir: Path, video_id: str) -> Optional[Path]:
        """
        Find downloaded audio file.

        Args:
            output_dir: Output directory.
            video_id: YouTube video ID.

        Returns:
            Path to audio file or None if not found.
        """
        # Check common audio extensions
        for ext in ["m4a", "webm", "mp3", "opus", "ogg"]:
            path = output_dir / f"{video_id}.{ext}"
            if path.exists():
                return path

        # Fallback: search for any file with video_id
        for file in output_dir.iterdir():
            if file.stem == video_id and file.suffix in [
                ".m4a",
                ".webm",
                ".mp3",
                ".opus",
                ".ogg",
            ]:
                return file

        return None

    def _map_ytdlp_error(self, error: Exception) -> tuple[ErrorCode, str]:
        """
        Map yt-dlp exception to error code and message.

        Args:
            error: yt-dlp exception.

        Returns:
            Tuple of (ErrorCode, error message).
        """
        error_msg = str(error).lower()

        if "private video" in error_msg:
            return ErrorCode.VIDEO_PRIVATE, "Video is private"

        if "video unavailable" in error_msg or "not available" in error_msg:
            return ErrorCode.VIDEO_UNAVAILABLE, "Video is unavailable"

        if "age-restricted" in error_msg or "sign in to confirm your age" in error_msg:
            return (
                ErrorCode.VIDEO_AGE_RESTRICTED,
                "Video is age-restricted, cookie required",
            )

        if "blocked" in error_msg and "country" in error_msg:
            return ErrorCode.VIDEO_REGION_BLOCKED, "Video is blocked in this region"

        if "is a livestream" in error_msg or "live event" in error_msg:
            return ErrorCode.VIDEO_LIVE_STREAM, "Live streams are not supported"

        if "http error 403" in error_msg or "forbidden" in error_msg:
            return ErrorCode.RATE_LIMITED, "Rate limited by YouTube"

        if "http error 429" in error_msg:
            return ErrorCode.RATE_LIMITED, "Too many requests"

        if (
            "network" in error_msg
            or "connection" in error_msg
            or "timeout" in error_msg
        ):
            return ErrorCode.NETWORK_ERROR, f"Network error: {error}"

        if "po token" in error_msg or "pot" in error_msg:
            return ErrorCode.POT_TOKEN_FAILED, "Failed to obtain PO Token"

        return ErrorCode.DOWNLOAD_FAILED, str(error)

    def _create_dry_run_result(self, output_dir: Path) -> DownloadResult:
        """
        Create a mock result for dry run mode.

        Args:
            output_dir: Output directory.

        Returns:
            Mock DownloadResult.
        """
        return DownloadResult(
            video_info=VideoInfo(
                title="Test Video (Dry Run)",
                author="Test Author",
                duration=60,
            ),
            audio_path=output_dir / "test.m4a",
            transcript_path=output_dir / "test.en.srt",
        )


async def get_video_info(video_url: str, settings: Settings) -> VideoInfo:
    """
    Get video information without downloading.

    Args:
        video_url: YouTube video URL.
        settings: Application settings.

    Returns:
        VideoInfo object.

    Raises:
        DownloadError: If info extraction fails.
    """
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        "skip_download": True,
    }

    if settings.http_proxy:
        opts["proxy"] = settings.http_proxy

    try:
        loop = asyncio.get_event_loop()

        def extract_info() -> dict[str, Any]:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                return info if info else {}

        info = await loop.run_in_executor(None, extract_info)

        return VideoInfo(
            title=info.get("title"),
            author=info.get("uploader"),
            channel_id=info.get("channel_id"),
            duration=info.get("duration"),
            description=info.get("description"),
            upload_date=info.get("upload_date"),
            view_count=info.get("view_count"),
            thumbnail=info.get("thumbnail"),
        )

    except Exception as e:
        logger.error(f"Failed to get video info: {e}")
        raise DownloadError(ErrorCode.DOWNLOAD_FAILED, str(e)) from e
