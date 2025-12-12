"""
YouTube downloader module using yt-dlp.

Handles audio and transcript downloads with error handling and retry logic.
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import yt_dlp

from src.config import Settings
from src.db.models import ErrorCode, VideoInfo
from src.utils.logger import logger


@dataclass
class DownloadResult:
    """Result of a download operation."""

    video_info: VideoInfo
    audio_path: Path
    transcript_path: Optional[Path] = None


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
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": ["zh-Hans", "zh-Hant", "zh", "en"],
            "subtitlesformat": "json3",
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
        opts["extractor_args"] = {
            "youtube": {
                "player_client": ["mweb"],
            },
            "youtubepot-bgutilhttp": {
                "base_url": self.settings.pot_server_url,
            },
        }

        return opts

    async def download(
        self,
        video_url: str,
        output_dir: Path,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> DownloadResult:
        """
        Download video audio and transcript.

        Args:
            video_url: YouTube video URL.
            output_dir: Directory to save downloaded files.
            progress_callback: Optional callback for progress updates.

        Returns:
            DownloadResult with paths to downloaded files.

        Raises:
            DownloadError: If download fails.
        """
        if self.settings.dry_run:
            logger.info(f"Dry run: would download {video_url}")
            return self._create_dry_run_result(output_dir)

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build options for this download
        opts = self._build_download_opts(output_dir, progress_callback)

        try:
            # Run download in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, self._do_download, video_url, opts, output_dir
            )
            return result

        except yt_dlp.utils.DownloadError as e:
            error_code, message = self._map_ytdlp_error(e)
            logger.error(f"Download failed: {error_code.value} - {message}")
            raise DownloadError(error_code, message) from e

        except Exception as e:
            logger.error(f"Unexpected download error: {e}")
            raise DownloadError(ErrorCode.DOWNLOAD_FAILED, str(e)) from e

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
    ) -> DownloadResult:
        """
        Perform the actual download (runs in thread pool).

        Args:
            video_url: YouTube video URL.
            opts: yt-dlp options.
            output_dir: Output directory.

        Returns:
            DownloadResult with video info and file paths.
        """
        with yt_dlp.YoutubeDL(opts) as ydl:
            # Extract info and download
            info = ydl.extract_info(video_url, download=True)

            if not info:
                raise DownloadError(
                    ErrorCode.DOWNLOAD_FAILED, "Failed to extract video info"
                )

            video_id = info["id"]

            # Extract video info
            video_info = self._extract_video_info(info)

            # Find downloaded files
            audio_path = self._find_audio_file(output_dir, video_id)
            transcript_path = self._find_transcript_file(output_dir, video_id)

            if not audio_path:
                raise DownloadError(
                    ErrorCode.DOWNLOAD_FAILED, "Audio file not found after download"
                )

            logger.info(f"Download completed: {video_id}")
            logger.debug(f"Audio: {audio_path}")
            logger.debug(f"Transcript: {transcript_path}")

            return DownloadResult(
                video_info=video_info,
                audio_path=audio_path,
                transcript_path=transcript_path,
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

    def _find_transcript_file(self, output_dir: Path, video_id: str) -> Optional[Path]:
        """
        Find downloaded transcript file.

        Args:
            output_dir: Output directory.
            video_id: YouTube video ID.

        Returns:
            Path to transcript file or None if not found.
        """
        # Look for JSON subtitle files with priority
        for lang in ["zh-Hans", "zh-Hant", "zh", "en"]:
            path = output_dir / f"{video_id}.{lang}.json3"
            if path.exists():
                return path

        # Fallback: any json3 file
        for file in output_dir.glob(f"{video_id}.*.json3"):
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
            transcript_path=output_dir / "test.en.json3",
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
