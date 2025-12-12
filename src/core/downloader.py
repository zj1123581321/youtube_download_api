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
            # 字幕语言优先级：中文 > 英文 > 其他
            # 实际下载时会根据可用字幕动态选择一种（见 _do_download）
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": ["all"],  # 先获取所有可用字幕信息
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

    # 字幕语言优先级：中文 > 英文 > 其他
    SUBTITLE_PRIORITY = [
        "zh-Hans",  # 简体中文
        "zh-Hant",  # 繁体中文
        "zh",       # 中文（通用）
        "en",       # 英文
    ]

    def _select_best_subtitle_lang(self, info: dict[str, Any]) -> Optional[str]:
        """
        根据优先级选择最佳字幕语言。

        Args:
            info: yt-dlp 提取的视频信息。

        Returns:
            选中的字幕语言代码，如果没有可用字幕则返回 None。
        """
        # 获取可用字幕（包括自动生成的）
        available_subs = set()

        # 手动字幕
        if info.get("subtitles"):
            available_subs.update(info["subtitles"].keys())

        # 自动生成字幕
        if info.get("automatic_captions"):
            available_subs.update(info["automatic_captions"].keys())

        if not available_subs:
            logger.debug("No subtitles available for this video")
            return None

        logger.debug(f"Available subtitle languages: {available_subs}")

        # 按优先级选择
        for lang in self.SUBTITLE_PRIORITY:
            if lang in available_subs:
                logger.info(f"Selected subtitle language: {lang}")
                return lang

        # 如果优先级列表中没有匹配的，选择第一个可用的
        first_available = next(iter(available_subs))
        logger.info(f"No preferred language found, using: {first_available}")
        return first_available

    def _do_download(
        self, video_url: str, opts: dict[str, Any], output_dir: Path
    ) -> DownloadResult:
        """
        执行下载（在线程池中运行）。

        优化策略：只发起 1 次 YouTube 页面请求，模拟正常用户行为。
        1. extract_info(download=False) 获取视频信息和可用字幕列表
        2. 根据优先级选择最佳字幕语言
        3. process_video_result() 复用已获取的信息下载音频和字幕

        Args:
            video_url: YouTube video URL.
            opts: yt-dlp options.
            output_dir: Output directory.

        Returns:
            DownloadResult with video info and file paths.
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

            # 第二步：选择最佳字幕语言
            best_lang = self._select_best_subtitle_lang(info)

            # 第三步：配置下载参数
            if best_lang:
                ydl.params["subtitleslangs"] = [best_lang]
                ydl.params["writesubtitles"] = True
                ydl.params["writeautomaticsub"] = True
                logger.info(f"Will download subtitle: {best_lang}")
            else:
                ydl.params["writesubtitles"] = False
                ydl.params["writeautomaticsub"] = False
                logger.info("No subtitles available to download")

            # 第四步：下载音频和字幕（复用 info，不再请求页面）
            logger.debug("Downloading audio and subtitle (reusing info)...")
            try:
                ydl.process_video_result(info, download=True)
            except yt_dlp.utils.DownloadError as e:
                # 如果是字幕下载失败，尝试只下载音频
                error_msg = str(e).lower()
                if "subtitle" in error_msg and best_lang:
                    logger.warning(f"Subtitle download failed, retrying audio only: {e}")
                    ydl.params["writesubtitles"] = False
                    ydl.params["writeautomaticsub"] = False
                    ydl.process_video_result(info, download=True)
                else:
                    raise

        # 查找下载的文件
        audio_path = self._find_audio_file(output_dir, video_id)
        transcript_path = self._find_transcript_file(output_dir, video_id)

        if not audio_path:
            raise DownloadError(
                ErrorCode.DOWNLOAD_FAILED, "Audio file not found after download"
            )

        logger.info(f"Download completed: {video_id}")
        logger.info(f"Audio: {audio_path}")
        logger.info(f"Transcript: {transcript_path}")
        if not transcript_path:
            logger.warning(f"No transcript found for video {video_id}")

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
