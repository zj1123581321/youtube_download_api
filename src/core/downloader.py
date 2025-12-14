"""
YouTube downloader module using yt-dlp.

Handles audio downloads with error handling and retry logic.
Subtitles are fetched separately via TikHub API to avoid YouTube rate limiting.

Supports graceful cancellation via threading.Event for responsive Ctrl+C handling.
"""

import asyncio
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import yt_dlp
from yt_dlp.networking.impersonate import ImpersonateTarget

from src.config import Settings
from src.db.models import ErrorCode, VideoInfo
from src.services.tikhub_service import TikHubService
from src.utils.logger import logger


class DownloadCancelledError(Exception):
    """
    Exception raised when download is cancelled by user.

    This is raised in progress hooks when cancel_event is set,
    allowing immediate interruption of yt-dlp operations.
    """

    pass


class YtDlpLogger:
    """
    自定义 yt-dlp 日志适配器。

    捕获 yt-dlp 的所有日志输出，特别关注风控绕过相关的信息。
    将日志转发到应用的 loguru logger。
    """

    # 风控绕过相关的关键词模式（这些日志会被提升为 INFO 级别）
    ANTI_BOT_PATTERNS = [
        # PO Token 相关
        r"\[pot\]",
        r"\[pot:",
        r"po\s*token",
        r"potoken",
        r"bgutil",
        r"botguard",
        r"attestation",
        r"content.?binding",
        r"LOGIN_REQUIRED",
        r"player.*response.*status",
        r"player_client",
        # Cookie 相关 - 捕获实际加载日志
        r"cookie",
        r"Netscape",
        r"Loaded \d+",
        # TLS/Impersonate 相关 - 捕获 request handler 使用日志
        r"impersonate",
        r"curl.?cffi",
        r"CurlCFFIRH",
        r"Request.*Handler",
        # 网络请求相关
        r"Using \w+RH",
        r"fetching.*player",
    ]

    def __init__(self) -> None:
        """初始化日志适配器。"""
        self._anti_bot_pattern = re.compile(
            "|".join(self.ANTI_BOT_PATTERNS), re.IGNORECASE
        )

    def _is_anti_bot_related(self, msg: str) -> bool:
        """检查消息是否与风控绕过相关。"""
        return bool(self._anti_bot_pattern.search(msg))

    def debug(self, msg: str) -> None:
        """处理 debug 级别日志。"""
        if self._is_anti_bot_related(msg):
            logger.info(f"[yt-dlp:ANTI-BOT] {msg}")
        else:
            logger.debug(f"[yt-dlp] {msg}")

    def info(self, msg: str) -> None:
        """处理 info 级别日志。"""
        if self._is_anti_bot_related(msg):
            logger.info(f"[yt-dlp:ANTI-BOT] {msg}")
        else:
            logger.info(f"[yt-dlp] {msg}")

    def warning(self, msg: str) -> None:
        """处理 warning 级别日志。"""
        if self._is_anti_bot_related(msg):
            logger.warning(f"[yt-dlp:ANTI-BOT] {msg}")
        else:
            logger.warning(f"[yt-dlp] {msg}")

    def error(self, msg: str) -> None:
        """处理 error 级别日志。"""
        if self._is_anti_bot_related(msg):
            logger.error(f"[yt-dlp:ANTI-BOT] {msg}")
        else:
            logger.error(f"[yt-dlp] {msg}")


@dataclass
class DownloadResult:
    """Result of a download operation."""

    video_info: VideoInfo
    audio_path: Optional[Path] = None  # May be None for transcript_only mode
    transcript_path: Optional[Path] = None


@dataclass
class TranscriptOnlyResult:
    """Result of transcript-only extraction (no audio download)."""

    video_info: VideoInfo
    has_transcript: bool  # Whether video has available transcript
    transcript_path: Optional[Path] = None  # Path to subtitle file if fetched


@dataclass
class _AudioDownloadResult:
    """Internal result of audio download (before subtitle fetch)."""

    video_info: VideoInfo
    audio_path: Path
    video_id: str
    raw_info: dict[str, Any]  # Raw yt-dlp info for subtitle URL extraction


@dataclass
class _InfoExtractionResult:
    """Internal result of video info extraction (no download)."""

    video_info: VideoInfo
    video_id: str
    raw_info: dict[str, Any]  # Raw yt-dlp info for subtitle URL extraction
    has_subtitle: bool  # Whether video has available subtitles


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

    Supports graceful cancellation via cancel() method for responsive shutdown.
    """

    def __init__(self, settings: Settings):
        """
        Initialize downloader with settings.

        Args:
            settings: Application settings.
        """
        self.settings = settings
        self._ytdlp_logger = YtDlpLogger()
        self._base_opts = self._build_base_opts()
        self._tikhub_service = TikHubService(settings)

        # 取消机制：使用 threading.Event 实现跨线程取消
        # 在 progress_hook 中检查此标志，实现下载阶段的快速取消
        self._cancel_event = threading.Event()

        # 输出风控绕过配置状态
        self._log_anti_bot_config()

    def cancel(self) -> None:
        """
        Request cancellation of ongoing download.

        Sets the cancel event which will be checked in progress hooks.
        Safe to call from any thread (typically from asyncio event loop).
        """
        logger.info("Download cancellation requested")
        self._cancel_event.set()

    def reset_cancel(self) -> None:
        """
        Reset cancellation state for new download.

        Must be called before starting a new download if cancel() was previously called.
        """
        self._cancel_event.clear()

    @property
    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested."""
        return self._cancel_event.is_set()

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
            # TLS 指纹模拟 - 使用 curl_cffi 模拟 Chrome 浏览器
            # 自动使用最新版本（当前为 chrome136），避免被 YouTube 识别为 bot
            # 参考: https://curl-cffi.readthedocs.io/en/latest/impersonate/targets.html
            # 注意: Python API 需要使用 ImpersonateTarget 对象，而非字符串
            "impersonate": ImpersonateTarget.from_str("chrome"),
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
            # Logging - 使用自定义 logger 捕获 PO Token 相关日志
            "quiet": False,  # 不静默，让日志输出到我们的 logger
            "verbose": True,  # 开启详细日志以捕获 POT 信息
            "logger": self._ytdlp_logger,  # 自定义日志适配器
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

        # PO Token Provider 配置
        #
        # yt-dlp extractor_args Python API 格式（与 CLI 解析结果一致）：
        # - 键: extractor 名称（如 "youtube", "youtubepot-bgutilhttp"）
        # - 值: 嵌套字典 {参数名: [参数值列表]}
        #
        # 参考: bgutil-ytdlp-pot-provider README
        #
        # 2024.12 更新：YouTube web 客户端已强制使用 SABR 协议，
        # 导致常规 HTTP 格式不可用 (yt-dlp#12482)。
        #
        # 客户端选择策略：
        # - tv_embedded 优先：嵌入式电视客户端，限制较少，兼容性好
        # - web_creator 备选：支持 cookies + PO Token
        # - ios 备选：不需要认证，速度快
        #
        # player_js_version=actual: 使用实际的 player.js 版本而非缓存版本
        # 这有助于解决 YouTube 更新 player.js 后的兼容性问题
        if self.settings.cookie_file:
            # 有 cookies 时，tv_embedded 优先，web_creator 备选
            youtube_args = {
                "player_client": ["tv_embedded", "web_creator"],
                "player_js_version": ["actual"],
            }
        else:
            # 无 cookies 时，tv_embedded 优先，ios 和 web_creator 备选
            youtube_args = {
                "player_client": ["tv_embedded", "ios", "web_creator"],
                "player_js_version": ["actual"],
            }

        opts["extractor_args"] = {
            "youtube": youtube_args,
            # bgutil:http provider 配置（web_creator 客户端需要 PO Token）
            "youtubepot-bgutilhttp": {
                "base_url": [self.settings.pot_server_url],
            },
        }

        # 启用远程组件下载，用于解决 n challenge
        # 这允许 deno 下载所需的 npm 包来解决 YouTube 的 JS 挑战
        # 格式必须是 set，包含 "ejs:github" 或 "ejs:npm"
        opts["remote_components"] = {"ejs:github"}

        # 记录 PO Token 配置信息
        logger.debug(
            f"[POT Config] youtube_args={youtube_args}, "
            f"pot_server={self.settings.pot_server_url}, "
            f"cookie_file={self.settings.cookie_file or 'None'}"
        )

        return opts

    def _log_anti_bot_config(self) -> None:
        """
        输出风控绕过配置状态日志。

        在初始化时调用，明确显示 TLS、Cookie、PO Token 三者的配置状态。
        """
        # TLS 指纹模拟状态
        impersonate_target = self._base_opts.get("impersonate")
        if impersonate_target:
            tls_status = f"✓ Enabled (target={impersonate_target})"
        else:
            tls_status = "✗ Disabled"

        # Cookie 状态
        cookie_file = self._base_opts.get("cookiefile")
        if cookie_file:
            cookie_path = Path(cookie_file)
            if cookie_path.exists():
                cookie_status = f"✓ Loaded ({cookie_file})"
            else:
                cookie_status = f"✗ File not found ({cookie_file})"
        else:
            cookie_status = "✗ Not configured"

        # PO Token Provider 状态
        extractor_args = self._base_opts.get("extractor_args", {})
        pot_config = extractor_args.get("youtubepot-bgutilhttp", {})
        pot_base_url = pot_config.get("base_url", [None])[0]
        if pot_base_url:
            pot_status = f"✓ Configured ({pot_base_url})"
        else:
            pot_status = "✗ Not configured"

        # Player Client 策略
        youtube_args = extractor_args.get("youtube", {})
        player_clients = youtube_args.get("player_client", [])

        logger.info("=" * 60)
        logger.info("Anti-Bot Configuration Status:")
        logger.info(f"  TLS Impersonate : {tls_status}")
        logger.info(f"  Cookie File     : {cookie_status}")
        logger.info(f"  PO Token Server : {pot_status}")
        logger.info(f"  Player Clients  : {player_clients}")
        logger.info("=" * 60)

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

        Supports cancellation via cancel() method - when called, the download
        will be interrupted at the next progress update.

        Args:
            video_url: YouTube video URL.
            output_dir: Directory to save downloaded files.
            progress_callback: Optional callback for progress updates.

        Returns:
            DownloadResult with paths to downloaded files.

        Raises:
            DownloadError: If audio download fails.
            DownloadCancelledError: If download is cancelled via cancel().
            Note: Subtitle fetch failures do NOT raise errors, only log warnings.
        """
        if self.settings.dry_run:
            logger.info(f"Dry run: would download {video_url}")
            return self._create_dry_run_result(output_dir)

        # 检查是否在开始前就已被取消
        if self._cancel_event.is_set():
            logger.info("Download cancelled before start")
            raise DownloadCancelledError("Download cancelled before start")

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

            # 下载完成后再次检查取消标志
            if self._cancel_event.is_set():
                logger.info("Download cancelled after audio download")
                raise DownloadCancelledError("Download cancelled after audio download")

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

        except DownloadCancelledError:
            # 直接重新抛出取消异常，不做包装
            raise

        except yt_dlp.utils.DownloadError as e:
            # 检查是否是因为取消导致的错误
            if self._cancel_event.is_set():
                logger.info("Download cancelled (detected via yt-dlp error)")
                raise DownloadCancelledError("Download cancelled") from e
            error_code, message = self._map_ytdlp_error(e)
            logger.error(f"Download failed: {error_code.value} - {message}")
            raise DownloadError(error_code, message) from e

        except Exception as e:
            # 检查是否是因为取消导致的错误
            if self._cancel_event.is_set():
                logger.info("Download cancelled (detected via exception)")
                raise DownloadCancelledError("Download cancelled") from e
            # 捕获详细的异常信息用于调试
            import traceback
            error_msg = str(e) or repr(e) or type(e).__name__
            logger.error(f"Unexpected download error: {error_msg}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            raise DownloadError(ErrorCode.DOWNLOAD_FAILED, error_msg) from e

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

    async def extract_transcript_only(
        self,
        video_url: str,
        output_dir: Path,
    ) -> TranscriptOnlyResult:
        """
        Extract video info and fetch transcript only (no audio download).

        This is used for transcript_only mode where client only wants subtitles.
        If subtitles are available, they are fetched via TikHub API.

        Supports cancellation via cancel() method.

        Args:
            video_url: YouTube video URL.
            output_dir: Directory to save subtitle file.

        Returns:
            TranscriptOnlyResult with video info and subtitle status.

        Raises:
            DownloadError: If video info extraction fails.
            DownloadCancelledError: If extraction is cancelled via cancel().
        """
        if self.settings.dry_run:
            logger.info(f"Dry run: would extract transcript for {video_url}")
            return TranscriptOnlyResult(
                video_info=VideoInfo(
                    title="Test Video (Dry Run)",
                    author="Test Author",
                    duration=60,
                ),
                has_transcript=True,
                transcript_path=output_dir / "test.en.srt",
            )

        # 检查是否在开始前就已被取消
        if self._cancel_event.is_set():
            logger.info("Transcript extraction cancelled before start")
            raise DownloadCancelledError("Extraction cancelled before start")

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Extract video info without downloading
            loop = asyncio.get_event_loop()
            info_result = await loop.run_in_executor(
                None, self._extract_info_only, video_url
            )

            # 检查取消标志
            if self._cancel_event.is_set():
                logger.info("Transcript extraction cancelled after info extraction")
                raise DownloadCancelledError("Extraction cancelled after info extraction")

            if not info_result.has_subtitle:
                logger.info(
                    f"No subtitle available for video {info_result.video_id}, "
                    "audio download required for ASR"
                )
                return TranscriptOnlyResult(
                    video_info=info_result.video_info,
                    has_transcript=False,
                    transcript_path=None,
                )

            # Fetch subtitle via TikHub API
            transcript_path = await self._fetch_subtitle_via_tikhub(
                info_result.raw_info,
                output_dir,
                info_result.video_id,
            )

            logger.info(f"Transcript extraction completed: {info_result.video_id}")
            logger.info(f"Transcript: {transcript_path}")

            return TranscriptOnlyResult(
                video_info=info_result.video_info,
                has_transcript=True,
                transcript_path=transcript_path,
            )

        except DownloadCancelledError:
            # 直接重新抛出取消异常
            raise

        except yt_dlp.utils.DownloadError as e:
            # 检查是否是因为取消导致的错误
            if self._cancel_event.is_set():
                logger.info("Extraction cancelled (detected via yt-dlp error)")
                raise DownloadCancelledError("Extraction cancelled") from e
            error_code, message = self._map_ytdlp_error(e)
            logger.error(f"Info extraction failed: {error_code.value} - {message}")
            raise DownloadError(error_code, message) from e

        except Exception as e:
            # 检查是否是因为取消导致的错误
            if self._cancel_event.is_set():
                logger.info("Extraction cancelled (detected via exception)")
                raise DownloadCancelledError("Extraction cancelled") from e
            logger.error(f"Unexpected error during info extraction: {e}")
            raise DownloadError(ErrorCode.DOWNLOAD_FAILED, str(e)) from e

    def _extract_info_only(self, video_url: str) -> _InfoExtractionResult:
        """
        Extract video info without downloading (runs in thread pool).

        Args:
            video_url: YouTube video URL.

        Returns:
            _InfoExtractionResult with video info and subtitle availability.
        """
        logger.info(f"[POT] Extracting info for: {video_url}")

        # Build minimal options for info extraction
        opts = {
            **self._base_opts,
            "skip_download": True,
        }

        with yt_dlp.YoutubeDL(opts) as ydl:
            logger.info("[POT] Calling extract_info (info only, no download)")
            info = ydl.extract_info(video_url, download=False)

            if not info:
                raise DownloadError(
                    ErrorCode.DOWNLOAD_FAILED, "Failed to extract video info"
                )

            video_id = info["id"]
            video_info = self._extract_video_info(info)

            # Check if subtitles are available
            has_subtitle = bool(
                info.get("subtitles") or info.get("automatic_captions")
            )

            logger.info(
                f"[POT] Video {video_id}: has_subtitle={has_subtitle}, "
                f"title='{info.get('title', 'N/A')[:50]}'"
            )

            return _InfoExtractionResult(
                video_info=video_info,
                video_id=video_id,
                raw_info=info,
                has_subtitle=has_subtitle,
            )

    def _build_download_opts(
        self,
        output_dir: Path,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> dict[str, Any]:
        """
        Build download-specific options.

        Includes a cancel-check hook that raises DownloadCancelledError
        when cancel_event is set, enabling responsive Ctrl+C handling.

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

        # 取消事件引用，用于闭包
        cancel_event = self._cancel_event

        def progress_hook(d: dict[str, Any]) -> None:
            """
            Progress hook with cancellation support.

            Checks cancel_event on every progress update, raises
            DownloadCancelledError if cancellation is requested.
            """
            # 首先检查取消标志 - 这是实现快速响应 Ctrl+C 的关键
            if cancel_event.is_set():
                logger.info("Cancellation detected in progress hook, aborting download")
                raise DownloadCancelledError("Download cancelled by user")

            # 然后处理进度更新
            if progress_callback:
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
        logger.info(f"[POT] Starting download for: {video_url}")
        logger.debug(
            f"[POT] yt-dlp extractor_args: {opts.get('extractor_args', {})}"
        )

        with yt_dlp.YoutubeDL(opts) as ydl:
            # 第一步：提取视频信息（唯一的页面请求）
            logger.debug("Extracting video info (single page request)...")
            logger.info("[POT] Calling extract_info - PO Token should be requested here if needed")
            info = ydl.extract_info(video_url, download=False)

            if not info:
                raise DownloadError(
                    ErrorCode.DOWNLOAD_FAILED, "Failed to extract video info"
                )

            video_id = info["id"]
            video_info = self._extract_video_info(info)
            logger.debug(f"Extracted video info: {video_info}")

            # 记录视频格式信息（可以看出是否成功获取了播放 URL）
            formats = info.get("formats", [])
            logger.info(
                f"[POT] Video {video_id}: found {len(formats)} formats, "
                f"title='{info.get('title', 'N/A')[:50]}'"
            )

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
