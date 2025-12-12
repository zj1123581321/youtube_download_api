"""
TikHub API service for fetching YouTube subtitles.

Uses TikHub's YouTube Web API to get subtitles, bypassing YouTube's rate limiting.
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import httpx

from src.config import Settings
from src.utils.logger import logger


# TikHub API endpoint
TIKHUB_SUBTITLE_API = "https://api.tikhub.io/api/v1/youtube/web/get_video_subtitles"

# Subtitle language priority: Chinese > English > Others
SUBTITLE_PRIORITY = [
    "zh-Hans",  # Simplified Chinese
    "zh-Hant",  # Traditional Chinese
    "zh",       # Chinese (generic)
    "en",       # English
]


@dataclass
class SubtitleInfo:
    """Subtitle URL information extracted from yt-dlp."""

    lang: str
    url: str
    is_auto: bool = False


class TikHubError(Exception):
    """Custom exception for TikHub API errors."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class TikHubService:
    """
    TikHub API service for fetching YouTube subtitles.

    This service uses TikHub's API to fetch subtitles instead of directly
    accessing YouTube, which helps avoid 429 rate limiting errors.
    """

    def __init__(self, settings: Settings):
        """
        Initialize TikHub service.

        Args:
            settings: Application settings containing TikHub API key.
        """
        self.settings = settings
        self.api_key = settings.tikhub_api_key

    @property
    def is_available(self) -> bool:
        """Check if TikHub service is configured and available."""
        return bool(self.api_key)

    def extract_subtitle_urls(self, info: dict[str, Any]) -> list[SubtitleInfo]:
        """
        Extract subtitle URLs from yt-dlp video info.

        Args:
            info: yt-dlp extracted video information.

        Returns:
            List of SubtitleInfo objects sorted by language priority.
        """
        subtitles: list[SubtitleInfo] = []

        # Extract manual subtitles
        if info.get("subtitles"):
            for lang, formats in info["subtitles"].items():
                url = self._find_best_subtitle_url(formats)
                if url:
                    subtitles.append(SubtitleInfo(lang=lang, url=url, is_auto=False))

        # Extract automatic captions
        if info.get("automatic_captions"):
            for lang, formats in info["automatic_captions"].items():
                # Skip if we already have manual subtitle for this language
                if any(s.lang == lang and not s.is_auto for s in subtitles):
                    continue
                url = self._find_best_subtitle_url(formats)
                if url:
                    subtitles.append(SubtitleInfo(lang=lang, url=url, is_auto=True))

        # Sort by priority
        def priority_key(s: SubtitleInfo) -> tuple[int, int]:
            try:
                lang_priority = SUBTITLE_PRIORITY.index(s.lang)
            except ValueError:
                lang_priority = len(SUBTITLE_PRIORITY)
            # Prefer manual over auto
            auto_priority = 1 if s.is_auto else 0
            return (lang_priority, auto_priority)

        subtitles.sort(key=priority_key)
        return subtitles

    def _find_best_subtitle_url(self, formats: list[dict[str, Any]]) -> Optional[str]:
        """
        Find the best subtitle URL from available formats.

        Prefers json3 format for TikHub API compatibility.

        Args:
            formats: List of subtitle format dictionaries.

        Returns:
            Best subtitle URL or None.
        """
        if not formats:
            return None

        # Prefer json3 format (required by TikHub API)
        for fmt in formats:
            if fmt.get("ext") == "json3":
                url = fmt.get("url")
                if url:
                    return str(url)

        # Fallback to first available
        for fmt in formats:
            url = fmt.get("url")
            if url:
                return str(url)

        return None

    async def fetch_subtitle(
        self,
        subtitle_url: str,
        output_path: Path,
        output_format: str = "srt",
    ) -> bool:
        """
        Fetch subtitle from TikHub API and save to file.

        Args:
            subtitle_url: YouTube subtitle URL from yt-dlp.
            output_path: Path to save the subtitle file.
            output_format: Output format (default: srt).

        Returns:
            True if subtitle was fetched successfully, False otherwise.
        """
        if not self.is_available:
            logger.warning("TikHub API key not configured, skipping subtitle fetch")
            return False

        try:
            # Build API request URL
            params = {
                "subtitle_url": subtitle_url,
                "format": output_format,
                "fix_overlap": "true",
            }

            api_url = f"{TIKHUB_SUBTITLE_API}?{urlencode(params)}"

            logger.debug(f"Fetching subtitle from TikHub API: {api_url[:100]}...")

            # Make API request
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(
                    api_url,
                    headers={
                        "accept": "application/json",
                        "Authorization": f"Bearer {self.api_key}",
                    },
                )

                if response.status_code != 200:
                    logger.error(
                        f"TikHub API returned status {response.status_code}: "
                        f"{response.text[:200]}"
                    )
                    return False

                data = response.json()

            # Check API response
            if data.get("code") != 200:
                logger.error(f"TikHub API error: {data.get('message', 'Unknown error')}")
                return False

            # Extract subtitle content
            subtitle_content = data.get("data")
            if not subtitle_content:
                logger.warning("TikHub API returned empty subtitle data")
                return False

            # Save subtitle file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(subtitle_content, encoding="utf-8")

            logger.info(f"Subtitle saved to: {output_path}")
            return True

        except httpx.TimeoutException:
            logger.warning("TikHub API request timed out")
            return False
        except httpx.RequestError as e:
            logger.warning(f"TikHub API request failed: {e}")
            return False
        except Exception as e:
            logger.warning(f"Failed to fetch subtitle via TikHub: {e}")
            return False

    async def fetch_best_subtitle(
        self,
        info: dict[str, Any],
        output_dir: Path,
        video_id: str,
    ) -> Optional[Path]:
        """
        Fetch the best available subtitle for a video.

        Tries subtitles in priority order until one succeeds.

        Args:
            info: yt-dlp extracted video information.
            output_dir: Directory to save subtitle file.
            video_id: YouTube video ID for filename.

        Returns:
            Path to saved subtitle file, or None if all attempts failed.
        """
        if not self.is_available:
            logger.warning("TikHub API key not configured, skipping subtitle fetch")
            return None

        # Extract available subtitle URLs
        subtitle_infos = self.extract_subtitle_urls(info)

        if not subtitle_infos:
            logger.info(f"No subtitles available for video {video_id}")
            return None

        logger.info(
            f"Found {len(subtitle_infos)} subtitle(s) for video {video_id}: "
            f"{[s.lang for s in subtitle_infos]}"
        )

        # Try each subtitle in priority order
        for subtitle_info in subtitle_infos:
            output_path = output_dir / f"{video_id}.{subtitle_info.lang}.srt"

            logger.debug(
                f"Trying to fetch subtitle: {subtitle_info.lang} "
                f"(auto={subtitle_info.is_auto})"
            )

            success = await self.fetch_subtitle(
                subtitle_url=subtitle_info.url,
                output_path=output_path,
            )

            if success:
                return output_path

            # Small delay between attempts
            await asyncio.sleep(0.5)

        logger.warning(f"Failed to fetch any subtitle for video {video_id}")
        return None
