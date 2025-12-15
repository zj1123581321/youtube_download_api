"""
Helper utilities and common functions.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qs, urlparse


def extract_video_id(url: str) -> Optional[str]:
    """
    Extract YouTube video ID from various URL formats.

    Supports:
        - https://www.youtube.com/watch?v=VIDEO_ID
        - https://youtu.be/VIDEO_ID
        - https://www.youtube.com/embed/VIDEO_ID
        - https://www.youtube.com/v/VIDEO_ID
        - https://www.youtube.com/shorts/VIDEO_ID

    Args:
        url: YouTube video URL.

    Returns:
        Video ID string or None if not found.
    """
    if not url:
        return None

    # Standard youtube.com/watch?v= format
    parsed = urlparse(url)
    if parsed.hostname in ("www.youtube.com", "youtube.com", "m.youtube.com"):
        if parsed.path == "/watch":
            query = parse_qs(parsed.query)
            if "v" in query:
                return query["v"][0]
        elif parsed.path.startswith("/embed/"):
            return parsed.path.split("/")[2]
        elif parsed.path.startswith("/v/"):
            return parsed.path.split("/")[2]
        elif parsed.path.startswith("/shorts/"):
            return parsed.path.split("/")[2]

    # Short youtu.be format
    if parsed.hostname == "youtu.be":
        return parsed.path[1:]  # Remove leading /

    # Try regex as fallback
    patterns = [
        r"(?:v=|/)([a-zA-Z0-9_-]{11})(?:[&?/]|$)",
        r"^([a-zA-Z0-9_-]{11})$",
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            video_id = match.group(1)
            # Validate video ID format (11 characters, alphanumeric with - and _)
            if re.match(r"^[a-zA-Z0-9_-]{11}$", video_id):
                return video_id

    return None


def format_duration(seconds: Optional[int]) -> str:
    """
    Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string like "1:23:45" or "12:34".
    """
    if seconds is None or seconds < 0:
        return "0:00"

    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def format_timedelta(td: Optional[timedelta]) -> str:
    """
    Format timedelta to human-readable string.

    Args:
        td: Timedelta object.

    Returns:
        Formatted string like "1小时23分钟" or "45秒".
    """
    if td is None:
        return "N/A"

    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return "0秒"

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}小时")
    if minutes > 0:
        parts.append(f"{minutes}分钟")
    if seconds > 0 or not parts:
        parts.append(f"{seconds}秒")

    return "".join(parts)


def format_file_size(size_bytes: Optional[int]) -> str:
    """
    Format file size to human-readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Formatted string like "1.5 MB" or "256 KB".
    """
    if size_bytes is None or size_bytes < 0:
        return "0 B"

    size: float = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}" if size != int(size) else f"{int(size)} {unit}"
        size /= 1024

    return f"{size:.1f} PB"


def get_utc_now() -> datetime:
    """
    Get current UTC datetime with timezone info.

    Returns:
        Current UTC datetime with tzinfo.
    """
    return datetime.now(timezone.utc)


def get_expiry_time(days: int) -> datetime:
    """
    Calculate expiry time from now.

    Args:
        days: Number of days until expiry.

    Returns:
        Expiry datetime with timezone info.
    """
    return get_utc_now() + timedelta(days=days)


def is_valid_youtube_url(url: str) -> bool:
    """
    Check if URL is a valid YouTube video URL.

    Args:
        url: URL to validate.

    Returns:
        True if valid YouTube URL, False otherwise.
    """
    return extract_video_id(url) is not None


def sanitize_filename(filename: str, max_bytes: int = 200) -> str:
    """
    Sanitize filename by removing invalid characters.

    Truncates by **byte count** (not character count) to comply with
    filesystem limits (e.g., ext4 allows max 255 bytes for filename).

    Args:
        filename: Original filename.
        max_bytes: Maximum filename length in bytes (UTF-8 encoded).

    Returns:
        Sanitized filename.
    """
    # Remove invalid characters for Windows/Unix
    invalid_chars = r'[<>:"/\\|?*\x00-\x1f]'
    sanitized = re.sub(invalid_chars, "_", filename)

    # Remove leading/trailing dots and spaces
    sanitized = sanitized.strip(". ")

    # Truncate by bytes (UTF-8), not by character count
    # This is important for CJK characters which use 3 bytes each
    if len(sanitized.encode("utf-8")) > max_bytes:
        sanitized = _truncate_to_bytes(sanitized, max_bytes)

    # Ensure not empty
    if not sanitized:
        sanitized = "unnamed"

    return sanitized


def _truncate_to_bytes(text: str, max_bytes: int) -> str:
    """
    Truncate string to fit within max_bytes when UTF-8 encoded.

    Ensures we don't cut in the middle of a multi-byte character.

    Args:
        text: String to truncate.
        max_bytes: Maximum byte count.

    Returns:
        Truncated string.
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text

    # Truncate bytes and decode, ignoring incomplete characters
    truncated = encoded[:max_bytes]
    # Find the last valid UTF-8 boundary
    while truncated:
        try:
            return truncated.decode("utf-8")
        except UnicodeDecodeError:
            # Remove last byte and try again
            truncated = truncated[:-1]

    return ""
