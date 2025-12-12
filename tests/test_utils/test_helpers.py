"""
Tests for utility helper functions.
"""

import pytest

from src.utils.helpers import (
    extract_video_id,
    format_duration,
    format_file_size,
    is_valid_youtube_url,
    sanitize_filename,
)


class TestExtractVideoId:
    """Test video ID extraction from URLs."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            # Standard format
            ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("http://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("https://youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            # Short format
            ("https://youtu.be/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("http://youtu.be/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            # Embed format
            ("https://www.youtube.com/embed/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            # Shorts format
            ("https://www.youtube.com/shorts/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            # With additional parameters
            (
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=10",
                "dQw4w9WgXcQ",
            ),
            (
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PLtest",
                "dQw4w9WgXcQ",
            ),
            # Mobile format
            ("https://m.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
        ],
    )
    def test_valid_urls(self, url: str, expected: str) -> None:
        """Test extraction from valid YouTube URLs."""
        assert extract_video_id(url) == expected

    @pytest.mark.parametrize(
        "url",
        [
            "",
            "https://google.com",
            "https://youtube.com/",
            "https://youtube.com/watch",
            "not a url",
            "https://www.youtube.com/watch?v=invalid",
        ],
    )
    def test_invalid_urls(self, url: str) -> None:
        """Test extraction from invalid URLs."""
        assert extract_video_id(url) is None


class TestFormatDuration:
    """Test duration formatting."""

    @pytest.mark.parametrize(
        "seconds,expected",
        [
            (0, "0:00"),
            (5, "0:05"),
            (65, "1:05"),
            (3600, "1:00:00"),
            (3665, "1:01:05"),
            (None, "0:00"),
            (-1, "0:00"),
        ],
    )
    def test_format_duration(self, seconds: int | None, expected: str) -> None:
        """Test duration formatting."""
        assert format_duration(seconds) == expected


class TestFormatFileSize:
    """Test file size formatting."""

    @pytest.mark.parametrize(
        "size,expected",
        [
            (0, "0 B"),
            (100, "100 B"),
            (1024, "1 KB"),
            (1536, "1.5 KB"),
            (1048576, "1 MB"),
            (1073741824, "1 GB"),
            (None, "0 B"),
            (-1, "0 B"),
        ],
    )
    def test_format_file_size(self, size: int | None, expected: str) -> None:
        """Test file size formatting."""
        assert format_file_size(size) == expected


class TestIsValidYoutubeUrl:
    """Test YouTube URL validation."""

    def test_valid_urls(self) -> None:
        """Test valid YouTube URLs."""
        assert is_valid_youtube_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
        assert is_valid_youtube_url("https://youtu.be/dQw4w9WgXcQ")

    def test_invalid_urls(self) -> None:
        """Test invalid URLs."""
        assert not is_valid_youtube_url("")
        assert not is_valid_youtube_url("https://google.com")


class TestSanitizeFilename:
    """Test filename sanitization."""

    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("normal.txt", "normal.txt"),
            ("file with spaces.txt", "file with spaces.txt"),
            ("file<>:.txt", "file___.txt"),
            ("file/with\\slashes.txt", "file_with_slashes.txt"),
            (".hidden", "hidden"),
            ("  spaces  ", "spaces"),
            ("", "unnamed"),
            ("a" * 300, "a" * 200),
        ],
    )
    def test_sanitize_filename(self, filename: str, expected: str) -> None:
        """Test filename sanitization."""
        assert sanitize_filename(filename) == expected
