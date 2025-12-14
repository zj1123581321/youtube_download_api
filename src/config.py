"""
Configuration management module.

Uses pydantic-settings to load and validate configuration from environment variables.
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ============ Required Configuration ============
    api_key: str = Field(..., description="API key for authentication")
    wecom_webhook_url: str = Field(
        default="", description="WeCom webhook URL for notifications"
    )

    # ============ Service Configuration ============
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, description="Server port")
    debug: bool = Field(default=False, description="Debug mode")
    base_url: str = Field(
        default="http://localhost:8000",
        description="Base URL for file download links (e.g., https://your-domain.com)",
    )

    # ============ PO Token Service ============
    pot_server_url: str = Field(
        default="http://pot-provider:4416",
        description="PO Token provider server URL",
    )

    # ============ Proxy Configuration ============
    http_proxy: Optional[str] = Field(default=None, description="HTTP proxy URL")
    https_proxy: Optional[str] = Field(default=None, description="HTTPS proxy URL")

    # ============ Download Configuration ============
    # TODO: 多 Worker 并发下载功能预留
    # 当前系统只启动单个 DownloadWorker，此配置暂未生效
    # 未来实现时需要在 main.py 中根据此值启动多个 worker
    # 注意：增加并发可能会提高 YouTube 风控风险
    download_concurrency: int = Field(
        default=1, ge=1, le=5, description="Number of concurrent downloads (reserved, not yet implemented)"
    )
    task_interval_min: int = Field(
        default=300, ge=5, description="Minimum interval between tasks (seconds)"
    )
    task_interval_max: int = Field(
        default=1800, ge=10, description="Maximum interval between tasks (seconds)"
    )
    audio_quality: int = Field(
        default=128, ge=64, le=320, description="Audio bitrate (kbps)"
    )

    # ============ Storage Configuration ============
    data_dir: Path = Field(default=Path("./data"), description="Data storage directory")
    file_retention_days: int = Field(
        default=60, ge=1, description="File retention period (days)"
    )

    # ============ Timezone ============
    tz: str = Field(default="Asia/Shanghai", description="Timezone")

    # ============ TikHub API Configuration ============
    tikhub_api_key: Optional[str] = Field(
        default=None, description="TikHub API key for subtitle fetching"
    )

    # ============ Optional Configuration ============
    cookie_file: Optional[str] = Field(
        default=None, description="Path to cookie file for age-restricted videos"
    )
    dry_run: bool = Field(
        default=False, description="Dry run mode (skip actual downloads)"
    )

    @field_validator("data_dir", mode="before")
    @classmethod
    def validate_data_dir(cls, v: str | Path) -> Path:
        """Ensure data_dir is a Path object."""
        return Path(v) if isinstance(v, str) else v

    @field_validator("task_interval_max")
    @classmethod
    def validate_interval_max(cls, v: int, info) -> int:
        """Ensure task_interval_max >= task_interval_min."""
        min_val = info.data.get("task_interval_min", 30)
        if v < min_val:
            raise ValueError(
                f"task_interval_max ({v}) must be >= task_interval_min ({min_val})"
            )
        return v

    @property
    def audio_dir(self) -> Path:
        """Directory for audio files."""
        return self.data_dir / "files" / "audio"

    @property
    def transcript_dir(self) -> Path:
        """Directory for transcript files."""
        return self.data_dir / "files" / "transcript"

    @property
    def db_path(self) -> Path:
        """Path to SQLite database file."""
        return self.data_dir / "db.sqlite"

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        self.transcript_dir.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses lru_cache to ensure settings are only loaded once.

    Returns:
        Settings: Application settings instance.
    """
    return Settings()
