"""
Logging configuration module.

Uses loguru for structured logging with file rotation and console output.
"""

import sys
from pathlib import Path
from typing import Any

from loguru import logger


def setup_logger(
    log_dir: Path | None = None,
    debug: bool = False,
    json_logs: bool = False,
) -> None:
    """
    Configure the application logger.

    Args:
        log_dir: Directory for log files. If None, logs only to console.
        debug: Enable debug level logging.
        json_logs: Output logs in JSON format (for production).
    """
    # Remove default handler
    logger.remove()

    # Log format
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    # Console handler
    logger.add(
        sys.stdout,
        format=log_format,
        level="DEBUG" if debug else "INFO",
        colorize=True,
        backtrace=True,
        diagnose=debug,
    )

    # File handler (if log_dir provided)
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        # General log file
        logger.add(
            log_dir / "app_{time:YYYY-MM-DD}.log",
            rotation="00:00",  # Rotate at midnight
            retention="30 days",
            compression="gz",
            format=log_format if not json_logs else None,
            serialize=json_logs,
            level="DEBUG" if debug else "INFO",
            backtrace=True,
            diagnose=debug,
            encoding="utf-8",
        )

        # Error log file
        logger.add(
            log_dir / "error_{time:YYYY-MM-DD}.log",
            rotation="00:00",
            retention="60 days",
            compression="gz",
            format=log_format if not json_logs else None,
            serialize=json_logs,
            level="ERROR",
            backtrace=True,
            diagnose=True,
            encoding="utf-8",
        )

    logger.info(f"Logger initialized (debug={debug})")


def get_logger(name: str) -> Any:
    """
    Get a logger instance with a specific name.

    Args:
        name: Logger name (usually __name__).

    Returns:
        Logger instance bound with the given name.
    """
    return logger.bind(name=name)


# Export the main logger
__all__ = ["logger", "setup_logger", "get_logger"]
