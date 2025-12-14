"""
FastAPI application entry point.

Initializes the application, services, and background workers.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src import __version__
from src.api.routes import router as api_router
from src.api.routes import set_services
from src.api.schemas import ComponentStatus, HealthResponse, QueueStatus
from src.config import Settings, get_settings
from src.core.worker import DownloadWorker
from src.db.database import Database
from src.services.callback_service import CallbackService
from src.services.file_service import FileService
from src.services.notify import NotificationService
from src.services.task_service import TaskService
from src.utils.logger import logger, setup_logger


# Global instances
db: Database | None = None
task_service: TaskService | None = None
file_service: FileService | None = None
callback_service: CallbackService | None = None
notify_service: NotificationService | None = None
download_worker: DownloadWorker | None = None
worker_task: asyncio.Task | None = None
scheduler: AsyncIOScheduler | None = None
startup_time: float = 0


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.

    Handles startup and shutdown of services, database, and background workers.
    """
    global db, task_service, file_service, callback_service, notify_service
    global download_worker, worker_task, scheduler, startup_time

    settings = get_settings()

    # Setup logging
    log_dir = settings.data_dir / "logs" if not settings.debug else None
    setup_logger(log_dir=log_dir, debug=settings.debug)

    logger.info(f"Starting YouTube Audio API v{__version__}")

    # Ensure directories exist
    settings.ensure_directories()

    # Initialize database with error handling
    # 数据库是关键组件，连接失败应该阻止应用启动
    db = Database(settings.db_path)
    try:
        await db.connect()
        logger.info("Database connected successfully")
    except Exception as e:
        logger.critical(f"Failed to connect to database: {e}")
        raise RuntimeError(f"Database initialization failed: {e}") from e

    # Reset any interrupted downloads with error handling
    try:
        await db.reset_downloading_tasks()
    except Exception as e:
        logger.error(f"Failed to reset downloading tasks: {e}")
        # 非关键操作，继续启动但记录错误

    # Initialize services with error handling
    try:
        file_service = FileService(db, settings)
        task_service = TaskService(db, settings, file_service)
        callback_service = CallbackService(db, file_service)
        notify_service = NotificationService(settings, db)
    except Exception as e:
        logger.critical(f"Failed to initialize services: {e}")
        await db.disconnect()
        raise RuntimeError(f"Service initialization failed: {e}") from e

    # Set services for API routes
    set_services(task_service, file_service)

    # Initialize download worker
    download_worker = DownloadWorker(
        db=db,
        settings=settings,
        task_service=task_service,
        file_service=file_service,
        callback_service=callback_service,
        notify_service=notify_service,
    )

    # Restore pending tasks to queue
    await task_service.restore_pending_tasks()

    # Start background worker
    # TODO: 未来支持多 Worker 并发时，根据 settings.download_concurrency 启动多个 worker
    # 示例实现：
    # worker_tasks = [
    #     asyncio.create_task(DownloadWorker(...).start())
    #     for _ in range(settings.download_concurrency)
    # ]
    # 注意：多 worker 需要共享同一个 task_queue，并考虑风控风险
    worker_task = asyncio.create_task(download_worker.start())

    # Setup scheduler for periodic tasks
    scheduler = AsyncIOScheduler(timezone=settings.tz)

    # File cleanup: daily at 3 AM
    scheduler.add_job(
        file_service.cleanup_expired_files,
        "cron",
        hour=3,
        minute=0,
        id="cleanup_files",
    )

    # Health check: every 5 minutes
    scheduler.add_job(
        _check_health,
        "interval",
        minutes=5,
        id="health_check",
    )

    scheduler.start()

    # Record startup time
    startup_time = time.time()

    # Send startup notification
    await notify_service.notify_startup(__version__)

    logger.info("Application started successfully")

    yield

    # Shutdown
    logger.info("Shutting down...")

    # 关闭超时设置（秒）
    # 给下载任务足够时间响应取消信号，但不要无限等待
    SHUTDOWN_TIMEOUT = 5.0

    # Stop scheduler
    if scheduler:
        scheduler.shutdown()

    # Stop worker - 这会触发 downloader.cancel()
    if download_worker:
        await download_worker.stop()

    # 等待 worker task 结束，但设置超时
    # 注意：worker.stop() 已经设置了取消标志，worker 会自行停止
    # 这里只是等待它完成，不需要再次 cancel
    if worker_task:
        try:
            # 等待 worker 任务完成，设置超时避免无限等待
            await asyncio.wait_for(worker_task, timeout=SHUTDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(
                f"Worker task did not finish within {SHUTDOWN_TIMEOUT}s, "
                "forcing cancellation"
            )
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        except asyncio.CancelledError:
            pass

    # Close database
    if db:
        await db.disconnect()

    logger.info("Application shutdown complete")


async def _check_health() -> None:
    """Periodic health check task."""
    global file_service, notify_service

    if not file_service or not notify_service:
        return

    # Check disk space
    if not file_service.check_disk_space(required_mb=500):
        usage = file_service.get_disk_usage()
        free_mb = usage["free_space"] // (1024 * 1024)
        await notify_service.notify_disk_space_warning(free_mb)


# Create FastAPI application
app = FastAPI(
    title="YouTube Audio API",
    description="API service for downloading YouTube audio and transcripts",
    version=__version__,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router)


# ==================== Health Check Endpoint ====================


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["Health"],
    summary="Health check",
    description="Check the health status of the service and its components.",
)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Returns service status, component health, and queue statistics.
    """
    global db, startup_time

    settings = get_settings()
    components = ComponentStatus()
    queue = QueueStatus()

    # Check database
    try:
        if db:
            stats = await db.get_queue_stats()
            queue.pending = stats["pending"]
            queue.downloading = stats["downloading"]
    except Exception as e:
        components.database = f"error: {e}"
        logger.error(f"Database health check failed: {e}")

    # Check PO Token provider
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{settings.pot_server_url}/health")
            if response.status_code != 200:
                components.pot_provider = f"unhealthy ({response.status_code})"
    except Exception as e:
        components.pot_provider = f"unreachable: {e}"
        logger.warning(f"PO Token provider health check failed: {e}")

    # Check disk space
    if file_service:
        if not file_service.check_disk_space(required_mb=100):
            components.disk_space = "low"

    # Calculate uptime
    uptime = int(time.time() - startup_time) if startup_time else 0

    # Determine overall status
    status = "healthy"
    if (
        "error" in components.database
        or "unreachable" in components.pot_provider
        or components.disk_space == "low"
    ):
        status = "degraded"

    return HealthResponse(
        status=status,
        version=__version__,
        components=components,
        queue=queue,
        uptime=uptime,
    )


# ==================== Root Endpoint ====================


@app.get("/", include_in_schema=False)
async def root() -> dict:
    """Redirect to docs."""
    return {
        "service": "YouTube Audio API",
        "version": __version__,
        "docs": "/docs",
    }


# ==================== CLI Entry Point ====================


def main() -> None:
    """Run the application using uvicorn."""
    import uvicorn

    settings = get_settings()

    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="debug" if settings.debug else "info",
    )


if __name__ == "__main__":
    main()
