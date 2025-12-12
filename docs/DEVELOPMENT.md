# YouTube Audio API - 开发文档

## 项目概述

Docker 部署的 YouTube 音频下载服务，提供 RESTful API 接口，支持下载 YouTube 视频的音频和字幕。

### 核心特性

- 对外暴露 API 接口（API Key 鉴权）
- 下载音频（m4a, 128kbps）+ 字幕（JSON 格式，优先中英文）
- 绕过 YouTube 风控（TLS 指纹 + PO Token）
- 任务队列管理、频率控制、错误重试
- Webhook 回调 + 轮询双模式
- 企业微信通知
- 文件 60 天自动清理

---

## 技术栈

| 层级 | 技术选型 | 版本要求 | 说明 |
|------|---------|---------|------|
| Web 框架 | FastAPI | ≥0.104 | 异步，自动 OpenAPI 文档 |
| ASGI 服务器 | uvicorn | ≥0.24 | 生产级 ASGI 服务器 |
| 下载核心 | yt-dlp | ≥2025.05.22 | 支持 PO Token 插件框架 |
| TLS 指纹 | curl_cffi | ≥0.6 | Chrome/Edge/Safari 指纹 |
| PO Token | bgutil-ytdlp-pot-provider | latest | Docker 容器部署 |
| 数据库 | SQLite + aiosqlite | ≥0.19 | 异步，单文件 |
| 配置管理 | pydantic-settings | ≥2.0 | 类型安全的配置 |
| 定时任务 | APScheduler | ≥3.10 | 文件清理、健康检查 |
| 通知 | wecom-notifier | ≥0.2 | 企业微信 Webhook |
| 日志 | loguru | ≥0.7 | 结构化日志 |
| HTTP 客户端 | httpx | ≥0.25 | 异步 HTTP（Webhook 回调） |

---

## 项目结构

```
youtube-audio-api/
├── docker-compose.yml          # 生产部署
├── docker-compose.dev.yml      # 开发环境（仅 pot-provider）
├── Dockerfile
├── requirements.txt
├── .env.example                # 环境变量模板
├── .gitignore
├── README.md
├── docs/
│   ├── DEVELOPMENT.md          # 本文档
│   ├── API.md                  # API 文档补充说明
│   └── ...
├── scripts/
│   ├── dev.ps1                 # Windows 开发启动
│   └── dev.sh                  # Linux/Mac 开发启动
├── src/
│   ├── __init__.py
│   ├── main.py                 # FastAPI 应用入口
│   ├── config.py               # 配置管理
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py           # 路由定义
│   │   ├── deps.py             # 依赖注入（鉴权）
│   │   └── schemas.py          # Pydantic 模型
│   ├── core/
│   │   ├── __init__.py
│   │   ├── downloader.py       # yt-dlp 封装
│   │   └── worker.py           # 下载任务 Worker
│   ├── db/
│   │   ├── __init__.py
│   │   ├── database.py         # SQLite 连接管理
│   │   └── models.py           # 数据模型
│   ├── services/
│   │   ├── __init__.py
│   │   ├── task_service.py     # 任务业务逻辑
│   │   ├── file_service.py     # 文件管理
│   │   ├── callback_service.py # Webhook 回调
│   │   └── notify.py           # 企微通知
│   └── utils/
│       ├── __init__.py
│       ├── logger.py           # 日志配置
│       └── helpers.py          # 工具函数
├── data/                       # 运行时数据（git ignored）
│   ├── db.sqlite
│   └── files/
│       ├── audio/
│       └── transcript/
├── cookies/                    # Cookie 文件（git ignored）
└── tests/
    ├── conftest.py
    ├── test_api/
    ├── test_core/
    └── test_services/
```

---

## 环境配置

### 配置文件

```bash
# .env.example - 复制为 .env.development 或 .env.production

# ============ 必填配置 ============
API_KEY=your-secure-api-key-here
WECOM_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx

# ============ 服务配置 ============
HOST=0.0.0.0
PORT=8000
DEBUG=false

# ============ PO Token 服务 ============
# 开发环境: http://localhost:4416
# 生产环境: http://pot-provider:4416
POT_SERVER_URL=http://pot-provider:4416

# ============ 代理配置 ============
# 开发环境需要配置，生产环境透明代理留空
HTTP_PROXY=
HTTPS_PROXY=

# ============ 下载配置 ============
DOWNLOAD_CONCURRENCY=1
# 任务间隔（秒），实际值在 MIN-MAX 之间随机
TASK_INTERVAL_MIN=30
TASK_INTERVAL_MAX=120
# 音频质量
AUDIO_QUALITY=128

# ============ 存储配置 ============
DATA_DIR=./data
FILE_RETENTION_DAYS=60

# ============ 时区 ============
TZ=Asia/Shanghai

# ============ 可选：Cookie 文件路径 ============
COOKIE_FILE=
```

### 开发环境配置示例

```bash
# .env.development
DEBUG=true
API_KEY=dev-test-key-12345
POT_SERVER_URL=http://localhost:4416
HTTP_PROXY=http://127.0.0.1:7890
HTTPS_PROXY=http://127.0.0.1:7890
WECOM_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dev

TASK_INTERVAL_MIN=5
TASK_INTERVAL_MAX=10
FILE_RETENTION_DAYS=1
```

---

## API 设计

### 接口概览

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/v1/tasks` | 创建下载任务 | 需要 |
| GET | `/api/v1/tasks` | 列出任务 | 需要 |
| GET | `/api/v1/tasks/{task_id}` | 查询任务详情 | 需要 |
| DELETE | `/api/v1/tasks/{task_id}` | 取消任务 | 需要 |
| GET | `/api/v1/files/{file_id}` | 下载文件 | 公开 |
| GET | `/health` | 健康检查 | 公开 |
| GET | `/docs` | Swagger UI | 公开 |

### 鉴权方式

```
Header: X-API-Key: your-api-key
```

### 创建任务

**请求**
```http
POST /api/v1/tasks
Content-Type: application/json
X-API-Key: your-api-key

{
    "video_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "callback_url": "https://your-server.com/webhook/youtube",
    "callback_secret": "your-hmac-secret"
}
```

**字段说明**
| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| video_url | string | 是 | YouTube 视频 URL |
| callback_url | string | 否 | 下载完成后的回调地址 |
| callback_secret | string | 否 | 回调签名密钥（HMAC-SHA256） |

**响应 - 新任务创建**
```json
{
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "pending",
    "video_id": "dQw4w9WgXcQ",
    "position": 3,
    "estimated_wait": 180,
    "created_at": "2025-12-12T10:00:00+08:00"
}
```

**响应 - 返回已有任务（去重）**
```json
{
    "task_id": "existing-task-uuid",
    "status": "completed",
    "video_id": "dQw4w9WgXcQ",
    "video_info": { ... },
    "files": { ... },
    "message": "Task already exists"
}
```

### 查询任务

**请求**
```http
GET /api/v1/tasks/{task_id}
X-API-Key: your-api-key
```

**响应 - 进行中**
```json
{
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "downloading",
    "video_id": "dQw4w9WgXcQ",
    "progress": 45,
    "created_at": "2025-12-12T10:00:00+08:00",
    "started_at": "2025-12-12T10:02:30+08:00"
}
```

**响应 - 已完成**
```json
{
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "completed",
    "video_id": "dQw4w9WgXcQ",
    "video_info": {
        "title": "Rick Astley - Never Gonna Give You Up",
        "author": "Rick Astley",
        "channel_id": "UCuAXFkgsw1L7xaCfnd5JJOw",
        "duration": 213,
        "description": "...",
        "upload_date": "20091025",
        "view_count": 1500000000,
        "thumbnail": "https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg"
    },
    "files": {
        "audio": {
            "url": "/api/v1/files/abc123.m4a",
            "size": 3456789,
            "format": "m4a",
            "bitrate": 128
        },
        "transcript": {
            "url": "/api/v1/files/abc123.json",
            "size": 12345,
            "language": "en"
        }
    },
    "expires_at": "2025-02-10T10:00:00+08:00",
    "created_at": "2025-12-12T10:00:00+08:00",
    "completed_at": "2025-12-12T10:03:45+08:00"
}
```

**注意**：`transcript` 字段可能为 `null`（视频没有字幕时），这不影响任务成功状态。

**响应 - 失败**
```json
{
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "failed",
    "video_id": "dQw4w9WgXcQ",
    "error": {
        "code": "VIDEO_UNAVAILABLE",
        "message": "Video is private or deleted",
        "retry_count": 3
    },
    "created_at": "2025-12-12T10:00:00+08:00",
    "failed_at": "2025-12-12T10:05:00+08:00"
}
```

### 列出任务

**请求**
```http
GET /api/v1/tasks?status=pending&limit=20&offset=0
X-API-Key: your-api-key
```

**查询参数**
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| status | string | 全部 | 筛选状态：pending/downloading/completed/failed |
| limit | int | 20 | 每页数量（最大 100） |
| offset | int | 0 | 偏移量 |

**响应**
```json
{
    "tasks": [ ... ],
    "total": 150,
    "limit": 20,
    "offset": 0
}
```

### 取消任务

**请求**
```http
DELETE /api/v1/tasks/{task_id}
X-API-Key: your-api-key
```

**响应**
```json
{
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "cancelled",
    "message": "Task cancelled successfully"
}
```

**注意**：只能取消 `pending` 状态的任务，已开始下载的任务无法取消。

### Webhook 回调

下载完成/失败后，如果任务指定了 `callback_url`，系统会主动 POST 通知。

**请求**
```http
POST {callback_url}
Content-Type: application/json
X-Signature: sha256=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
X-Task-Id: 550e8400-e29b-41d4-a716-446655440000
X-Timestamp: 1702357425

{
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "completed",
    "video_id": "dQw4w9WgXcQ",
    "video_info": { ... },
    "files": {
        "audio": {
            "url": "https://your-server.com/api/v1/files/abc123.m4a",
            "size": 3456789
        },
        "transcript": {
            "url": "https://your-server.com/api/v1/files/abc123.json",
            "size": 12345
        }
    },
    "expires_at": "2025-02-10T10:00:00+08:00"
}
```

**注意**：`transcript` 字段可能为 `null`（视频没有字幕时）。

**签名验证**（客户端实现）
```python
import hmac
import hashlib

def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    expected = hmac.new(
        secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)
```

**回调重试策略**
- 超时时间：10 秒
- 重试次数：3 次
- 重试间隔：5s, 10s, 20s
- 成功条件：HTTP 2xx 响应

### 健康检查

**请求**
```http
GET /health
```

**响应**
```json
{
    "status": "healthy",
    "version": "1.0.0",
    "components": {
        "database": "ok",
        "pot_provider": "ok",
        "disk_space": "ok"
    },
    "queue": {
        "pending": 5,
        "downloading": 1
    },
    "uptime": 86400
}
```

---

## 数据模型

### Task 表结构

```sql
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,                    -- UUID
    video_id TEXT NOT NULL,                 -- YouTube video ID
    video_url TEXT NOT NULL,                -- 原始 URL
    status TEXT NOT NULL DEFAULT 'pending', -- 任务状态

    -- 视频信息（下载后填充）
    video_info TEXT,                        -- JSON: title, author, duration 等

    -- 文件信息
    audio_file_id TEXT,                     -- 音频文件 ID
    transcript_file_id TEXT,                -- 字幕文件 ID

    -- 回调配置
    callback_url TEXT,
    callback_secret TEXT,
    callback_status TEXT,                   -- pending/success/failed
    callback_attempts INTEGER DEFAULT 0,

    -- 错误信息
    error_code TEXT,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,

    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    expires_at TIMESTAMP,

    -- 索引（不使用唯一约束，因为同一视频可能有多条失败记录）
);

CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_video_id ON tasks(video_id);
CREATE INDEX idx_tasks_created_at ON tasks(created_at);
CREATE INDEX idx_tasks_expires_at ON tasks(expires_at);
```

### File 表结构

```sql
CREATE TABLE files (
    id TEXT PRIMARY KEY,                    -- UUID，用于 URL
    task_id TEXT NOT NULL,
    type TEXT NOT NULL,                     -- audio / transcript
    filename TEXT NOT NULL,                 -- 实际文件名
    filepath TEXT NOT NULL,                 -- 相对路径
    size INTEGER,                           -- 文件大小（字节）
    format TEXT,                            -- m4a / json
    metadata TEXT,                          -- JSON: bitrate, language 等

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed_at TIMESTAMP,             -- 用于清理策略
    expires_at TIMESTAMP,

    FOREIGN KEY (task_id) REFERENCES tasks(id)
);

CREATE INDEX idx_files_task_id ON files(task_id);
CREATE INDEX idx_files_expires_at ON files(expires_at);
CREATE INDEX idx_files_last_accessed ON files(last_accessed_at);
```

### 任务状态枚举

```python
class TaskStatus(str, Enum):
    PENDING = "pending"           # 等待下载
    DOWNLOADING = "downloading"   # 下载中
    COMPLETED = "completed"       # 已完成
    FAILED = "failed"             # 失败（已重试完）
    CANCELLED = "cancelled"       # 已取消
```

### 错误码枚举

```python
class ErrorCode(str, Enum):
    # 视频问题
    VIDEO_UNAVAILABLE = "VIDEO_UNAVAILABLE"       # 视频不存在/已删除
    VIDEO_PRIVATE = "VIDEO_PRIVATE"               # 私有视频
    VIDEO_REGION_BLOCKED = "VIDEO_REGION_BLOCKED" # 地区限制
    VIDEO_AGE_RESTRICTED = "VIDEO_AGE_RESTRICTED" # 年龄限制
    VIDEO_LIVE_STREAM = "VIDEO_LIVE_STREAM"       # 直播流，不支持

    # 下载问题
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"           # 下载失败（通用）
    RATE_LIMITED = "RATE_LIMITED"                 # 被限流
    NETWORK_ERROR = "NETWORK_ERROR"               # 网络错误

    # 系统问题
    POT_TOKEN_FAILED = "POT_TOKEN_FAILED"         # PO Token 获取失败
    INTERNAL_ERROR = "INTERNAL_ERROR"             # 内部错误
```

---

## 核心流程

### 任务状态机

```
                    ┌──────────────┐
                    │   pending    │
                    └──────┬───────┘
                           │ Worker 取出
                           ▼
                    ┌──────────────┐
          ┌────────│ downloading  │────────┐
          │        └──────────────┘        │
          │ 失败                            │ 成功
          ▼                                ▼
   ┌─────────────┐                  ┌─────────────┐
   │   重试?     │                  │  completed  │
   └──────┬──────┘                  └─────────────┘
          │
    ┌─────┴─────┐
    │ < 3次     │ ≥ 3次
    ▼           ▼
┌────────┐  ┌────────┐
│pending │  │ failed │
│(重新入队)│  │        │
└────────┘  └────────┘
```

### 下载 Worker 流程

```python
async def worker_loop():
    while True:
        # 1. 从队列获取任务
        task = await task_queue.get()

        # 2. 更新状态为 downloading
        await update_task_status(task.id, "downloading")

        # 3. 执行下载
        try:
            result = await download_video(task)

            # 4. 保存文件信息
            await save_files(task.id, result.files)

            # 5. 更新任务完成
            await update_task_completed(task.id, result.video_info)

            # 6. 发送通知
            await notify_completed(task)

            # 7. 触发回调
            if task.callback_url:
                await send_callback(task)

        except RetryableError as e:
            # 可重试错误
            if task.retry_count < 3:
                await schedule_retry(task, e)
            else:
                await update_task_failed(task.id, e)
                await notify_failed(task, e)

        except NonRetryableError as e:
            # 不可重试错误
            await update_task_failed(task.id, e)
            await notify_failed(task, e)

        finally:
            # 8. 随机等待后处理下一个
            wait_time = random.uniform(
                settings.task_interval_min,
                settings.task_interval_max
            )
            await asyncio.sleep(wait_time)
```

### 错误重试策略

```python
RETRY_CONFIG = {
    # 可重试错误
    ErrorCode.NETWORK_ERROR: {
        "max_retries": 3,
        "backoff": [120, 240, 480],  # 指数退避（秒）
        "jitter": 30,                 # 随机抖动范围（秒）
    },
    ErrorCode.RATE_LIMITED: {
        "max_retries": 3,
        "backoff": [120, 240, 480],
        "jitter": 60,
    },
    ErrorCode.POT_TOKEN_FAILED: {
        "max_retries": 3,
        "backoff": [120, 240, 480],
        "jitter": 30,
    },
    ErrorCode.DOWNLOAD_FAILED: {
        "max_retries": 3,
        "backoff": [120, 240, 480],
        "jitter": 30,
    },

    # 不可重试错误（直接失败）
    ErrorCode.VIDEO_UNAVAILABLE: {"max_retries": 0},
    ErrorCode.VIDEO_PRIVATE: {"max_retries": 0},
    ErrorCode.VIDEO_REGION_BLOCKED: {"max_retries": 0},
    ErrorCode.VIDEO_AGE_RESTRICTED: {"max_retries": 0},
    ErrorCode.VIDEO_LIVE_STREAM: {"max_retries": 0},
}

def get_retry_delay(error_code: ErrorCode, retry_count: int) -> float:
    """计算重试延迟时间"""
    config = RETRY_CONFIG.get(error_code)
    if not config or retry_count >= config["max_retries"]:
        return -1  # 不重试

    base_delay = config["backoff"][retry_count]
    jitter = random.uniform(0, config.get("jitter", 0))
    return base_delay + jitter
```

### 任务去重逻辑

```python
async def create_task(video_url: str, callback_url: str = None) -> Task:
    video_id = extract_video_id(video_url)

    # 查找已有任务（未过期的）
    existing = await find_existing_task(video_id)

    if existing:
        if existing.status == TaskStatus.COMPLETED:
            # 已完成且文件未过期，直接返回
            return existing
        elif existing.status in [TaskStatus.PENDING, TaskStatus.DOWNLOADING]:
            # 进行中，返回已有任务
            return existing
        elif existing.status == TaskStatus.FAILED:
            # 之前失败的，可以重新创建
            pass

    # 创建新任务
    task = Task(
        id=uuid4(),
        video_id=video_id,
        video_url=video_url,
        callback_url=callback_url,
        # ...
    )
    await save_task(task)
    await task_queue.put(task)

    return task
```

---

## yt-dlp 配置

### 下载器封装

```python
# src/core/downloader.py

import yt_dlp
from pathlib import Path

class YouTubeDownloader:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.base_opts = self._build_base_opts()

    def _build_base_opts(self) -> dict:
        opts = {
            # 格式选择：仅音频，优先 m4a 128kbps
            "format": "bestaudio[ext=m4a][abr<=128]/bestaudio[ext=m4a]/bestaudio",
            "extract_flat": False,

            # 输出模板
            "outtmpl": {
                "default": "%(id)s.%(ext)s",
            },

            # 字幕配置
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": ["zh-Hans", "zh-Hant", "zh", "en"],
            "subtitlesformat": "json3",  # JSON 格式

            # 网络配置
            "socket_timeout": 30,
            "retries": 3,
            "fragment_retries": 3,

            # 安全配置
            "no_warnings": False,
            "ignoreerrors": False,
            "no_color": True,

            # 禁用不需要的功能
            "skip_download": False,
            "extract_flat": False,
            "writethumbnail": False,

            # 日志
            "quiet": False,
            "verbose": self.settings.debug,
        }

        # 代理配置
        if self.settings.http_proxy:
            opts["proxy"] = self.settings.http_proxy

        # Cookie 配置
        if self.settings.cookie_file and Path(self.settings.cookie_file).exists():
            opts["cookiefile"] = self.settings.cookie_file

        # PO Token Provider 配置
        opts["extractor_args"] = {
            "youtube": {
                "player_client": ["mweb"],
            },
            "youtubepot-bgutilhttp": {
                "base_url": self.settings.pot_server_url,
            }
        }

        return opts

    async def download(self, video_url: str, output_dir: Path) -> DownloadResult:
        """
        下载视频音频和字幕

        Returns:
            DownloadResult: 包含视频信息和文件路径
        """
        opts = {
            **self.base_opts,
            "outtmpl": {
                "default": str(output_dir / "%(id)s.%(ext)s"),
            },
            "paths": {
                "home": str(output_dir),
            }
        }

        with yt_dlp.YoutubeDL(opts) as ydl:
            # 提取信息并下载
            info = ydl.extract_info(video_url, download=True)

            # transcript_path 可能为 None（视频没有字幕）
            return DownloadResult(
                video_info=self._extract_video_info(info),
                audio_path=self._find_audio_file(output_dir, info["id"]),
                transcript_path=self._find_transcript_file(output_dir, info["id"]),  # 可能为 None
            )

    def _extract_video_info(self, info: dict) -> VideoInfo:
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
```

### 错误处理映射

```python
def map_ytdlp_error(error: Exception) -> tuple[ErrorCode, str]:
    """将 yt-dlp 异常映射为错误码"""
    error_msg = str(error).lower()

    if "private video" in error_msg:
        return ErrorCode.VIDEO_PRIVATE, "Video is private"

    if "video unavailable" in error_msg or "not available" in error_msg:
        return ErrorCode.VIDEO_UNAVAILABLE, "Video is unavailable"

    if "age-restricted" in error_msg or "sign in to confirm your age" in error_msg:
        return ErrorCode.VIDEO_AGE_RESTRICTED, "Video is age-restricted, cookie required"

    if "blocked" in error_msg and "country" in error_msg:
        return ErrorCode.VIDEO_REGION_BLOCKED, "Video is blocked in this region"

    if "is a livestream" in error_msg or "live event" in error_msg:
        return ErrorCode.VIDEO_LIVE_STREAM, "Live streams are not supported"

    if "http error 403" in error_msg or "forbidden" in error_msg:
        return ErrorCode.RATE_LIMITED, "Rate limited by YouTube"

    if "http error 429" in error_msg:
        return ErrorCode.RATE_LIMITED, "Too many requests"

    if "network" in error_msg or "connection" in error_msg or "timeout" in error_msg:
        return ErrorCode.NETWORK_ERROR, f"Network error: {error}"

    if "po token" in error_msg or "pot" in error_msg:
        return ErrorCode.POT_TOKEN_FAILED, "Failed to obtain PO Token"

    return ErrorCode.DOWNLOAD_FAILED, str(error)
```

---

## 企业微信通知

### 通知场景

```python
# src/services/notify.py

from wecom_notifier import WeComNotifier

class NotificationService:
    def __init__(self, settings: Settings):
        self.notifier = WeComNotifier()
        self.webhook_url = settings.wecom_webhook_url
        self.enabled = bool(settings.wecom_webhook_url)

    async def notify_startup(self, ip: str, version: str):
        """系统启动通知"""
        if not self.enabled:
            return

        content = f"""# 🚀 YouTube Audio API 启动

**服务器**: {ip}
**版本**: {version}
**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

**配置摘要**:
- 并发数: {settings.download_concurrency}
- 任务间隔: {settings.task_interval_min}-{settings.task_interval_max}s
- 文件保留: {settings.file_retention_days} 天
- PO Token: {settings.pot_server_url}
"""
        self.notifier.send_markdown(
            webhook_url=self.webhook_url,
            content=content
        )

    async def notify_completed(self, task: Task):
        """下载完成通知"""
        if not self.enabled:
            return

        content = f"""# ✅ 下载完成

**视频**: {task.video_info.title}
**作者**: {task.video_info.author}
**时长**: {format_duration(task.video_info.duration)}
**任务ID**: `{task.id}`
"""
        self.notifier.send_markdown(
            webhook_url=self.webhook_url,
            content=content
        )

    async def notify_failed(self, task: Task, error: str):
        """下载失败通知"""
        if not self.enabled:
            return

        content = f"""# ❌ 下载失败

**视频URL**: {task.video_url}
**错误**: {error}
**重试次数**: {task.retry_count}
**任务ID**: `{task.id}`
"""
        self.notifier.send_markdown(
            webhook_url=self.webhook_url,
            content=content,
            mention_all=True  # 失败时 @all
        )

    async def notify_cookie_expired(self):
        """Cookie 过期通知"""
        if not self.enabled:
            return

        content = """# ⚠️ Cookie 已过期

检测到 YouTube Cookie 已过期，部分功能可能受限：
- 年龄限制视频无法下载
- 会员专属内容无法下载

请更新 Cookie 文件后重启服务。
"""
        self.notifier.send_markdown(
            webhook_url=self.webhook_url,
            content=content,
            mention_all=True
        )
```

---

## 文件清理

### 清理策略

```python
# src/services/file_service.py

class FileCleanupService:
    def __init__(self, settings: Settings, db: Database):
        self.settings = settings
        self.db = db
        self.data_dir = Path(settings.data_dir)

    async def cleanup_expired_files(self):
        """
        清理过期文件
        - 基于最后访问时间
        - 超过 FILE_RETENTION_DAYS 天未访问的文件
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(
            days=self.settings.file_retention_days
        )

        # 查询过期文件
        expired_files = await self.db.query_expired_files(cutoff_time)

        for file in expired_files:
            try:
                # 删除物理文件
                file_path = self.data_dir / file.filepath
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"Deleted expired file: {file_path}")

                # 删除数据库记录
                await self.db.delete_file(file.id)

            except Exception as e:
                logger.error(f"Failed to delete file {file.id}: {e}")

        # 清理空目录
        self._cleanup_empty_dirs()

        # 清理孤立的任务记录
        await self._cleanup_orphan_tasks()

        logger.info(f"Cleanup completed: {len(expired_files)} files removed")

    async def update_access_time(self, file_id: str):
        """更新文件最后访问时间"""
        await self.db.update_file_access_time(file_id, datetime.now(timezone.utc))
```

### 定时任务配置

```python
# src/main.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

@app.on_event("startup")
async def startup():
    # 恢复中断的任务：将 downloading 状态重置为 pending
    await db.execute("UPDATE tasks SET status='pending' WHERE status='downloading'")

    # 文件清理：每天凌晨 3 点执行
    scheduler.add_job(
        file_service.cleanup_expired_files,
        "cron",
        hour=3,
        minute=0,
    )

    # 健康检查：每 5 分钟
    scheduler.add_job(
        health_service.check_components,
        "interval",
        minutes=5,
    )

    scheduler.start()
```

---

## 本地开发

### 环境要求

- Python 3.11+
- Docker Desktop（用于 pot-provider）
- Clash 或其他代理（端口 7890）

### 首次设置

```powershell
# 1. 克隆项目
git clone <repo>
cd youtube-audio-api

# 2. 创建虚拟环境
python -m venv venv
.\venv\Scripts\Activate.ps1

# 3. 安装依赖
pip install -r requirements.txt

# 4. 复制配置文件
copy .env.example .env.development
# 编辑 .env.development，填入必要配置

# 5. 启动 pot-provider
docker-compose -f docker-compose.dev.yml up -d

# 6. 初始化数据库
python -m src.db.init

# 7. 启动开发服务器
$env:ENV_FILE=".env.development"
uvicorn src.main:app --reload --host 127.0.0.1 --port 8000
```

### 日常开发

```powershell
# 使用开发脚本一键启动
.\scripts\dev.ps1

# 或手动启动
.\venv\Scripts\Activate.ps1
docker-compose -f docker-compose.dev.yml up -d
$env:ENV_FILE=".env.development"
uvicorn src.main:app --reload
```

### 访问地址

- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### 开发调试技巧

```python
# 1. 跳过实际下载（测试 API）
# 在 .env.development 中设置
DRY_RUN=true

# 2. 快速测试用的短视频
# https://www.youtube.com/watch?v=BaW_jenozKc  # 1 秒测试视频

# 3. 查看详细日志
DEBUG=true
# 日志会输出 yt-dlp 的详细信息

# 4. 手动测试 PO Token 服务
curl http://localhost:4416/health

# 5. 测试 API 请求
curl -X POST http://localhost:8000/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-test-key" \
  -d '{"video_url": "https://www.youtube.com/watch?v=BaW_jenozKc"}'
```

---

## 生产部署

### Docker 构建

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY src/ ./src/

# 创建数据目录
RUN mkdir -p /app/data/files/audio /app/data/files/transcript

# 运行
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Docker Compose

```yaml
# docker-compose.yml
version: "3.8"

services:
  youtube-api:
    build: .
    container_name: youtube-api
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
      - ./cookies:/app/cookies:ro
    environment:
      - TZ=Asia/Shanghai
    env_file:
      - .env.production
    depends_on:
      - pot-provider
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  pot-provider:
    image: brainicism/bgutil-ytdlp-pot-provider
    container_name: pot-provider
    restart: unless-stopped
    # 仅内部通信，不暴露端口
```

### 部署命令

```bash
# 构建并启动
docker-compose up -d --build

# 查看日志
docker-compose logs -f youtube-api

# 重启
docker-compose restart youtube-api

# 更新
docker-compose pull
docker-compose up -d --build
```

---

## 测试

### 测试结构

```
tests/
├── conftest.py                 # 公共 fixtures
├── test_api/
│   ├── test_tasks.py           # 任务 API 测试
│   ├── test_files.py           # 文件 API 测试
│   └── test_auth.py            # 鉴权测试
├── test_core/
│   ├── test_downloader.py      # 下载器测试（mock）
│   └── test_worker.py          # Worker 测试
├── test_services/
│   ├── test_task_service.py    # 任务服务测试
│   ├── test_file_service.py    # 文件服务测试
│   └── test_callback.py        # 回调测试
└── test_integration/
    └── test_full_flow.py       # 完整流程测试
```

### 运行测试

```bash
# 运行所有测试
pytest

# 运行单个测试文件
pytest tests/test_api/test_tasks.py

# 运行带覆盖率
pytest --cov=src --cov-report=html

# 跳过集成测试（需要网络）
pytest -m "not integration"
```

### Mock 策略

```python
# tests/conftest.py

import pytest
from unittest.mock import AsyncMock, MagicMock

@pytest.fixture
def mock_downloader():
    """Mock yt-dlp 下载器"""
    downloader = AsyncMock()
    downloader.download.return_value = DownloadResult(
        video_info=VideoInfo(
            title="Test Video",
            author="Test Author",
            duration=60,
        ),
        audio_path=Path("/tmp/test.m4a"),
        transcript_path=Path("/tmp/test.json"),
    )
    return downloader

@pytest.fixture
def mock_notifier():
    """Mock 企微通知"""
    notifier = MagicMock()
    notifier.send_markdown.return_value = MagicMock(is_success=lambda: True)
    return notifier
```

---

## 注意事项

### 安全

1. **API Key 保护**：不要将 API Key 提交到代码仓库
2. **文件访问**：虽然公开，但使用 UUID 防止枚举
3. **回调验证**：客户端必须验证 HMAC 签名
4. **代理安全**：生产环境使用透明代理，不在代码中暴露代理地址

### 性能

1. **并发控制**：默认单并发，避免触发风控
2. **任务间隔**：随机间隔模拟人类行为
3. **文件清理**：定时清理避免磁盘占满
4. **数据库**：SQLite 足够处理 60/天的量级

### 可靠性

1. **任务持久化**：重启后自动恢复未完成任务（downloading 状态重置为 pending）
2. **错误重试**：可重试错误自动重试（指数退避）
3. **回调重试**：Webhook 失败自动重试
4. **健康检查**：定期检查各组件状态

### YouTube 风控

1. **TLS 指纹**：使用 curl_cffi 模拟浏览器
2. **PO Token**：使用官方推荐的 bgutil-ytdlp-pot-provider
3. **请求频率**：严格控制下载间隔
4. **IP 质量**：使用高质量代理节点
