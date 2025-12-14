# Stage 1: Download ffmpeg static binary
FROM alpine:3.19 AS ffmpeg-downloader

RUN apk add --no-cache curl tar xz

# Download ffmpeg LGPL static build from BtbN (smaller than GPL version)
# https://github.com/BtbN/FFmpeg-Builds
# LGPL version is sufficient for audio transcoding (m4a/AAC)
# GPL version (~386MB) vs LGPL version (~140MB)
RUN curl -L --retry 3 --retry-delay 5 --retry-connrefused \
    "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-lgpl.tar.xz" \
    -o /tmp/ffmpeg.tar.xz && \
    mkdir -p /tmp/ffmpeg && \
    tar -xf /tmp/ffmpeg.tar.xz -C /tmp/ffmpeg --strip-components=1 && \
    cp /tmp/ffmpeg/bin/ffmpeg /tmp/ffmpeg/bin/ffprobe /usr/local/bin/

# Stage 2: Build Python dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements-prod.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements-prod.txt

# Stage 3: Final runtime image
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies (curl for healthcheck, unzip for deno)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /root/.cache

# Install Deno for yt-dlp n challenge solving (nsig decryption)
# https://github.com/yt-dlp/yt-dlp/wiki/EJS
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh \
    && deno --version

# Copy ffmpeg binaries from downloader stage
COPY --from=ffmpeg-downloader /usr/local/bin/ffmpeg /usr/local/bin/
COPY --from=ffmpeg-downloader /usr/local/bin/ffprobe /usr/local/bin/

# Copy Python dependencies from builder stage
COPY --from=builder /app/deps /usr/local/lib/python3.11/site-packages/

# Copy source code
COPY src/ ./src/

# Create data directories
RUN mkdir -p /app/data/files/audio /app/data/files/transcript /app/data/logs

# Remove unnecessary files to reduce size
RUN find /usr/local/lib/python3.11/site-packages -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.11/site-packages -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.11/site-packages -type d -name "test" -exec rm -rf {} + 2>/dev/null || true && \
    rm -rf /usr/local/lib/python3.11/site-packages/pip* && \
    rm -rf /usr/local/lib/python3.11/site-packages/setuptools*

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DATA_DIR=/app/data

# Default port (can be overridden by PORT env var)
ENV PORT=8000

# Expose port
EXPOSE ${PORT}

# Health check (uses PORT env var)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Run the application (shell form to expand PORT env var)
CMD ["sh", "-c", "python -m uvicorn src.main:app --host 0.0.0.0 --port ${PORT}"]
