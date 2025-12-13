#!/usr/bin/env bash
# ============================================
# Docker Image Import Script (Linux)
# Project: YouTube Audio API
# ============================================

set -e
set -o pipefail

echo "======================================"
echo "  YouTube Audio API - Image Import"
echo "======================================"
echo ""

# Image information (placeholders replaced by BAT script)
IMAGE_NAME="__IMAGE_NAME__"
IMAGE_TAG="__IMAGE_TAG__"
TAR_FILE="__TAR_FILE__"

# Check tar file exists
if [ ! -f "$TAR_FILE" ]; then
    echo "[ERROR] Image file not found: $TAR_FILE"
    echo "Please ensure the image file is in the current directory."
    exit 1
fi

echo "[1/4] Checking Docker service..."
if ! docker info >/dev/null 2>&1; then
    echo "[ERROR] Docker service is not running."
    echo "Start Docker service first, e.g.: systemctl start docker"
    exit 1
fi
echo "Docker service is running"
echo ""

echo "[2/4] Removing old images..."

# Remove old images
OLD_IMAGE_COUNT=$(docker images "$IMAGE_NAME" -q | wc -l)
if [ "$OLD_IMAGE_COUNT" -gt 0 ]; then
    echo "Found old images:"
    docker images "$IMAGE_NAME"
    echo ""
    echo "Stopping and removing related containers..."
    CONTAINERS=$(docker ps -a -q --filter "ancestor=$IMAGE_NAME" 2>/dev/null || true)
    if [ -n "$CONTAINERS" ]; then
        docker stop $CONTAINERS 2>/dev/null || true
        docker rm $CONTAINERS 2>/dev/null || true
    fi
    docker rmi -f $(docker images "$IMAGE_NAME" -q | uniq) 2>/dev/null || true
    echo "Old images removed"
else
    echo "No old images found"
fi
echo ""

echo "[3/4] Importing new image..."
echo "Import file: $TAR_FILE"
if ! docker load -i "$TAR_FILE"; then
    echo "[ERROR] Image import failed."
    exit 1
fi
echo ""

echo "[4/4] Verifying import..."
echo ""
echo "Imported images:"
docker images "$IMAGE_NAME"
echo ""

echo "======================================"
echo "  Image import completed!"
echo "======================================"
echo ""
echo "Image tag: $IMAGE_TAG"
echo ""
echo "Next steps:"
echo ""
echo "1. Configure environment variables:"
echo "   cp .env.example .env.production"
echo "   vim .env.production  # Fill in your actual configuration"
echo ""
echo "2. Create data directories:"
echo "   mkdir -p data cookies"
echo ""
echo "3. Start services with docker compose:"
echo "   docker compose up -d"
echo ""
echo "4. Check service status:"
echo "   docker compose ps"
echo "   docker compose logs -f youtube-api"
echo ""
echo "Note: The pot-provider image will be pulled automatically"
echo "      from Docker Hub when starting services."
echo ""
