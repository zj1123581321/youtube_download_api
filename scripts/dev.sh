#!/bin/bash
# Development startup script for Linux/Mac
# Usage: ./scripts/dev.sh

set -e

echo -e "\033[36mStarting YouTube Audio API development environment...\033[0m"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "\033[33mCreating virtual environment...\033[0m"
    python3 -m venv venv
fi

# Activate virtual environment
echo -e "\033[33mActivating virtual environment...\033[0m"
source venv/bin/activate

# Install dependencies
echo -e "\033[33mInstalling dependencies...\033[0m"
pip install -r requirements.txt

# Check if .env.development exists
if [ ! -f ".env.development" ]; then
    echo -e "\033[33mCreating .env.development from template...\033[0m"
    cp .env.example .env.development
    echo -e "\033[31mPlease edit .env.development with your configuration\033[0m"
    exit 1
fi

# Start pot-provider container
echo -e "\033[33mStarting pot-provider container...\033[0m"
docker-compose -f docker-compose.dev.yml up -d

# Wait for pot-provider to be ready
echo -e "\033[33mWaiting for pot-provider to be ready...\033[0m"
sleep 5

# Check pot-provider health
if curl -s -f http://localhost:4416/health > /dev/null 2>&1; then
    echo -e "\033[32mpot-provider is ready\033[0m"
else
    echo -e "\033[33mWarning: pot-provider may not be ready yet\033[0m"
fi

# Set environment file
export ENV_FILE=".env.development"

# Start the development server
echo -e "\033[32mStarting development server...\033[0m"
uvicorn src.main:app --reload --host 127.0.0.1 --port 8000
