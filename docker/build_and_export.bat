@echo off
chcp 65001 >nul
REM ============================================
REM Docker Image Build and Export Script (Windows)
REM Project: YouTube Audio API
REM Function: Build Docker image and export to tar file
REM ============================================

echo ======================================
echo   YouTube Audio API - Image Build
echo ======================================
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running or not installed!
    echo Please start Docker Desktop first.
    pause
    exit /b 1
)

REM Set image name
set IMAGE_NAME=youtube-api

REM Get current datetime (format: YYYYMMDD-HHMMSS)
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set TIMESTAMP=%datetime:~0,8%-%datetime:~8,6%

REM Set full image tags
set IMAGE_TAG=%IMAGE_NAME%:%TIMESTAMP%
set IMAGE_LATEST=%IMAGE_NAME%:latest

REM Set output file names
set OUTPUT_DIR=%~dp0output
set TAR_FILE=%OUTPUT_DIR%\youtube-api-%TIMESTAMP%.tar
set IMPORT_SCRIPT=%OUTPUT_DIR%\import_image.sh

echo [1/5] Preparing output directory...
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"
echo Output directory: %OUTPUT_DIR%
echo.

REM Switch to project root directory
cd /d "%~dp0.."

echo [2/5] Building Docker image...
echo Image tag: %IMAGE_TAG%
echo.
docker build -t %IMAGE_TAG% -t %IMAGE_LATEST% -f Dockerfile .
if errorlevel 1 (
    echo [ERROR] Image build failed!
    pause
    exit /b 1
)
echo.

echo [3/5] Exporting Docker image to tar file...
echo Export file: %TAR_FILE%
echo Including images:
echo   - %IMAGE_TAG%
echo   - %IMAGE_LATEST%
echo.
docker save -o "%TAR_FILE%" %IMAGE_TAG% %IMAGE_LATEST%
if errorlevel 1 (
    echo [ERROR] Docker image export failed!
    pause
    exit /b 1
)
echo.

echo [4/5] Generating Linux import script...
echo Script file: %IMPORT_SCRIPT%
echo.

REM Generate Linux import script from template
set TEMPLATE_FILE=%~dp0import_image_template.sh
if not exist "%TEMPLATE_FILE%" (
    echo [ERROR] Template file not found: %TEMPLATE_FILE%
    pause
    exit /b 1
)

REM Use PowerShell to replace placeholders in template (with Unix line endings LF)
powershell -Command "$content = Get-Content '%TEMPLATE_FILE%' -Raw; $content = $content -replace '__IMAGE_NAME__', '%IMAGE_NAME%'; $content = $content -replace '__IMAGE_TAG__', '%IMAGE_TAG%'; $content = $content -replace '__TIMESTAMP__', '%TIMESTAMP%'; $content = $content -replace '__TAR_FILE__', 'youtube-api-%TIMESTAMP%.tar'; $content = $content -replace \"`r`n\", \"`n\"; [System.IO.File]::WriteAllText('%IMPORT_SCRIPT%', $content, [System.Text.UTF8Encoding]::new($false))"

if errorlevel 1 (
    echo [ERROR] Failed to generate import script!
    pause
    exit /b 1
)

echo.

echo [5/5] Copying configuration files...
copy /Y "%~dp0docker-compose.prod.yml" "%OUTPUT_DIR%\docker-compose.yml" >nul
copy /Y "%~dp0..\.env.example" "%OUTPUT_DIR%\.env.example" >nul
echo Copied docker-compose.yml and .env.example
echo.

echo ======================================
echo   Build completed!
echo ======================================
echo.
echo Image file: %TAR_FILE%
echo Import script: %IMPORT_SCRIPT%
echo.
echo File size:
for %%A in ("%TAR_FILE%") do echo   - Image package: %%~zA bytes
echo.
echo ======================================
echo   Next steps
echo ======================================
echo.
echo 1. Upload the following files from output folder to Linux server:
echo    - youtube-api-%TIMESTAMP%.tar  (image package)
echo    - import_image.sh              (import script)
echo    - docker-compose.yml           (compose configuration)
echo    - .env.example                 (environment template)
echo.
echo 2. On Linux server, execute:
echo    chmod +x import_image.sh
echo    ./import_image.sh
echo.
echo 3. Configure environment variables:
echo    cp .env.example .env.production
echo    vim .env.production  # Fill in actual configuration
echo.
echo 4. Create data directories and start services:
echo    mkdir -p data cookies
echo    docker compose up -d
echo.
pause
