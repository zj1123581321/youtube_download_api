"""
Test POT (PO Token) configuration.

Usage:
    python tests/test_pot_config.py
"""

import os
import sys

# Fix Windows encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Set environment variable
os.environ["ENV_FILE"] = ".env.development"

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import yt_dlp

from src.config import get_settings


def test_pot_server_reachable():
    """Test if POT server is reachable."""
    settings = get_settings()
    print(f"\n[1] Testing POT Server connection: {settings.pot_server_url}")

    try:
        response = httpx.get(f"{settings.pot_server_url}/ping", timeout=5)
        print(f"    [OK] POT Server response: {response.status_code}")
        return True
    except Exception as e:
        print(f"    [FAIL] POT Server unreachable: {e}")
        return False


def test_extractor_args_format():
    """Test extractor_args format."""
    settings = get_settings()
    print(f"\n[2] Testing extractor_args format")

    # Correct Python API format (matches CLI parse result):
    # - Key: extractor name
    # - Value: nested dict {param_name: [param_values]}
    extractor_args = {
        "youtube": {"player_client": ["web"]},
        "youtubepot-bgutilhttp": {
            "base_url": [settings.pot_server_url],
        },
    }

    print(f"    extractor_args: {extractor_args}")
    return extractor_args


def test_ytdlp_with_pot(extractor_args: dict):
    """Test yt-dlp with POT config."""
    print(f"\n[3] Testing yt-dlp POT config")

    # Test video
    test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

    opts = {
        "quiet": False,
        "verbose": True,
        "no_warnings": False,
        "extract_flat": False,
        "skip_download": True,
        "format": "best",  # Use 'best' to avoid format availability issues
        "extractor_args": extractor_args,
    }

    print(f"    Test URL: {test_url}")
    print(f"    yt-dlp opts: {opts}")

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            print("\n    Extracting video info...")
            info = ydl.extract_info(test_url, download=False)

            if info:
                print(f"\n    [OK] Successfully got video info!")
                print(f"    Title: {info.get('title', 'N/A')}")
                print(f"    Formats count: {len(info.get('formats', []))}")
                return True
            else:
                print(f"    [FAIL] Could not get video info")
                return False

    except Exception as e:
        print(f"\n    [FAIL] yt-dlp error: {e}")
        return False


def main():
    print("=" * 60)
    print("POT Config Test")
    print("=" * 60)

    settings = get_settings()
    print(f"\nConfig:")
    print(f"  POT_SERVER_URL: {settings.pot_server_url}")
    print(f"  COOKIE_FILE: {settings.cookie_file}")

    # Test 1: POT server connection
    pot_ok = test_pot_server_reachable()

    if not pot_ok:
        print("\n[WARNING] POT Server unreachable, please ensure:")
        print("   1. POT provider container is running")
        print("   2. Port 4416 is correctly mapped")
        print("   3. Firewall allows access")
        return

    # Test 2: extractor_args format
    extractor_args = test_extractor_args_format()

    # Test 3: yt-dlp actual call
    success = test_ytdlp_with_pot(extractor_args)

    print("\n" + "=" * 60)
    if success:
        print("[OK] POT config test PASSED!")
    else:
        print("[FAIL] POT config test FAILED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
