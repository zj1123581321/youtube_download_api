"""
API dependencies module.

Provides dependency injection for authentication and services.
"""

from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from src.config import Settings, get_settings


async def verify_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    settings: Settings = Depends(get_settings),
) -> str:
    """
    Verify API key from header.

    Args:
        x_api_key: API key from X-API-Key header.
        settings: Application settings.

    Returns:
        Valid API key.

    Raises:
        HTTPException: If API key is missing or invalid.
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    if x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )

    return x_api_key


# Type alias for dependency injection
ApiKeyDep = Annotated[str, Depends(verify_api_key)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
