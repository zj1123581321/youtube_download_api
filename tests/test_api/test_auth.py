"""
Tests for API authentication.
"""

import pytest
from fastapi import status
from fastapi.testclient import TestClient


class TestApiKeyAuthentication:
    """Test API key authentication."""

    def test_missing_api_key(self, client: TestClient) -> None:
        """Test request without API key."""
        response = client.get("/api/v1/tasks")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert "Missing API key" in response.json()["detail"]

    def test_invalid_api_key(self, client: TestClient) -> None:
        """Test request with invalid API key."""
        response = client.get(
            "/api/v1/tasks",
            headers={"X-API-Key": "invalid-key"},
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "Invalid API key" in response.json()["detail"]

    def test_valid_api_key(self, client: TestClient, api_key: str) -> None:
        """Test request with valid API key."""
        response = client.get(
            "/api/v1/tasks",
            headers={"X-API-Key": api_key},
        )
        assert response.status_code == status.HTTP_200_OK


@pytest.fixture
def client() -> TestClient:
    """Create test client."""
    # This would need proper app initialization with mocks
    # For now, this is a placeholder showing the test structure
    pytest.skip("Requires full app initialization")


@pytest.fixture
def api_key() -> str:
    """Return test API key."""
    return "test-api-key-12345"
