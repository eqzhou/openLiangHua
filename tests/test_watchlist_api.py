import pytest
from fastapi.testclient import TestClient

from src.web_api.app import app
from src.web_api.auth import get_optional_authenticated_user, AuthenticatedUser

client = TestClient(app)

def mock_auth_user():
    return AuthenticatedUser(
        user_id="system",
        username="admin",
        display_name="Admin"
    )

app.dependency_overrides[get_optional_authenticated_user] = mock_auth_user

def test_add_watchlist_item_success():
    payload = {
        "ts_code": "000001.SZ",
        "name": "Ping An Bank",
        "type": "holding",
        "cost": 10.5,
        "shares": 1000
    }
    # This assumes require_authenticated_user is mocked or we can just mock the dependency
    # Let's mock require_authenticated_user instead
    from src.web_api.auth import require_authenticated_user
    app.dependency_overrides[require_authenticated_user] = mock_auth_user
    
    response = client.post("/api/watchlist-config/items", json=payload)
    assert response.status_code == 200
    assert response.json()["ok"] is True
    
    # Clean up after test
    client.delete("/api/watchlist-config/items/000001.SZ/holding")
