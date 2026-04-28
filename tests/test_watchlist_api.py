from __future__ import annotations

from fastapi.testclient import TestClient

from src.web_api.app import app
from src.web_api.auth import AuthenticatedUser, require_authenticated_user
from src.web_api.routers.watchlist import get_watchlist_store


class FakeWatchlistStore:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.added: list[tuple[object, ...]] = []
        self.removed: list[tuple[object, ...]] = []
        self.loaded_user_ids: list[str] = []
        self.watchlist = {
            "holdings": [{"ts_code": "000001.SZ", "name": "Ping An Bank", "cost": 10.5, "shares": 1000}],
            "focus_pool": [],
        }

    def add_item(self, *args, **kwargs) -> None:
        if self.fail:
            raise RuntimeError("database password leaked in driver error")
        self.added.append((args, kwargs))

    def remove_item(self, *args, **kwargs) -> None:
        if self.fail:
            raise RuntimeError("database password leaked in driver error")
        self.removed.append((args, kwargs))

    def load_watchlist(self, user_id: str) -> dict[str, list[dict[str, object]]]:
        if self.fail:
            raise RuntimeError("database password leaked in driver error")
        self.loaded_user_ids.append(user_id)
        return self.watchlist


def _mock_auth_user() -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id="user-1",
        username="admin",
        display_name="Admin",
    )


def test_add_watchlist_item_success() -> None:
    fake_store = FakeWatchlistStore()
    app.dependency_overrides[require_authenticated_user] = _mock_auth_user
    app.dependency_overrides[get_watchlist_store] = lambda: fake_store
    client = TestClient(app)
    try:
        from unittest.mock import patch

        with patch("src.web_api.routers.watchlist.sync_watchlist_snapshot_artifact") as sync_snapshot:
            sync_snapshot.return_value.ok = True
            sync_snapshot.return_value.message = "synced"
            response = client.post(
                "/api/watchlist-config/items",
                json={
                    "ts_code": "000001.SZ",
                    "name": "Ping An Bank",
                    "type": "holding",
                    "cost": 10.5,
                    "shares": 1000,
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["snapshotSynced"] is True
    assert fake_store.added
    add_args, add_kwargs = fake_store.added[0]
    assert add_args[0] == "user-1"
    assert fake_store.loaded_user_ids == ["user-1"]
    assert sync_snapshot.call_args.kwargs["user_id"] == "user-1"


def test_delete_watchlist_item_uses_authenticated_user_scope() -> None:
    fake_store = FakeWatchlistStore()
    app.dependency_overrides[require_authenticated_user] = _mock_auth_user
    app.dependency_overrides[get_watchlist_store] = lambda: fake_store
    client = TestClient(app)
    try:
        from unittest.mock import patch

        with patch("src.web_api.routers.watchlist.sync_watchlist_snapshot_artifact") as sync_snapshot:
            sync_snapshot.return_value.ok = True
            sync_snapshot.return_value.message = "synced"
            response = client.delete("/api/watchlist-config/items/000001.SZ/holding")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_store.removed
    remove_args, remove_kwargs = fake_store.removed[0]
    assert remove_args[0] == "user-1"
    assert fake_store.loaded_user_ids == ["user-1"]
    assert sync_snapshot.call_args.kwargs["user_id"] == "user-1"


def test_add_watchlist_item_rejects_invalid_type_before_store_call() -> None:
    fake_store = FakeWatchlistStore()
    app.dependency_overrides[require_authenticated_user] = _mock_auth_user
    app.dependency_overrides[get_watchlist_store] = lambda: fake_store
    client = TestClient(app)
    try:
        response = client.post(
            "/api/watchlist-config/items",
            json={
                "ts_code": "000001.SZ",
                "name": "Ping An Bank",
                "type": "portfolio",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert fake_store.added == []


def test_watchlist_store_errors_are_redacted() -> None:
    app.dependency_overrides[require_authenticated_user] = _mock_auth_user
    app.dependency_overrides[get_watchlist_store] = lambda: FakeWatchlistStore(fail=True)
    client = TestClient(app)
    try:
        response = client.post(
            "/api/watchlist-config/items",
            json={
                "ts_code": "000001.SZ",
                "name": "Ping An Bank",
                "type": "holding",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["detail"] == "Watchlist configuration store unavailable."


def test_watchlist_snapshot_sync_failure_is_reported() -> None:
    fake_store = FakeWatchlistStore()
    app.dependency_overrides[require_authenticated_user] = _mock_auth_user
    app.dependency_overrides[get_watchlist_store] = lambda: fake_store
    client = TestClient(app)
    try:
        from unittest.mock import patch

        with patch("src.web_api.routers.watchlist.sync_watchlist_snapshot_artifact") as sync_snapshot:
            sync_snapshot.return_value.ok = False
            sync_snapshot.return_value.message = "missing prediction artifacts"
            response = client.post(
                "/api/watchlist-config/items",
                json={
                    "ts_code": "000001.SZ",
                    "name": "Ping An Bank",
                    "type": "holding",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["detail"] == "Watchlist snapshot sync failed."
