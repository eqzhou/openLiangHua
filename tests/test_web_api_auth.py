from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.web_api.app import app
from src.web_api.auth import AuthenticatedUser, LoginResult, get_auth_store


class FakeAuthStore:
    def __init__(self) -> None:
        self._user = AuthenticatedUser(
            user_id="user-1",
            username="admin",
            display_name="系统管理员",
            title="研究管理员",
        )
        self._session_token = "test-session-token"

    def get_session_user(self, session_token: str | None) -> AuthenticatedUser | None:
        if session_token == self._session_token:
            return self._user
        return None

    def login(self, username: str, password: str) -> LoginResult | None:
        if username == "admin" and password == "secret":
            return LoginResult(user=self._user, session_token=self._session_token)
        return None

    def logout(self, session_token: str | None) -> None:
        return None


class WebApiAuthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def setUp(self) -> None:
        app.dependency_overrides[get_auth_store] = lambda: FakeAuthStore()
        self.client.cookies.clear()

    def tearDown(self) -> None:
        app.dependency_overrides.clear()

    def test_auth_session_reports_logged_out_when_cookie_missing(self) -> None:
        response = self.client.get("/api/auth/session")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"authenticated": False, "user": None})

    def test_auth_login_sets_cookie_and_returns_user_payload(self) -> None:
        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["authenticated"])
        self.assertEqual(payload["user"]["username"], "admin")
        self.assertIn("openlianghua_session", response.headers.get("set-cookie", ""))

    def test_auth_login_rejects_invalid_password(self) -> None:
        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "wrong"},
        )

        self.assertEqual(response.status_code, 401)

    def test_mutating_endpoint_requires_authenticated_session(self) -> None:
        response = self.client.post("/api/cache/clear")

        self.assertEqual(response.status_code, 401)

    def test_realtime_refresh_requires_authenticated_session(self) -> None:
        response = self.client.post("/api/realtime/refresh")

        self.assertEqual(response.status_code, 401)

    def test_data_management_endpoint_redacts_sensitive_fields_without_login(self) -> None:
        with patch(
            "src.web_api.app.get_data_management_payload",
            side_effect=lambda include_sensitive=True: {"includeSensitive": include_sensitive},
        ):
            response = self.client.get("/api/data-management")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["includeSensitive"])

    def test_mutating_endpoint_succeeds_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        response = self.client.post("/api/cache/clear")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])

    def test_realtime_refresh_succeeds_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        with patch("src.web_api.app.refresh_realtime_payload", return_value={"ok": True, "realtimeStatus": {"available": True}}):
            response = self.client.post("/api/realtime/refresh")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])

    def test_data_management_endpoint_includes_sensitive_fields_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        with patch(
            "src.web_api.app.get_data_management_payload",
            side_effect=lambda include_sensitive=True: {"includeSensitive": include_sensitive},
        ):
            response = self.client.get("/api/data-management")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["includeSensitive"])

    def test_watchlist_realtime_read_requires_login_when_refresh_requested(self) -> None:
        response = self.client.get("/api/watchlist/summary?include_realtime=true")

        self.assertEqual(response.status_code, 401)

    def test_watch_plan_endpoint_succeeds_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        with patch("src.web_api.app.generate_watch_plan", return_value={"actionName": "watch_plan", "ok": True}):
            response = self.client.post("/api/actions/watch-plan")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])

    def test_action_memo_endpoint_succeeds_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        with patch("src.web_api.app.generate_action_memo", return_value={"actionName": "action_memo", "ok": True}):
            response = self.client.post("/api/actions/action-memo")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])

    def test_logout_clears_session_cookie(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(login.status_code, 200)

        logout = self.client.post("/api/auth/logout")

        self.assertEqual(logout.status_code, 200)
        self.assertIn("Max-Age=0", logout.headers.get("set-cookie", ""))


if __name__ == "__main__":
    unittest.main()
