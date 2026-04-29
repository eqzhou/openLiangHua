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

    def change_password(self, user_id: str, old_password: str, new_password: str) -> bool:
        return user_id == "user-1" and old_password == "secret" and new_password == "NewSecret123"


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

    def test_auth_routes_are_registered_once(self) -> None:
        auth_routes = [
            (route.path, tuple(sorted(route.methods)))
            for route in app.routes
            if getattr(route, "path", "").startswith("/api/auth")
        ]

        self.assertEqual(len(auth_routes), len(set(auth_routes)))

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

        with patch("src.web_api.app.refresh_realtime_payload", return_value={"ok": True, "realtimeStatus": {"available": True}}) as refresh_payload:
            response = self.client.post("/api/realtime/refresh")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(refresh_payload.call_args.kwargs["user_id"], "user-1")

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

    def test_market_bars_refresh_reports_empty_watchlist_as_business_error(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(login.status_code, 200)

        with patch(
            "src.web_api.app.run_market_bars_refresh_payload",
            side_effect=RuntimeError("No symbols were provided and no watchlist_items symbols were found."),
        ):
            response = self.client.post("/api/data-management/market-bars-refresh", json={"target_source": "tushare"})

        self.assertEqual(response.status_code, 409)
        self.assertIn("观察池", response.json()["detail"])

    def test_myquant_status_endpoint_requires_login_for_sensitive_fields(self) -> None:
        with patch("src.web_api.app.get_myquant_status_payload", return_value={"tokenConfigured": None}) as status_payload:
            response = self.client.get("/api/data-management/myquant-status")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["tokenConfigured"])
        self.assertFalse(status_payload.call_args.kwargs["include_sensitive"])

    def test_myquant_actions_require_login_and_pass_user_scope(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(login.status_code, 200)

        with patch("src.web_api.app.run_myquant_enrich_payload", return_value={"ok": True}) as enrich_payload:
            response = self.client.post("/api/data-management/myquant-enrich", json={"target_source": "myquant"})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(enrich_payload.call_args.kwargs["user_id"], "user-1")

    def test_experiment_config_update_deep_merges_nested_sections(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        current_config = {
            "label_col": "ret_t1_t10",
            "top_n": 20,
            "rolling": {
                "enabled": True,
                "retrain_frequency": "monthly",
                "min_history_size": 252,
            },
        }

        with (
            patch("src.web_api.app.get_experiment_config_payload", return_value=current_config),
            patch("src.web_api.app.update_experiment_config_payload", side_effect=lambda payload: payload) as update_payload,
        ):
            response = self.client.put("/api/config/experiment", json={"rolling": {"enabled": False}})

        self.assertEqual(response.status_code, 200)
        merged_payload = update_payload.call_args.args[0]
        self.assertFalse(merged_payload["rolling"]["enabled"])
        self.assertEqual(merged_payload["rolling"]["retrain_frequency"], "monthly")
        self.assertEqual(merged_payload["rolling"]["min_history_size"], 252)

    def test_watchlist_realtime_read_requires_login_when_refresh_requested(self) -> None:
        response = self.client.get("/api/watchlist/summary?include_realtime=true")

        self.assertEqual(response.status_code, 401)

    def test_watchlist_summary_read_passes_logged_in_user_scope(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        with patch(
            "src.web_api.app.get_watchlist_summary_payload",
            return_value={"records": [], "overview": {}},
        ) as payload_loader:
            response = self.client.get("/api/watchlist/summary")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload_loader.call_args.kwargs["user_id"], "user-1")

    def test_watchlist_summary_read_keeps_public_scope_without_login(self) -> None:
        with patch(
            "src.web_api.app.get_watchlist_summary_payload",
            return_value={"records": [], "overview": {}},
        ) as payload_loader:
            response = self.client.get("/api/watchlist/summary")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(payload_loader.call_args.kwargs["user_id"])

    def test_watch_plan_endpoint_succeeds_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        with patch("src.web_api.app.generate_watch_plan", return_value={"actionName": "watch_plan", "ok": True}) as generate_payload:
            response = self.client.post("/api/actions/watch-plan")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(generate_payload.call_args.kwargs["user_id"], "user-1")

    def test_action_memo_endpoint_succeeds_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )

        self.assertEqual(login.status_code, 200)

        with patch("src.web_api.app.generate_action_memo", return_value={"actionName": "action_memo", "ok": True}) as generate_payload:
            response = self.client.post("/api/actions/action-memo")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(generate_payload.call_args.kwargs["user_id"], "user-1")

    def test_logout_clears_session_cookie(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(login.status_code, 200)

        logout = self.client.post("/api/auth/logout")

        self.assertEqual(logout.status_code, 200)
        self.assertIn("Max-Age=0", logout.headers.get("set-cookie", ""))

    def test_change_password_requires_login(self) -> None:
        response = self.client.post(
            "/api/auth/change-password",
            json={"oldPassword": "secret", "newPassword": "NewSecret123"},
        )

        self.assertEqual(response.status_code, 401)

    def test_change_password_rejects_weak_password(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(login.status_code, 200)

        response = self.client.post(
            "/api/auth/change-password",
            json={"oldPassword": "secret", "newPassword": "short"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertNotIn("secret", response.text)

    def test_change_password_rejects_wrong_old_password(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(login.status_code, 200)

        response = self.client.post(
            "/api/auth/change-password",
            json={"oldPassword": "wrong", "newPassword": "NewSecret123"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "Current password is incorrect.")

    def test_change_password_succeeds_after_login(self) -> None:
        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(login.status_code, 200)

        response = self.client.post(
            "/api/auth/change-password",
            json={"oldPassword": "secret", "newPassword": "NewSecret123"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])


if __name__ == "__main__":
    unittest.main()
