from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from src.web_api.auth import PostgresAuthStore
from src.web_api.settings import ApiSettings


def build_api_settings(*, auth_touch_interval_seconds: int = 300) -> ApiSettings:
    return ApiSettings(
        db_host="127.0.0.1",
        db_port=5432,
        db_name="openlianghua",
        db_schema="public",
        db_user="postgres",
        db_password="postgres",
        db_connect_timeout=5,
        cors_origins=("http://127.0.0.1:5174",),
        auth_cookie_name="openlianghua_session",
        auth_session_days=7,
        auth_touch_interval_seconds=auth_touch_interval_seconds,
        bootstrap_username="admin",
        bootstrap_password="secret",
        bootstrap_display_name="Admin",
        bootstrap_title="Research Admin",
    )


def build_connection_mock(row: dict[str, object]) -> tuple[MagicMock, MagicMock]:
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = None

    conn = MagicMock()
    conn.cursor.return_value = cursor_cm
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    return conn, cursor


class PostgresAuthStoreTests(unittest.TestCase):
    def test_ensure_schema_does_not_overwrite_existing_bootstrap_password(self) -> None:
        store = PostgresAuthStore(build_api_settings())
        conn, cursor = build_connection_mock({})

        with patch("src.web_api.auth.ensure_sql_script") as ensure_script, patch.object(store, "_connect", return_value=conn):
            store._ensure_schema()

        ensure_script.assert_called_once()
        executed_sql = "\n".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        self.assertIn("ON CONFLICT (username) DO NOTHING", executed_sql)
        self.assertNotIn("password_hash = EXCLUDED.password_hash", executed_sql)
        conn.commit.assert_called_once()

    def test_get_session_user_skips_last_seen_update_within_touch_interval(self) -> None:
        store = PostgresAuthStore(build_api_settings(auth_touch_interval_seconds=300))
        recent_last_seen = datetime.now(UTC) - timedelta(seconds=60)
        conn, cursor = build_connection_mock(
            {
                "id": "user-1",
                "username": "admin",
                "display_name": "系统管理员",
                "title": "研究管理员",
                "expires_at": datetime.now(UTC) + timedelta(days=1),
                "last_seen_at": recent_last_seen,
            }
        )

        with patch.object(store, "_ensure_schema"), patch.object(store, "_connect", return_value=conn):
            user = store.get_session_user("session-token")

        self.assertIsNotNone(user)
        executed_sql = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertFalse(any("UPDATE app_sessions SET last_seen_at = NOW()" in sql for sql in executed_sql))
        conn.commit.assert_called_once()

    def test_get_session_user_updates_last_seen_after_touch_interval(self) -> None:
        store = PostgresAuthStore(build_api_settings(auth_touch_interval_seconds=300))
        stale_last_seen = datetime.now(UTC) - timedelta(seconds=301)
        conn, cursor = build_connection_mock(
            {
                "id": "user-1",
                "username": "admin",
                "display_name": "系统管理员",
                "title": "研究管理员",
                "expires_at": datetime.now(UTC) + timedelta(days=1),
                "last_seen_at": stale_last_seen,
            }
        )

        with patch.object(store, "_ensure_schema"), patch.object(store, "_connect", return_value=conn):
            user = store.get_session_user("session-token")

        self.assertIsNotNone(user)
        executed_sql = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("UPDATE app_sessions SET last_seen_at = NOW()" in sql for sql in executed_sql))
        conn.commit.assert_called_once()

    def test_change_password_updates_hash_only_when_old_password_matches(self) -> None:
        store = PostgresAuthStore(build_api_settings())
        old_salt, old_hash = store.hash_password_for_test("old-secret")

        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "password_salt": old_salt,
            "password_hash": old_hash,
        }
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None

        conn = MagicMock()
        conn.cursor.return_value = cursor_cm
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = None

        with patch.object(store, "_ensure_schema"), patch.object(store, "_connect", return_value=conn):
            changed = store.change_password("user-1", "old-secret", "NewSecret123")

        self.assertTrue(changed)
        executed_sql = "\n".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        self.assertIn("UPDATE app_users", executed_sql)
        conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
