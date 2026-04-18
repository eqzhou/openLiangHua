from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from src.db.schema import run_sql_script
from src.db.settings import get_database_settings


class DatabaseSettingsTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_database_settings.cache_clear()

    def test_database_defaults_use_safe_placeholders(self) -> None:
        with patch("src.db.settings.load_dotenv", return_value=False), patch.dict(os.environ, {}, clear=True):
            get_database_settings.cache_clear()

            settings = get_database_settings()

        self.assertEqual(settings.host, "localhost")
        self.assertEqual(settings.port, 5432)
        self.assertEqual(settings.name, "replace_with_database_name")
        self.assertEqual(settings.schema, "replace_with_database_schema")
        self.assertEqual(settings.user, "replace_with_database_user")
        self.assertEqual(settings.password, "")
        self.assertEqual(settings.connect_timeout, 5)

    def test_run_sql_script_does_not_bootstrap_database_by_default(self) -> None:
        with TemporaryDirectory() as temp_dir:
            script_path = Path(temp_dir) / "schema.sql"
            script_path.write_text("select 1;", encoding="utf-8")

            cursor = MagicMock()
            cursor_cm = MagicMock()
            cursor_cm.__enter__.return_value = cursor
            cursor_cm.__exit__.return_value = None

            conn = MagicMock()
            conn.cursor.return_value = cursor_cm
            conn_cm = MagicMock()
            conn_cm.__enter__.return_value = conn
            conn_cm.__exit__.return_value = None

            with patch("src.db.schema.ensure_database_schema_exists") as ensure_schema, patch(
                "src.db.schema.connect_database",
                return_value=conn_cm,
            ):
                run_sql_script(script_path)

        ensure_schema.assert_not_called()
        cursor.execute.assert_called_once_with("select 1;")
        conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
