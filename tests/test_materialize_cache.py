from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.data.materialize_cache import materialize_cache


class MaterializeCacheTests(unittest.TestCase):
    def test_materialize_cache_returns_noop_summary_when_no_cache_files_are_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "data" / "raw" / "akshare").mkdir(parents=True)
            (root / "reports" / "weekly").mkdir(parents=True)

            with patch("src.data.materialize_cache.project_root", return_value=root):
                summary = materialize_cache(min_age_seconds=30)

            self.assertEqual(summary["status"], "noop")
            self.assertEqual(summary["cached_files_materialized"], 0)
            self.assertEqual(summary["rows_in_panel"], 0)

            summary_path = root / "reports" / "weekly" / "materialize_cache_summary.json"
            self.assertTrue(summary_path.exists())
            saved = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["status"], "noop")
            self.assertIn("No cached symbol files were safe to materialize yet.", saved["message"])


if __name__ == "__main__":
    unittest.main()
