from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd

from src.agents.overlay_report import _load_industry_name_map


TEST_TMP_ROOT = Path(__file__).resolve().parent / ".tmp"


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


class OverlayReportTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.case_root = TEST_TMP_ROOT / f"overlay_report_{uuid.uuid4().hex}"
        self.case_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root, ignore_errors=True)

    def test_load_industry_name_map_supports_daily_bar_without_industry_current(self) -> None:
        _write_parquet(
            self.case_root / "data" / "staging" / "daily_bar.parquet",
            pd.DataFrame(
                [
                    {
                        "trade_date": "2026-04-01",
                        "ts_code": "002583.SZ",
                        "industry": "通信设备",
                    },
                    {
                        "trade_date": "2026-04-02",
                        "ts_code": "002583.SZ",
                        "industry": "通信设备",
                    },
                ]
            ),
        )

        mapping = _load_industry_name_map(self.case_root, "akshare")

        self.assertEqual(mapping["002583.SZ"], "通信设备")


if __name__ == "__main__":
    unittest.main()
