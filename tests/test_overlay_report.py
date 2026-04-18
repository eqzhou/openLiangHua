from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.agents.overlay_report import (
    _load_industry_name_map,
    _load_metrics,
    _load_portfolio,
    _load_predictions,
)


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

    def test_overlay_report_loaders_prefer_repository_contracts(self) -> None:
        prediction_frame = pd.DataFrame([{"trade_date": "2026-04-03", "ts_code": "000001.SZ", "score": 0.9}])
        portfolio_frame = pd.DataFrame([{"trade_date": "2026-04-03", "risk_on": True}])
        metrics_payload = {"daily_portfolio_sharpe": 1.2}

        with (
            patch("src.agents.overlay_report.repo_load_predictions", return_value=prediction_frame.copy()),
            patch("src.agents.overlay_report.repo_load_portfolio", return_value=portfolio_frame.copy()),
            patch("src.agents.overlay_report.repo_load_metrics", return_value=dict(metrics_payload)),
        ):
            loaded_predictions = _load_predictions(self.case_root, "myquant", "lgbm", "test")
            loaded_portfolio = _load_portfolio(self.case_root, "myquant", "lgbm", "test")
            loaded_metrics = _load_metrics(self.case_root, "myquant", "lgbm", "test")

        self.assertEqual(len(loaded_predictions), 1)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(loaded_predictions["trade_date"]))
        self.assertEqual(len(loaded_portfolio), 1)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(loaded_portfolio["trade_date"]))
        self.assertEqual(loaded_metrics["daily_portfolio_sharpe"], 1.2)


if __name__ == "__main__":
    unittest.main()
