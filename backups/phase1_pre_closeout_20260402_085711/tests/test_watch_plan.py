from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd
import yaml

from src.agents.watch_plan import _next_trade_date, generate_watch_plans


TEST_TMP_ROOT = Path(__file__).resolve().parent / ".tmp"


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


class GenerateWatchPlansTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.case_root = TEST_TMP_ROOT / f"watch_plan_{uuid.uuid4().hex}"
        self.case_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root, ignore_errors=True)

    def test_generate_watch_plan_uses_manual_mark_and_canonical_trade_calendar(self) -> None:
        _write_yaml(
            self.case_root / "config" / "universe.yaml",
            {
                "data_source": "myquant",
            },
        )
        _write_yaml(
            self.case_root / "config" / "watchlist.yaml",
            {
                "holdings": [
                    {
                        "ts_code": "000078.SZ",
                        "name": "海王生物",
                        "cost": 3.851,
                        "shares": 15000,
                        "manual_mark_price": 3.45,
                        "manual_mark_date": "2026-04-01",
                        "manual_mark_note": "2026-04-01 涨停估算价",
                    }
                ]
            },
        )

        _write_parquet(
            self.case_root / "data" / "staging" / "myquant_daily_bar.parquet",
            pd.DataFrame(
                [
                    {
                        "trade_date": "2026-03-31",
                        "ts_code": "000078.SZ",
                        "name": "海王生物",
                        "close": 3.14,
                        "open": 3.10,
                        "high": 3.18,
                        "low": 3.06,
                        "pct_chg": -0.018,
                        "amount": 125_000_000,
                        "industry": "医药商业",
                    },
                    {
                        "trade_date": "2026-04-01",
                        "ts_code": "000078.SZ",
                        "name": "海王生物",
                        "close": None,
                        "open": None,
                        "high": None,
                        "low": None,
                        "pct_chg": None,
                        "amount": None,
                        "industry": "医药商业",
                    },
                ]
            ),
        )
        _write_parquet(
            self.case_root / "data" / "staging" / "myquant_trade_calendar.parquet",
            pd.DataFrame({"trade_date": ["2026-03-31", "2026-04-01"]}),
        )
        _write_parquet(
            self.case_root / "data" / "staging" / "trade_calendar.parquet",
            pd.DataFrame({"trade_date": ["2026-03-31", "2026-04-01", "2026-04-02"]}),
        )

        _write_csv(
            self.case_root / "reports" / "weekly" / "myquant_ridge_test_predictions.csv",
            pd.DataFrame(
                [
                    {"trade_date": "2026-03-17", "ts_code": "000078.SZ", "name": "海王生物", "score": 0.91},
                    {"trade_date": "2026-03-17", "ts_code": "000001.SZ", "name": "平安银行", "score": 0.42},
                ]
            ),
        )
        _write_csv(
            self.case_root / "reports" / "weekly" / "myquant_lgbm_test_predictions.csv",
            pd.DataFrame(
                [
                    {"trade_date": "2026-03-17", "ts_code": "000078.SZ", "name": "海王生物", "score": 0.72},
                    {"trade_date": "2026-03-17", "ts_code": "000001.SZ", "name": "平安银行", "score": 0.18},
                ]
            ),
        )
        _write_csv(
            self.case_root / "reports" / "weekly" / "myquant_ensemble_test_predictions.csv",
            pd.DataFrame(
                [
                    {
                        "trade_date": "2026-03-17",
                        "ts_code": "000078.SZ",
                        "name": "海王生物",
                        "score": 0.88,
                        "mom_5": 0.05,
                        "mom_20": 0.08,
                        "mom_60": -0.12,
                        "close_to_ma_20": 0.02,
                        "close_to_ma_60": -0.10,
                        "drawdown_60": -0.30,
                    },
                    {
                        "trade_date": "2026-03-17",
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "score": 0.35,
                        "mom_5": 0.01,
                        "mom_20": 0.03,
                        "mom_60": 0.02,
                        "close_to_ma_20": 0.01,
                        "close_to_ma_60": 0.02,
                        "drawdown_60": -0.08,
                    },
                ]
            ),
        )

        generated_paths = generate_watch_plans(root=self.case_root)

        self.assertEqual(len(generated_paths), 1)
        self.assertEqual(generated_paths[0].name, "000078_watch_plan_2026-04-02.md")

        content = generated_paths[0].read_text(encoding="utf-8")
        self.assertIn("日期：2026-04-02", content)
        self.assertIn("价格来源：2026-04-01 涨停估算价", content)
        self.assertIn("融合策略排名：1 / 2", content)
        self.assertIn("20日动量：8.00%", content)
        self.assertIn("量化信号日期可能早于参考价格日期", content)

    def test_next_trade_date_falls_back_to_next_business_day_when_calendar_has_no_future_day(self) -> None:
        trade_dates = pd.Series(pd.to_datetime(["2026-03-31", "2026-04-01"]))

        result = _next_trade_date(trade_dates, pd.Timestamp("2026-04-01"))

        self.assertEqual(str(result.date()), "2026-04-02")


if __name__ == "__main__":
    unittest.main()
