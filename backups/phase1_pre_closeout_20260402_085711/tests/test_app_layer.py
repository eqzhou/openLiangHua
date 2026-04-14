from __future__ import annotations

import json
import os
import shutil
import time
import unittest
import uuid
from pathlib import Path

import pandas as pd
import yaml

from src.app.repositories.config_repository import (
    load_experiment_config,
    load_watchlist_config,
    save_experiment_config,
)
from src.app.repositories.report_repository import load_latest_symbol_markdown
from src.app.services.watchlist_service import build_watchlist_view


TEST_TMP_ROOT = Path(__file__).resolve().parent / ".tmp"


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8")


class AppLayerTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.case_root = TEST_TMP_ROOT / f"app_layer_{uuid.uuid4().hex}"
        self.case_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root, ignore_errors=True)

    def test_config_repository_round_trips_experiment_and_defaults_watchlist(self) -> None:
        (self.case_root / "config").mkdir(parents=True, exist_ok=True)

        payload = {
            "label_name": "ret_t1_t10",
            "top_n": 20,
            "models": ["ridge", "lgbm", "ensemble"],
        }

        save_experiment_config(payload, self.case_root)

        self.assertEqual(load_experiment_config(self.case_root), payload)
        self.assertEqual(load_watchlist_config(self.case_root), {"holdings": []})

    def test_report_repository_loads_latest_symbol_markdown(self) -> None:
        reports_dir = self.case_root / "reports" / "weekly"
        older_path = reports_dir / "000078_watch_plan_2026-04-01.md"
        newer_path = reports_dir / "myquant_000078_watch_plan_2026-04-02.md"
        older_path.parent.mkdir(parents=True, exist_ok=True)
        older_path.write_text("older", encoding="utf-8")
        newer_path.write_text("newer", encoding="utf-8")

        now = time.time()
        os.utime(older_path, (now - 60, now - 60))
        os.utime(newer_path, (now, now))

        loaded = load_latest_symbol_markdown(
            "000078.SZ",
            "watch_plan",
            root=self.case_root,
            data_source="myquant",
        )

        self.assertEqual(loaded["name"], "myquant_000078_watch_plan_2026-04-02.md")
        self.assertEqual(loaded["plan_date"], "2026-04-02")
        self.assertEqual(loaded["content"], "newer")

    def test_watchlist_service_builds_holdings_with_discussion_and_inference_rank(self) -> None:
        reports_dir = self.case_root / "reports" / "weekly"
        response_path = reports_dir / "myquant_overlay_llm_responses.jsonl"
        inference_response_path = reports_dir / "myquant_overlay_inference_llm_responses.jsonl"
        _write_csv(
            reports_dir / "myquant_overlay_latest_candidates.csv",
            pd.DataFrame([{"ts_code": "000078.SZ"}]),
        )
        _write_json(
            reports_dir / "myquant_overlay_latest_packet.json",
            {
                "latest_date": "2026-03-18",
                "top_n": 1,
                "selected_candidates": [
                    {
                        "ts_code": "000078.SZ",
                        "thesis_summary": "Historical discussion accepted the setup.",
                        "action_hint": "Watch 3.45 first.",
                    }
                ],
                "llm_bridge": {
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "execution_status": "success",
                    "response_jsonl_path": str(response_path),
                },
            },
        )
        _write_jsonl(
            response_path,
            [
                {
                    "custom_id": "000078.SZ",
                    "status": "success",
                    "output_text": "Historical round says watch the 3.45 level.",
                }
            ],
        )

        _write_csv(
            reports_dir / "myquant_overlay_inference_candidates.csv",
            pd.DataFrame([{"ts_code": "000078.SZ"}]),
        )
        _write_json(
            reports_dir / "myquant_overlay_inference_packet.json",
            {
                "latest_date": "2026-04-01",
                "top_n": 1,
                "selected_candidates": [],
                "llm_bridge": {
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "execution_status": "export_only",
                    "response_jsonl_path": str(inference_response_path),
                },
            },
        )
        _write_jsonl(inference_response_path, [])

        daily_bar = pd.DataFrame(
            [
                {
                    "trade_date": "2026-03-31",
                    "ts_code": "000078.SZ",
                    "name": "Haiwang",
                    "close": 3.14,
                    "open": 3.10,
                    "high": 3.18,
                    "low": 3.06,
                    "pct_chg": -0.018,
                    "amount": 125_000_000,
                    "industry": "Pharma",
                },
                {
                    "trade_date": "2026-04-01",
                    "ts_code": "000078.SZ",
                    "name": "Haiwang",
                    "close": 3.45,
                    "open": 3.17,
                    "high": 3.45,
                    "low": 3.08,
                    "pct_chg": 0.0987,
                    "amount": 287_653_406,
                    "industry": "Pharma",
                },
            ]
        )
        daily_bar["trade_date"] = pd.to_datetime(daily_bar["trade_date"])

        ridge_predictions = pd.DataFrame(
            [
                {"trade_date": "2026-03-18", "ts_code": "000078.SZ", "name": "Haiwang", "score": 0.81},
                {"trade_date": "2026-03-18", "ts_code": "000001.SZ", "name": "Other", "score": 0.55},
            ]
        )
        lgbm_predictions = pd.DataFrame(
            [
                {"trade_date": "2026-03-18", "ts_code": "000078.SZ", "name": "Haiwang", "score": 0.84},
                {"trade_date": "2026-03-18", "ts_code": "000001.SZ", "name": "Other", "score": 0.31},
            ]
        )
        ensemble_predictions = pd.DataFrame(
            [
                {
                    "trade_date": "2026-03-18",
                    "ts_code": "000078.SZ",
                    "name": "Haiwang",
                    "score": 0.92,
                    "mom_5": 0.05,
                    "mom_20": 0.08,
                    "mom_60": -0.12,
                    "close_to_ma_20": 0.02,
                    "close_to_ma_60": -0.10,
                    "drawdown_60": -0.30,
                },
                {
                    "trade_date": "2026-03-18",
                    "ts_code": "000001.SZ",
                    "name": "Other",
                    "score": 0.30,
                    "mom_5": 0.01,
                    "mom_20": 0.02,
                    "mom_60": 0.03,
                    "close_to_ma_20": 0.01,
                    "close_to_ma_60": 0.03,
                    "drawdown_60": -0.08,
                },
            ]
        )
        inference_predictions = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "name": "Other", "score": 0.95},
                {"trade_date": "2026-04-01", "ts_code": "000078.SZ", "name": "Haiwang", "score": 0.62},
                {"trade_date": "2026-04-01", "ts_code": "000002.SZ", "name": "Else", "score": 0.40},
            ]
        )

        watchlist_view = build_watchlist_view(
            root=self.case_root,
            data_source="myquant",
            watchlist_config={
                "holdings": [
                    {
                        "ts_code": "000078.SZ",
                        "name": "Haiwang",
                        "cost": 3.851,
                        "shares": 15000,
                    }
                ]
            },
            daily_bar=daily_bar,
            ridge_predictions=ridge_predictions,
            lgbm_predictions=lgbm_predictions,
            ensemble_predictions=ensemble_predictions,
            overlay_candidates=pd.DataFrame([{"ts_code": "000078.SZ"}]),
            ensemble_inference_predictions=inference_predictions,
            overlay_inference_candidates=pd.DataFrame([{"ts_code": "000078.SZ"}]),
        )

        self.assertEqual(len(watchlist_view), 1)
        row = watchlist_view.iloc[0]
        self.assertEqual(row["ts_code"], "000078.SZ")
        self.assertEqual(int(row["ensemble_rank"]), 1)
        self.assertEqual(int(row["inference_ensemble_rank"]), 2)
        self.assertTrue(bool(row["is_overlay_selected"]))
        self.assertTrue(bool(row["is_inference_overlay_selected"]))
        self.assertEqual(int(row["llm_round_count"]), 2)
        self.assertEqual(int(row["llm_success_round_count"]), 1)
        discussion_snapshot = row["llm_discussion_snapshot"]
        round_summaries = [
            str(round_info.get("summary_text", ""))
            for round_info in discussion_snapshot.get("rounds", [])
        ]
        self.assertTrue(any("Historical round says watch the 3.45 level." in summary for summary in round_summaries))
        self.assertIn("3.45", str(row["premarket_plan"]))


if __name__ == "__main__":
    unittest.main()
