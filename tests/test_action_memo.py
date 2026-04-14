from __future__ import annotations

import json
import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd
import yaml

from src.agents.action_memo import generate_action_memos


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


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")


class GenerateActionMemosTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.case_root = TEST_TMP_ROOT / f"action_memo_{uuid.uuid4().hex}"
        self.case_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root, ignore_errors=True)

    def test_generate_action_memo_includes_manual_mark_status(self) -> None:
        _write_yaml(self.case_root / "config" / "universe.yaml", {"data_source": "myquant"})
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
                        "open": 3.16,
                        "high": 3.24,
                        "low": 3.13,
                        "pct_chg": -0.01875,
                        "amount": 187653406.00,
                        "industry": "医药商业",
                    }
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

        generated_paths = generate_action_memos(root=self.case_root)

        self.assertEqual(len(generated_paths), 1)
        self.assertEqual(generated_paths[0].name, "000078_action_memo_2026-04-02.md")
        content = generated_paths[0].read_text(encoding="utf-8")
        self.assertIn("价格状态：`手工参考价(日线未到)`", content)
        self.assertIn("最新落库日线日期：`2026-03-31`", content)
        self.assertIn("融合策略排名：1 / 2", content)
        self.assertIn("盘前执行清单", content)

    def test_generate_action_memo_writes_llm_discussion_rounds(self) -> None:
        _write_yaml(self.case_root / "config" / "universe.yaml", {"data_source": "myquant"})
        _write_yaml(
            self.case_root / "config" / "watchlist.yaml",
            {"holdings": [{"ts_code": "000078.SZ", "name": "海王生物", "cost": 3.851, "shares": 15000}]},
        )
        _write_parquet(
            self.case_root / "data" / "staging" / "myquant_daily_bar.parquet",
            pd.DataFrame(
                [
                    {
                        "trade_date": "2026-04-01",
                        "ts_code": "000078.SZ",
                        "name": "海王生物",
                        "close": 3.45,
                        "open": 3.17,
                        "high": 3.45,
                        "low": 3.08,
                        "pct_chg": 0.0987,
                        "amount": 287653406.00,
                        "industry": "医药商业",
                    }
                ]
            ),
        )
        _write_parquet(
            self.case_root / "data" / "staging" / "trade_calendar.parquet",
            pd.DataFrame({"trade_date": ["2026-04-01", "2026-04-02"]}),
        )
        _write_csv(
            self.case_root / "reports" / "weekly" / "myquant_ridge_test_predictions.csv",
            pd.DataFrame([{"trade_date": "2026-03-18", "ts_code": "000078.SZ", "name": "海王生物", "score": 0.91}]),
        )
        _write_csv(
            self.case_root / "reports" / "weekly" / "myquant_lgbm_test_predictions.csv",
            pd.DataFrame([{"trade_date": "2026-03-18", "ts_code": "000078.SZ", "name": "海王生物", "score": 0.72}]),
        )
        _write_csv(
            self.case_root / "reports" / "weekly" / "myquant_ensemble_test_predictions.csv",
            pd.DataFrame(
                [
                    {
                        "trade_date": "2026-03-18",
                        "ts_code": "000078.SZ",
                        "name": "海王生物",
                        "score": 0.88,
                        "mom_5": 0.05,
                        "mom_20": 0.08,
                        "mom_60": -0.12,
                        "close_to_ma_20": 0.02,
                        "close_to_ma_60": -0.10,
                        "drawdown_60": -0.30,
                    }
                ]
            ),
        )

        reports_dir = self.case_root / "reports" / "weekly"
        response_path = reports_dir / "myquant_overlay_inference_llm_responses.jsonl"
        _write_jsonl(
            response_path,
            [
                {
                    "custom_id": "000078.SZ",
                    "status": "success",
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "output_text": "外部模型判断这是资金修复而非中期反转，明天更适合看 3.45 是否站稳。",
                }
            ],
        )
        _write_json(
            reports_dir / "myquant_overlay_inference_packet.json",
            {
                "latest_date": "2026-04-01",
                "top_n": 10,
                "selected_candidates": [
                    {
                        "ts_code": "000078.SZ",
                        "action_hint": "继续观察",
                        "thesis_summary": "更像修复观察，不宜直接当成中期反转。",
                    }
                ],
                "llm_bridge": {
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "execution_status": "executed",
                    "response_jsonl_path": str(response_path),
                },
            },
        )

        generated_paths = generate_action_memos(root=self.case_root)

        content = generated_paths[0].read_text(encoding="utf-8")
        self.assertIn("多轮 AI 研讨回写", content)
        self.assertIn("最新推理研讨", content)
        self.assertIn("已完成", content)
        self.assertIn("3.45 是否站稳", content)


if __name__ == "__main__":
    unittest.main()
