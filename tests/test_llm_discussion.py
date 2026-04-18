from __future__ import annotations

import json
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from src.utils.llm_discussion import discussion_round_rows, load_symbol_discussion_snapshot


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


class LlmDiscussionSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.case_root = TEST_TMP_ROOT / f"llm_discussion_{uuid.uuid4().hex}"
        self.case_root.mkdir(parents=True, exist_ok=True)
        _write_yaml(self.case_root / "config" / "universe.yaml", {"data_source": "myquant"})

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root, ignore_errors=True)

    def test_load_symbol_discussion_snapshot_prefers_successful_response(self) -> None:
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
                    "output_text": "最新推理认为明天重点看 3.45 附近承接。",
                }
            ],
        )
        _write_json(
            reports_dir / "myquant_overlay_inference_packet.json",
            {
                "latest_date": "2026-04-01",
                "top_n": 10,
                "selected_candidates": [{"ts_code": "000078.SZ", "thesis_summary": "更像修复观察。"}],
                "llm_bridge": {
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "execution_status": "executed",
                    "response_jsonl_path": str(response_path),
                },
            },
        )

        snapshot = load_symbol_discussion_snapshot(self.case_root, "myquant", "000078.SZ")

        self.assertEqual(snapshot["round_count"], 1)
        self.assertEqual(snapshot["success_round_count"], 1)
        self.assertEqual(snapshot["latest_status"], "已完成")
        self.assertIn("3.45", snapshot["latest_summary"])
        self.assertEqual(discussion_round_rows(snapshot)[0]["状态"], "已完成")

    def test_load_symbol_discussion_snapshot_marks_candidate_pool_only(self) -> None:
        reports_dir = self.case_root / "reports" / "weekly"
        _write_csv(
            reports_dir / "myquant_overlay_latest_candidates.csv",
            pd.DataFrame([{"ts_code": "000078.SZ"}, {"ts_code": "000001.SZ"}]),
        )
        _write_json(
            reports_dir / "myquant_overlay_latest_packet.json",
            {
                "latest_date": "2026-03-18",
                "top_n": 1,
                "selected_candidates": [{"ts_code": "000001.SZ"}],
                "llm_bridge": {
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "execution_status": "export_only",
                    "blocking_reason": "缺少 OPENAI_API_KEY。",
                },
            },
        )

        snapshot = load_symbol_discussion_snapshot(self.case_root, "myquant", "000078.SZ")

        self.assertEqual(snapshot["round_count"], 1)
        self.assertEqual(snapshot["selected_round_count"], 0)
        self.assertEqual(snapshot["latest_status"], "仅在候选池")
        self.assertIn("未进入前 1 名自动研讨名单", snapshot["latest_summary"])

    def test_load_symbol_discussion_snapshot_prefers_repository_backed_artifacts(self) -> None:
        with (
            patch(
                "src.utils.llm_discussion.load_overlay_inference_packet",
                return_value={
                    "latest_date": "2026-04-01",
                    "top_n": 5,
                    "selected_candidates": [{"ts_code": "000078.SZ", "thesis_summary": "仓位更适合观察。"}],
                    "llm_bridge": {"execution_status": "executed"},
                },
            ),
            patch(
                "src.utils.llm_discussion.load_overlay_inference_candidates",
                return_value=pd.DataFrame([{"ts_code": "000078.SZ"}]),
            ),
            patch(
                "src.utils.llm_discussion.load_overlay_llm_bundle",
                return_value={
                    "response_lookup": {
                        "000078.SZ": {
                            "custom_id": "000078.SZ",
                            "status": "success",
                            "output_text": "数据库里的研讨结论提到 3.45 一线承接。",
                        }
                    },
                    "response_summary": "数据库摘要",
                },
            ),
            patch("src.utils.llm_discussion.load_overlay_packet", return_value={}),
            patch("src.utils.llm_discussion.load_overlay_candidates", return_value=pd.DataFrame()),
        ):
            snapshot = load_symbol_discussion_snapshot(self.case_root, "myquant", "000078.SZ")

        self.assertEqual(snapshot["round_count"], 1)
        self.assertEqual(snapshot["success_round_count"], 1)
        self.assertIn("3.45", snapshot["latest_summary"])


if __name__ == "__main__":
    unittest.main()
