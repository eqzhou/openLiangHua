from __future__ import annotations

import unittest

from src.app.services.ai_shortlist_service import (
    build_trader_shortlist_markdown,
    build_trader_shortlist_rows,
)


class AiShortlistServiceTests(unittest.TestCase):
    def test_build_trader_shortlist_rows_prefers_llm_response_text(self) -> None:
        packet = {
            "selected_candidates": [
                {
                    "ts_code": "300567.SZ",
                    "name": "精测电子",
                    "action_hint": "重点跟踪",
                    "confidence_level": "高",
                    "final_score": 0.89,
                    "thesis_summary": "旧摘要",
                }
            ]
        }
        rows = build_trader_shortlist_rows(
            packet=packet,
            response_lookup={
                "300567.SZ": {
                    "output_text": "第一段。\n\n第二段。",
                }
            },
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ts_code"], "300567.SZ")
        self.assertEqual(rows[0]["trader_note"], "第一段。")

    def test_build_trader_shortlist_markdown_renders_compact_list(self) -> None:
        markdown = build_trader_shortlist_markdown(
            data_source="tushare",
            latest_date="2026-04-16",
            rows=[
                {
                    "rank": 1,
                    "ts_code": "300567.SZ",
                    "name": "精测电子",
                    "action_hint": "重点跟踪",
                    "confidence_level": "高",
                    "final_score": 0.89,
                    "trader_note": "保持跟踪，等待验证。",
                }
            ],
        )

        self.assertIn("AI 交易员 Shortlist", markdown)
        self.assertIn("300567.SZ 精测电子", markdown)
        self.assertIn("保持跟踪，等待验证。", markdown)
