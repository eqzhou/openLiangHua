from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.agents.news_context import _fetch_notice_day, _fetch_research_reports, _summarize_news


class NewsContextTests(unittest.TestCase):
    def test_fetch_notice_day_prefers_repository_cache(self) -> None:
        cached = pd.DataFrame([{"代码": "000001", "公告标题": "缓存公告"}])

        with (
            patch("src.agents.news_context.repo_load_notice_cache", return_value=cached.copy()),
            patch("src.agents.news_context.ak.stock_notice_report", side_effect=AssertionError("should not fetch remotely")),
        ):
            frame = _fetch_notice_day(Path("/tmp/event-cache"), pd.Timestamp("2026-04-03"), "myquant")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["公告标题"], "缓存公告")

    def test_fetch_research_reports_prefers_repository_cache(self) -> None:
        cached = pd.DataFrame([{"日期": "2026-04-03", "报告名称": "缓存研报"}])

        with (
            patch("src.agents.news_context.repo_load_research_cache", return_value=cached.copy()),
            patch("src.agents.news_context.ak.stock_research_report_em", side_effect=AssertionError("should not fetch remotely")),
        ):
            frame = _fetch_research_reports(Path("/tmp/event-cache"), "000001.SZ", "myquant")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["报告名称"], "缓存研报")

    def test_summarize_news_prefers_repository_cache(self) -> None:
        cached = pd.DataFrame(
            [
                {
                    "col0": "000001",
                    "标题": "缓存新闻",
                    "col2": "",
                    "发布时间": "2026-04-03 09:30:00",
                    "文章来源": "缓存源",
                }
            ]
        )

        with (
            patch("src.agents.news_context.repo_load_news_cache", return_value=cached.copy()),
            patch("src.agents.news_context.ak.stock_news_em", side_effect=AssertionError("should not fetch remotely")),
        ):
            summary = _summarize_news(
                cache_dir=Path("/tmp/event-cache"),
                ts_code="000001.SZ",
                as_of_date=pd.Timestamp("2026-04-03"),
                data_source="myquant",
                lookback_days=7,
                max_items=3,
            )

        self.assertEqual(summary["news_count"], 1)
        self.assertIn("缓存新闻", str(summary["news_digest"]))


if __name__ == "__main__":
    unittest.main()
