from __future__ import annotations

import unittest

from src.app.pages.watchlist_page import (
    describe_realtime_source,
    format_realtime_timestamp,
    summarize_failed_symbols,
)


class WatchlistPageTests(unittest.TestCase):
    def test_describe_realtime_source_uses_chinese_labels(self) -> None:
        self.assertEqual(describe_realtime_source("sina-quote"), "新浪实时报价")
        self.assertEqual(describe_realtime_source("eastmoney-minute"), "东财分时回退")
        self.assertEqual(describe_realtime_source(""), "暂未刷新")

    def test_format_realtime_timestamp_returns_hour_minute(self) -> None:
        self.assertEqual(format_realtime_timestamp("2026-04-03T15:45:03"), "15:45")
        self.assertEqual(format_realtime_timestamp(None), "暂未刷新")

    def test_summarize_failed_symbols_compacts_long_lists(self) -> None:
        self.assertEqual(summarize_failed_symbols([]), "无")
        self.assertEqual(summarize_failed_symbols(["000078.SZ", "002583.SZ"]), "000078.SZ、002583.SZ")
        self.assertEqual(
            summarize_failed_symbols(["000078.SZ", "002583.SZ", "600487.SH", "600339.SH"]),
            "000078.SZ、002583.SZ、600487.SH 等 4 只",
        )


if __name__ == "__main__":
    unittest.main()
