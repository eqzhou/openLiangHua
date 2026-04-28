from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import pandas as pd


class MarketBarsTushareSyncTests(unittest.TestCase):
    def test_normalize_qfq_bars_maps_tushare_columns(self) -> None:
        from src.data.market_bars_tushare_sync import normalize_qfq_bars

        raw = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260420",
                    "open": "10.1",
                    "high": "10.5",
                    "low": "10.0",
                    "close": "10.3",
                    "vol": "1200",
                    "amount": "12345.6",
                }
            ]
        )

        normalized = normalize_qfq_bars(raw)

        self.assertEqual(normalized["symbol"].tolist(), ["000001.SZ"])
        self.assertEqual(str(normalized["trade_date"].iloc[0].date()), "2026-04-20")
        self.assertEqual(int(normalized["volume"].iloc[0]), 1200)
        self.assertEqual(normalized["adjust_type"].tolist(), ["qfq"])
        self.assertEqual(normalized["source"].tolist(), ["tushare_pro_bar"])

    def test_sync_market_bars_uses_watchlist_and_upserts_qfq_rows(self) -> None:
        from src.data.market_bars_tushare_sync import sync_market_bars_from_tushare

        store = Mock()
        store.load_watchlist.return_value = {
            "holdings": [{"ts_code": "000001.SZ"}],
            "focus_pool": [{"ts_code": "000002.SZ"}],
        }

        fetched = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260420",
                    "open": 10.1,
                    "high": 10.5,
                    "low": 10.0,
                    "close": 10.3,
                    "vol": 1200,
                    "amount": 12345.6,
                }
            ]
        )

        with (
            patch("src.data.market_bars_tushare_sync.PostgresWatchlistStore", return_value=store),
            patch("src.data.market_bars_tushare_sync.get_api_settings", return_value=Mock()),
            patch("src.data.market_bars_tushare_sync.TushareClient", return_value=Mock()),
            patch(
                "src.data.market_bars_tushare_sync.load_latest_market_trade_dates",
                return_value={"000001.SZ": pd.Timestamp("2026-04-17"), "000002.SZ": pd.Timestamp("2026-04-17")},
            ),
            patch("src.data.market_bars_tushare_sync.fetch_qfq_bars", return_value=fetched) as fetch_bars,
            patch("src.data.market_bars_tushare_sync.upsert_market_bars", return_value=1) as upsert,
            patch("src.data.market_bars_tushare_sync.load_latest_market_trade_date", return_value=pd.Timestamp("2026-04-20")),
        ):
            summary = sync_market_bars_from_tushare(user_id="user-1", end_date="2026-04-20")

        store.load_watchlist.assert_called_once_with("user-1")
        fetch_bars.assert_called_once()
        self.assertEqual(fetch_bars.call_args.kwargs["symbols"], ["000001.SZ", "000002.SZ"])
        self.assertEqual(fetch_bars.call_args.kwargs["start_date"], "20260418")
        self.assertEqual(fetch_bars.call_args.kwargs["end_date"], "20260420")
        upsert.assert_called_once()
        self.assertEqual(summary.requested_symbols, 2)
        self.assertEqual(summary.fetched_rows, 1)
        self.assertEqual(summary.upserted_rows, 1)

    def test_sync_market_bars_uses_each_symbols_own_latest_date(self) -> None:
        from src.data.market_bars_tushare_sync import sync_market_bars_from_tushare

        with (
            patch("src.data.market_bars_tushare_sync.TushareClient", return_value=Mock()),
            patch(
                "src.data.market_bars_tushare_sync.load_latest_market_trade_dates",
                return_value={"000001.SZ": pd.Timestamp("2026-04-27"), "000002.SZ": None},
            ),
            patch("src.data.market_bars_tushare_sync.fetch_qfq_bars", return_value=pd.DataFrame()) as fetch_bars,
            patch("src.data.market_bars_tushare_sync.upsert_market_bars", return_value=0),
            patch("src.data.market_bars_tushare_sync.load_latest_market_trade_date", return_value=pd.Timestamp("2026-04-27")),
        ):
            sync_market_bars_from_tushare(symbols=["000001.SZ", "000002.SZ"], end_date="2026-04-28")

        self.assertEqual(
            fetch_bars.call_args.kwargs["start_dates_by_symbol"],
            {"000001.SZ": "20260428", "000002.SZ": "19900101"},
        )


if __name__ == "__main__":
    unittest.main()
