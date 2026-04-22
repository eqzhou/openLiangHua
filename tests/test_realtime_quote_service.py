from __future__ import annotations

import unittest

import pandas as pd

import src.app.services.realtime_quote_service as realtime_quote_service
from src.app.services.realtime_quote_service import (
    fetch_managed_realtime_quotes,
    fetch_realtime_quotes,
    merge_realtime_quote_record,
    merge_realtime_quote_records,
    merge_realtime_quotes,
)
from src.db.realtime_quote_store import RealtimeQuoteSnapshot


class _FakeRealtimeQuoteStore:
    def __init__(self, snapshots: dict[tuple[str, str], RealtimeQuoteSnapshot] | None = None) -> None:
        self.snapshots = dict(snapshots or {})
        self.upserts: list[tuple[str, str, pd.DataFrame, dict[str, object]]] = []

    def get_snapshot(self, *, trade_date: str | object, snapshot_bucket: str):
        key = (str(pd.Timestamp(trade_date).date()), snapshot_bucket)
        return self.snapshots.get(key)

    def upsert_snapshot(self, *, trade_date: str | object, snapshot_bucket: str, quotes: pd.DataFrame, status: dict[str, object]) -> None:
        key = (str(pd.Timestamp(trade_date).date()), snapshot_bucket)
        snapshot = RealtimeQuoteSnapshot(
            trade_date=key[0],
            snapshot_bucket=snapshot_bucket,
            quotes=quotes.copy(),
            status=dict(status),
        )
        self.snapshots[key] = snapshot
        self.upserts.append((key[0], snapshot_bucket, quotes.copy(), dict(status)))

    def get_latest_snapshot(self):
        if not self.snapshots:
            return None
        ordered = sorted(
            self.snapshots.items(),
            key=lambda item: (item[0][0], 0 if item[0][1] == "post_close" else 1),
            reverse=True,
        )
        return ordered[0][1]


class RealtimeQuoteServiceTests(unittest.TestCase):
    def test_fetch_realtime_quotes_prefers_raw_sina_quote_payload(self) -> None:
        def quote_fetcher(*, symbols: list[str]) -> str:
            self.assertEqual(symbols, ["sz000078"])
            return (
                'var hq_str_sz000078="海王生物,3.450,3.450,3.800,3.800,3.750,3.800,3.810,250000,944800,'
                '0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-04-02,09:31:00,00,";'
            )

        quotes, status = fetch_realtime_quotes(
            ["000078.SZ"],
            previous_close_lookup={"000078.SZ": 3.45},
            trade_date=pd.Timestamp("2026-04-02"),
            quote_fetcher=quote_fetcher,
            tick_fetcher=None,
            minute_fetcher=None,
        )

        self.assertTrue(bool(status["available"]))
        self.assertEqual(status["source"], "sina-quote")
        self.assertEqual(len(quotes), 1)

        row = quotes.iloc[0]
        self.assertEqual(row["ts_code"], "000078.SZ")
        self.assertAlmostEqual(float(row["realtime_price"]), 3.80)
        self.assertAlmostEqual(float(row["realtime_open"]), 3.45)
        self.assertAlmostEqual(float(row["realtime_high"]), 3.80)
        self.assertAlmostEqual(float(row["realtime_low"]), 3.75)
        self.assertAlmostEqual(float(row["realtime_volume"]), 250000.0)
        self.assertAlmostEqual(float(row["realtime_amount"]), 944800.0)
        self.assertAlmostEqual(float(row["realtime_avg_price"]), 944800.0 / 250000.0)
        self.assertEqual(pd.Timestamp(row["realtime_time"]), pd.Timestamp("2026-04-02 09:31:00"))
        self.assertAlmostEqual(float(row["realtime_prev_close"]), 3.45)
        self.assertAlmostEqual(float(row["realtime_change"]), 0.35)
        self.assertAlmostEqual(float(row["realtime_pct_chg"]), 3.80 / 3.45 - 1.0)
        self.assertAlmostEqual(float(row["realtime_amplitude"]), (3.80 - 3.75) / 3.45)
        self.assertTrue(bool(row["realtime_is_limit_up"]))
        self.assertEqual(row["realtime_quote_source"], "sina-quote")
        self.assertEqual(row["realtime_trade_date"], "2026-04-02")

    def test_fetch_realtime_quotes_builds_snapshot_from_tick_bars(self) -> None:
        def quote_fetcher(*, symbols: list[str]) -> str:
            self.assertEqual(symbols, ["sz000078"])
            return ""

        def tick_fetcher(*, symbol: str) -> pd.DataFrame:
            self.assertEqual(symbol, "000078")
            return pd.DataFrame(
                [
                    {
                        "时间": "09:25:00",
                        "成交价": 3.79,
                        "手数": 1200,
                        "买卖盘性质": "中性盘",
                    },
                    {
                        "时间": "09:30:00",
                        "成交价": 3.75,
                        "手数": 800,
                        "买卖盘性质": "中性盘",
                    },
                    {
                        "时间": "09:31:00",
                        "成交价": 3.80,
                        "手数": 500,
                        "买卖盘性质": "买盘",
                    },
                ]
            )

        quotes, status = fetch_realtime_quotes(
            ["000078.SZ"],
            previous_close_lookup={"000078.SZ": 3.45},
            trade_date=pd.Timestamp("2026-04-02"),
            quote_fetcher=quote_fetcher,
            tick_fetcher=tick_fetcher,
            minute_fetcher=None,
        )

        self.assertTrue(bool(status["available"]))
        self.assertEqual(int(status["success_symbol_count"]), 1)
        self.assertEqual(int(status["requested_symbol_count"]), 1)
        self.assertEqual(status["failed_symbols"], [])
        self.assertEqual(len(quotes), 1)

        row = quotes.iloc[0]
        self.assertEqual(row["ts_code"], "000078.SZ")
        self.assertAlmostEqual(float(row["realtime_price"]), 3.80)
        self.assertAlmostEqual(float(row["realtime_open"]), 3.79)
        self.assertAlmostEqual(float(row["realtime_high"]), 3.80)
        self.assertAlmostEqual(float(row["realtime_low"]), 3.75)
        self.assertAlmostEqual(float(row["realtime_volume"]), 250000.0)
        self.assertAlmostEqual(float(row["realtime_amount"]), 944800.0)
        self.assertAlmostEqual(float(row["realtime_avg_price"]), 944800.0 / 250000.0)
        self.assertEqual(pd.Timestamp(row["realtime_time"]), pd.Timestamp("2026-04-02 09:31:00"))
        self.assertAlmostEqual(float(row["realtime_prev_close"]), 3.45)
        self.assertAlmostEqual(float(row["realtime_change"]), 0.35)
        self.assertAlmostEqual(float(row["realtime_pct_chg"]), 3.80 / 3.45 - 1.0)
        self.assertAlmostEqual(float(row["realtime_amplitude"]), (3.80 - 3.75) / 3.45)
        self.assertTrue(bool(row["realtime_is_limit_up"]))
        self.assertEqual(row["realtime_quote_source"], "eastmoney-tick")
        self.assertEqual(row["realtime_trade_date"], "2026-04-02")

    def test_fetch_realtime_quotes_falls_back_to_minute_bars(self) -> None:
        def quote_fetcher(*, symbols: list[str]) -> str:
            self.assertEqual(symbols, ["sz000078"])
            return ""

        def tick_fetcher(*, symbol: str) -> pd.DataFrame:
            raise RuntimeError("tick unavailable")

        def minute_fetcher(*, symbol: str, start_date: str, end_date: str, period: str, adjust: str) -> pd.DataFrame:
            self.assertEqual(symbol, "000078")
            self.assertEqual(start_date, "2026-04-02 09:30:00")
            self.assertEqual(end_date, "2026-04-02 15:00:00")
            self.assertEqual(period, "1")
            self.assertEqual(adjust, "")
            return pd.DataFrame(
                [
                    {
                        "时间": "2026-04-02 09:30:00",
                        "开盘": 3.79,
                        "收盘": 3.79,
                        "最高": 3.79,
                        "最低": 3.75,
                        "成交量": 120000,
                        "成交额": 454800,
                        "均价": 3.79,
                    },
                    {
                        "时间": "2026-04-02 09:31:00",
                        "开盘": 3.79,
                        "收盘": 3.80,
                        "最高": 3.80,
                        "最低": 3.78,
                        "成交量": 80000,
                        "成交额": 304000,
                        "均价": 3.795,
                    },
                ]
            )

        quotes, status = fetch_realtime_quotes(
            ["000078.SZ"],
            previous_close_lookup={"000078.SZ": 3.45},
            trade_date=pd.Timestamp("2026-04-02"),
            quote_fetcher=quote_fetcher,
            tick_fetcher=tick_fetcher,
            minute_fetcher=minute_fetcher,
        )

        self.assertTrue(bool(status["available"]))
        self.assertEqual(status["source"], "eastmoney-minute")
        self.assertEqual(len(quotes), 1)
        self.assertEqual(quotes.iloc[0]["realtime_quote_source"], "eastmoney-minute")
        self.assertAlmostEqual(float(quotes.iloc[0]["realtime_price"]), 3.80)

    def test_fetch_realtime_quotes_reports_failed_symbols(self) -> None:
        def quote_fetcher(*, symbols: list[str]) -> str:
            self.assertEqual(symbols, ["sz000001", "sz000002"])
            return ""

        def tick_fetcher(*, symbol: str) -> pd.DataFrame:
            raise RuntimeError("tick unavailable")

        def minute_fetcher(*, symbol: str, start_date: str, end_date: str, period: str, adjust: str) -> pd.DataFrame:
            if symbol == "000001":
                raise RuntimeError("no data")
            return pd.DataFrame(
                [
                    {
                        "时间": "2026-04-02 09:30:00",
                        "开盘": 10.0,
                        "收盘": 10.1,
                        "最高": 10.1,
                        "最低": 10.0,
                        "成交量": 1000,
                        "成交额": 10100,
                        "均价": 10.1,
                    }
                ]
            )

        quotes, status = fetch_realtime_quotes(
            ["000001.SZ", "000002.SZ"],
            previous_close_lookup={"000002.SZ": 10.0},
            trade_date=pd.Timestamp("2026-04-02"),
            quote_fetcher=quote_fetcher,
            tick_fetcher=tick_fetcher,
            minute_fetcher=minute_fetcher,
        )

        self.assertTrue(bool(status["available"]))
        self.assertEqual(int(status["success_symbol_count"]), 1)
        self.assertEqual(status["failed_symbols"], ["000001.SZ"])
        self.assertIn("000001.SZ", str(status["error_message"]))
        self.assertEqual(quotes["ts_code"].tolist(), ["000002.SZ"])

    def test_merge_realtime_quotes_calculates_realtime_pnl_fields(self) -> None:
        watchlist_view = pd.DataFrame(
            [
                {
                    "ts_code": "000078.SZ",
                    "cost_basis": 3.851,
                    "shares": 15000,
                    "mark_price": 3.45,
                }
            ]
        )
        realtime_quotes = pd.DataFrame(
            [
                {
                    "ts_code": "000078.SZ",
                    "realtime_price": 3.80,
                    "realtime_time": pd.Timestamp("2026-04-02 09:31:00"),
                }
            ]
        )

        merged = merge_realtime_quotes(watchlist_view, realtime_quotes)

        self.assertEqual(len(merged), 1)
        row = merged.iloc[0]
        self.assertAlmostEqual(float(row["realtime_market_value"]), 57000.0)
        self.assertAlmostEqual(float(row["realtime_unrealized_pnl"]), (3.80 - 3.851) * 15000)
        self.assertAlmostEqual(float(row["realtime_unrealized_pnl_pct"]), 3.80 / 3.851 - 1.0)
        self.assertAlmostEqual(float(row["realtime_vs_mark_pct"]), 3.80 / 3.45 - 1.0)

    def test_merge_realtime_quotes_tolerates_partial_watchlist_records(self) -> None:
        watchlist_view = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "示例股票",
                    "latest_bar_close": 9.8,
                }
            ]
        )
        realtime_quotes = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "realtime_price": 10.5,
                    "realtime_time": pd.Timestamp("2026-04-02 10:00:00"),
                }
            ]
        )

        merged = merge_realtime_quotes(watchlist_view, realtime_quotes)

        self.assertEqual(len(merged), 1)
        row = merged.iloc[0]
        self.assertAlmostEqual(float(row["realtime_price"]), 10.5)
        self.assertTrue(pd.isna(row["realtime_market_value"]))
        self.assertTrue(pd.isna(row["realtime_unrealized_pnl"]))
        self.assertTrue(pd.isna(row["realtime_unrealized_pnl_pct"]))
        self.assertTrue(pd.isna(row["realtime_vs_mark_pct"]))

    def test_merge_realtime_quote_record_updates_single_record(self) -> None:
        record = {
            "ts_code": "000078.SZ",
            "cost_basis": 3.851,
            "shares": 15000,
            "mark_price": 3.45,
        }
        realtime_quote = {
            "ts_code": "000078.SZ",
            "realtime_price": 3.80,
            "realtime_time": pd.Timestamp("2026-04-02 09:31:00"),
        }

        merged = merge_realtime_quote_record(record, realtime_quote)

        self.assertAlmostEqual(float(merged["realtime_market_value"]), 57000.0)
        self.assertAlmostEqual(float(merged["realtime_unrealized_pnl"]), (3.80 - 3.851) * 15000)
        self.assertAlmostEqual(float(merged["realtime_unrealized_pnl_pct"]), 3.80 / 3.851 - 1.0)
        self.assertAlmostEqual(float(merged["realtime_vs_mark_pct"]), 3.80 / 3.45 - 1.0)

    def test_merge_realtime_quote_records_updates_matching_symbols_only(self) -> None:
        records = [
            {"ts_code": "000001.SZ", "mark_price": 10.0},
            {"ts_code": "000002.SZ", "mark_price": 8.0},
        ]
        realtime_quotes = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "realtime_price": 10.5,
                    "realtime_time": pd.Timestamp("2026-04-02 10:00:00"),
                }
            ]
        )

        merged = merge_realtime_quote_records(records, realtime_quotes)

        self.assertAlmostEqual(float(merged[0]["realtime_price"]), 10.5)
        self.assertNotIn("realtime_price", merged[1])

    def test_fetch_managed_realtime_quotes_reuses_post_close_snapshot_after_market_close(self) -> None:
        cached_quotes = pd.DataFrame(
            [
                {
                    "ts_code": "000078.SZ",
                    "realtime_price": 3.61,
                    "realtime_time": pd.Timestamp("2026-04-03 15:01:00"),
                    "realtime_quote_source": "sina-quote",
                }
            ]
        )
        fake_store = _FakeRealtimeQuoteStore(
            {
                ("2026-04-03", "post_close"): RealtimeQuoteSnapshot(
                    trade_date="2026-04-03",
                    snapshot_bucket="post_close",
                    quotes=cached_quotes,
                    status={
                        "available": True,
                        "source": "sina-quote",
                        "trade_date": "2026-04-03",
                        "fetched_at": "2026-04-03T15:01:02+08:00",
                        "requested_symbol_count": 1,
                        "success_symbol_count": 1,
                        "failed_symbols": [],
                        "error_message": "",
                    },
                )
            }
        )
        original_store_getter = realtime_quote_service.get_realtime_quote_store
        try:
            realtime_quote_service.get_realtime_quote_store = lambda: fake_store

            quotes, status = fetch_managed_realtime_quotes(
                ["000078.SZ"],
                previous_close_lookup={"000078.SZ": 3.45},
                trade_date=pd.Timestamp("2026-04-03 15:08:00", tz="Asia/Shanghai"),
                quote_fetcher=lambda **_: self.fail("post-close cache should prevent provider requests"),
            )
        finally:
            realtime_quote_service.get_realtime_quote_store = original_store_getter

        self.assertEqual(len(quotes), 1)
        self.assertAlmostEqual(float(quotes.iloc[0]["realtime_price"]), 3.61)
        self.assertEqual(status["snapshot_bucket"], "post_close")
        self.assertEqual(status["served_from"], "database")

    def test_fetch_managed_realtime_quotes_persists_post_close_snapshot(self) -> None:
        fake_store = _FakeRealtimeQuoteStore()
        original_store_getter = realtime_quote_service.get_realtime_quote_store
        try:
            realtime_quote_service.get_realtime_quote_store = lambda: fake_store

            quotes, status = fetch_managed_realtime_quotes(
                ["000078.SZ"],
                previous_close_lookup={"000078.SZ": 3.45},
                trade_date=pd.Timestamp("2026-04-03 15:05:00", tz="Asia/Shanghai"),
                quote_fetcher=lambda **_: (
                    'var hq_str_sz000078="海王生物,3.450,3.450,3.610,3.620,3.580,3.600,3.620,100000,361000,'
                    '0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-04-03,15:01:00,00,";'
                ),
                tick_fetcher=None,
                minute_fetcher=None,
            )
        finally:
            realtime_quote_service.get_realtime_quote_store = original_store_getter

        self.assertEqual(len(quotes), 1)
        self.assertEqual(status["snapshot_bucket"], "post_close")
        self.assertEqual(status["served_from"], "provider")
        self.assertEqual(len(fake_store.upserts), 2)
        self.assertEqual(fake_store.upserts[0][1], "post_close")
        self.assertEqual(fake_store.upserts[1][1], "latest")


if __name__ == "__main__":
    unittest.main()
