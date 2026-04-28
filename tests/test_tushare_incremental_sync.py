from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pandas as pd

from src.utils.io import ensure_dir


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    frame.to_parquet(path, index=False)


class TushareIncrementalSyncTests(unittest.TestCase):
    def test_load_existing_inputs_prefers_database_artifacts_for_primary_root(self) -> None:
        from src.data.tushare_incremental_sync import _load_existing_inputs

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            staging_dir = ensure_dir(root / "data" / "staging")
            _write_parquet(
                staging_dir / "tushare_daily_bar.parquet",
                pd.DataFrame([{"trade_date": pd.Timestamp("2026-04-17"), "ts_code": "000001.SZ"}]),
            )

            database_panel = pd.DataFrame([{"trade_date": pd.Timestamp("2026-04-27"), "ts_code": "000001.SZ"}])
            database_stock_basic = pd.DataFrame(
                [{"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "list_date": pd.Timestamp("1991-04-03")}]
            )
            database_calendar = pd.DataFrame({"trade_date": pd.to_datetime(["2026-04-27", "2026-04-28"])})

            with (
                patch("src.data.tushare_incremental_sync._uses_primary_project_root", return_value=True),
                patch("src.data.tushare_incremental_sync.load_daily_bar", return_value=database_panel),
                patch("src.data.tushare_incremental_sync.load_stock_basic", return_value=database_stock_basic),
                patch("src.data.tushare_incremental_sync.load_trade_calendar", return_value=database_calendar),
            ):
                daily_bar, stock_basic, trade_calendar, *_ = _load_existing_inputs(root, "tushare")

        self.assertEqual(pd.to_datetime(daily_bar["trade_date"]).max().date().isoformat(), "2026-04-27")
        self.assertEqual(stock_basic["ts_code"].tolist(), ["000001.SZ"])
        self.assertEqual(pd.to_datetime(trade_calendar["trade_date"]).max().date().isoformat(), "2026-04-28")

    def test_sync_incremental_daily_bar_appends_only_missing_trade_dates(self) -> None:
        from src.data.tushare_incremental_sync import sync_incremental_daily_bar

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            staging_dir = ensure_dir(root / "data" / "staging")

            existing_panel = pd.DataFrame(
                [
                    {
                        "trade_date": pd.Timestamp("2026-04-14"),
                        "ts_code": "000001.SZ",
                        "open": 10.0,
                        "high": 10.4,
                        "low": 9.9,
                        "close": 10.2,
                        "vol": 1000.0,
                        "amount": 10000.0,
                        "name": "平安银行",
                        "industry": "银行",
                        "list_date": pd.Timestamp("1991-04-03"),
                        "index_code": "000905.SH",
                        "is_index_member": True,
                        "is_current_name_st": False,
                        "is_st": False,
                        "is_suspend": False,
                        "is_limit_up_close": False,
                        "is_limit_down_close": False,
                        "is_buy_locked": False,
                        "is_sell_locked": False,
                        "up_limit": 11.22,
                        "down_limit": 9.18,
                        "adj_factor": 1.0,
                        "open_adj": 10.0,
                        "high_adj": 10.4,
                        "low_adj": 9.9,
                        "close_adj": 10.2,
                        "pre_close": 10.0,
                        "pre_close_adj": 10.0,
                        "pct_chg": 2.0,
                    },
                    {
                        "trade_date": pd.Timestamp("2026-04-14"),
                        "ts_code": "000002.SZ",
                        "open": 8.0,
                        "high": 8.2,
                        "low": 7.9,
                        "close": 8.1,
                        "vol": 800.0,
                        "amount": 6400.0,
                        "name": "万科A",
                        "industry": "房地产",
                        "list_date": pd.Timestamp("1991-01-29"),
                        "index_code": "000905.SH",
                        "is_index_member": True,
                        "is_current_name_st": False,
                        "is_st": False,
                        "is_suspend": False,
                        "is_limit_up_close": False,
                        "is_limit_down_close": False,
                        "is_buy_locked": False,
                        "is_sell_locked": False,
                        "up_limit": 8.91,
                        "down_limit": 7.29,
                        "adj_factor": 1.0,
                        "open_adj": 8.0,
                        "high_adj": 8.2,
                        "low_adj": 7.9,
                        "close_adj": 8.1,
                        "pre_close": 8.0,
                        "pre_close_adj": 8.0,
                        "pct_chg": 1.25,
                    },
                ]
            )
            existing_stock_basic = pd.DataFrame(
                [
                    {"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "list_date": "19910403"},
                    {"ts_code": "000002.SZ", "name": "万科A", "industry": "房地产", "list_date": "19910129"},
                ]
            )
            existing_calendar = pd.DataFrame({"trade_date": pd.to_datetime(["2026-04-14"])})

            _write_parquet(staging_dir / "akshare_daily_bar.parquet", existing_panel)
            _write_parquet(staging_dir / "akshare_stock_basic.parquet", existing_stock_basic)
            _write_parquet(staging_dir / "akshare_trade_calendar.parquet", existing_calendar)

            client = Mock()
            client.trade_cal.return_value = pd.DataFrame(
                {"cal_date": ["20260414", "20260415"], "is_open": [1, 1]}
            )
            client.stock_basic.return_value = pd.DataFrame(
                [
                    {"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "list_date": "19910403"},
                    {"ts_code": "000002.SZ", "name": "万科A", "industry": "房地产", "list_date": "19910129"},
                ]
            )
            client.daily.return_value = pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260415",
                        "open": 10.3,
                        "high": 10.6,
                        "low": 10.2,
                        "close": 10.5,
                        "pre_close": 10.2,
                        "pct_chg": 2.94,
                        "vol": 1200.0,
                        "amount": 12600.0,
                    }
                ]
            )
            client.daily_basic.return_value = pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": "20260415", "turnover_rate": 0.81}]
            )
            client.stk_limit.return_value = pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": "20260415", "up_limit": 11.22, "down_limit": 9.18}]
            )

            with patch("src.data.tushare_incremental_sync.TushareClient", return_value=client):
                with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=Mock()):
                    summary = sync_incremental_daily_bar(
                        root=root,
                        target_source="akshare",
                        end_date="20260415",
                        write_canonical=False,
                    )

            updated_panel = pd.read_parquet(staging_dir / "akshare_daily_bar.parquet")
            updated_panel["trade_date"] = pd.to_datetime(updated_panel["trade_date"], errors="coerce")

            self.assertEqual(summary.appended_trade_dates, 1)
            self.assertEqual(summary.appended_rows, 2)
            self.assertEqual(len(updated_panel), 4)
            self.assertEqual(
                updated_panel.loc[
                    (updated_panel["trade_date"] == pd.Timestamp("2026-04-14"))
                    & (updated_panel["ts_code"] == "000001.SZ"),
                    "close",
                ].iloc[0],
                10.2,
            )

            appended = updated_panel.loc[updated_panel["trade_date"] == pd.Timestamp("2026-04-15")].sort_values("ts_code")
            self.assertEqual(appended["ts_code"].tolist(), ["000001.SZ", "000002.SZ"])
            self.assertEqual(appended.loc[appended["ts_code"] == "000001.SZ", "close"].iloc[0], 10.5)
            self.assertTrue(bool(appended.loc[appended["ts_code"] == "000002.SZ", "is_suspend"].iloc[0]))
            self.assertTrue(pd.isna(appended.loc[appended["ts_code"] == "000002.SZ", "close"].iloc[0]))

            updated_calendar = pd.read_parquet(staging_dir / "akshare_trade_calendar.parquet")
            updated_calendar["trade_date"] = pd.to_datetime(updated_calendar["trade_date"], errors="coerce")
            self.assertEqual(updated_calendar["trade_date"].dt.strftime("%Y-%m-%d").tolist(), ["2026-04-14", "2026-04-15"])

    def test_sync_incremental_daily_bar_skips_when_no_new_trade_dates(self) -> None:
        from src.data.tushare_incremental_sync import sync_incremental_daily_bar

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            staging_dir = ensure_dir(root / "data" / "staging")

            existing_panel = pd.DataFrame(
                [
                    {
                        "trade_date": pd.Timestamp("2026-04-15"),
                        "ts_code": "000001.SZ",
                        "open": 10.3,
                        "high": 10.6,
                        "low": 10.2,
                        "close": 10.5,
                        "vol": 1200.0,
                        "amount": 12600.0,
                        "name": "平安银行",
                        "industry": "银行",
                        "list_date": pd.Timestamp("1991-04-03"),
                        "index_code": "000905.SH",
                        "is_index_member": True,
                        "is_current_name_st": False,
                        "is_st": False,
                        "is_suspend": False,
                        "is_limit_up_close": False,
                        "is_limit_down_close": False,
                        "is_buy_locked": False,
                        "is_sell_locked": False,
                        "up_limit": 11.55,
                        "down_limit": 9.45,
                        "adj_factor": 1.0,
                        "open_adj": 10.3,
                        "high_adj": 10.6,
                        "low_adj": 10.2,
                        "close_adj": 10.5,
                        "pre_close": 10.2,
                        "pre_close_adj": 10.2,
                        "pct_chg": 2.94,
                    }
                ]
            )

            _write_parquet(staging_dir / "akshare_daily_bar.parquet", existing_panel)
            _write_parquet(staging_dir / "akshare_stock_basic.parquet", pd.DataFrame([{"ts_code": "000001.SZ"}]))
            _write_parquet(staging_dir / "akshare_trade_calendar.parquet", pd.DataFrame({"trade_date": pd.to_datetime(["2026-04-15"])}))

            client = Mock()
            client.trade_cal.return_value = pd.DataFrame({"cal_date": ["20260415"], "is_open": [1]})
            client.stock_basic.return_value = pd.DataFrame([{"ts_code": "000001.SZ", "name": "平安银行", "list_date": "19910403"}])

            with patch("src.data.tushare_incremental_sync.TushareClient", return_value=client):
                with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=Mock()):
                    summary = sync_incremental_daily_bar(
                        root=root,
                        target_source="akshare",
                        end_date="20260415",
                        write_canonical=False,
                    )

            self.assertEqual(summary.appended_trade_dates, 0)
            self.assertEqual(summary.appended_rows, 0)
            client.daily.assert_not_called()
            client.daily_basic.assert_not_called()
            client.stk_limit.assert_not_called()

    def test_sync_incremental_daily_bar_skips_open_trade_day_when_daily_rows_not_ready(self) -> None:
        from src.data.tushare_incremental_sync import sync_incremental_daily_bar

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            staging_dir = ensure_dir(root / "data" / "staging")

            existing_panel = pd.DataFrame(
                [
                    {
                        "trade_date": pd.Timestamp("2026-04-17"),
                        "ts_code": "000001.SZ",
                        "open": 10.3,
                        "high": 10.6,
                        "low": 10.2,
                        "close": 10.5,
                        "vol": 1200.0,
                        "amount": 12600.0,
                        "name": "平安银行",
                        "industry": "银行",
                        "list_date": pd.Timestamp("1991-04-03"),
                        "index_code": "000905.SH",
                        "is_index_member": True,
                        "is_current_name_st": False,
                        "is_st": False,
                        "is_suspend": False,
                        "is_limit_up_close": False,
                        "is_limit_down_close": False,
                        "is_buy_locked": False,
                        "is_sell_locked": False,
                        "up_limit": 11.55,
                        "down_limit": 9.45,
                        "adj_factor": 1.0,
                        "open_adj": 10.3,
                        "high_adj": 10.6,
                        "low_adj": 10.2,
                        "close_adj": 10.5,
                        "pre_close": 10.2,
                        "pre_close_adj": 10.2,
                        "pct_chg": 2.94,
                    }
                ]
            )

            _write_parquet(staging_dir / "akshare_daily_bar.parquet", existing_panel)
            _write_parquet(staging_dir / "akshare_stock_basic.parquet", pd.DataFrame([{"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "list_date": "19910403"}]))
            _write_parquet(staging_dir / "akshare_trade_calendar.parquet", pd.DataFrame({"trade_date": pd.to_datetime(["2026-04-17"])}))

            client = Mock()
            client.trade_cal.return_value = pd.DataFrame({"cal_date": ["20260417", "20260420"], "is_open": [1, 1]})
            client.stock_basic.return_value = pd.DataFrame([{"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "list_date": "19910403"}])
            client.daily.return_value = pd.DataFrame(columns=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"])
            client.daily_basic.return_value = pd.DataFrame(columns=["ts_code", "trade_date", "turnover_rate"])
            client.stk_limit.return_value = pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])

            with patch("src.data.tushare_incremental_sync.TushareClient", return_value=client):
                with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=Mock()):
                    summary = sync_incremental_daily_bar(
                        root=root,
                        target_source="akshare",
                        end_date="20260420",
                        write_canonical=False,
                    )

            updated_panel = pd.read_parquet(staging_dir / "akshare_daily_bar.parquet")
            updated_panel["trade_date"] = pd.to_datetime(updated_panel["trade_date"], errors="coerce")

            self.assertEqual(summary.appended_trade_dates, 0)
            self.assertEqual(summary.appended_rows, 0)
            self.assertEqual(summary.latest_trade_date, "2026-04-17")
            self.assertEqual(len(updated_panel), 1)


if __name__ == "__main__":
    unittest.main()
