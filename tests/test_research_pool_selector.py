from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pandas as pd


class ResearchPoolSelectorTests(unittest.TestCase):
    def test_select_research_pool_filters_scores_and_caps_industries(self) -> None:
        from src.data.research_pool_selector import select_research_pool

        dates = pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-06"])
        symbols = [
            ("000001.SZ", "好银行", "银行", "20200101", [10.0, 10.2, 10.5, 11.0], [100, 110, 120, 140]),
            ("000002.SZ", "好地产", "地产", "20200101", [8.0, 8.2, 8.4, 8.5], [90, 95, 100, 105]),
            ("000003.SZ", "低流动", "地产", "20200101", [7.0, 7.1, 7.2, 7.3], [1, 1, 1, 1]),
            ("000004.SZ", "ST样本", "医药", "20200101", [6.0, 6.1, 6.2, 6.3], [120, 120, 120, 120]),
            ("000005.SZ", "新股", "科技", "20260301", [5.0, 5.1, 5.2, 5.3], [120, 130, 140, 150]),
        ]
        rows: list[dict[str, object]] = []
        for ts_code, _, _, _, closes, amounts in symbols:
            for trade_date, close, amount in zip(dates, closes, amounts, strict=True):
                rows.append(
                    {
                        "trade_date": trade_date,
                        "ts_code": ts_code,
                        "open": close - 0.1,
                        "high": close + 0.1,
                        "low": close - 0.2,
                        "close": close,
                        "pre_close": close - 0.1,
                        "vol": amount * 10,
                        "amount": amount,
                    }
                )
        daily_history = pd.DataFrame(rows)
        stock_basic = pd.DataFrame(
            [
                {"ts_code": ts_code, "name": name, "industry": industry, "list_date": list_date, "list_status": "L"}
                for ts_code, name, industry, list_date, *_ in symbols
            ]
        )

        selected = select_research_pool(
            daily_history,
            stock_basic,
            limit=2,
            min_list_days=100,
            min_history_days=4,
            liquidity_quantile=0.25,
            industry_cap=1,
        )

        self.assertEqual(selected["ts_code"].tolist(), ["000001.SZ", "000002.SZ"])
        self.assertEqual(selected["research_rank"].tolist(), [1, 2])
        self.assertNotIn("000003.SZ", selected["ts_code"].tolist())
        self.assertNotIn("000004.SZ", selected["ts_code"].tolist())
        self.assertNotIn("000005.SZ", selected["ts_code"].tolist())

    def test_prepare_selected_daily_bar_adds_watchlist_schema_columns(self) -> None:
        from src.data.research_pool_selector import prepare_selected_daily_bar

        daily_history = pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-27",
                    "ts_code": "000001.SZ",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                    "pre_close": 10,
                    "pct_chg": 5,
                    "vol": 100,
                    "amount": 1000,
                }
            ]
        )
        selected = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "list_date": pd.Timestamp("1991-04-03"),
                }
            ]
        )

        prepared = prepare_selected_daily_bar(daily_history, selected, benchmark="000905.SH")

        for column in ("name", "industry", "list_date", "close_adj", "is_suspend", "is_buy_locked", "is_sell_locked"):
            self.assertIn(column, prepared.columns)
        self.assertEqual(prepared.iloc[0]["name"], "平安银行")
        self.assertFalse(bool(prepared.iloc[0]["is_suspend"]))
        self.assertEqual(float(prepared.iloc[0]["close_adj"]), 10.5)

    def test_write_research_pool_to_watchlist_replaces_focus_items(self) -> None:
        from src.data.research_pool_selector import write_research_pool_to_watchlist

        store = Mock()
        selected = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "research_rank": 1, "research_score": 0.8},
                {"ts_code": "000002.SZ", "name": "万科A", "industry": "地产", "research_rank": 2, "research_score": 0.5},
            ]
        )

        written = write_research_pool_to_watchlist(
            selected,
            user_id="user-1",
            store=store,
            replace_focus=True,
        )

        store.clear_items.assert_called_once_with("user-1", "focus")
        self.assertEqual(store.add_item.call_count, 2)
        self.assertEqual(written, 2)
        first_call = store.add_item.call_args_list[0]
        self.assertEqual(first_call.args[:4], ("user-1", "000001.SZ", "平安银行", "focus"))
        self.assertIn("自动研究池#1", first_call.kwargs["note"])

    def test_run_research_pool_refresh_merges_selected_rows_and_syncs_user_snapshot(self) -> None:
        from src.data.research_pool_selector import ResearchPoolSummary, run_research_pool_refresh

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            selected = pd.DataFrame(
                [
                    {"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "research_rank": 1, "research_score": 0.8}
                ]
            )
            selected_daily = pd.DataFrame(
                [
                    {
                        "trade_date": pd.Timestamp("2026-04-27"),
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "industry": "银行",
                        "list_date": pd.Timestamp("1991-04-03"),
                        "close": 10.5,
                    }
                ]
            )

            with (
                patch(
                    "src.data.research_pool_selector.fetch_research_pool_inputs",
                    return_value=(pd.DataFrame(), pd.DataFrame(), pd.DataFrame({"trade_date": pd.to_datetime(["2026-04-27"])}), "2026-04-27"),
                ),
                patch("src.data.research_pool_selector.select_research_pool", return_value=selected),
                patch("src.data.research_pool_selector.prepare_selected_daily_bar", return_value=selected_daily),
                patch("src.data.research_pool_selector.load_daily_bar", return_value=pd.DataFrame()),
                patch("src.data.research_pool_selector.load_trade_calendar", return_value=pd.DataFrame()),
                patch("src.data.research_pool_selector.load_stock_basic", return_value=pd.DataFrame()),
                patch("src.data.research_pool_selector.save_binary_dataset", return_value="artifact://saved") as save_binary,
                patch("src.data.research_pool_selector.write_research_pool_to_watchlist", return_value=1) as write_watchlist,
                patch("src.data.research_pool_selector.sync_watchlist_snapshot_artifact") as sync_watchlist,
                patch("src.data.research_pool_selector.build_feature_label_artifacts", return_value={"feature_rows": 1}) as build_features,
            ):
                sync_watchlist.return_value.ok = True
                sync_watchlist.return_value.message = "synced"
                summary = run_research_pool_refresh(root=root, user_id="user-1")

        self.assertIsInstance(summary, ResearchPoolSummary)
        self.assertEqual(summary.selected_count, 1)
        self.assertEqual(summary.watchlist_written, 1)
        self.assertEqual(save_binary.call_count, 3)
        write_watchlist.assert_called_once()
        sync_watchlist.assert_called_once()
        build_features.assert_called_once_with(
            root=root,
            data_source="tushare",
            market_universe_user_id="user-1",
        )


if __name__ == "__main__":
    unittest.main()
