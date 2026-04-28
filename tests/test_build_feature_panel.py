from __future__ import annotations

import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from src.db.dashboard_sync import SyncSummary


class BuildFeaturePanelTests(unittest.TestCase):
    def test_primary_source_mode_builds_from_daily_bar_artifact_and_replaces_source_rows(self) -> None:
        from src.features.build_feature_panel import build_feature_label_artifacts

        root = Path("/repo")
        daily_bar = pd.DataFrame(
            [
                {"trade_date": "2026-04-27", "ts_code": "000001.SZ", "close_adj": 12.3},
            ]
        )
        feature_panel = pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-27"),
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "index_code": "000905.SH",
                    "is_current_name_st": False,
                    "is_index_member": True,
                    "days_since_list": 1000,
                    "mom_5": 0.1,
                }
            ]
        )
        label_panel = pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-27"),
                    "ts_code": "000001.SZ",
                    "ret_t1_t10": 0.02,
                }
            ]
        )

        with (
            patch("src.features.build_feature_panel._uses_primary_project_root", return_value=True),
            patch("src.features.build_feature_panel.uuid.uuid4", return_value=uuid.UUID("12345678-1234-5678-1234-567812345678")),
            patch("src.features.build_feature_panel.load_daily_bar", return_value=daily_bar) as load_daily_bar,
            patch(
                "src.features.build_feature_panel.load_equity_symbols_from_market_database",
                side_effect=AssertionError("should not read market.bars_1d in source mode"),
            ),
            patch("src.features.build_feature_panel.build_feature_and_label_panels", return_value=(feature_panel, label_panel)),
            patch("src.features.build_feature_panel.delete_research_panel_source", return_value=99) as delete_source,
            patch("src.features.build_feature_panel.save_panel_run") as save_panel_run,
            patch("src.features.build_feature_panel.save_research_panel", return_value=1) as save_research_panel,
            patch(
                "src.db.dashboard_sync.sync_dataset_summary_artifact",
                return_value=SyncSummary(ok=True, synced_items=1, message="dataset ok"),
            ),
            patch(
                "src.db.dashboard_sync.sync_factor_explorer_snapshot_artifact",
                return_value=SyncSummary(ok=True, synced_items=1, message="factor ok"),
            ) as sync_factor,
        ):
            summary = build_feature_label_artifacts(root=root, data_source="tushare", prefer_source_daily_bar=True)

        load_daily_bar.assert_called_once_with(root, data_source="tushare", prefer_database=True)
        delete_source.assert_called_once_with(data_source="tushare")
        save_research_panel.assert_called_once()
        self.assertEqual(save_panel_run.call_count, 2)
        self.assertIs(sync_factor.call_args.kwargs["feature_panel"], feature_panel)
        self.assertEqual(summary["data_source"], "tushare")
        self.assertEqual(summary["feature_rows"], 1)
        self.assertEqual(summary["label_rows"], 1)
        self.assertEqual(summary["replaced_rows"], 99)

    def test_primary_source_mode_fails_when_source_daily_bar_is_missing(self) -> None:
        from src.features.build_feature_panel import build_feature_label_artifacts

        with (
            patch("src.features.build_feature_panel._uses_primary_project_root", return_value=True),
            patch("src.features.build_feature_panel.load_daily_bar", return_value=pd.DataFrame()),
            patch(
                "src.features.build_feature_panel.load_equity_symbols_from_market_database",
                side_effect=AssertionError("should not fall back to market.bars_1d in source mode"),
            ),
        ):
            with self.assertRaises(RuntimeError):
                build_feature_label_artifacts(root=Path("/repo"), data_source="tushare", prefer_source_daily_bar=True)

    def test_primary_market_mode_can_scope_to_watchlist_symbols(self) -> None:
        from src.features.build_feature_panel import build_feature_label_artifacts

        feature_panel = pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-17"),
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "index_code": "000905.SH",
                    "is_current_name_st": False,
                    "is_index_member": False,
                    "days_since_list": 9000,
                    "mom_20": 0.1,
                }
            ]
        )
        label_panel = pd.DataFrame(
            [{"trade_date": pd.Timestamp("2026-04-17"), "ts_code": "000001.SZ", "ret_t1_t10": None}]
        )
        daily_bar = pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-17"),
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "list_date": pd.Timestamp("1991-04-03"),
                    "close": 10.0,
                }
            ]
        )
        store = Mock()
        store.load_watchlist.return_value = {
            "holdings": [{"ts_code": "000001.SZ"}],
            "focus_pool": [{"ts_code": "000002.SZ"}],
        }

        with (
            patch("src.features.build_feature_panel._uses_primary_project_root", return_value=True),
            patch("src.features.build_feature_panel.uuid.uuid4", return_value=uuid.UUID("12345678-1234-5678-1234-567812345678")),
            patch("src.features.build_feature_panel.PostgresWatchlistStore", return_value=store),
            patch("src.features.build_feature_panel.get_api_settings", return_value=Mock()),
            patch("src.features.build_feature_panel.load_universe", return_value={"benchmark": "000905.SH"}),
            patch("src.features.build_feature_panel.load_equity_symbols_from_market_database", side_effect=AssertionError("should use watchlist scope")),
            patch("src.features.build_feature_panel.load_daily_bar_batch_from_market_database", return_value=daily_bar) as load_market,
            patch("src.features.build_feature_panel.build_feature_and_label_panels", return_value=(feature_panel, label_panel)),
            patch("src.features.build_feature_panel.delete_research_panel_source", side_effect=AssertionError("scoped rebuild must not delete source")),
            patch("src.features.build_feature_panel.delete_research_panel_symbols", return_value=7) as delete_symbols,
            patch("src.features.build_feature_panel.save_panel_run") as save_panel_run,
            patch("src.features.build_feature_panel.save_research_panel", return_value=1),
            patch(
                "src.db.dashboard_sync.sync_dataset_summary_artifact",
                return_value=SyncSummary(ok=True, synced_items=1, message="dataset ok"),
            ),
            patch(
                "src.db.dashboard_sync.sync_factor_explorer_snapshot_artifact",
                return_value=SyncSummary(ok=True, synced_items=1, message="factor ok"),
            ),
        ):
            summary = build_feature_label_artifacts(root=Path("/repo"), data_source="tushare", market_universe_user_id="user-1")

        store.load_watchlist.assert_called_once_with("user-1")
        load_market.assert_called_once_with(["000001.SZ", "000002.SZ"], benchmark_code="000905.SH")
        delete_symbols.assert_called_once_with(data_source="tushare", symbols=["000001.SZ", "000002.SZ"])
        self.assertEqual(summary["feature_rows"], 1)
        self.assertEqual(summary["panel_rows"], 1)
        self.assertEqual(summary["symbol_count"], 2)
        self.assertEqual(save_panel_run.call_args_list[-1].args[0]["date_max"], "2026-04-17")


if __name__ == "__main__":
    unittest.main()
