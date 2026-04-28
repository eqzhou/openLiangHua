from __future__ import annotations

import unittest
from unittest.mock import patch

from src.data.tushare_incremental_sync import IncrementalSyncSummary
from src.db.dashboard_sync import SyncSummary


class TushareWorkflowTests(unittest.TestCase):
    def test_run_tushare_full_refresh_executes_incremental_then_features_then_snapshot_sync(self) -> None:
        from src.data.tushare_workflows import run_tushare_full_refresh

        with (
            patch(
                "src.data.tushare_workflows.sync_incremental_daily_bar",
                return_value=IncrementalSyncSummary(
                    target_source="akshare",
                    previous_latest_trade_date="2026-04-10",
                    latest_trade_date="2026-04-16",
                    appended_trade_dates=4,
                    appended_rows=116,
                    symbols=29,
                    daily_bar_artifact_ref="artifact://akshare:binary:daily_bar",
                    trade_calendar_artifact_ref="artifact://akshare:binary:trade_calendar",
                    stock_basic_artifact_ref="artifact://akshare:binary:stock_basic",
                ),
            ) as incremental,
            patch(
                "src.data.tushare_workflows.build_feature_label_artifacts",
                return_value={
                    "data_source": "akshare",
                    "feature_rows": 1200,
                    "label_rows": 1180,
                    "feature_artifact_ref": "artifact://akshare:binary:feature_panel",
                    "label_artifact_ref": "artifact://akshare:binary:label_panel",
                },
            ) as feature_builder,
            patch(
                "src.data.tushare_workflows.sync_dashboard_artifacts",
                return_value=SyncSummary(ok=True, synced_items=8, message="dashboard synced"),
            ) as dashboard_sync,
        ):
            summary = run_tushare_full_refresh(target_source="akshare", end_date="2026-04-16")

        incremental.assert_called_once()
        feature_builder.assert_called_once()
        self.assertTrue(feature_builder.call_args.kwargs["prefer_source_daily_bar"])
        dashboard_sync.assert_called_once()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["target_source"], "akshare")
        self.assertEqual(summary["incremental"]["latest_trade_date"], "2026-04-16")
        self.assertEqual(summary["features"]["feature_rows"], 1200)
        self.assertEqual(summary["dashboardSync"]["message"], "dashboard synced")
