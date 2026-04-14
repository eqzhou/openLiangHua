from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from src.app.services.dashboard_data_service import (
    build_candidate_snapshot,
    build_factor_explorer_snapshot,
    build_metrics_table,
    build_watchlist_base_frame,
    clear_dashboard_data_caches,
    list_available_actions,
    load_experiment_config,
)


class DashboardDataServiceTests(unittest.TestCase):
    def test_action_catalog_has_expected_shape(self) -> None:
        actions = list_available_actions()

        self.assertGreater(len(actions), 0)
        self.assertTrue(all("actionName" in action for action in actions))
        self.assertTrue(all("label" in action for action in actions))
        self.assertTrue(all("moduleName" in action for action in actions))
        self.assertTrue(all("spinnerText" in action for action in actions))
        self.assertTrue(all("buttonKey" in action for action in actions))
        self.assertEqual(len({action["buttonKey"] for action in actions}), len(actions))

    def test_metrics_table_returns_dataframe(self) -> None:
        frame = build_metrics_table()

        self.assertIsInstance(frame, pd.DataFrame)

    def test_config_loader_and_cache_clear_smoke(self) -> None:
        payload = load_experiment_config()
        clear_dashboard_data_caches()

        self.assertIsInstance(payload, dict)

    def test_watchlist_base_frame_prefers_stored_snapshot(self) -> None:
        stored_snapshot = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "示例股票",
                    "entry_group": "持仓",
                    "mark_price": 10.0,
                }
            ]
        )

        clear_dashboard_data_caches()
        with (
            patch("src.app.services.dashboard_data_service.repo_load_watchlist_snapshot", return_value=stored_snapshot) as snapshot_loader,
            patch("src.app.services.dashboard_data_service.build_watchlist_view", side_effect=AssertionError("should not rebuild watchlist view")),
        ):
            frame = build_watchlist_base_frame()

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["ts_code"], "000001.SZ")
        snapshot_loader.assert_called_once()

    def test_watchlist_base_frame_persists_built_snapshot_for_future_cold_starts(self) -> None:
        built_snapshot = pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "name": "后备股票",
                    "entry_group": "重点关注",
                    "mark_price": 12.5,
                }
            ]
        )

        clear_dashboard_data_caches()
        with (
            patch("src.app.services.dashboard_data_service.repo_load_watchlist_snapshot", return_value=None),
            patch("src.app.services.dashboard_data_service.load_watchlist_config", return_value={"holdings": [], "focus_pool": []}),
            patch("src.app.services.dashboard_data_service.load_daily_bar", return_value=pd.DataFrame()),
            patch("src.app.services.dashboard_data_service.load_predictions", return_value=pd.DataFrame()),
            patch("src.app.services.dashboard_data_service.load_overlay_candidates", return_value=pd.DataFrame()),
            patch("src.app.services.dashboard_data_service.load_overlay_inference_candidates", return_value=pd.DataFrame()),
            patch("src.app.services.dashboard_data_service.build_watchlist_view", return_value=built_snapshot),
            patch("src.app.services.dashboard_data_service.sync_watchlist_snapshot_artifact") as sync_snapshot,
        ):
            frame = build_watchlist_base_frame()

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["ts_code"], "000002.SZ")
        sync_snapshot.assert_called_once()

    def test_candidate_snapshot_prefers_stored_artifact(self) -> None:
        stored_snapshot = pd.DataFrame(
            [
                {
                    "ts_code": "000003.SZ",
                    "name": "候选快照",
                    "trade_date": pd.Timestamp("2026-04-03"),
                    "score": 0.91,
                }
            ]
        )

        clear_dashboard_data_caches()
        with (
            patch("src.app.services.dashboard_data_service.repo_load_candidate_snapshot", return_value=stored_snapshot) as snapshot_loader,
            patch("src.app.services.dashboard_data_service.load_predictions", side_effect=AssertionError("should not load predictions")),
        ):
            frame = build_candidate_snapshot("ensemble", "test")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["ts_code"], "000003.SZ")
        snapshot_loader.assert_called_once()

    def test_candidate_snapshot_persists_built_snapshot_when_missing(self) -> None:
        predictions = pd.DataFrame(
            [
                {"ts_code": "000004.SZ", "trade_date": "2026-04-01", "score": 0.3},
                {"ts_code": "000004.SZ", "trade_date": "2026-04-02", "score": 0.8},
            ]
        )

        clear_dashboard_data_caches()
        with (
            patch("src.app.services.dashboard_data_service.repo_load_candidate_snapshot", return_value=None),
            patch("src.app.services.dashboard_data_service.load_predictions", return_value=predictions),
            patch("src.app.services.dashboard_data_service.sync_candidate_snapshot_artifact") as sync_snapshot,
        ):
            frame = build_candidate_snapshot("ensemble", "test")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["ts_code"], "000004.SZ")
        sync_snapshot.assert_called_once()

    def test_factor_explorer_snapshot_prefers_stored_artifact(self) -> None:
        stored_snapshot = {
            "available": True,
            "latestDate": "2026-04-03T00:00:00",
            "factorOptions": [{"key": "mom_20", "label": "mom_20", "description": ""}],
            "symbolOptions": ["000001.SZ"],
            "crossSection": [{"ts_code": "000001.SZ", "name": "示例股票", "mom_20": 0.3}],
            "missingRates": [{"feature": "mom_20", "missing_rate": 0.0}],
        }

        clear_dashboard_data_caches()
        with (
            patch("src.app.services.dashboard_data_service.repo_load_factor_explorer_snapshot", return_value=stored_snapshot) as snapshot_loader,
            patch("src.app.services.dashboard_data_service.load_feature_panel", side_effect=AssertionError("should not load full feature panel")),
        ):
            payload = build_factor_explorer_snapshot()

        self.assertTrue(payload["available"])
        self.assertEqual(payload["symbolOptions"], ["000001.SZ"])
        snapshot_loader.assert_called_once()

    def test_factor_explorer_snapshot_persists_built_payload_when_missing(self) -> None:
        feature_panel = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "name": "A", "mom_20": 0.1},
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "name": "A", "mom_20": 0.2},
                {"trade_date": "2026-04-02", "ts_code": "000002.SZ", "name": "B", "mom_20": 0.3},
            ]
        )

        clear_dashboard_data_caches()
        with (
            patch("src.app.services.dashboard_data_service.repo_load_factor_explorer_snapshot", return_value=None),
            patch("src.app.services.dashboard_data_service.load_feature_panel", return_value=feature_panel),
            patch("src.app.services.dashboard_data_service.sync_factor_explorer_snapshot_artifact") as sync_snapshot,
        ):
            payload = build_factor_explorer_snapshot()

        self.assertTrue(payload["available"])
        self.assertEqual(len(payload["crossSection"]), 2)
        sync_snapshot.assert_called_once()


if __name__ == "__main__":
    unittest.main()
