from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd


class DataManagementServiceTests(unittest.TestCase):
    def test_build_data_management_payload_reports_token_and_dataset_status(self) -> None:
        from src.app.services.data_management_service import build_data_management_payload

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text("TUSHARE_TOKEN=test-token\n", encoding="utf-8")

            staging_dir = root / "data" / "staging"
            features_dir = root / "data" / "features"
            labels_dir = root / "data" / "labels"
            staging_dir.mkdir(parents=True)
            features_dir.mkdir(parents=True)
            labels_dir.mkdir(parents=True)

            pd.DataFrame(
                [
                    {"trade_date": "2026-04-15", "ts_code": "000001.SZ", "close": 10.2},
                    {"trade_date": "2026-04-16", "ts_code": "000001.SZ", "close": 10.5},
                    {"trade_date": "2026-04-16", "ts_code": "000002.SZ", "close": 8.3},
                ]
            ).to_parquet(staging_dir / "akshare_daily_bar.parquet", index=False)
            pd.DataFrame(
                [
                    {"trade_date": "2026-04-15", "ts_code": "000001.SZ", "mom_20": 0.12},
                    {"trade_date": "2026-04-16", "ts_code": "000001.SZ", "mom_20": 0.18},
                ]
            ).to_parquet(features_dir / "akshare_feature_panel.parquet", index=False)
            pd.DataFrame(
                [
                    {"trade_date": "2026-04-15", "ts_code": "000001.SZ", "ret_t1_t10": 0.03},
                    {"trade_date": "2026-04-16", "ts_code": "000001.SZ", "ret_t1_t10": 0.05},
                ]
            ).to_parquet(labels_dir / "akshare_label_panel.parquet", index=False)

            payload = build_data_management_payload(root=root, target_source="akshare")

        self.assertEqual(payload["targetSource"], "akshare")
        self.assertEqual(payload["activeDataSource"], "akshare")
        self.assertEqual(payload["configuredDataSource"], "akshare")
        self.assertFalse(payload["sourceMismatch"])
        self.assertTrue(payload["tokenConfigured"])
        self.assertTrue(payload["envFileExists"])
        self.assertIn("researchPanel", payload)
        self.assertEqual(payload["dailyBar"]["latestTradeDate"], "2026-04-16")
        self.assertEqual(payload["dailyBar"]["rowCount"], 3)
        self.assertEqual(payload["dailyBar"]["symbolCount"], 2)
        self.assertEqual(payload["legacyFeatureView"]["latestTradeDate"], "2026-04-16")
        self.assertEqual(payload["legacyFeatureView"]["rowCount"], 2)
        self.assertEqual(payload["legacyLabelView"]["latestTradeDate"], "2026-04-16")
        self.assertEqual(payload["legacyLabelView"]["rowCount"], 2)
        self.assertEqual(payload["envPath"], ".env")
        self.assertIn("refresh_daily_bar_tushare.ps1", payload["scripts"]["incremental"])
        self.assertIn("refresh_full_pipeline_tushare.ps1", payload["scripts"]["fullRefresh"])

    def test_build_data_management_payload_detects_process_env_token_without_env_file(self) -> None:
        from src.app.services.data_management_service import build_data_management_payload

        with TemporaryDirectory() as temp_dir, patch.dict(os.environ, {"TUSHARE_TOKEN": "runtime-token"}, clear=True):
            root = Path(temp_dir)

            staging_dir = root / "data" / "staging"
            features_dir = root / "data" / "features"
            labels_dir = root / "data" / "labels"
            staging_dir.mkdir(parents=True)
            features_dir.mkdir(parents=True)
            labels_dir.mkdir(parents=True)

            pd.DataFrame([{"trade_date": "2026-04-16", "ts_code": "000001.SZ", "close": 10.5}]).to_parquet(
                staging_dir / "akshare_daily_bar.parquet",
                index=False,
            )
            pd.DataFrame([{"trade_date": "2026-04-16", "ts_code": "000001.SZ", "mom_20": 0.18}]).to_parquet(
                features_dir / "akshare_feature_panel.parquet",
                index=False,
            )
            pd.DataFrame([{"trade_date": "2026-04-16", "ts_code": "000001.SZ", "ret_t1_t10": 0.05}]).to_parquet(
                labels_dir / "akshare_label_panel.parquet",
                index=False,
            )

            payload = build_data_management_payload(root=root, target_source="akshare")

        self.assertFalse(payload["envFileExists"])
        self.assertTrue(payload["tokenConfigured"])

    def test_build_data_management_payload_reports_materialized_source_when_config_mismatches(self) -> None:
        from src.app.services.data_management_service import build_data_management_payload

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text("TUSHARE_TOKEN=test-token\n", encoding="utf-8")

            staging_dir = root / "data" / "staging"
            features_dir = root / "data" / "features"
            labels_dir = root / "data" / "labels"
            staging_dir.mkdir(parents=True)
            features_dir.mkdir(parents=True)
            labels_dir.mkdir(parents=True)

            pd.DataFrame([{"trade_date": "2026-04-16", "ts_code": "000001.SZ", "close": 10.5}]).to_parquet(
                staging_dir / "akshare_daily_bar.parquet",
                index=False,
            )
            pd.DataFrame([{"trade_date": "2026-04-16", "ts_code": "000001.SZ", "mom_20": 0.18}]).to_parquet(
                features_dir / "akshare_feature_panel.parquet",
                index=False,
            )
            pd.DataFrame([{"trade_date": "2026-04-16", "ts_code": "000001.SZ", "ret_t1_t10": 0.05}]).to_parquet(
                labels_dir / "akshare_label_panel.parquet",
                index=False,
            )

            payload = build_data_management_payload(root=root, target_source="tushare")

        self.assertEqual(payload["configuredDataSource"], "tushare")
        self.assertEqual(payload["activeDataSource"], "akshare")
        self.assertEqual(payload["targetSource"], "akshare")
        self.assertTrue(payload["sourceMismatch"])

    def test_build_data_management_payload_skips_full_daily_bar_load_for_primary_database_mode(self) -> None:
        from src.app.services.data_management_service import build_data_management_payload

        with (
            patch("src.app.services.data_management_service._use_database_artifacts", return_value=True),
            patch("src.app.services.data_management_service.load_daily_bar", side_effect=AssertionError("should not load full daily bar")),
            patch("src.app.services.data_management_service.load_dataset_summary", return_value={"date_max": "2026-04-16"}),
            patch("src.app.services.data_management_service.get_artifact_metadata", return_value={"updated_at": "2026-04-16T00:00:00"}),
            patch("src.app.services.data_management_service.load_latest_successful_panel_run", return_value={}),
            patch("src.app.services.data_management_service.load_research_panel_summary", return_value={}),
            patch("src.app.services.data_management_service.load_stock_bar_summary_from_market_database", return_value={"rowCount": 3, "symbolCount": 2, "latestTradeDate": "2026-04-16"}),
        ):
            payload = build_data_management_payload(root=Path("/Users/eqzhou/Public/openLiangHua"), target_source="akshare")

        self.assertEqual(payload["dailyBar"]["rowCount"], 3)
        self.assertEqual(payload["dailyBar"]["symbolCount"], 2)
        self.assertEqual(payload["dailyBar"]["latestTradeDate"], "2026-04-16")

    def test_build_myquant_status_payload_reports_token_sdk_and_dataset(self) -> None:
        from src.app.services.data_management_service import build_myquant_status_payload

        with TemporaryDirectory() as temp_dir, patch.dict(os.environ, {}, clear=True):
            root = Path(temp_dir)
            (root / ".env").write_text("MYQUANT_TOKEN=myquant-token\n", encoding="utf-8")
            staging_dir = root / "data" / "staging"
            staging_dir.mkdir(parents=True)
            pd.DataFrame(
                [
                    {"trade_date": "2026-04-15", "ts_code": "000001.SZ", "close": 10.2},
                    {"trade_date": "2026-04-16", "ts_code": "000002.SZ", "close": 8.3},
                ]
            ).to_parquet(staging_dir / "myquant_daily_bar.parquet", index=False)

            with patch("src.app.services.data_management_service.importlib.util.find_spec", return_value=object()):
                payload = build_myquant_status_payload(root=root, include_sensitive=True)

        self.assertTrue(payload["tokenConfigured"])
        self.assertTrue(payload["sdkAvailable"])
        self.assertEqual(payload["dailyBar"]["rowCount"], 2)
        self.assertEqual(payload["dailyBar"]["symbolCount"], 2)
        self.assertEqual(payload["dailyBar"]["latestTradeDate"], "2026-04-16")

    def test_build_myquant_status_payload_uses_metadata_for_primary_database_mode(self) -> None:
        from src.app.services.data_management_service import build_myquant_status_payload

        with (
            patch("src.app.services.data_management_service._use_database_artifacts", return_value=True),
            patch("src.app.services.data_management_service.load_daily_bar", side_effect=AssertionError("should not load full daily bar")),
            patch(
                "src.app.services.data_management_service.get_artifact_metadata",
                return_value={
                    "rows": 123,
                    "symbol_count": 5,
                    "latest_trade_date": "2026-04-16",
                    "updated_at": "2026-04-16T00:00:00",
                },
            ),
            patch(
                "src.app.services.data_management_service.load_dataset_summary",
                return_value={"feature_symbols": 9, "date_max": "2026-04-18"},
            ),
            patch("src.app.services.data_management_service.importlib.util.find_spec", return_value=object()),
        ):
            payload = build_myquant_status_payload(root=Path("/Users/eqzhou/Public/openLiangHua"), include_sensitive=False)

        self.assertEqual(payload["dailyBar"]["rowCount"], 123)
        self.assertEqual(payload["dailyBar"]["symbolCount"], 5)
        self.assertEqual(payload["dailyBar"]["latestTradeDate"], "2026-04-16")
        self.assertEqual(payload["dailyBar"]["updatedAt"], "2026-04-16T00:00:00")

    def test_build_myquant_status_payload_preserves_empty_metadata_values(self) -> None:
        from src.app.services.data_management_service import build_myquant_status_payload

        with (
            patch("src.app.services.data_management_service._use_database_artifacts", return_value=True),
            patch("src.app.services.data_management_service.load_daily_bar", side_effect=AssertionError("should not load full daily bar")),
            patch(
                "src.app.services.data_management_service.get_artifact_metadata",
                return_value={
                    "rows": 0,
                    "symbol_count": 0,
                    "latest_trade_date": None,
                    "updated_at": "2026-04-16T00:00:00",
                },
            ),
            patch(
                "src.app.services.data_management_service.load_dataset_summary",
                return_value={"feature_symbols": 5, "date_max": "2026-04-16"},
            ),
            patch("src.app.services.data_management_service.importlib.util.find_spec", return_value=object()),
        ):
            payload = build_myquant_status_payload(root=Path("/Users/eqzhou/Public/openLiangHua"), include_sensitive=False)

        self.assertEqual(payload["dailyBar"]["rowCount"], 0)
        self.assertEqual(payload["dailyBar"]["symbolCount"], 0)
        self.assertIsNone(payload["dailyBar"]["latestTradeDate"])

    def test_build_myquant_status_payload_falls_back_for_legacy_metadata(self) -> None:
        from src.app.services.data_management_service import build_myquant_status_payload

        with (
            patch("src.app.services.data_management_service._use_database_artifacts", return_value=True),
            patch("src.app.services.data_management_service.load_daily_bar", side_effect=AssertionError("should not load full daily bar")),
            patch(
                "src.app.services.data_management_service.get_artifact_metadata",
                return_value={
                    "rows": 123,
                    "updated_at": "2026-04-16T00:00:00",
                },
            ),
            patch(
                "src.app.services.data_management_service.load_dataset_summary",
                return_value={"feature_symbols": 5, "date_max": "2026-04-16"},
            ),
            patch("src.app.services.data_management_service.importlib.util.find_spec", return_value=object()),
        ):
            payload = build_myquant_status_payload(root=Path("/Users/eqzhou/Public/openLiangHua"), include_sensitive=False)

        self.assertEqual(payload["dailyBar"]["rowCount"], 123)
        self.assertEqual(payload["dailyBar"]["symbolCount"], 5)
        self.assertEqual(payload["dailyBar"]["latestTradeDate"], "2026-04-16")

    def test_build_myquant_status_payload_redacts_token_status_when_requested(self) -> None:
        from src.app.services.data_management_service import build_myquant_status_payload

        with TemporaryDirectory() as temp_dir, patch.dict(os.environ, {"MYQUANT_TOKEN": "runtime-token"}, clear=True):
            root = Path(temp_dir)
            with patch("src.app.services.data_management_service.importlib.util.find_spec", return_value=None):
                payload = build_myquant_status_payload(root=root, include_sensitive=False)

        self.assertIsNone(payload["tokenConfigured"])
        self.assertFalse(payload["sdkAvailable"])

    def test_myquant_action_error_payload_redacts_exception_text(self) -> None:
        from src.app.facades.data_management_facade import run_myquant_enrich_payload

        with (
            patch("src.data.myquant_enrich.run", side_effect=RuntimeError("MYQUANT_TOKEN=secret-token failed")),
            patch("src.app.facades.base.clear_dashboard_caches"),
        ):
            payload = run_myquant_enrich_payload(user_id="user-1")

        self.assertFalse(payload["ok"])
        self.assertIn("RuntimeError", payload["output"])
        self.assertNotIn("secret-token", payload["output"])
        self.assertNotIn("MYQUANT_TOKEN", payload["output"])

    def test_myquant_research_refresh_passes_myquant_source_to_inference(self) -> None:
        from src.app.facades.data_management_facade import run_myquant_research_refresh_payload

        with (
            patch(
                "src.app.facades.data_management_facade.build_feature_label_artifacts",
                return_value={"panel_rows": 3, "date_max": "2026-04-16"},
            ) as build_features,
            patch(
                "src.app.facades.data_management_facade.generate_overlay_inference_report",
                return_value={"latest_feature_date": "2026-04-16", "candidate_count": 2},
            ) as generate_inference,
            patch("src.app.facades.base.clear_dashboard_caches"),
        ):
            payload = run_myquant_research_refresh_payload(user_id="user-1")

        self.assertTrue(payload["ok"])
        self.assertEqual(build_features.call_args.kwargs["data_source"], "myquant")
        self.assertEqual(generate_inference.call_args.kwargs["data_source"], "myquant")
