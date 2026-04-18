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
        self.assertTrue(payload["tokenConfigured"])
        self.assertTrue(payload["envFileExists"])
        self.assertEqual(payload["dailyBar"]["latestTradeDate"], "2026-04-16")
        self.assertEqual(payload["dailyBar"]["rowCount"], 3)
        self.assertEqual(payload["dailyBar"]["symbolCount"], 2)
        self.assertEqual(payload["featurePanel"]["latestTradeDate"], "2026-04-16")
        self.assertEqual(payload["featurePanel"]["rowCount"], 2)
        self.assertEqual(payload["labelPanel"]["latestTradeDate"], "2026-04-16")
        self.assertEqual(payload["labelPanel"]["rowCount"], 2)
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
