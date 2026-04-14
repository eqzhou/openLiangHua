from __future__ import annotations

import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from src.app.repositories import config_repository, report_repository
from src.db.dashboard_artifact_store import DashboardArtifact


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


class DatabaseRepositoryTests(unittest.TestCase):
    def test_load_experiment_config_prefers_database_payload(self) -> None:
        original = config_repository._load_config_from_database
        try:
            config_repository._load_config_from_database = lambda name: {"label_col": "ret_t1_t5", "top_n": 8} if name == "experiment" else None

            payload = config_repository.load_experiment_config(prefer_database=True)

            self.assertEqual(payload["label_col"], "ret_t1_t5")
            self.assertEqual(payload["top_n"], 8)
        finally:
            config_repository._load_config_from_database = original

    def test_load_daily_bar_prefers_database_binary_artifact(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "close": 12.3},
                {"trade_date": "2026-04-03", "ts_code": "000001.SZ", "close": 12.6},
            ]
        )
        artifact = DashboardArtifact(
            artifact_key="akshare:binary:daily_bar",
            data_source="akshare",
            artifact_kind="parquet",
            payload_json=None,
            payload_text=None,
            payload_bytes=_parquet_bytes(frame),
            metadata_json={},
        )
        original = report_repository._artifact_or_none
        try:
            report_repository._artifact_or_none = lambda key: artifact if key == "akshare:binary:daily_bar" else None

            loaded = report_repository.load_daily_bar(data_source="akshare", prefer_database=True)

            self.assertEqual(len(loaded), 2)
            self.assertEqual(loaded["ts_code"].tolist(), ["000001.SZ", "000001.SZ"])
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(loaded["trade_date"]))
        finally:
            report_repository._artifact_or_none = original

    def test_load_feature_panel_falls_back_to_file_when_database_missing(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "mom_5": 0.12},
            ]
        )
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            feature_dir = root / "data" / "features"
            feature_dir.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(feature_dir / "akshare_feature_panel.parquet", index=False)

            original = report_repository._artifact_or_none
            try:
                report_repository._artifact_or_none = lambda key: None
                loaded = report_repository.load_feature_panel(root=root, data_source="akshare", prefer_database=True)
            finally:
                report_repository._artifact_or_none = original

            self.assertEqual(len(loaded), 1)
            self.assertEqual(str(loaded.iloc[0]["ts_code"]), "000001.SZ")


if __name__ == "__main__":
    unittest.main()
