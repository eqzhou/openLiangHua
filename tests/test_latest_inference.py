from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd
import yaml

from src.models.latest_inference import generate_latest_inference


TEST_TMP_ROOT = Path(__file__).resolve().parent / ".tmp"


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


class LatestInferenceTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.case_root = TEST_TMP_ROOT / f"latest_inference_{uuid.uuid4().hex}"
        self.case_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root, ignore_errors=True)

    def test_generate_latest_inference_keeps_latest_unlabeled_snapshot(self) -> None:
        _write_yaml(self.case_root / "config" / "universe.yaml", {"data_source": "myquant"})
        _write_yaml(
            self.case_root / "config" / "experiment.yaml",
            {
                "train_start": "2026-03-01",
                "train_end": "2026-03-31",
                "valid_end": "2026-03-31",
                "test_end": "2026-04-01",
                "label_col": "ret_t1_t10",
                "top_n": 2,
                "rolling": {
                    "enabled": True,
                    "retrain_frequency": "monthly",
                    "train_window_size": 10,
                    "min_history_size": 2,
                },
                "selection": {
                    "neutralize_by_industry": False,
                    "industry_column": "industry",
                    "max_per_industry": None,
                },
                "filters": {
                    "exclude_current_name_st": True,
                    "require_can_enter_next_day": True,
                    "require_label_valid": True,
                    "min_listing_days": 30,
                },
                "risk_filter": {
                    "enabled": False,
                    "benchmark_code": "000905.SH",
                },
                "overlay": {
                    "candidate_pool_size": 5,
                    "top_n": 3,
                    "weight_mode": "validation_adaptive",
                    "weight_evaluation_split": "valid",
                    "min_model_weight": 0.2,
                    "lgbm_weight": 0.6,
                    "ridge_weight": 0.4,
                    "quant_weight": 0.7,
                    "factor_weight": 0.2,
                    "consensus_weight": 0.1,
                },
                "feature_selection": {
                    "enabled": False,
                },
                "lgbm": {
                    "n_estimators": 20,
                    "learning_rate": 0.05,
                    "num_leaves": 7,
                    "max_depth": 3,
                    "subsample": 1.0,
                    "colsample_bytree": 1.0,
                    "random_state": 42,
                    "min_child_samples": 1,
                    "reg_lambda": 0.0,
                    "reg_alpha": 0.0,
                },
            },
        )

        dates = pd.to_datetime(["2026-03-27", "2026-03-30", "2026-03-31", "2026-04-01"])
        symbols = [
            ("000001.SZ", "平安银行", "银行"),
            ("000002.SZ", "万科A", "地产"),
            ("000003.SZ", "国华网安", "软件"),
            ("000004.SZ", "国农科技", "电子"),
        ]

        feature_rows: list[dict[str, object]] = []
        label_rows: list[dict[str, object]] = []
        for date_idx, trade_date in enumerate(dates):
            for symbol_idx, (ts_code, name, industry) in enumerate(symbols):
                strength = (symbol_idx + 1) * 0.03 + date_idx * 0.01
                feature_rows.append(
                    {
                        "trade_date": trade_date,
                        "ts_code": ts_code,
                        "name": name,
                        "industry": industry,
                        "index_code": "000905.SH",
                        "is_current_name_st": False,
                        "is_index_member": True,
                        "days_since_list": 1000 + symbol_idx,
                        "index_weight": 0.25,
                        "pct_chg": 1.0 + strength,
                        "ret_1d": strength / 10.0,
                        "mom_5": strength,
                        "mom_20": strength * 1.1,
                        "mom_60": strength * 1.2,
                        "mom_120": strength * 1.3,
                        "vol_20": 0.02 + symbol_idx * 0.001,
                        "close_to_ma_20": strength * 0.8,
                        "vol_60": 0.03 + symbol_idx * 0.001,
                        "close_to_ma_60": strength * 0.9,
                        "amount_20": 1.0e8 + symbol_idx * 1.0e7,
                        "downside_vol_20": 0.01 + symbol_idx * 0.001,
                        "ret_skew_20": 0.1 + symbol_idx * 0.02,
                        "drawdown_60": -0.05 - symbol_idx * 0.01,
                    }
                )

                is_latest = trade_date == dates.max()
                label_rows.append(
                    {
                        "trade_date": trade_date,
                        "ts_code": ts_code,
                        "can_enter_next_day": False if is_latest else True,
                        "ret_next_1d": None if is_latest else strength / 20.0,
                        "label_valid_t5": not is_latest,
                        "ret_t1_t5": None if is_latest else strength * 0.5,
                        "label_valid_t10": not is_latest,
                        "ret_t1_t10": None if is_latest else strength * 0.8,
                        "label_valid_t20": not is_latest,
                        "ret_t1_t20": None if is_latest else strength,
                    }
                )

        _write_parquet(
            self.case_root / "data" / "features" / "myquant_feature_panel.parquet",
            pd.DataFrame(feature_rows),
        )
        _write_parquet(
            self.case_root / "data" / "labels" / "myquant_label_panel.parquet",
            pd.DataFrame(label_rows),
        )

        packet = generate_latest_inference(root=self.case_root)

        self.assertEqual(packet["latest_feature_date"], "2026-04-01")
        self.assertEqual(packet["latest_labeled_date"], "2026-03-31")
        self.assertEqual(packet["inference_universe_size"], 4)
        self.assertIn("can_enter_next_day", packet["skipped_filters"])
        self.assertEqual(packet["ensemble_weights"]["mode"], "manual_fallback")

        reports_dir = self.case_root / "reports" / "weekly"
        ridge_path = reports_dir / "myquant_ridge_inference_predictions.csv"
        lgbm_path = reports_dir / "myquant_lgbm_inference_predictions.csv"
        ensemble_path = reports_dir / "myquant_ensemble_inference_predictions.csv"
        packet_path = reports_dir / "myquant_inference_packet.json"
        self.assertTrue(ridge_path.exists())
        self.assertTrue(lgbm_path.exists())
        self.assertTrue(ensemble_path.exists())
        self.assertTrue(packet_path.exists())

        ridge_frame = pd.read_csv(ridge_path)
        ensemble_frame = pd.read_csv(ensemble_path)
        self.assertEqual(len(ridge_frame), 4)
        self.assertTrue((ridge_frame["can_enter_next_day"].fillna(False) == False).all())
        self.assertIn("ridge_score", ensemble_frame.columns)
        self.assertIn("lgbm_score", ensemble_frame.columns)
        self.assertIn("score", ensemble_frame.columns)
        self.assertGreater(ensemble_frame["score"].nunique(), 1)


if __name__ == "__main__":
    unittest.main()
