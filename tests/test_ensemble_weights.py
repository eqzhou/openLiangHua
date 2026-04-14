from __future__ import annotations

import json
import shutil
import unittest
import uuid
from pathlib import Path

from src.agents.ensemble_weights import resolve_model_weights


TEST_TMP_ROOT = Path(__file__).resolve().parent / ".tmp"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _metrics(
    annualized_return: float,
    sharpe: float,
    rank_ic: float,
    drawdown: float,
    turnover: float,
) -> dict[str, float]:
    return {
        "observations": 100.0,
        "dates": 20.0,
        "daily_portfolio_annualized_return": annualized_return,
        "daily_portfolio_sharpe": sharpe,
        "rank_ic_mean": rank_ic,
        "daily_portfolio_max_drawdown": drawdown,
        "avg_turnover_ratio": turnover,
    }


class ResolveModelWeightsTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.reports_dir = TEST_TMP_ROOT / f"case_{uuid.uuid4().hex}"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.reports_dir, ignore_errors=True)

    def test_missing_metrics_falls_back_to_manual_weights(self) -> None:
        _write_json(self.reports_dir / "myquant_lgbm_valid_metrics.json", _metrics(0.12, 0.7, 0.05, -0.10, 0.35))
        _write_json(self.reports_dir / "myquant_lgbm_stability.json", {"grade": "较稳"})
        _write_json(self.reports_dir / "myquant_ridge_stability.json", {"grade": "较稳"})

        result = resolve_model_weights(
            reports_dir=self.reports_dir,
            data_source="myquant",
            overlay_config={
                "weight_mode": "validation_adaptive",
                "weight_evaluation_split": "valid",
                "lgbm_weight": 0.6,
                "ridge_weight": 0.4,
                "min_model_weight": 0.2,
            },
        )

        self.assertEqual(result["mode"], "manual_fallback")
        self.assertAlmostEqual(result["weights"]["lgbm"], 0.6)
        self.assertAlmostEqual(result["weights"]["ridge"], 0.4)

    def test_validation_adaptive_prefers_stronger_model(self) -> None:
        _write_json(self.reports_dir / "myquant_lgbm_valid_metrics.json", _metrics(0.03, 0.20, 0.02, -0.15, 0.65))
        _write_json(self.reports_dir / "myquant_ridge_valid_metrics.json", _metrics(0.14, 0.80, 0.09, -0.12, 0.38))
        _write_json(self.reports_dir / "myquant_lgbm_stability.json", {"grade": "较稳"})
        _write_json(self.reports_dir / "myquant_ridge_stability.json", {"grade": "较稳"})

        result = resolve_model_weights(
            reports_dir=self.reports_dir,
            data_source="myquant",
            overlay_config={
                "weight_mode": "validation_adaptive",
                "weight_evaluation_split": "valid",
                "lgbm_weight": 0.6,
                "ridge_weight": 0.4,
                "min_model_weight": 0.2,
            },
        )

        self.assertEqual(result["mode"], "validation_adaptive")
        self.assertGreater(result["weights"]["ridge"], result["weights"]["lgbm"])
        self.assertGreater(result["weights"]["ridge"], 0.5)


if __name__ == "__main__":
    unittest.main()
