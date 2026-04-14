from __future__ import annotations

import unittest

import pandas as pd

from src.app.viewmodels.overview_vm import build_equity_curve_frame, build_model_comparison_frame


class OverviewPageTests(unittest.TestCase):
    def test_build_model_comparison_frame_keeps_requested_columns(self) -> None:
        metrics_table = pd.DataFrame(
            [
                {"model": "A", "split": "test", "rank_ic_mean": 0.1, "top_n_hit_rate": 0.6, "ignored": 1},
            ]
        )

        result = build_model_comparison_frame(metrics_table, ["model", "split", "rank_ic_mean"])

        self.assertEqual(result.columns.tolist(), ["model", "split", "rank_ic_mean"])
        self.assertEqual(result.iloc[0]["model"], "A")

    def test_build_equity_curve_frame_concatenates_available_models(self) -> None:
        portfolios = {
            ("ridge", "test"): pd.DataFrame(
                [
                    {"trade_date": "2026-04-01", "equity_curve": 1.01},
                    {"trade_date": "2026-04-02", "equity_curve": 1.02},
                ]
            ),
            ("lgbm", "test"): pd.DataFrame(),
            ("ensemble", "test"): pd.DataFrame(
                [
                    {"trade_date": "2026-04-01", "equity_curve": 1.03},
                    {"trade_date": "2026-04-02", "equity_curve": 1.05},
                ]
            ),
        }

        frame = build_equity_curve_frame(
            model_names=["ridge", "lgbm", "ensemble"],
            split_name="test",
            model_labels={"ridge": "Ridge", "lgbm": "LGBM", "ensemble": "Ensemble"},
            load_portfolio=lambda model_name, split_name: portfolios.get((model_name, split_name), pd.DataFrame()),
        )

        self.assertEqual(frame.columns.tolist(), ["Ridge", "Ensemble"])
        self.assertAlmostEqual(float(frame.iloc[-1]["Ensemble"]), 1.05)


if __name__ == "__main__":
    unittest.main()
