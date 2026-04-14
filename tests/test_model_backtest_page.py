from __future__ import annotations

import unittest

import pandas as pd

from src.app.viewmodels.model_backtest_vm import build_monthly_summary, normalize_regime_view


class ModelBacktestPageTests(unittest.TestCase):
    def test_build_monthly_summary_groups_net_returns_by_month(self) -> None:
        portfolio = pd.DataFrame(
            [
                {"trade_date": "2026-03-30", "net_return": 0.01},
                {"trade_date": "2026-03-31", "net_return": -0.02},
                {"trade_date": "2026-04-01", "net_return": 0.03},
            ]
        )

        summary = build_monthly_summary(portfolio)

        self.assertEqual(summary["month"].tolist(), ["2026-03", "2026-04"])
        self.assertAlmostEqual(float(summary.iloc[0]["net_return"]), -0.01)
        self.assertAlmostEqual(float(summary.iloc[1]["net_return"]), 0.03)

    def test_normalize_regime_view_relabels_known_regimes(self) -> None:
        frame = pd.DataFrame(
            [
                {"regime": "trend_on", "value": 1},
                {"regime": "trend_off", "value": 2},
                {"regime": "other", "value": 3},
            ]
        )

        normalized = normalize_regime_view(frame)

        self.assertEqual(normalized["regime"].tolist(), ["趋势开启", "趋势过滤", "other"])


if __name__ == "__main__":
    unittest.main()
