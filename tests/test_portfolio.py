from __future__ import annotations

import unittest

import pandas as pd

from src.backtest.portfolio import select_top_n, top_n_daily_portfolio


class PortfolioTests(unittest.TestCase):
    def test_top_n_daily_portfolio_reports_actual_selected_count(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "score": 0.9, "ret_next_1d": 0.02},
                {"trade_date": "2026-04-01", "ts_code": "000002.SZ", "score": 0.8, "ret_next_1d": 0.01},
            ]
        )

        portfolio = top_n_daily_portfolio(frame, score_col="score", top_n=5)

        self.assertEqual(len(portfolio), 1)
        self.assertEqual(int(portfolio.iloc[0]["selected_count"]), 2)

    def test_select_top_n_ignores_missing_group_column(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "score": 0.9},
                {"trade_date": "2026-04-01", "ts_code": "000002.SZ", "score": 0.8},
                {"trade_date": "2026-04-01", "ts_code": "000003.SZ", "score": 0.7},
            ]
        )

        selected = select_top_n(
            frame,
            score_col="score",
            top_n=2,
            group_col="industry",
            max_per_group=1,
        )

        self.assertEqual(selected["ts_code"].tolist(), ["000001.SZ", "000002.SZ"])


if __name__ == "__main__":
    unittest.main()
