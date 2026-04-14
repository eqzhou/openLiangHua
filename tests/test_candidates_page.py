from __future__ import annotations

import unittest

import pandas as pd

from src.app.viewmodels.candidates_vm import build_candidate_score_history, build_top_candidates_snapshot


class CandidatesPageTests(unittest.TestCase):
    def test_build_top_candidates_snapshot_uses_latest_trade_date_and_top_n(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-03-31", "ts_code": "000001.SZ", "score": 0.2},
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "score": 0.7},
                {"trade_date": "2026-04-01", "ts_code": "000002.SZ", "score": 0.5},
                {"trade_date": "2026-04-01", "ts_code": "000003.SZ", "score": 0.9},
            ]
        )

        snapshot = build_top_candidates_snapshot(frame, top_n=2)

        self.assertEqual(snapshot["ts_code"].tolist(), ["000003.SZ", "000001.SZ"])
        self.assertTrue((pd.to_datetime(snapshot["trade_date"]) == pd.Timestamp("2026-04-01")).all())

    def test_build_candidate_score_history_returns_recent_sorted_history(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-03-30", "ts_code": "000001.SZ", "score": 0.2, "ret_t1_t10": 0.01},
                {"trade_date": "2026-03-31", "ts_code": "000001.SZ", "score": 0.4, "ret_t1_t10": 0.03},
                {"trade_date": "2026-04-01", "ts_code": "000002.SZ", "score": 0.9, "ret_t1_t10": 0.08},
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "score": 0.7, "ret_t1_t10": 0.06},
            ]
        )

        history = build_candidate_score_history(frame, symbol="000001.SZ")

        self.assertEqual(history.columns.tolist(), ["综合评分", "未来10日收益(T+1建仓)"])
        self.assertEqual(history.index[-1], pd.Timestamp("2026-04-01"))
        self.assertAlmostEqual(float(history.iloc[-1]["综合评分"]), 0.7)


if __name__ == "__main__":
    unittest.main()
