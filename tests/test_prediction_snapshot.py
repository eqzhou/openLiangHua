from __future__ import annotations

import unittest

import pandas as pd

from src.utils.prediction_snapshot import build_latest_prediction_snapshot, latest_prediction_details


class PredictionSnapshotTests(unittest.TestCase):
    def test_build_latest_prediction_snapshot_ranks_latest_date_only(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-03-31", "ts_code": "000001.SZ", "score": 0.2},
                {"trade_date": "2026-03-31", "ts_code": "000002.SZ", "score": 0.4},
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "score": 0.7},
                {"trade_date": "2026-04-01", "ts_code": "000002.SZ", "score": 0.5},
                {"trade_date": "2026-04-01", "ts_code": "000003.SZ", "score": 0.9},
            ]
        )

        snapshot = build_latest_prediction_snapshot(frame)

        self.assertEqual(len(snapshot), 3)
        self.assertTrue((snapshot["trade_date"] == pd.Timestamp("2026-04-01")).all())
        self.assertEqual(snapshot.iloc[0]["ts_code"], "000003.SZ")
        self.assertEqual(int(snapshot.iloc[0]["rank"]), 1)
        self.assertAlmostEqual(float(snapshot.iloc[0]["rank_pct"]), 1.0)

    def test_latest_prediction_details_returns_rank_and_signal_date(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "name": "平安银行", "score": 0.7},
                {"trade_date": "2026-04-01", "ts_code": "000078.SZ", "name": "海王生物", "score": 0.3},
                {"trade_date": "2026-04-01", "ts_code": "000002.SZ", "name": "万科A", "score": 0.1},
            ]
        )

        details = latest_prediction_details(frame, "000078.SZ")

        self.assertEqual(pd.Timestamp(details["signal_date"]), pd.Timestamp("2026-04-01"))
        self.assertEqual(int(details["rank"]), 2)
        self.assertEqual(int(details["universe_size"]), 3)
        self.assertAlmostEqual(float(details["rank_pct"]), 2 / 3)


if __name__ == "__main__":
    unittest.main()
