from __future__ import annotations

import unittest

import pandas as pd

from src.app.services.holding_snapshot_service import latest_symbol_bar


class HoldingSnapshotServiceTests(unittest.TestCase):
    def test_latest_symbol_bar_returns_none_when_symbol_column_missing(self) -> None:
        latest_row, latest_valid = latest_symbol_bar(pd.DataFrame([{"close": 10.2}]), "000001.SZ")

        self.assertIsNone(latest_row)
        self.assertIsNone(latest_valid)


if __name__ == "__main__":
    unittest.main()
