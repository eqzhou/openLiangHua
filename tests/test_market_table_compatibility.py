from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from src.app.repositories import holding_repository, report_repository


class MarketTableCompatibilityTests(unittest.TestCase):
    def test_report_daily_bar_falls_back_to_market_database_when_file_missing(self) -> None:
        frame = pd.DataFrame(
            [{"trade_date": "2026-04-08", "ts_code": "000001.SZ", "name": "平安银行", "close": 10.2}]
        )

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch("src.app.repositories.report_repository.load_daily_bar_from_market_database", return_value=frame):
                loaded = report_repository.load_daily_bar(root=root, data_source="akshare", prefer_database=False)

        self.assertEqual(len(loaded), 1)
        self.assertEqual(str(loaded.iloc[0]["ts_code"]), "000001.SZ")

    def test_holding_daily_bar_falls_back_to_market_database_when_file_missing(self) -> None:
        frame = pd.DataFrame(
            [{"trade_date": "2026-04-08", "ts_code": "000001.SZ", "name": "平安银行", "close": 10.2}]
        )

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch("src.app.repositories.holding_repository.load_daily_bar_from_market_database", return_value=frame):
                loaded = holding_repository.load_daily_bar_for_symbols(root=root, data_source="akshare", symbols=["000001.SZ"])

        self.assertEqual(len(loaded), 1)
        self.assertEqual(str(loaded.iloc[0]["ts_code"]), "000001.SZ")

    def test_holding_trade_dates_fall_back_to_market_database_when_file_missing(self) -> None:
        trade_dates = pd.Series(pd.to_datetime(["2026-04-07", "2026-04-08"]))

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch("src.app.repositories.holding_repository.load_trade_dates_from_market_database", return_value=trade_dates):
                loaded = holding_repository.load_trade_dates(root=root, data_source="akshare")

        self.assertEqual(len(loaded), 2)
        self.assertEqual(str(loaded.iloc[-1].date()), "2026-04-08")


if __name__ == "__main__":
    unittest.main()
