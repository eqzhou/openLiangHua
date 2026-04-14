from __future__ import annotations

import unittest

import pandas as pd

from src.data.myquant_panel import drop_trailing_empty_price_dates, trim_open_dates_to_bars


class MyQuantPanelHelpersTests(unittest.TestCase):
    def test_trim_open_dates_to_latest_bar_date(self) -> None:
        open_dates = pd.Series(pd.to_datetime(["2026-03-31", "2026-04-01", "2026-04-02"]))
        bars = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2026-03-31", "2026-04-01"]),
                "ts_code": ["000001.SZ", "000001.SZ"],
                "close": [10.0, 10.2],
            }
        )

        trimmed, latest_bar_date = trim_open_dates_to_bars(open_dates, bars)

        self.assertEqual(trimmed.dt.strftime("%Y-%m-%d").tolist(), ["2026-03-31", "2026-04-01"])
        self.assertEqual(str(latest_bar_date.date()), "2026-04-01")

    def test_drop_trailing_empty_price_dates_only_removes_tail(self) -> None:
        panel = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(
                    [
                        "2026-03-31",
                        "2026-03-31",
                        "2026-04-01",
                        "2026-04-01",
                        "2026-04-02",
                        "2026-04-02",
                    ]
                ),
                "ts_code": [
                    "000001.SZ",
                    "000002.SZ",
                    "000001.SZ",
                    "000002.SZ",
                    "000001.SZ",
                    "000002.SZ",
                ],
                "open": [10.0, 8.0, 10.2, None, None, None],
                "high": [10.3, 8.2, 10.5, None, None, None],
                "low": [9.9, 7.9, 10.0, None, None, None],
                "close": [10.1, None, 10.4, None, None, None],
                "vol": [100, None, 110, None, None, None],
                "amount": [1000.0, None, 1200.0, None, None, None],
            }
        )

        trimmed, trailing_dates = drop_trailing_empty_price_dates(panel)

        self.assertEqual(trimmed["trade_date"].dt.strftime("%Y-%m-%d").unique().tolist(), ["2026-03-31", "2026-04-01"])
        self.assertEqual([str(value.date()) for value in trailing_dates], ["2026-04-02"])


if __name__ == "__main__":
    unittest.main()
