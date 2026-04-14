from __future__ import annotations

import unittest

from src.utils.holding_marks import describe_price_reference


class DescribePriceReferenceTests(unittest.TestCase):
    def test_auto_price_uses_latest_bar_label(self) -> None:
        result = describe_price_reference(
            is_manual_mark=False,
            mark_date="2026-04-01",
            latest_bar_date="2026-04-01",
        )

        self.assertEqual(result["mark_status"], "最新日线收盘价")
        self.assertIn("2026-04-01", str(result["mark_status_note"]))
        self.assertEqual(result["mark_vs_latest_bar_days"], 0)

    def test_manual_price_ahead_of_latest_bar_is_flagged(self) -> None:
        result = describe_price_reference(
            is_manual_mark=True,
            mark_date="2026-04-01",
            latest_bar_date="2026-03-31",
        )

        self.assertEqual(result["mark_status"], "手工参考价(日线未到)")
        self.assertIn("2026-03-31", str(result["mark_status_note"]))
        self.assertIn("2026-04-01", str(result["mark_status_note"]))
        self.assertEqual(result["mark_vs_latest_bar_days"], 1)

    def test_manual_price_older_than_latest_bar_is_flagged(self) -> None:
        result = describe_price_reference(
            is_manual_mark=True,
            mark_date="2026-03-30",
            latest_bar_date="2026-03-31",
        )

        self.assertEqual(result["mark_status"], "手工参考价(早于日线)")
        self.assertIn("2026-03-31", str(result["mark_status_note"]))
        self.assertEqual(result["mark_vs_latest_bar_days"], -1)


if __name__ == "__main__":
    unittest.main()
