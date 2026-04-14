from __future__ import annotations

import unittest

import pandas as pd

from src.app.viewmodels.factor_explorer_vm import (
    build_factor_ranking,
    build_latest_factor_snapshot,
    build_missing_rate_table,
    list_numeric_factor_columns,
)


class FactorExplorerPageTests(unittest.TestCase):
    def test_list_numeric_factor_columns_excludes_identity_columns(self) -> None:
        feature_panel = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "name": "A", "mom_20": 1.2, "industry": "医药"},
            ]
        )

        columns = list_numeric_factor_columns(feature_panel)

        self.assertEqual(columns, ["mom_20"])

    def test_build_factor_ranking_orders_descending(self) -> None:
        cross_section = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "A", "mom_20": 0.1},
                {"ts_code": "000002.SZ", "name": "B", "mom_20": 0.5},
            ]
        )

        ranking = build_factor_ranking(cross_section, "mom_20")

        self.assertEqual(ranking["ts_code"].tolist(), ["000002.SZ", "000001.SZ"])

    def test_build_missing_rate_table_summarizes_na_ratio(self) -> None:
        feature_panel = pd.DataFrame(
            [
                {"mom_20": 1.0, "mom_60": None},
                {"mom_20": None, "mom_60": None},
            ]
        )

        result = build_missing_rate_table(feature_panel, ["mom_20", "mom_60"])

        self.assertEqual(result.iloc[0]["feature"], "mom_60")
        self.assertAlmostEqual(float(result.iloc[0]["missing_rate"]), 1.0)

    def test_build_latest_factor_snapshot_formats_values(self) -> None:
        cross_section = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "A", "mom_20": 0.3},
            ]
        )

        snapshot = build_latest_factor_snapshot(cross_section, symbol="000001.SZ", zh=lambda name: f"ZH:{name}")

        self.assertEqual(snapshot.columns.tolist(), ["字段", "原始列名", "数值"])
        self.assertIn("ZH:ts_code", snapshot["字段"].tolist())


if __name__ == "__main__":
    unittest.main()
