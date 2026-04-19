from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd


class ReportRepositoryTests(unittest.TestCase):
    def test_load_daily_bar_backfills_missing_industry_from_snapshot_metadata(self) -> None:
        from src.app.repositories.report_repository import load_daily_bar

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            staging_dir = root / "data" / "staging"
            staging_dir.mkdir(parents=True)

            pd.DataFrame(
                [
                    {"trade_date": "2026-04-16", "ts_code": "600036.SH", "name": "招商银行", "industry": "", "close": 39.91},
                    {"trade_date": "2026-04-16", "ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "close": 12.31},
                ]
            ).to_parquet(staging_dir / "akshare_daily_bar.parquet", index=False)

            with patch(
                "src.app.repositories.report_repository._fetch_symbol_snapshot_industries",
                return_value={"600036.SH": "银行Ⅱ"},
            ):
                frame = load_daily_bar(root, data_source="akshare", prefer_database=False)

        industry_map = frame.set_index("ts_code")["industry"].to_dict()
        self.assertEqual(industry_map["600036.SH"], "银行Ⅱ")
        self.assertEqual(industry_map["000001.SZ"], "银行")


if __name__ == "__main__":
    unittest.main()
