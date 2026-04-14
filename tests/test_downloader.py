from __future__ import annotations

import unittest

import pandas as pd

from src.data.downloader import _extend_symbols_with_watchlist


class DownloaderTests(unittest.TestCase):
    def test_extend_symbols_with_watchlist_appends_missing_watch_symbols(self) -> None:
        symbols = ["000001.SZ", "600036.SH"]
        metadata = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "平安银行", "index_code": "000905.SH"},
                {"ts_code": "600036.SH", "name": "招商银行", "index_code": "000905.SH"},
            ]
        )

        updated_symbols, updated_metadata = _extend_symbols_with_watchlist(
            symbols=symbols,
            metadata=metadata,
            watch_symbols=["000078.SZ", "000001.SZ"],
            default_index_code="000905.SH",
        )

        self.assertEqual(updated_symbols, ["000001.SZ", "600036.SH", "000078.SZ"])
        self.assertEqual(updated_metadata["ts_code"].tolist(), ["000001.SZ", "600036.SH", "000078.SZ"])
        self.assertEqual(
            updated_metadata.loc[updated_metadata["ts_code"] == "000078.SZ", "name"].iloc[0],
            "000078.SZ",
        )


if __name__ == "__main__":
    unittest.main()
