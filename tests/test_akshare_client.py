from __future__ import annotations

import unittest
from unittest.mock import patch

from src.data.akshare_client import AKShareClient


class AKShareClientTests(unittest.TestCase):
    def test_current_st_symbols_returns_empty_set_when_endpoint_fails(self) -> None:
        client = AKShareClient()

        with patch("src.data.akshare_client.ak.stock_zh_a_st_em", side_effect=RuntimeError("boom")):
            self.assertEqual(client.current_st_symbols(), set())


if __name__ == "__main__":
    unittest.main()
