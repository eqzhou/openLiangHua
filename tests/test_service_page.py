from __future__ import annotations

import unittest

from src.app.pages.service_page import format_listener_pids


class ServicePageTests(unittest.TestCase):
    def test_format_listener_pids_handles_list(self) -> None:
        self.assertEqual(format_listener_pids([1234, 5678]), "1234, 5678")

    def test_format_listener_pids_handles_none(self) -> None:
        self.assertEqual(format_listener_pids(None), "")


if __name__ == "__main__":
    unittest.main()
