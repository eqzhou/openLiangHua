from __future__ import annotations

import unittest

from src.app.ui.ui_text import PAGE_OPTIONS


class AppShellTextTests(unittest.TestCase):
    def test_page_options_are_unique(self) -> None:
        self.assertEqual(len(PAGE_OPTIONS), len(set(PAGE_OPTIONS)))


if __name__ == "__main__":
    unittest.main()
