from __future__ import annotations

import unittest
from datetime import date

from src.app.ui.sidebar_actions import (
    build_current_config_caption,
    has_valid_config_dates,
    port_status_text,
)


class SidebarActionsTests(unittest.TestCase):
    def test_has_valid_config_dates_respects_order(self) -> None:
        self.assertTrue(has_valid_config_dates(date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1), date(2026, 4, 1)))
        self.assertFalse(has_valid_config_dates(date(2026, 2, 1), date(2026, 1, 1), date(2026, 3, 1), date(2026, 4, 1)))

    def test_port_status_text(self) -> None:
        self.assertEqual(port_status_text(True), "监听中")
        self.assertEqual(port_status_text(False), "未监听")

    def test_build_current_config_caption(self) -> None:
        self.assertEqual(build_current_config_caption("训练起点：2026-01-01"), "当前研究参数：训练起点：2026-01-01")


if __name__ == "__main__":
    unittest.main()
