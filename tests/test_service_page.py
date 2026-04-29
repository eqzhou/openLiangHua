from __future__ import annotations

import unittest
import subprocess
from unittest.mock import patch

from src.app.pages.service_page import format_listener_pids


class ServicePageTests(unittest.TestCase):
    def test_format_listener_pids_handles_list(self) -> None:
        self.assertEqual(format_listener_pids([1234, 5678]), "1234, 5678")

    def test_format_listener_pids_handles_none(self) -> None:
        self.assertEqual(format_listener_pids(None), "")

    def test_service_payload_includes_read_only_health_sections(self) -> None:
        from src.app.facades.service_facade import _get_service_payload_cached, get_service_payload

        _get_service_payload_cached.cache_clear()
        self.addCleanup(_get_service_payload_cached.cache_clear)
        with (
            patch(
                "src.app.facades.service_facade.get_streamlit_service_status",
                return_value={
                    "effective_state": "stopped",
                    "status_label_display": "已停止",
                    "listener_present": False,
                    "out_log_tail": "streamlit out",
                    "err_log_tail": "",
                },
            ),
            patch("src.app.facades.service_facade._get_realtime_snapshot_summary", return_value={"available": False}),
            patch("src.app.facades.service_facade.build_service_health_payload", return_value={
                "apiStatus": {"available": True},
                "webStatus": {"available": False},
                "pm2Status": {"available": False},
                "logs": {"api": "api log"},
            }),
        ):
            payload = get_service_payload()

        self.assertIn("streamlitStatus", payload)
        self.assertIn("apiStatus", payload)
        self.assertIn("webStatus", payload)
        self.assertIn("pm2Status", payload)
        self.assertIn("logs", payload)

    def test_pm2_status_reports_timeout_without_raising(self) -> None:
        from src.app.facades.service_facade import _pm2_status

        with (
            patch("src.app.facades.service_facade.shutil.which", return_value="/usr/local/bin/pm2"),
            patch("src.app.facades.service_facade.subprocess.run", side_effect=subprocess.TimeoutExpired(["pm2", "jlist"], 5)),
        ):
            payload = _pm2_status()

        self.assertFalse(payload["available"])
        self.assertEqual(payload["processes"], [])
        self.assertIn("timed out", payload["message"])


if __name__ == "__main__":
    unittest.main()
