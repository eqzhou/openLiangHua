from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.app.services.streamlit_runtime_service import get_streamlit_service_status


class StreamlitRuntimeServiceTests(unittest.TestCase):
    def test_missing_powershell_returns_fallback_status_instead_of_raising(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            scripts_dir = root / "scripts"
            scripts_dir.mkdir(parents=True, exist_ok=True)
            (scripts_dir / "streamlit_status.ps1").write_text("{}", encoding="utf-8")

            with patch(
                "src.app.services.streamlit_runtime_service.read_streamlit_status_payload",
                return_value=(None, "missing_powershell", "当前环境未安装 powershell/pwsh，已跳过 Windows 状态脚本。"),
            ):
                payload = get_streamlit_service_status(root)

        self.assertEqual(payload["status_label"], "状态脚本不可用")
        self.assertEqual(payload["status_label_display"], "状态脚本不可用")
        self.assertIn("powershell/pwsh", str(payload["err_log_tail"]))


if __name__ == "__main__":
    unittest.main()
