from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from src.utils.io import project_root


def read_streamlit_status_payload(root: Path | None = None) -> tuple[dict[str, object] | None, str | None, str | None]:
    resolved_root = root or project_root()
    status_script_path = resolved_root / "scripts" / "streamlit_status.ps1"
    if not status_script_path.exists():
        return None, "missing_script", None

    shell_command = next((command for command in ("pwsh", "powershell") if shutil.which(command)), None)
    if shell_command is None:
        return None, "missing_powershell", "当前环境未安装 powershell/pwsh，已跳过 Windows 状态脚本。"

    result = subprocess.run(
        [
            shell_command,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(status_script_path),
        ],
        cwd=str(resolved_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None, "script_failed", result.stderr.strip() if result.stderr else None

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None, "parse_failed", result.stdout.strip()[-2000:]

    return payload, None, None
