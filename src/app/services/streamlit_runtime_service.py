from __future__ import annotations

import json
import subprocess
from pathlib import Path

from src.utils.io import project_root


def _join_log_lines(value: object) -> str:
    if isinstance(value, list):
        return "\n".join(str(item) for item in value if str(item).strip())
    if value is None:
        return ""
    return str(value)


def get_streamlit_service_status(root: Path | None = None) -> dict[str, object]:
    resolved_root = root or project_root()
    status_script_path = resolved_root / "scripts" / "streamlit_status.ps1"
    status = {
        "supervisor_pid": None,
        "streamlit_pid": None,
        "supervisor_running": False,
        "streamlit_running": False,
        "listener_present": False,
        "listener_pids": [],
        "effective_state": "unknown",
        "status_label": "未知",
        "stale_supervisor_pid": False,
        "stale_streamlit_pid": False,
        "stale_status": False,
        "last_status": None,
        "status_label_display": "未知",
        "out_log_tail": "",
        "err_log_tail": "",
    }
    if not status_script_path.exists():
        return status

    result = subprocess.run(
        [
            "powershell",
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
        status["status_label"] = "状态脚本失败"
        if result.stderr:
            status["err_log_tail"] = result.stderr.strip()
        return status

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        status["status_label"] = "状态解析失败"
        status["err_log_tail"] = result.stdout.strip()[-2000:]
        return status

    status.update(payload)
    listener_pids = status.get("listener_pids")
    if listener_pids is None:
        status["listener_pids"] = []
    elif isinstance(listener_pids, list):
        status["listener_pids"] = listener_pids
    else:
        status["listener_pids"] = [listener_pids]

    label_map = {
        "running": "运行中",
        "starting": "启动中",
        "stopped": "已停止",
        "port_busy": "端口被占用",
        "listener_without_supervisor": "端口监听中(无守护进程)",
    }
    status["status_label_display"] = label_map.get(
        str(status.get("status_label", "")),
        str(status.get("status_label", "未知")),
    )
    status["out_log_tail"] = _join_log_lines(status.get("out_log_tail"))
    status["err_log_tail"] = _join_log_lines(status.get("err_log_tail"))
    return status
