from __future__ import annotations

from pathlib import Path

from src.app.repositories.runtime_repository import read_streamlit_status_payload
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
    payload, error_code, error_detail = read_streamlit_status_payload(resolved_root)
    if error_code == "missing_script":
        return status
    if error_code == "missing_powershell":
        status["status_label"] = "状态脚本不可用"
        status["status_label_display"] = "状态脚本不可用"
        status["err_log_tail"] = error_detail or ""
        return status
    if error_code == "script_failed":
        status["status_label"] = "状态脚本失败"
        if error_detail:
            status["err_log_tail"] = error_detail
        return status
    if error_code == "parse_failed" or payload is None:
        status["status_label"] = "状态解析失败"
        status["err_log_tail"] = error_detail or ""
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
