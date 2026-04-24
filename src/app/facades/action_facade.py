from __future__ import annotations

from typing import Any

from src.app.services.dashboard_data_service import (
    list_available_actions,
    run_module,
    sync_dashboard_database,
)


def run_named_action(action_name: str) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    specs = {spec["actionName"]: spec for spec in list_available_actions()}
    if action_name not in specs:
        raise KeyError(action_name)
    spec = specs[action_name]
    ok, output = run_module(spec["moduleName"])
    if ok:
        sync_ok, sync_message = sync_dashboard_database()
        output = f"{output}\n\n[dashboard-db] {sync_message}".strip() if output else f"[dashboard-db] {sync_message}"
        ok = ok and sync_ok
    clear_dashboard_caches()
    return {
        "actionName": action_name,
        "label": spec["label"],
        "ok": ok,
        "output": output,
    }


def generate_watch_plan() -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    ok, output = run_module("src.agents.watch_plan")
    if ok:
        sync_ok, sync_message = sync_dashboard_database()
        output = f"{output}\n\n[dashboard-db] {sync_message}".strip() if output else f"[dashboard-db] {sync_message}"
        ok = ok and sync_ok
    clear_dashboard_caches()
    return {"actionName": "watch_plan", "ok": ok, "output": output}


def generate_action_memo() -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    ok, output = run_module("src.agents.action_memo")
    if ok:
        sync_ok, sync_message = sync_dashboard_database()
        output = f"{output}\n\n[dashboard-db] {sync_message}".strip() if output else f"[dashboard-db] {sync_message}"
        ok = ok and sync_ok
    clear_dashboard_caches()
    return {"actionName": "action_memo", "ok": ok, "output": output}


def clear_cache_payload() -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    clear_dashboard_caches()
    return {"ok": True}
