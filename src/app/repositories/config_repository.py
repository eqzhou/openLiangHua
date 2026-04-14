from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.db.dashboard_artifact_keys import config_artifact_key
from src.db.dashboard_artifact_store import get_dashboard_artifact_store
from src.utils.io import project_root


def _uses_primary_project_root(root: Path | None) -> bool:
    if root is None:
        return True
    return root.resolve() == project_root().resolve()


def load_yaml_config(path: Path, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return dict(default or {})
    return yaml.safe_load(path.read_text(encoding="utf-8")) or dict(default or {})


def save_yaml_config(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _load_config_from_database(name: str) -> dict[str, Any] | None:
    try:
        artifact = get_dashboard_artifact_store().get_artifact(config_artifact_key(name))
    except Exception:
        return None

    if not artifact or artifact.payload_json is None:
        return None
    if isinstance(artifact.payload_json, dict):
        return artifact.payload_json
    return None


def _save_config_to_database(name: str, payload: dict[str, Any]) -> None:
    try:
        get_dashboard_artifact_store().upsert_json(
            artifact_key=config_artifact_key(name),
            data_source="shared",
            artifact_kind="config",
            payload=payload,
            metadata={"config_name": name},
        )
    except Exception:
        return


def load_experiment_config(root: Path | None = None, *, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database and _uses_primary_project_root(root):
        database_payload = _load_config_from_database("experiment")
        if database_payload is not None:
            return database_payload

    resolved_root = root or project_root()
    return load_yaml_config(resolved_root / "config" / "experiment.yaml")


def save_experiment_config(payload: dict[str, Any], root: Path | None = None) -> None:
    resolved_root = root or project_root()
    save_yaml_config(resolved_root / "config" / "experiment.yaml", payload)
    if _uses_primary_project_root(root):
        _save_config_to_database("experiment", payload)


def load_watchlist_config(root: Path | None = None, *, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database and _uses_primary_project_root(root):
        database_payload = _load_config_from_database("watchlist")
        if database_payload is not None:
            return database_payload

    resolved_root = root or project_root()
    return load_yaml_config(resolved_root / "config" / "watchlist.yaml", default={"holdings": []})


def load_universe_config(root: Path | None = None, *, prefer_database: bool = False) -> dict[str, Any]:
    resolved_root = root or project_root()
    return load_yaml_config(resolved_root / "config" / "universe.yaml", default={})
