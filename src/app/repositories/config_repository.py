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


def _load_primary_config(name: str, path: Path, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    database_payload = _load_config_from_database(name)
    if database_payload is not None:
        return database_payload
    payload = load_yaml_config(path, default=default)
    if payload:
        _save_config_to_database(name, payload)
    return payload


def load_experiment_config(root: Path | None = None, *, prefer_database: bool = True) -> dict[str, Any]:
    resolved_root = root or project_root()
    if prefer_database and _uses_primary_project_root(root):
        return _load_primary_config("experiment", resolved_root / "config" / "experiment.yaml")
    return load_yaml_config(resolved_root / "config" / "experiment.yaml")


def save_experiment_config(payload: dict[str, Any], root: Path | None = None) -> None:
    resolved_root = root or project_root()
    if _uses_primary_project_root(root):
        _save_config_to_database("experiment", payload)
        return
    save_yaml_config(resolved_root / "config" / "experiment.yaml", payload)


def load_watchlist_config(
    root: Path | None = None,
    *,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> dict[str, Any]:
    resolved_root = root or project_root()
    if prefer_database and _uses_primary_project_root(root):
        from src.app.repositories.postgres_watchlist_store import PostgresWatchlistStore
        from src.web_api.settings import get_api_settings

        try:
            resolved_user_id = str(user_id or "system").strip() or "system"
            store = PostgresWatchlistStore(get_api_settings())
            db_data = store.load_watchlist(resolved_user_id)
            if resolved_user_id == "system" and not db_data["holdings"] and not db_data["focus_pool"]:
                # Seed from YAML if DB is empty
                yaml_data = load_yaml_config(resolved_root / "config" / "watchlist.yaml", default={"holdings": [], "focus_pool": []})
                for item in yaml_data.get("holdings", []):
                    store.add_item(resolved_user_id, item["ts_code"], item.get("name", ""), "holding", cost=item.get("cost"), shares=item.get("shares"))
                for item in yaml_data.get("focus_pool", []):
                    store.add_item(resolved_user_id, item["ts_code"], item.get("name", ""), "focus", note=item.get("note"))
                return yaml_data
            return db_data
        except Exception as exc:
            import logging
            logging.getLogger("openlianghua.config").error(f"Failed to load watchlist from DB, falling back to YAML: {exc}")
            if user_id:
                return {"holdings": [], "focus_pool": []}
            return load_yaml_config(resolved_root / "config" / "watchlist.yaml", default={"holdings": [], "focus_pool": []})
            
    return load_yaml_config(resolved_root / "config" / "watchlist.yaml", default={"holdings": [], "focus_pool": []})


def load_universe_config(root: Path | None = None, *, prefer_database: bool = True) -> dict[str, Any]:
    resolved_root = root or project_root()
    if prefer_database and _uses_primary_project_root(root):
        return _load_primary_config("universe", resolved_root / "config" / "universe.yaml", default={})
    return load_yaml_config(resolved_root / "config" / "universe.yaml", default={})
