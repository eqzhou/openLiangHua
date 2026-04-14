from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.utils.io import project_root


def load_yaml_config(path: Path, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return dict(default or {})
    return yaml.safe_load(path.read_text(encoding="utf-8")) or dict(default or {})


def save_yaml_config(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def load_experiment_config(root: Path | None = None) -> dict[str, Any]:
    resolved_root = root or project_root()
    return load_yaml_config(resolved_root / "config" / "experiment.yaml")


def save_experiment_config(payload: dict[str, Any], root: Path | None = None) -> None:
    resolved_root = root or project_root()
    save_yaml_config(resolved_root / "config" / "experiment.yaml", payload)


def load_watchlist_config(root: Path | None = None) -> dict[str, Any]:
    resolved_root = root or project_root()
    return load_yaml_config(resolved_root / "config" / "watchlist.yaml", default={"holdings": []})
