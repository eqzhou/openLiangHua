from __future__ import annotations

from pathlib import Path

from src.app.repositories.config_repository import load_universe_config
from src.utils.io import project_root


def load_universe(config_path: Path | None = None) -> dict:
    if config_path is None:
        config = load_universe_config(project_root(), prefer_database=True)
    else:
        resolved = config_path
        if resolved.is_file() and resolved.name == "universe.yaml":
            root = resolved.parent.parent
        elif resolved.is_dir():
            root = resolved
        else:
            root = resolved.parent
        config = load_universe_config(root, prefer_database=False)
    mode = config.get("mode")
    if mode not in {"explicit", "current_index"}:
        raise ValueError("config/universe.yaml mode must be either explicit or current_index")
    if mode == "explicit" and not config.get("symbols"):
        raise ValueError("No symbols were found in config/universe.yaml")
    if mode == "current_index" and not config.get("index_code"):
        raise ValueError("index_code is required when mode is current_index")
    config["symbols"] = list(dict.fromkeys(config.get("symbols", []) or []))
    config["watch_symbols"] = list(dict.fromkeys(config.get("watch_symbols", []) or []))
    return config
