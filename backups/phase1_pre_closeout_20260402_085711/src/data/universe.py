from __future__ import annotations

from pathlib import Path

from src.utils.io import load_yaml, project_root


def load_universe(config_path: Path | None = None) -> dict:
    path = config_path or project_root() / "config" / "universe.yaml"
    config = load_yaml(path)
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
