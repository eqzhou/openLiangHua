from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def save_parquet(frame: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    frame.to_parquet(path, index=False)


def save_text(text: str, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")
