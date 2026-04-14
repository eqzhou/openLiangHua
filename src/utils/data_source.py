from __future__ import annotations

from pathlib import Path

from src.utils.io import load_yaml, project_root

SUPPORTED_DATA_SOURCES = {"akshare", "myquant"}


def normalize_data_source(value: str | None) -> str:
    source = (value or "akshare").strip().lower()
    if source not in SUPPORTED_DATA_SOURCES:
        raise ValueError(f"Unsupported data_source: {value}")
    return source


def active_data_source() -> str:
    config = load_yaml(project_root() / "config" / "universe.yaml")
    return normalize_data_source(config.get("data_source", "akshare"))


def source_prefixed_path(directory: Path, filename: str, source: str) -> Path:
    normalized = normalize_data_source(source)
    return directory / f"{normalized}_{filename}"


def source_or_canonical_path(directory: Path, filename: str, source: str) -> Path:
    normalized = normalize_data_source(source)
    source_path = source_prefixed_path(directory, filename, normalized)
    if source_path.exists():
        return source_path
    if normalized == "akshare":
        return directory / filename
    return source_path
