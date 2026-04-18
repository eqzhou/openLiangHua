from __future__ import annotations

from pathlib import Path

from src.utils.io import load_yaml, project_root

SUPPORTED_DATA_SOURCES = {"akshare", "tushare", "myquant"}
SOURCE_ALIASES = {
    "akshare": ("akshare", "tushare"),
    "tushare": ("tushare", "akshare"),
    "myquant": ("myquant",),
}


def normalize_data_source(value: str | None) -> str:
    source = (value or "akshare").strip().lower()
    if source not in SUPPORTED_DATA_SOURCES:
        raise ValueError(f"Unsupported data_source: {value}")
    return source


def source_aliases(value: str | None) -> tuple[str, ...]:
    normalized = normalize_data_source(value)
    return SOURCE_ALIASES.get(normalized, (normalized,))


def active_data_source() -> str:
    config = load_yaml(project_root() / "config" / "universe.yaml")
    return normalize_data_source(config.get("data_source", "akshare"))


def source_prefixed_path(directory: Path, filename: str, source: str) -> Path:
    normalized = normalize_data_source(source)
    return directory / f"{normalized}_{filename}"


def source_or_canonical_path(directory: Path, filename: str, source: str) -> Path:
    aliases = source_aliases(source)
    for alias in aliases:
        source_path = source_prefixed_path(directory, filename, alias)
        if source_path.exists():
            return source_path

    normalized = aliases[0]
    if normalized in {"akshare", "tushare"}:
        canonical_path = directory / filename
        if canonical_path.exists():
            return canonical_path
    return source_prefixed_path(directory, filename, normalized)
