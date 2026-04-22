from __future__ import annotations

from pathlib import Path

from src.app.repositories.config_repository import load_universe_config
from src.utils.io import project_root

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
    config = load_universe_config(project_root(), prefer_database=True)
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


def detect_materialized_data_source(root: Path | None = None, preferred_source: str | None = None) -> str:
    resolved_root = root or project_root()
    preferred = normalize_data_source(preferred_source or active_data_source())

    tracked_files = [
        (resolved_root / "data" / "staging", "daily_bar.parquet"),
        (resolved_root / "data" / "features", "feature_panel.parquet"),
        (resolved_root / "data" / "labels", "label_panel.parquet"),
        (resolved_root / "reports" / "weekly", "ensemble_inference_predictions.csv"),
    ]

    source_scores = {source: 0 for source in SUPPORTED_DATA_SOURCES}
    for source in SUPPORTED_DATA_SOURCES:
        for directory, filename in tracked_files:
            if source_prefixed_path(directory, filename, source).exists():
                source_scores[source] += 1

    best_score = max(source_scores.values(), default=0)
    if best_score > 0:
        if source_scores.get(preferred, 0) == best_score:
            return preferred
        for source in (preferred, "akshare", "tushare", "myquant"):
            if source_scores.get(source, 0) == best_score:
                return source

    return preferred
