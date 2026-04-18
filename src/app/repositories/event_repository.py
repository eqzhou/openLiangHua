from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd

from src.db.dashboard_artifact_keys import (
    event_news_cache_artifact_key,
    event_notice_cache_artifact_key,
    event_research_cache_artifact_key,
)
from src.db.dashboard_artifact_store import DashboardArtifact, get_dashboard_artifact_store
from src.utils.io import ensure_dir


def _artifact_or_none(artifact_key: str) -> DashboardArtifact | None:
    try:
        return get_dashboard_artifact_store().get_artifact(artifact_key)
    except Exception:
        return None


def _frame_from_bytes(content: bytes | None) -> pd.DataFrame:
    if not content:
        return pd.DataFrame()
    return pd.read_parquet(BytesIO(content))


def _frame_to_bytes(frame: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _notice_cache_path(cache_dir: Path, notice_date: pd.Timestamp) -> Path:
    return cache_dir / f"notice_{notice_date.strftime('%Y%m%d')}.parquet"


def _research_cache_path(cache_dir: Path, symbol_code: str) -> Path:
    return cache_dir / f"research_{symbol_code}.parquet"


def _news_cache_path(cache_dir: Path, symbol_code: str) -> Path:
    return cache_dir / f"news_{symbol_code}.parquet"


def load_notice_cache(
    cache_dir: Path,
    notice_date: pd.Timestamp,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database:
        artifact = _artifact_or_none(event_notice_cache_artifact_key(data_source, notice_date.strftime("%Y%m%d")))
        if artifact and artifact.payload_bytes is not None:
            return _frame_from_bytes(artifact.payload_bytes)

    path = _notice_cache_path(cache_dir, notice_date)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def save_notice_cache(
    cache_dir: Path,
    notice_date: pd.Timestamp,
    frame: pd.DataFrame,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> None:
    ensure_dir(cache_dir)
    path = _notice_cache_path(cache_dir, notice_date)
    frame.to_parquet(path, index=False)

    if prefer_database:
        content = _frame_to_bytes(frame)
        get_dashboard_artifact_store().upsert_bytes(
            artifact_key=event_notice_cache_artifact_key(data_source, notice_date.strftime("%Y%m%d")),
            data_source=data_source,
            artifact_kind="parquet",
            content=content,
            metadata={
                "cache_kind": "event_notice",
                "notice_date": notice_date.strftime("%Y-%m-%d"),
                "source_path": str(path),
                "size_bytes": len(content),
            },
        )


def load_news_cache(
    cache_dir: Path,
    symbol_code: str,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database:
        artifact = _artifact_or_none(event_news_cache_artifact_key(data_source, symbol_code))
        if artifact and artifact.payload_bytes is not None:
            return _frame_from_bytes(artifact.payload_bytes)

    path = _news_cache_path(cache_dir, symbol_code)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def save_news_cache(
    cache_dir: Path,
    symbol_code: str,
    frame: pd.DataFrame,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> None:
    ensure_dir(cache_dir)
    path = _news_cache_path(cache_dir, symbol_code)
    frame.to_parquet(path, index=False)

    if prefer_database:
        content = _frame_to_bytes(frame)
        get_dashboard_artifact_store().upsert_bytes(
            artifact_key=event_news_cache_artifact_key(data_source, symbol_code),
            data_source=data_source,
            artifact_kind="parquet",
            content=content,
            metadata={
                "cache_kind": "event_news",
                "symbol_code": symbol_code,
                "source_path": str(path),
                "size_bytes": len(content),
            },
        )


def load_research_cache(
    cache_dir: Path,
    symbol_code: str,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database:
        artifact = _artifact_or_none(event_research_cache_artifact_key(data_source, symbol_code))
        if artifact and artifact.payload_bytes is not None:
            return _frame_from_bytes(artifact.payload_bytes)

    path = _research_cache_path(cache_dir, symbol_code)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def save_research_cache(
    cache_dir: Path,
    symbol_code: str,
    frame: pd.DataFrame,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> None:
    ensure_dir(cache_dir)
    path = _research_cache_path(cache_dir, symbol_code)
    frame.to_parquet(path, index=False)

    if prefer_database:
        content = _frame_to_bytes(frame)
        get_dashboard_artifact_store().upsert_bytes(
            artifact_key=event_research_cache_artifact_key(data_source, symbol_code),
            data_source=data_source,
            artifact_kind="parquet",
            content=content,
            metadata={
                "cache_kind": "event_research",
                "symbol_code": symbol_code,
                "source_path": str(path),
                "size_bytes": len(content),
            },
        )
