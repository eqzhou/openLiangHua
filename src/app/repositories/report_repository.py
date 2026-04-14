from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from src.db.dashboard_artifact_keys import (
    binary_artifact_key,
    candidate_snapshot_artifact_key,
    factor_explorer_artifact_key,
    json_artifact_key,
    note_artifact_key,
    table_artifact_key,
    text_artifact_key,
    watchlist_artifact_key,
)
from src.db.dashboard_artifact_store import DashboardArtifact, get_dashboard_artifact_store
from src.utils.data_source import source_or_canonical_path
from src.utils.io import project_root


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        payload = line.strip()
        if not payload:
            continue
        try:
            records.append(json.loads(payload))
        except json.JSONDecodeError:
            continue
    return records


def file_summary(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"exists": False}
    stat = path.stat()
    return {
        "exists": True,
        "size_mb": round(stat.st_size / (1024 * 1024), 2),
        "updated": pd.Timestamp(stat.st_mtime, unit="s"),
    }


def _artifact_or_none(artifact_key: str) -> DashboardArtifact | None:
    try:
        return get_dashboard_artifact_store().get_artifact(artifact_key)
    except Exception:
        return None


def _json_artifact_payload(artifact_key: str) -> dict[str, Any] | list[dict[str, Any]] | None:
    artifact = _artifact_or_none(artifact_key)
    if not artifact or artifact.payload_json is None:
        return None
    return artifact.payload_json


def _text_artifact_payload(artifact_key: str) -> tuple[str, dict[str, Any]] | None:
    artifact = _artifact_or_none(artifact_key)
    if not artifact or artifact.payload_text is None:
        return None
    return artifact.payload_text, artifact.metadata_json


def _binary_artifact_payload(artifact_key: str) -> tuple[bytes, dict[str, Any]] | None:
    artifact = _artifact_or_none(artifact_key)
    if not artifact or artifact.payload_bytes is None:
        return None
    return artifact.payload_bytes, artifact.metadata_json


def _frame_from_records(records: Any) -> pd.DataFrame:
    if not isinstance(records, list):
        return pd.DataFrame()
    frame = pd.DataFrame(records)
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def _read_parquet_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def _read_parquet_bytes(content: bytes) -> pd.DataFrame:
    if not content:
        return pd.DataFrame()
    frame = pd.read_parquet(BytesIO(content))
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def _read_csv_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def load_dataset_summary(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, object]:
    if prefer_database:
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, "dataset_summary"))
        if isinstance(artifact_payload, dict):
            return artifact_payload

    resolved_root = root or project_root()
    data_dir = resolved_root / "data"
    raw_dir = data_dir / "raw" / data_source
    staging_dir = data_dir / "staging"
    feature_path = source_or_canonical_path(data_dir / "features", "feature_panel.parquet", data_source)
    label_path = source_or_canonical_path(data_dir / "labels", "label_panel.parquet", data_source)
    daily_bar_path = source_or_canonical_path(staging_dir, "daily_bar.parquet", data_source)

    summary: dict[str, object] = {
        "cached_symbols": len(list(raw_dir.glob("*.parquet"))) if raw_dir.exists() else 0,
        "daily_bar": file_summary(daily_bar_path),
        "features": file_summary(feature_path),
        "labels": file_summary(label_path),
    }
    if feature_path.exists():
        feature_panel = pd.read_parquet(feature_path, columns=["trade_date", "ts_code"])
        feature_panel["trade_date"] = pd.to_datetime(feature_panel["trade_date"], errors="coerce")
        summary["feature_rows"] = len(feature_panel)
        summary["feature_symbols"] = feature_panel["ts_code"].nunique()
        if not feature_panel.empty:
            summary["date_min"] = str(feature_panel["trade_date"].min().date())
            summary["date_max"] = str(feature_panel["trade_date"].max().date())
    return summary


def load_feature_panel(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "feature_panel"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "features", "feature_panel.parquet", data_source)
    return _read_parquet_frame(path)


def load_label_panel(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "label_panel"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "labels", "label_panel.parquet", data_source)
    return _read_parquet_frame(path)


def load_daily_bar(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "daily_bar"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "staging", "daily_bar.parquet", data_source)
    return _read_parquet_frame(path)


def load_metrics(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    split_name: str,
    prefer_database: bool = True,
) -> dict[str, Any]:
    if prefer_database:
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, f"metrics:{model_name}:{split_name}"))
        if isinstance(artifact_payload, dict):
            return artifact_payload

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_{split_name}_metrics.json",
        data_source,
    )
    return read_json(path)


def load_stability(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    prefer_database: bool = True,
) -> dict[str, Any]:
    if prefer_database:
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, f"stability:{model_name}"))
        if isinstance(artifact_payload, dict):
            return artifact_payload

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_stability.json",
        data_source,
    )
    return read_json(path)


def load_portfolio(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    split_name: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, f"portfolio:{model_name}:{split_name}"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_{split_name}_portfolio.csv",
        data_source,
    )
    return _read_csv_frame(path)


def load_predictions(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    split_name: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, f"predictions:{model_name}:{split_name}"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_{split_name}_predictions.csv",
        data_source,
    )
    return _read_csv_frame(path)


def load_feature_importance(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, f"feature_importance:{model_name}"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_feature_importance.csv",
        data_source,
    )
    return _read_csv_frame(path)


def load_diagnostic_table(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    split_name: str,
    table_name: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, f"diagnostic:{model_name}:{split_name}:{table_name}"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_{split_name}_{table_name}.csv",
        data_source,
    )
    return _read_csv_frame(path)


def load_overlay_candidates(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, "overlay_candidates"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "overlay_latest_candidates.csv", data_source)
    return _read_csv_frame(path)


def load_overlay_packet(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database:
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, "overlay_packet"))
        if isinstance(artifact_payload, dict):
            return artifact_payload

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "overlay_latest_packet.json", data_source)
    return read_json(path)


def load_overlay_brief(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> str:
    if prefer_database:
        artifact_payload = _text_artifact_payload(text_artifact_key(data_source, "overlay_brief"))
        if artifact_payload is not None:
            return artifact_payload[0]

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "overlay_latest_brief.md", data_source)
    return read_text(path)


def load_overlay_inference_candidates(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, "overlay_inference_candidates"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_candidates.csv",
        data_source,
    )
    return _read_csv_frame(path)


def load_overlay_inference_packet(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database:
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, "overlay_inference_packet"))
        if isinstance(artifact_payload, dict):
            return artifact_payload

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_packet.json",
        data_source,
    )
    return read_json(path)


def load_overlay_inference_brief(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> str:
    if prefer_database:
        artifact_payload = _text_artifact_payload(text_artifact_key(data_source, "overlay_inference_brief"))
        if artifact_payload is not None:
            return artifact_payload[0]

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_brief.md",
        data_source,
    )
    return read_text(path)


def load_watchlist_snapshot(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> pd.DataFrame | None:
    if prefer_database:
        artifact_payload = _json_artifact_payload(watchlist_artifact_key(data_source))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
    return None


def load_candidate_snapshot(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    split_name: str,
    prefer_database: bool = True,
) -> pd.DataFrame | None:
    if prefer_database:
        artifact_payload = _json_artifact_payload(candidate_snapshot_artifact_key(data_source, model_name, split_name))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
    return None


def load_factor_explorer_snapshot(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
) -> dict[str, Any] | None:
    if prefer_database:
        artifact_payload = _json_artifact_payload(factor_explorer_artifact_key(data_source))
        if isinstance(artifact_payload, dict):
            return artifact_payload
    return None


def load_latest_symbol_markdown(
    symbol: str,
    note_kind: str,
    *,
    root: Path | None = None,
    data_source: str,
    prefer_database: bool = True,
) -> dict[str, str]:
    normalized_symbol = str(symbol or "").strip()
    if not normalized_symbol:
        return {}

    if prefer_database:
        artifact_payload = _text_artifact_payload(note_artifact_key(data_source, normalized_symbol, note_kind))
        if artifact_payload is not None:
            content, metadata = artifact_payload
            return {
                "path": str(metadata.get("path", "")),
                "name": str(metadata.get("name", "")),
                "plan_date": str(metadata.get("plan_date", "")),
                "content": content,
            }

    resolved_root = root or project_root()
    reports_dir = resolved_root / "reports" / "weekly"
    base_code = normalized_symbol.split(".")[0]
    candidates: list[Path] = []
    seen_paths: set[Path] = set()
    patterns = [
        f"{base_code}_{note_kind}_*.md",
        f"{data_source}_{base_code}_{note_kind}_*.md",
    ]
    for pattern in patterns:
        for path in reports_dir.glob(pattern):
            resolved_path = path.resolve()
            if resolved_path in seen_paths:
                continue
            seen_paths.add(resolved_path)
            candidates.append(path)

    if not candidates:
        return {}

    latest_path = max(candidates, key=lambda item: item.stat().st_mtime)
    latest_name = latest_path.name
    latest_stem_parts = latest_path.stem.split("_")
    plan_date = latest_stem_parts[-1] if latest_stem_parts and len(latest_stem_parts[-1]) == 10 else ""
    return {
        "path": str(latest_path),
        "name": latest_name,
        "plan_date": plan_date,
        "content": read_text(latest_path),
    }
