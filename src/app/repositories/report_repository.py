from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from src.app.repositories.postgres_market_repository import load_daily_bar_from_market_database
from src.db.dashboard_artifact_keys import (
    binary_artifact_key,
    candidate_snapshot_artifact_key,
    factor_explorer_artifact_key,
    json_artifact_key,
    llm_bridge_export_artifact_key,
    note_artifact_key,
    overlay_llm_response_summary_artifact_key,
    overlay_llm_responses_artifact_key,
    table_artifact_key,
    text_artifact_key,
    watchlist_artifact_key,
)
from src.db.dashboard_artifact_store import DashboardArtifact, get_dashboard_artifact_store
from src.utils.data_source import source_or_canonical_path, source_prefixed_path
from src.utils.io import ensure_dir, project_root, save_text


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


def _parse_jsonl_text(content: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in str(content or "").splitlines():
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


def _write_csv_variants(reports_dir: Path, filename: str, data_source: str, frame: pd.DataFrame) -> Path:
    source_path = source_prefixed_path(reports_dir, filename, data_source)
    frame.to_csv(source_path, index=False, encoding="utf-8-sig")
    frame.to_csv(reports_dir / filename, index=False, encoding="utf-8-sig")
    return source_path


def _write_json_variants(reports_dir: Path, filename: str, data_source: str, payload: Any) -> Path:
    source_path = source_prefixed_path(reports_dir, filename, data_source)
    text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    source_path.write_text(text, encoding="utf-8")
    (reports_dir / filename).write_text(text, encoding="utf-8")
    return source_path


def save_binary_dataset(
    root: Path | None = None,
    *,
    data_source: str,
    directory: str,
    filename: str,
    artifact_name: str,
    frame: pd.DataFrame,
    write_canonical: bool = True,
) -> Path:
    resolved_root = root or project_root()
    output_dir = ensure_dir(resolved_root / directory)
    source_path = source_prefixed_path(output_dir, filename, data_source)
    frame.to_parquet(source_path, index=False)
    if write_canonical:
        frame.to_parquet(output_dir / filename, index=False)

    get_dashboard_artifact_store().upsert_bytes(
        artifact_key=binary_artifact_key(data_source, artifact_name),
        data_source=data_source,
        artifact_kind="parquet",
        content=source_path.read_bytes(),
        metadata={
            "artifact_name": artifact_name,
            "source_path": str(source_path),
            "rows": int(len(frame)),
            "columns": list(frame.columns),
        },
    )
    return source_path


def save_json_report(
    root: Path | None = None,
    *,
    data_source: str,
    filename: str,
    payload: dict[str, Any],
    artifact_name: str | None = None,
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    source_path = _write_json_variants(reports_dir, filename, data_source, payload)
    if artifact_name:
        get_dashboard_artifact_store().upsert_json(
            artifact_key=json_artifact_key(data_source, artifact_name),
            data_source=data_source,
            artifact_kind="json",
            payload=payload,
            metadata={"source_path": str(source_path)},
        )
    return source_path


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


def load_trade_calendar(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "trade_calendar"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "staging", "trade_calendar.parquet", data_source)
    return _read_parquet_frame(path)


def load_daily_bar(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database:
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "daily_bar"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "staging", "daily_bar.parquet", data_source)
    frame = _read_parquet_frame(path)
    if not frame.empty and "ts_code" in frame.columns:
        return frame

    fallback_symbols: list[str] = []
    try:
        from src.app.repositories.config_repository import load_watchlist_config

        watchlist_config = load_watchlist_config(resolved_root, prefer_database=False)
        fallback_symbols = [
            str(item.get("ts_code", "") or "").strip()
            for item in watchlist_config.get("holdings", [])
            if str(item.get("ts_code", "") or "").strip()
        ]
    except Exception:
        fallback_symbols = []

    return load_daily_bar_from_market_database(fallback_symbols)


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


def load_overlay_inference_shortlist(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> str:
    if prefer_database:
        artifact_payload = _text_artifact_payload(text_artifact_key(data_source, "overlay_inference_shortlist"))
        if artifact_payload is not None:
            return artifact_payload[0]

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_shortlist.md",
        data_source,
    )
    return read_text(path)


def load_inference_packet(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database:
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, "inference_packet"))
        if isinstance(artifact_payload, dict):
            return artifact_payload

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "inference_packet.json", data_source)
    return read_json(path)


def load_ensemble_weights(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database:
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, "ensemble_weights"))
        if isinstance(artifact_payload, dict):
            return artifact_payload

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "ensemble_weights.json", data_source)
    return read_json(path)


def load_overlay_llm_bundle(
    root: Path | None = None,
    *,
    data_source: str,
    scope: str,
    packet: dict[str, Any] | None = None,
    prefer_database: bool = True,
) -> dict[str, Any]:
    normalized_scope = "inference" if scope == "inference" else "historical"
    response_records: list[dict[str, Any]] = []
    response_summary = ""
    records_loaded_from_store = False
    summary_loaded_from_store = False

    if prefer_database:
        records_artifact = _text_artifact_payload(overlay_llm_responses_artifact_key(data_source, normalized_scope))
        if records_artifact is not None:
            response_records = _parse_jsonl_text(records_artifact[0])
            records_loaded_from_store = True

        summary_artifact = _text_artifact_payload(overlay_llm_response_summary_artifact_key(data_source, normalized_scope))
        if summary_artifact is not None:
            response_summary = summary_artifact[0]
            summary_loaded_from_store = True

    llm_bridge = dict((packet or {}).get("llm_bridge", {}) or {})
    if not records_loaded_from_store:
        response_path_text = str(llm_bridge.get("response_jsonl_path", "") or "").strip()
        if response_path_text:
            response_records = read_jsonl_records(Path(response_path_text))

    if not summary_loaded_from_store:
        summary_path_text = str(llm_bridge.get("response_summary_path", "") or "").strip()
        if summary_path_text:
            response_summary = read_text(Path(summary_path_text))

    response_lookup = {
        str(record.get("custom_id", "")).strip(): record
        for record in response_records
        if str(record.get("custom_id", "")).strip()
    }
    return {
        "response_lookup": response_lookup,
        "response_summary": response_summary,
    }


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


def save_feature_quality_report(
    root: Path | None = None,
    *,
    data_source: str,
    filename: str,
    frame: pd.DataFrame,
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    source_path = _write_csv_variants(reports_dir, filename, data_source, frame)
    get_dashboard_artifact_store().upsert_json(
        artifact_key=table_artifact_key(data_source, filename.removesuffix(".csv")),
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(frame),
        metadata={"rows": int(len(frame)), "source_path": str(source_path)},
    )
    return source_path


def save_text_report(
    root: Path | None = None,
    *,
    data_source: str,
    filename: str,
    content: str,
    artifact_name: str,
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    source_path = source_prefixed_path(reports_dir, filename, data_source)
    save_text(content, source_path)
    save_text(content, reports_dir / filename)
    get_dashboard_artifact_store().upsert_text(
        artifact_key=text_artifact_key(data_source, artifact_name),
        data_source=data_source,
        artifact_kind="markdown",
        content=content,
        metadata={"source_path": str(source_path)},
    )
    return source_path


def save_model_split_reports(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    split_name: str,
    predictions: pd.DataFrame,
    portfolio: pd.DataFrame,
    metrics: dict[str, Any],
    diagnostics: dict[str, pd.DataFrame] | None = None,
) -> dict[str, str]:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")

    prediction_filename = f"{model_name}_{split_name}_predictions.csv"
    portfolio_filename = f"{model_name}_{split_name}_portfolio.csv"
    metrics_filename = f"{model_name}_{split_name}_metrics.json"

    prediction_path = _write_csv_variants(reports_dir, prediction_filename, data_source, predictions)
    portfolio_path = _write_csv_variants(reports_dir, portfolio_filename, data_source, portfolio)
    metrics_path = _write_json_variants(reports_dir, metrics_filename, data_source, metrics)

    store = get_dashboard_artifact_store()
    store.upsert_json(
        artifact_key=table_artifact_key(data_source, f"predictions:{model_name}:{split_name}"),
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(predictions),
        metadata={"rows": int(len(predictions)), "source_path": str(prediction_path)},
    )
    if not predictions.empty:
        from src.utils.prediction_snapshot import build_latest_prediction_snapshot

        candidate_snapshot = build_latest_prediction_snapshot(predictions)
        store.upsert_json(
            artifact_key=candidate_snapshot_artifact_key(data_source, model_name, split_name),
            data_source=data_source,
            artifact_kind="table",
            payload=_frame_records_for_artifact(candidate_snapshot),
            metadata={"rows": int(len(candidate_snapshot)), "source_path": str(prediction_path)},
        )
    store.upsert_json(
        artifact_key=table_artifact_key(data_source, f"portfolio:{model_name}:{split_name}"),
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(portfolio),
        metadata={"rows": int(len(portfolio)), "source_path": str(portfolio_path)},
    )
    store.upsert_json(
        artifact_key=json_artifact_key(data_source, f"metrics:{model_name}:{split_name}"),
        data_source=data_source,
        artifact_kind="json",
        payload=metrics,
        metadata={"source_path": str(metrics_path)},
    )

    output = {
        "prediction_path": str(prediction_path),
        "portfolio_path": str(portfolio_path),
        "metrics_path": str(metrics_path),
    }
    for table_name, table in (diagnostics or {}).items():
        if table.empty:
            continue
        diagnostic_filename = f"{model_name}_{split_name}_{table_name}.csv"
        diagnostic_path = _write_csv_variants(reports_dir, diagnostic_filename, data_source, table)
        store.upsert_json(
            artifact_key=table_artifact_key(data_source, f"diagnostic:{model_name}:{split_name}:{table_name}"),
            data_source=data_source,
            artifact_kind="table",
            payload=_frame_records_for_artifact(table),
            metadata={"rows": int(len(table)), "source_path": str(diagnostic_path)},
        )
        output[f"diagnostic_{table_name}_path"] = str(diagnostic_path)
    return output


def save_feature_importance_report(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    frame: pd.DataFrame,
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    filename = f"{model_name}_feature_importance.csv"
    source_path = _write_csv_variants(reports_dir, filename, data_source, frame)
    get_dashboard_artifact_store().upsert_json(
        artifact_key=table_artifact_key(data_source, f"feature_importance:{model_name}"),
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(frame),
        metadata={"rows": int(len(frame)), "source_path": str(source_path)},
    )
    return source_path


def save_model_stability_report(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    summary: dict[str, Any],
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    filename = f"{model_name}_stability.json"
    source_path = _write_json_variants(reports_dir, filename, data_source, summary)
    get_dashboard_artifact_store().upsert_json(
        artifact_key=json_artifact_key(data_source, f"stability:{model_name}"),
        data_source=data_source,
        artifact_kind="json",
        payload=summary,
        metadata={"source_path": str(source_path)},
    )
    return source_path


def save_ensemble_weights_report(
    root: Path | None = None,
    *,
    data_source: str,
    payload: dict[str, Any],
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    filename = "ensemble_weights.json"
    source_path = _write_json_variants(reports_dir, filename, data_source, payload)
    get_dashboard_artifact_store().upsert_json(
        artifact_key=json_artifact_key(data_source, "ensemble_weights"),
        data_source=data_source,
        artifact_kind="json",
        payload=payload,
        metadata={"source_path": str(source_path)},
    )
    return source_path


def save_inference_packet(
    root: Path | None = None,
    *,
    data_source: str,
    payload: dict[str, Any],
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    filename = "inference_packet.json"
    source_path = _write_json_variants(reports_dir, filename, data_source, payload)
    get_dashboard_artifact_store().upsert_json(
        artifact_key=json_artifact_key(data_source, "inference_packet"),
        data_source=data_source,
        artifact_kind="json",
        payload=payload,
        metadata={"source_path": str(source_path)},
    )
    return source_path


def save_symbol_note(
    root: Path | None = None,
    *,
    data_source: str,
    symbol: str,
    note_kind: str,
    plan_date: str,
    content: str,
) -> Path:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    normalized_symbol = str(symbol or "").strip()
    base_code = normalized_symbol.split(".")[0]
    output_path = reports_dir / f"{base_code}_{note_kind}_{plan_date}.md"
    save_text(content, output_path)

    get_dashboard_artifact_store().upsert_text(
        artifact_key=note_artifact_key(data_source, normalized_symbol, note_kind),
        data_source=data_source,
        artifact_kind="markdown",
        content=content,
        metadata={
            "path": str(output_path),
            "name": output_path.name,
            "plan_date": plan_date,
            "symbol": normalized_symbol,
            "note_kind": note_kind,
        },
    )
    return output_path


def save_overlay_outputs(
    root: Path | None = None,
    *,
    data_source: str,
    scope: str,
    candidates: pd.DataFrame,
    packet: dict[str, Any],
    brief: str,
) -> dict[str, str]:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    is_inference = scope == "inference"

    csv_name = "overlay_inference_candidates.csv" if is_inference else "overlay_latest_candidates.csv"
    packet_name = "overlay_inference_packet.json" if is_inference else "overlay_latest_packet.json"
    brief_name = "overlay_inference_brief.md" if is_inference else "overlay_latest_brief.md"

    csv_source_path = source_prefixed_path(reports_dir, csv_name, data_source)
    packet_source_path = source_prefixed_path(reports_dir, packet_name, data_source)
    brief_source_path = source_prefixed_path(reports_dir, brief_name, data_source)

    candidates.to_csv(csv_source_path, index=False, encoding="utf-8-sig")
    candidates.to_csv(reports_dir / csv_name, index=False, encoding="utf-8-sig")
    packet_text = json.dumps(packet, ensure_ascii=False, indent=2)
    packet_source_path.write_text(packet_text, encoding="utf-8")
    (reports_dir / packet_name).write_text(packet_text, encoding="utf-8")
    save_text(brief, brief_source_path)
    save_text(brief, reports_dir / brief_name)

    store = get_dashboard_artifact_store()
    candidate_key = "overlay_inference_candidates" if is_inference else "overlay_candidates"
    packet_key = "overlay_inference_packet" if is_inference else "overlay_packet"
    brief_key = "overlay_inference_brief" if is_inference else "overlay_brief"

    store.upsert_json(
        artifact_key=table_artifact_key(data_source, candidate_key),
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(candidates),
        metadata={"rows": int(len(candidates)), "source_path": str(csv_source_path)},
    )
    store.upsert_json(
        artifact_key=json_artifact_key(data_source, packet_key),
        data_source=data_source,
        artifact_kind="json",
        payload=packet,
        metadata={"source_path": str(packet_source_path)},
    )
    store.upsert_text(
        artifact_key=text_artifact_key(data_source, brief_key),
        data_source=data_source,
        artifact_kind="text",
        content=brief,
        metadata={"source_path": str(brief_source_path)},
    )

    return {
        "csv_source_path": str(csv_source_path),
        "packet_source_path": str(packet_source_path),
        "brief_source_path": str(brief_source_path),
    }


def save_llm_bridge_outputs(
    reports_dir: Path,
    *,
    data_source: str,
    output_prefix: str,
    request_jsonl_text: str,
    request_summary_text: str,
    response_jsonl_text: str = "",
    response_summary_text: str,
) -> dict[str, str]:
    resolved_reports_dir = ensure_dir(reports_dir)

    request_jsonl_filename = f"{output_prefix}_requests.jsonl"
    request_summary_filename = f"{output_prefix}_summary.md"
    response_jsonl_filename = f"{output_prefix}_responses.jsonl"
    response_summary_filename = f"{output_prefix}_response_summary.md"

    request_jsonl_path = source_prefixed_path(resolved_reports_dir, request_jsonl_filename, data_source)
    save_text(request_jsonl_text, request_jsonl_path)
    save_text(request_jsonl_text, resolved_reports_dir / request_jsonl_filename)

    request_summary_path = source_prefixed_path(resolved_reports_dir, request_summary_filename, data_source)
    save_text(request_summary_text, request_summary_path)
    save_text(request_summary_text, resolved_reports_dir / request_summary_filename)

    response_jsonl_path = Path()
    if response_jsonl_text:
        response_jsonl_path = source_prefixed_path(resolved_reports_dir, response_jsonl_filename, data_source)
        save_text(response_jsonl_text, response_jsonl_path)
        save_text(response_jsonl_text, resolved_reports_dir / response_jsonl_filename)
    else:
        for stale_path in (
            source_prefixed_path(resolved_reports_dir, response_jsonl_filename, data_source),
            resolved_reports_dir / response_jsonl_filename,
        ):
            if stale_path.exists():
                stale_path.unlink()

    response_summary_path = source_prefixed_path(resolved_reports_dir, response_summary_filename, data_source)
    save_text(response_summary_text, response_summary_path)
    save_text(response_summary_text, resolved_reports_dir / response_summary_filename)

    store = get_dashboard_artifact_store()
    store.upsert_text(
        artifact_key=llm_bridge_export_artifact_key(data_source, output_prefix, "requests"),
        data_source=data_source,
        artifact_kind="jsonl",
        content=request_jsonl_text,
        metadata={"path": str(request_jsonl_path), "name": request_jsonl_path.name},
    )
    store.upsert_text(
        artifact_key=llm_bridge_export_artifact_key(data_source, output_prefix, "summary"),
        data_source=data_source,
        artifact_kind="markdown",
        content=request_summary_text,
        metadata={"path": str(request_summary_path), "name": request_summary_path.name},
    )

    is_inference = output_prefix == "overlay_inference_llm"
    response_jsonl_artifact_key = overlay_llm_responses_artifact_key(data_source, "inference" if is_inference else "historical")
    response_summary_artifact_key = overlay_llm_response_summary_artifact_key(data_source, "inference" if is_inference else "historical")

    if response_jsonl_text:
        store.upsert_text(
            artifact_key=response_jsonl_artifact_key,
            data_source=data_source,
            artifact_kind="jsonl",
            content=response_jsonl_text,
            metadata={"path": str(response_jsonl_path), "name": response_jsonl_path.name},
        )
    store.upsert_text(
        artifact_key=response_summary_artifact_key,
        data_source=data_source,
        artifact_kind="markdown",
        content=response_summary_text,
        metadata={"path": str(response_summary_path), "name": response_summary_path.name},
    )

    return {
        "jsonl_path": str(request_jsonl_path),
        "summary_path": str(request_summary_path),
        "response_jsonl_path": str(response_jsonl_path) if response_jsonl_text else "",
        "response_summary_path": str(response_summary_path),
    }


def _frame_records_for_artifact(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    working = frame.copy()
    rows = working.to_dict(orient="records")
    return [_artifact_json_ready(record) for record in rows]


def _artifact_json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _artifact_json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_artifact_json_ready(item) for item in value]
    if isinstance(value, str | int | bool) or value is None:
        return value
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value
