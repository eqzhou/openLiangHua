from __future__ import annotations

import json
import time
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import UUID

import pandas as pd

from src.app.repositories.postgres_market_repository import load_daily_bar_from_market_database
from src.app.repositories.research_panel_repository import (
    load_feature_frame_from_research_panel,
    load_label_frame_from_research_panel,
    load_latest_successful_panel_run,
    load_research_panel,
    load_research_panel_summary,
)
from src.db.connection import connect_database
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
    user_json_artifact_key,
    user_note_artifact_key,
    user_table_artifact_key,
    user_text_artifact_key,
    user_watchlist_artifact_key,
    watchlist_artifact_key,
)
from src.db.dashboard_artifact_store import DashboardArtifact, get_dashboard_artifact_store
from src.utils.data_source import source_or_canonical_path, source_prefixed_path
from src.utils.io import ensure_dir, project_root, save_text


def _uses_primary_project_root(root: Path | None) -> bool:
    if root is None:
        return True
    try:
        return root.resolve() == project_root().resolve()
    except OSError:
        return False


def _is_within_primary_project_root(path: Path) -> bool:
    try:
        resolved = path.resolve()
        primary = project_root().resolve()
        return resolved == primary or primary in resolved.parents
    except OSError:
        return False


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


def get_artifact_metadata(artifact_key: str) -> dict[str, Any]:
    artifact = _artifact_or_none(artifact_key)
    if artifact is None:
        return {}
    payload = dict(artifact.metadata_json or {})
    payload["updated_at"] = artifact.updated_at.isoformat() if artifact.updated_at is not None else None
    payload["artifact_kind"] = artifact.artifact_kind
    payload["data_source"] = artifact.data_source
    return payload


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


def _artifact_ref(artifact_key: str) -> str:
    return f"artifact://{artifact_key}"


def _watchlist_artifact_key(data_source: str, user_id: str | None = None) -> str:
    return user_watchlist_artifact_key(data_source, user_id) if user_id else watchlist_artifact_key(data_source)


def _note_artifact_key(data_source: str, symbol: str, note_kind: str, user_id: str | None = None) -> str:
    return user_note_artifact_key(data_source, symbol, note_kind, user_id) if user_id else note_artifact_key(data_source, symbol, note_kind)


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    working = frame.copy()
    object_columns = working.select_dtypes(include=["object"]).columns.tolist()
    for column in object_columns:
        if working[column].map(lambda value: isinstance(value, UUID)).any():
            working[column] = working[column].map(lambda value: str(value) if isinstance(value, UUID) else value)
    buffer = BytesIO()
    working.to_parquet(buffer, index=False)
    return buffer.getvalue()


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


def _normalize_industry_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _symbol_snapshot_cache_path(root: Path) -> Path:
    return root / "data" / "staging" / "stock_snapshot.parquet"


def _board_industry_cache_path(root: Path) -> Path:
    return root / "data" / "staging" / "industry_board_map.parquet"


def _read_snapshot_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["ts_code", "industry", "list_date"])
    frame = pd.read_parquet(path)
    if "list_date" in frame.columns:
        frame["list_date"] = pd.to_datetime(frame["list_date"], errors="coerce")
    return frame


def _industry_mapping_from_cache(frame: pd.DataFrame) -> dict[str, str]:
    if frame.empty or "ts_code" not in frame.columns or "industry" not in frame.columns:
        return {}
    working = frame.copy()
    working["industry"] = working["industry"].map(_normalize_industry_text)
    working = working.loc[working["industry"].notna()].drop_duplicates(subset=["ts_code"], keep="last")
    if working.empty:
        return {}
    return working.set_index("ts_code")["industry"].astype(str).to_dict()


def _fetch_symbol_snapshot_industries(root: Path, symbols: list[str], *, allow_network: bool = False) -> dict[str, str]:
    normalized_symbols = sorted({str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()})
    if not normalized_symbols:
        return {}

    snapshot_path = _symbol_snapshot_cache_path(root)
    board_map_path = _board_industry_cache_path(root)
    snapshot_cache = _read_snapshot_cache(snapshot_path)
    mapping = _industry_mapping_from_cache(snapshot_cache)
    client = None

    missing_symbols = [symbol for symbol in normalized_symbols if symbol not in mapping]
    if missing_symbols and allow_network:
        try:
            from src.data.akshare_client import AKShareClient

            client = AKShareClient()
        except Exception:
            client = None

        fetched_rows: list[dict[str, Any]] = []
        if client is not None:
            for symbol in missing_symbols:
                snapshot: dict[str, Any] | None = None
                for attempt in range(3):
                    try:
                        snapshot = client.stock_individual_snapshot(symbol)
                        break
                    except Exception:
                        if attempt == 2:
                            snapshot = None
                        else:
                            time.sleep(0.6 * (attempt + 1))
                if snapshot is None:
                    continue
                fetched_rows.append(
                    {
                        "ts_code": symbol,
                        "industry": _normalize_industry_text(snapshot.get("industry")),
                        "list_date": pd.to_datetime(snapshot.get("list_date"), errors="coerce"),
                    }
                )

        if fetched_rows:
            updated_snapshot = pd.concat([snapshot_cache, pd.DataFrame(fetched_rows)], ignore_index=True)
            updated_snapshot = updated_snapshot.drop_duplicates(subset=["ts_code"], keep="last").sort_values("ts_code")
            ensure_dir(snapshot_path.parent)
            updated_snapshot.to_parquet(snapshot_path, index=False)
            mapping.update(_industry_mapping_from_cache(updated_snapshot))

    missing_symbols = [symbol for symbol in normalized_symbols if symbol not in mapping]
    if missing_symbols and allow_network:
        board_cache = _read_snapshot_cache(board_map_path)
        board_mapping = _industry_mapping_from_cache(board_cache)
        if not board_mapping and client is not None:
            for attempt in range(3):
                try:
                    board_cache = client.current_board_industry_map()
                    if not board_cache.empty:
                        ensure_dir(board_map_path.parent)
                        board_cache.to_parquet(board_map_path, index=False)
                    board_mapping = _industry_mapping_from_cache(board_cache)
                    break
                except Exception:
                    board_mapping = {}
                    if attempt < 2:
                        time.sleep(1.0 * (attempt + 1))
        mapping.update({symbol: board_mapping[symbol] for symbol in missing_symbols if symbol in board_mapping})

    return {symbol: mapping[symbol] for symbol in normalized_symbols if symbol in mapping}


def _enrich_missing_industries(root: Path, frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "ts_code" not in frame.columns:
        return frame

    working = frame.copy()
    if "industry" not in working.columns:
        working["industry"] = pd.NA
    working["industry"] = working["industry"].map(_normalize_industry_text)

    missing_mask = working["industry"].isna()
    if not missing_mask.any():
        return working

    industry_mapping = _fetch_symbol_snapshot_industries(
        root,
        working.loc[missing_mask, "ts_code"].astype(str).tolist(),
        allow_network=False,
    )
    if not industry_mapping:
        return working

    fill_values = working.loc[missing_mask, "ts_code"].astype(str).map(industry_mapping)
    working.loc[missing_mask, "industry"] = fill_values.where(fill_values.notna(), working.loc[missing_mask, "industry"])
    return working


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
) -> str:
    if not _uses_primary_project_root(root):
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

    artifact_key = binary_artifact_key(data_source, artifact_name)
    content = _parquet_bytes(frame)
    get_dashboard_artifact_store().upsert_bytes(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="parquet",
        content=content,
        metadata={
            "artifact_name": artifact_name,
            "rows": int(len(frame)),
            "columns": list(frame.columns),
            "directory": directory,
            "filename": filename,
            "write_canonical": bool(write_canonical),
            "size_bytes": len(content),
        },
    )
    return _artifact_ref(artifact_key)


def save_json_report(
    root: Path | None = None,
    *,
    data_source: str,
    filename: str,
    payload: dict[str, Any],
    artifact_name: str | None = None,
) -> str:
    if not _uses_primary_project_root(root):
        resolved_root = root or project_root()
        reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
        source_path = _write_json_variants(reports_dir, filename, data_source, payload)
        if artifact_name:
            artifact_key = json_artifact_key(data_source, artifact_name)
            get_dashboard_artifact_store().upsert_json(
                artifact_key=artifact_key,
                data_source=data_source,
                artifact_kind="json",
                payload=payload,
                metadata={"source_path": str(source_path)},
            )
        return source_path

    if artifact_name:
        artifact_key = json_artifact_key(data_source, artifact_name)
        get_dashboard_artifact_store().upsert_json(
            artifact_key=artifact_key,
            data_source=data_source,
            artifact_kind="json",
            payload=payload,
            metadata={"filename": filename},
        )
        return _artifact_ref(artifact_key)
    return ""


def load_dataset_summary(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, object]:
    if prefer_database and _uses_primary_project_root(root):
        daily_bar_metadata = get_artifact_metadata(binary_artifact_key(data_source, "daily_bar"))
        panel_run = load_latest_successful_panel_run(data_source=data_source)
        panel_summary = load_research_panel_summary(data_source=data_source)

        summary: dict[str, object] = {
            "cached_symbols": 0,
            "daily_bar": {
                "exists": bool(daily_bar_metadata),
                "size_mb": round((int(daily_bar_metadata.get("size_bytes", 0) or 0) / (1024 * 1024)), 2) if daily_bar_metadata else None,
                "updated": daily_bar_metadata.get("updated_at"),
            },
            "features": {
                "exists": bool(panel_run),
                "size_mb": None,
                "updated": panel_run.get("updated_at"),
            },
            "labels": {
                "exists": bool(panel_run),
                "size_mb": None,
                "updated": panel_run.get("updated_at"),
            },
        }
        if panel_summary:
            summary["feature_rows"] = int(panel_summary.get("row_count", 0) or 0)
            summary["label_rows"] = int(panel_summary.get("row_count", 0) or 0)
            summary["feature_symbols"] = int(panel_summary.get("symbol_count", 0) or 0)
            summary["label_symbols"] = int(panel_summary.get("symbol_count", 0) or 0)
            summary["date_min"] = panel_summary.get("date_min")
            summary["date_max"] = panel_summary.get("date_max")
        elif panel_run:
            summary["feature_rows"] = int(panel_run.get("row_count", 0) or 0)
            summary["label_rows"] = int(panel_run.get("row_count", 0) or 0)
            summary["feature_symbols"] = int(panel_run.get("symbol_count", 0) or 0)
            summary["label_symbols"] = int(panel_run.get("symbol_count", 0) or 0)
            summary["date_min"] = panel_run.get("date_min")
            summary["date_max"] = panel_run.get("date_max")
        return summary

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
    if prefer_database and _uses_primary_project_root(root):
        panel_frame = load_feature_frame_from_research_panel(data_source=data_source)
        if not panel_frame.empty:
            return panel_frame
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "feature_panel"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "features", "feature_panel.parquet", data_source)
    return _read_parquet_frame(path)


def load_feature_history_for_symbol(
    root: Path | None = None,
    *,
    data_source: str,
    symbol: str,
    factor_name: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    normalized_symbol = str(symbol or "").strip()
    normalized_factor = str(factor_name or "").strip()
    if not normalized_symbol or not normalized_factor:
        return pd.DataFrame()

    columns = ["trade_date", "ts_code", normalized_factor]
    if prefer_database and _uses_primary_project_root(root):
        panel_frame = load_research_panel(
            data_source=data_source,
            symbols=[normalized_symbol],
            columns=columns,
        )
        if panel_frame.empty:
            return panel_frame
        available_columns = [column for column in columns if column in panel_frame.columns]
        return panel_frame[available_columns].copy()

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "features", "feature_panel.parquet", data_source)
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path, columns=columns)
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    if "ts_code" in frame.columns:
        frame = frame.loc[frame["ts_code"].astype(str) == normalized_symbol].copy()
    return frame


def load_label_panel(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        panel_frame = load_label_frame_from_research_panel(data_source=data_source)
        if not panel_frame.empty:
            return panel_frame
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "label_panel"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "labels", "label_panel.parquet", data_source)
    return _read_parquet_frame(path)


def load_trade_calendar(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "trade_calendar"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "staging", "trade_calendar.parquet", data_source)
    return _read_parquet_frame(path)


def load_stock_basic(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "stock_basic"))
        if artifact_payload is not None:
            return _read_parquet_bytes(artifact_payload[0])
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "staging", "stock_basic.parquet", data_source)
    return _read_parquet_frame(path)


def load_daily_bar(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _binary_artifact_payload(binary_artifact_key(data_source, "daily_bar"))
        if artifact_payload is not None:
            return _enrich_missing_industries(root or project_root(), _read_parquet_bytes(artifact_payload[0]))
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "data" / "staging", "daily_bar.parquet", data_source)
    frame = _enrich_missing_industries(resolved_root, _read_parquet_frame(path))
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
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, f"metrics:{model_name}:{split_name}"))
        if isinstance(artifact_payload, dict):
            return artifact_payload
        return {}

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
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, f"stability:{model_name}"))
        if isinstance(artifact_payload, dict):
            return artifact_payload
        return {}

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
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, f"portfolio:{model_name}:{split_name}"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
        return pd.DataFrame()

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
    user_id: str | None = None,
) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        artifact_key = user_table_artifact_key(data_source, f"predictions:{model_name}:{split_name}", user_id)
        artifact = _artifact_or_none(artifact_key)
        if artifact is not None:
            if artifact.payload_json is not None:
                return _frame_from_records(artifact.payload_json)
            if artifact.payload_bytes is not None:
                return _read_parquet_bytes(artifact.payload_bytes)
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_{split_name}_predictions.csv",
        data_source,
    )
    return _read_csv_frame(path)


def load_prediction_history_for_symbol(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    split_name: str,
    symbol: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    normalized_symbol = str(symbol or "").strip()
    if not normalized_symbol:
        return pd.DataFrame()

    if prefer_database and _uses_primary_project_root(root):
        artifact = _artifact_or_none(table_artifact_key(data_source, f"predictions:{model_name}:{split_name}"))
        if artifact is not None:
            if artifact.payload_json is not None:
                records = get_dashboard_artifact_store().get_projected_json_records(
                    artifact_key=table_artifact_key(data_source, f"predictions:{model_name}:{split_name}"),
                    field_names=["trade_date", "ts_code", "score", "ret_t1_t10"],
                    filter_field_name="ts_code",
                    filter_field_value=normalized_symbol,
                    order_by_field="trade_date",
                    descending=True,
                    limit=240,
                )
                return _frame_from_records(records)
            if artifact.payload_bytes is not None:
                frame = _read_parquet_bytes(artifact.payload_bytes)
                if frame.empty:
                    return frame
                filtered = frame.loc[frame["ts_code"].astype(str) == normalized_symbol, ["trade_date", "ts_code", "score", "ret_t1_t10"]].copy()
                return filtered.sort_values("trade_date").tail(240).reset_index(drop=True)
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_{split_name}_predictions.csv",
        data_source,
    )
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path, usecols=lambda value: value in {"trade_date", "ts_code", "score", "ret_t1_t10"})
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame.loc[frame["ts_code"].astype(str) == normalized_symbol].copy()


def load_feature_importance(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    prefer_database: bool = True,
) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, f"feature_importance:{model_name}"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
        return pd.DataFrame()

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
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(table_artifact_key(data_source, f"diagnostic:{model_name}:{split_name}:{table_name}"))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        f"{model_name}_{split_name}_{table_name}.csv",
        data_source,
    )
    return _read_csv_frame(path)


def _overlay_table_artifact_key(data_source: str, artifact_name: str, user_id: str | None = None) -> str:
    return user_table_artifact_key(data_source, artifact_name, user_id) if user_id else table_artifact_key(data_source, artifact_name)


def _overlay_json_artifact_key(data_source: str, artifact_name: str, user_id: str | None = None) -> str:
    return user_json_artifact_key(data_source, artifact_name, user_id) if user_id else json_artifact_key(data_source, artifact_name)


def _overlay_text_artifact_key(data_source: str, artifact_name: str, user_id: str | None = None) -> str:
    return user_text_artifact_key(data_source, artifact_name, user_id) if user_id else text_artifact_key(data_source, artifact_name)


def load_overlay_candidates(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(_overlay_table_artifact_key(data_source, "overlay_candidates", user_id))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "overlay_latest_candidates.csv", data_source)
    return _read_csv_frame(path)


def load_overlay_packet(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> dict[str, Any]:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(_overlay_json_artifact_key(data_source, "overlay_packet", user_id))
        if isinstance(artifact_payload, dict):
            return artifact_payload
        return {}

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "overlay_latest_packet.json", data_source)
    return read_json(path)


def load_overlay_brief(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> str:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _text_artifact_payload(_overlay_text_artifact_key(data_source, "overlay_brief", user_id))
        if artifact_payload is not None:
            return artifact_payload[0]
        return ""

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "overlay_latest_brief.md", data_source)
    return read_text(path)


def load_overlay_inference_candidates(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> pd.DataFrame:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(
            _overlay_table_artifact_key(data_source, "overlay_inference_candidates", user_id)
        )
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
        return pd.DataFrame()

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_candidates.csv",
        data_source,
    )
    return _read_csv_frame(path)


def load_overlay_inference_packet(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> dict[str, Any]:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(
            _overlay_json_artifact_key(data_source, "overlay_inference_packet", user_id)
        )
        if isinstance(artifact_payload, dict):
            return artifact_payload
        return {}

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_packet.json",
        data_source,
    )
    return read_json(path)


def load_overlay_inference_brief(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> str:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _text_artifact_payload(
            _overlay_text_artifact_key(data_source, "overlay_inference_brief", user_id)
        )
        if artifact_payload is not None:
            return artifact_payload[0]
        return ""

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_brief.md",
        data_source,
    )
    return read_text(path)


def load_overlay_inference_shortlist(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> str:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _text_artifact_payload(
            _overlay_text_artifact_key(data_source, "overlay_inference_shortlist", user_id)
        )
        if artifact_payload is not None:
            return artifact_payload[0]
        return ""

    resolved_root = root or project_root()
    path = source_or_canonical_path(
        resolved_root / "reports" / "weekly",
        "overlay_inference_shortlist.md",
        data_source,
    )
    return read_text(path)


def load_inference_packet(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, "inference_packet"))
        if isinstance(artifact_payload, dict):
            return artifact_payload
        return {}

    resolved_root = root or project_root()
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", "inference_packet.json", data_source)
    return read_json(path)


def load_ensemble_weights(root: Path | None = None, *, data_source: str, prefer_database: bool = True) -> dict[str, Any]:
    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _json_artifact_payload(json_artifact_key(data_source, "ensemble_weights"))
        if isinstance(artifact_payload, dict):
            return artifact_payload
        return {}

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
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized_scope = "inference" if scope == "inference" else "historical"
    artifact_prefix = "overlay_inference" if normalized_scope == "inference" else "overlay"
    response_records: list[dict[str, Any]] = []
    response_summary = ""
    records_loaded_from_store = False
    summary_loaded_from_store = False

    if prefer_database:
        records_key = (
            _overlay_text_artifact_key(data_source, f"{artifact_prefix}_llm_responses", user_id)
            if user_id
            else overlay_llm_responses_artifact_key(data_source, normalized_scope)
        )
        records_artifact = _text_artifact_payload(records_key)
        if records_artifact is not None:
            response_records = _parse_jsonl_text(records_artifact[0])
            records_loaded_from_store = True

        summary_key = (
            _overlay_text_artifact_key(data_source, f"{artifact_prefix}_llm_response_summary", user_id)
            if user_id
            else overlay_llm_response_summary_artifact_key(data_source, normalized_scope)
        )
        summary_artifact = _text_artifact_payload(summary_key)
        if summary_artifact is not None:
            response_summary = summary_artifact[0]
            summary_loaded_from_store = True

    llm_bridge = dict((packet or {}).get("llm_bridge", {}) or {})
    if not records_loaded_from_store:
        if prefer_database and _uses_primary_project_root(root):
            response_records = []
        else:
            response_path_text = str(llm_bridge.get("response_jsonl_path", "") or "").strip()
            if response_path_text:
                response_records = read_jsonl_records(Path(response_path_text))

    if not summary_loaded_from_store:
        if prefer_database and _uses_primary_project_root(root):
            response_summary = ""
        else:
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
    user_id: str | None = None,
) -> pd.DataFrame | None:
    if prefer_database:
        artifact_payload = _json_artifact_payload(_watchlist_artifact_key(data_source, user_id))
        if artifact_payload is not None:
            return _frame_from_records(artifact_payload)
    return None


def _projected_table_artifact_records(
    *,
    artifact_key: str,
    field_names: list[str],
    filter_symbol: str | None = None,
) -> list[dict[str, Any]]:
    return get_dashboard_artifact_store().get_projected_json_records(
        artifact_key=artifact_key,
        field_names=field_names,
        filter_field_name="ts_code" if filter_symbol else None,
        filter_field_value=filter_symbol,
    )


def load_watchlist_summary_records(
    root: Path | None = None,
    *,
    data_source: str,
    field_names: list[str],
    keyword: str = "",
    scope: str = "all",
    sort_by: str = "inference_rank",
    page: int = 1,
    page_size: int = 30,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> list[dict[str, Any]]:
    if prefer_database and _uses_primary_project_root(root):
        return _load_watchlist_summary_records_from_database(
            artifact_key=_watchlist_artifact_key(data_source, user_id),
            field_names=field_names,
            keyword=keyword,
            scope=scope,
            sort_by=sort_by,
            page=page,
            page_size=page_size,
        )

    snapshot = load_watchlist_snapshot(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
    if snapshot is None or snapshot.empty:
        return []
    filtered = _filter_watchlist_snapshot_frame(snapshot, keyword=keyword, scope=scope, sort_by=sort_by)
    normalized_page_size = max(1, int(page_size))
    normalized_page = max(1, int(page))
    page_start = (normalized_page - 1) * normalized_page_size
    page_end = page_start + normalized_page_size
    filtered = filtered.iloc[page_start:page_end].copy()
    available_columns = [column for column in field_names if column in filtered.columns]
    if not available_columns:
        return []
    return _frame_records_for_artifact(filtered[available_columns].copy())


def load_watchlist_record(
    root: Path | None = None,
    *,
    data_source: str,
    symbol: str,
    field_names: list[str] | None = None,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip()
    if not normalized_symbol:
        return {}

    if prefer_database and _uses_primary_project_root(root):
        if field_names:
            rows = _projected_table_artifact_records(
                artifact_key=_watchlist_artifact_key(data_source, user_id),
                field_names=field_names,
                filter_symbol=normalized_symbol,
            )
            return rows[0] if rows else {}

        rows = get_dashboard_artifact_store().get_json_records_by_field(
            artifact_key=_watchlist_artifact_key(data_source, user_id),
            field_name="ts_code",
            field_value=normalized_symbol,
        )
        return rows[0] if rows else {}

    snapshot = load_watchlist_snapshot(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
    if snapshot is None or snapshot.empty:
        return {}
    matched = snapshot.loc[snapshot["ts_code"].astype(str) == normalized_symbol].head(1)
    if matched.empty:
        return {}
    if field_names:
        available_columns = [column for column in field_names if column in matched.columns]
        return dict(matched[available_columns].iloc[0].to_dict()) if available_columns else {}
    return dict(matched.iloc[0].to_dict())


def load_watchlist_overview(
    root: Path | None = None,
    *,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> dict[str, Any]:
    if prefer_database and _uses_primary_project_root(root):
        return _load_watchlist_overview_from_database(artifact_key=_watchlist_artifact_key(data_source, user_id))

    snapshot = load_watchlist_snapshot(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
    if snapshot is None or snapshot.empty:
        return {
            "totalCount": 0,
            "overlayCount": 0,
            "inferenceOverlayCount": 0,
            "marketValue": 0.0,
            "unrealizedPnl": 0.0,
        }
    return {
        "totalCount": int(len(snapshot)),
        "overlayCount": int(snapshot["is_overlay_selected"].fillna(False).sum()) if "is_overlay_selected" in snapshot.columns else 0,
        "inferenceOverlayCount": int(snapshot["is_inference_overlay_selected"].fillna(False).sum()) if "is_inference_overlay_selected" in snapshot.columns else 0,
        "marketValue": float(pd.to_numeric(snapshot.get("market_value"), errors="coerce").fillna(0).sum()) if "market_value" in snapshot.columns else 0.0,
        "unrealizedPnl": float(pd.to_numeric(snapshot.get("unrealized_pnl"), errors="coerce").fillna(0).sum()) if "unrealized_pnl" in snapshot.columns else 0.0,
    }


def load_watchlist_filtered_count(
    root: Path | None = None,
    *,
    data_source: str,
    keyword: str = "",
    scope: str = "all",
    prefer_database: bool = True,
    user_id: str | None = None,
) -> int:
    if prefer_database and _uses_primary_project_root(root):
        return _load_watchlist_filtered_count_from_database(
            artifact_key=_watchlist_artifact_key(data_source, user_id),
            keyword=keyword,
            scope=scope,
        )

    snapshot = load_watchlist_snapshot(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
    if snapshot is None or snapshot.empty:
        return 0
    filtered = _filter_watchlist_snapshot_frame(snapshot, keyword=keyword, scope=scope, sort_by="inference_rank")
    return int(len(filtered))


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


def _overlay_candidate_artifact_key(data_source: str, scope: str, user_id: str | None = None) -> str:
    normalized_scope = "inference" if scope == "inference" else "historical"
    artifact_name = "overlay_inference_candidates" if normalized_scope == "inference" else "overlay_candidates"
    return _overlay_table_artifact_key(data_source, artifact_name, user_id)


def load_overlay_candidate_summary_records(
    root: Path | None = None,
    *,
    data_source: str,
    scope: str,
    field_names: list[str],
    prefer_database: bool = True,
    user_id: str | None = None,
) -> list[dict[str, Any]]:
    if prefer_database and _uses_primary_project_root(root):
        return _projected_table_artifact_records(
            artifact_key=_overlay_candidate_artifact_key(data_source, scope, user_id),
            field_names=field_names,
        )

    candidates = (
        load_overlay_inference_candidates(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
        if scope == "inference"
        else load_overlay_candidates(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
    )
    if candidates.empty:
        return []
    available_columns = [column for column in field_names if column in candidates.columns]
    if not available_columns:
        return []
    return _frame_records_for_artifact(candidates[available_columns].copy())


def load_overlay_candidate_record(
    root: Path | None = None,
    *,
    data_source: str,
    scope: str,
    symbol: str,
    field_names: list[str] | None = None,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip()
    if not normalized_symbol:
        return {}

    if prefer_database and _uses_primary_project_root(root):
        if field_names:
            rows = _projected_table_artifact_records(
                artifact_key=_overlay_candidate_artifact_key(data_source, scope, user_id),
                field_names=field_names,
                filter_symbol=normalized_symbol,
            )
            return rows[0] if rows else {}

        rows = get_dashboard_artifact_store().get_json_records_by_field(
            artifact_key=_overlay_candidate_artifact_key(data_source, scope, user_id),
            field_name="ts_code",
            field_value=normalized_symbol,
        )
        return rows[0] if rows else {}

    candidates = (
        load_overlay_inference_candidates(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
        if scope == "inference"
        else load_overlay_candidates(root, data_source=data_source, prefer_database=prefer_database, user_id=user_id)
    )
    if candidates.empty:
        return {}
    matched = candidates.loc[candidates["ts_code"].astype(str) == normalized_symbol].head(1)
    if matched.empty:
        return {}
    if field_names:
        available_columns = [column for column in field_names if column in matched.columns]
        return dict(matched[available_columns].iloc[0].to_dict()) if available_columns else {}
    return dict(matched.iloc[0].to_dict())


def load_latest_symbol_markdown(
    symbol: str,
    note_kind: str,
    *,
    root: Path | None = None,
    data_source: str,
    prefer_database: bool = True,
    user_id: str | None = None,
) -> dict[str, str]:
    normalized_symbol = str(symbol or "").strip()
    if not normalized_symbol:
        return {}

    if prefer_database and _uses_primary_project_root(root):
        artifact_payload = _text_artifact_payload(_note_artifact_key(data_source, normalized_symbol, note_kind, user_id))
        if artifact_payload is not None:
            content, metadata = artifact_payload
            return {
                "path": str(metadata.get("path", "")),
                "name": str(metadata.get("name", "")),
                "plan_date": str(metadata.get("plan_date", "")),
                "content": content,
            }
        return {}

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
) -> str:
    if not _uses_primary_project_root(root):
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

    artifact_key = table_artifact_key(data_source, filename.removesuffix(".csv"))
    get_dashboard_artifact_store().upsert_json(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(frame),
        metadata={"rows": int(len(frame)), "filename": filename},
    )
    return _artifact_ref(artifact_key)


def save_text_report(
    root: Path | None = None,
    *,
    data_source: str,
    filename: str,
    content: str,
    artifact_name: str,
    user_id: str | None = None,
) -> str:
    if not _uses_primary_project_root(root):
        resolved_root = root or project_root()
        reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
        source_path = source_prefixed_path(reports_dir, filename, data_source)
        save_text(content, source_path)
        save_text(content, reports_dir / filename)
        get_dashboard_artifact_store().upsert_text(
            artifact_key=_overlay_text_artifact_key(data_source, artifact_name, user_id),
            data_source=data_source,
            artifact_kind="markdown",
            content=content,
            metadata={"source_path": str(source_path), "user_id": user_id},
        )
        return source_path

    artifact_key = _overlay_text_artifact_key(data_source, artifact_name, user_id)
    get_dashboard_artifact_store().upsert_text(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="markdown",
        content=content,
        metadata={"filename": filename, "user_id": user_id},
    )
    return _artifact_ref(artifact_key)


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
    user_id: str | None = None,
) -> dict[str, str]:
    if not _uses_primary_project_root(root):
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
            artifact_key=user_table_artifact_key(data_source, f"predictions:{model_name}:{split_name}", user_id),
            data_source=data_source,
            artifact_kind="table",
            payload=_frame_records_for_artifact(predictions),
            metadata={"rows": int(len(predictions)), "source_path": str(prediction_path), "user_id": user_id},
        )
        if not predictions.empty:
            from src.utils.prediction_snapshot import build_latest_prediction_snapshot

            candidate_snapshot = build_latest_prediction_snapshot(predictions)
            store.upsert_json(
                artifact_key=user_table_artifact_key(data_source, f"candidate_snapshot:{model_name}:{split_name}", user_id),
                data_source=data_source,
                artifact_kind="table",
                payload=_frame_records_for_artifact(candidate_snapshot),
                metadata={"rows": int(len(candidate_snapshot)), "source_path": str(prediction_path), "user_id": user_id},
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

    store = get_dashboard_artifact_store()
    prediction_key = user_table_artifact_key(data_source, f"predictions:{model_name}:{split_name}", user_id)
    portfolio_key = user_table_artifact_key(data_source, f"portfolio:{model_name}:{split_name}", user_id)
    metrics_key = user_json_artifact_key(data_source, f"metrics:{model_name}:{split_name}", user_id)
    store.upsert_bytes(
        artifact_key=prediction_key,
        data_source=data_source,
        artifact_kind="parquet",
        content=_parquet_bytes(predictions),
        metadata={"rows": int(len(predictions)), "model_name": model_name, "split_name": split_name, "user_id": user_id},
    )
    if not predictions.empty:
        from src.utils.prediction_snapshot import build_latest_prediction_snapshot

        candidate_snapshot = build_latest_prediction_snapshot(predictions)
        store.upsert_json(
            artifact_key=user_table_artifact_key(data_source, f"candidate_snapshot:{model_name}:{split_name}", user_id),
            data_source=data_source,
            artifact_kind="table",
            payload=_frame_records_for_artifact(candidate_snapshot),
            metadata={"rows": int(len(candidate_snapshot)), "model_name": model_name, "split_name": split_name, "user_id": user_id},
        )
    store.upsert_json(
        artifact_key=portfolio_key,
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(portfolio),
        metadata={"rows": int(len(portfolio)), "model_name": model_name, "split_name": split_name},
    )
    store.upsert_json(
        artifact_key=metrics_key,
        data_source=data_source,
        artifact_kind="json",
        payload=metrics,
        metadata={"model_name": model_name, "split_name": split_name},
    )

    output = {
        "prediction_path": _artifact_ref(prediction_key),
        "portfolio_path": _artifact_ref(portfolio_key),
        "metrics_path": _artifact_ref(metrics_key),
    }
    for table_name, table in (diagnostics or {}).items():
        if table.empty:
            continue
        diagnostic_key = table_artifact_key(data_source, f"diagnostic:{model_name}:{split_name}:{table_name}")
        store.upsert_json(
            artifact_key=diagnostic_key,
            data_source=data_source,
            artifact_kind="table",
            payload=_frame_records_for_artifact(table),
            metadata={"rows": int(len(table)), "model_name": model_name, "split_name": split_name, "table_name": table_name},
        )
        output[f"diagnostic_{table_name}_path"] = _artifact_ref(diagnostic_key)
    return output


def save_feature_importance_report(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    frame: pd.DataFrame,
) -> str:
    if not _uses_primary_project_root(root):
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

    artifact_key = table_artifact_key(data_source, f"feature_importance:{model_name}")
    get_dashboard_artifact_store().upsert_json(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(frame),
        metadata={"rows": int(len(frame)), "model_name": model_name},
    )
    return _artifact_ref(artifact_key)


def save_model_stability_report(
    root: Path | None = None,
    *,
    data_source: str,
    model_name: str,
    summary: dict[str, Any],
) -> str:
    if not _uses_primary_project_root(root):
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

    artifact_key = json_artifact_key(data_source, f"stability:{model_name}")
    get_dashboard_artifact_store().upsert_json(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="json",
        payload=summary,
        metadata={"model_name": model_name},
    )
    return _artifact_ref(artifact_key)


def save_ensemble_weights_report(
    root: Path | None = None,
    *,
    data_source: str,
    payload: dict[str, Any],
) -> str:
    if not _uses_primary_project_root(root):
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

    artifact_key = json_artifact_key(data_source, "ensemble_weights")
    get_dashboard_artifact_store().upsert_json(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="json",
        payload=payload,
        metadata={"artifact_name": "ensemble_weights"},
    )
    return _artifact_ref(artifact_key)


def save_inference_packet(
    root: Path | None = None,
    *,
    data_source: str,
    payload: dict[str, Any],
    user_id: str | None = None,
) -> str:
    if not _uses_primary_project_root(root):
        resolved_root = root or project_root()
        reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
        filename = "inference_packet.json"
        source_path = _write_json_variants(reports_dir, filename, data_source, payload)
        get_dashboard_artifact_store().upsert_json(
            artifact_key=user_json_artifact_key(data_source, "inference_packet", user_id),
            data_source=data_source,
            artifact_kind="json",
            payload=payload,
            metadata={"source_path": str(source_path), "user_id": user_id},
        )
        return source_path

    artifact_key = user_json_artifact_key(data_source, "inference_packet", user_id)
    get_dashboard_artifact_store().upsert_json(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="json",
        payload=payload,
        metadata={"artifact_name": "inference_packet", "user_id": user_id},
    )
    return _artifact_ref(artifact_key)


def save_symbol_note(
    root: Path | None = None,
    *,
    data_source: str,
    symbol: str,
    note_kind: str,
    plan_date: str,
    content: str,
    user_id: str | None = None,
) -> str:
    if not _uses_primary_project_root(root):
        resolved_root = root or project_root()
        reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
        normalized_symbol = str(symbol or "").strip()
        base_code = normalized_symbol.split(".")[0]
        output_path = reports_dir / f"{base_code}_{note_kind}_{plan_date}.md"
        save_text(content, output_path)

        get_dashboard_artifact_store().upsert_text(
            artifact_key=_note_artifact_key(data_source, normalized_symbol, note_kind, user_id),
            data_source=data_source,
            artifact_kind="markdown",
            content=content,
            metadata={
                "path": str(output_path),
                "name": output_path.name,
                "plan_date": plan_date,
                "symbol": normalized_symbol,
                "note_kind": note_kind,
                "user_id": user_id,
            },
        )
        return output_path

    normalized_symbol = str(symbol or "").strip()
    artifact_key = _note_artifact_key(data_source, normalized_symbol, note_kind, user_id)
    get_dashboard_artifact_store().upsert_text(
        artifact_key=artifact_key,
        data_source=data_source,
        artifact_kind="markdown",
        content=content,
        metadata={
            "plan_date": plan_date,
            "symbol": normalized_symbol,
            "note_kind": note_kind,
            "user_id": user_id,
        },
    )
    return _artifact_ref(artifact_key)


def save_overlay_outputs(
    root: Path | None = None,
    *,
    data_source: str,
    scope: str,
    candidates: pd.DataFrame,
    packet: dict[str, Any],
    brief: str,
    user_id: str | None = None,
) -> dict[str, str]:
    if not _uses_primary_project_root(root):
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

    is_inference = scope == "inference"
    store = get_dashboard_artifact_store()
    candidate_key = "overlay_inference_candidates" if is_inference else "overlay_candidates"
    packet_key = "overlay_inference_packet" if is_inference else "overlay_packet"
    brief_key = "overlay_inference_brief" if is_inference else "overlay_brief"
    candidate_artifact_key = _overlay_table_artifact_key(data_source, candidate_key, user_id)
    packet_artifact_key = _overlay_json_artifact_key(data_source, packet_key, user_id)
    brief_artifact_key = _overlay_text_artifact_key(data_source, brief_key, user_id)
    metadata = {"rows": int(len(candidates)), "scope": scope, "user_id": user_id}

    store.upsert_json(
        artifact_key=candidate_artifact_key,
        data_source=data_source,
        artifact_kind="table",
        payload=_frame_records_for_artifact(candidates),
        metadata=metadata,
    )
    store.upsert_json(
        artifact_key=packet_artifact_key,
        data_source=data_source,
        artifact_kind="json",
        payload=packet,
        metadata={"scope": scope, "user_id": user_id},
    )
    store.upsert_text(
        artifact_key=brief_artifact_key,
        data_source=data_source,
        artifact_kind="text",
        content=brief,
        metadata={"scope": scope, "user_id": user_id},
    )

    return {
        "csv_source_path": _artifact_ref(candidate_artifact_key),
        "packet_source_path": _artifact_ref(packet_artifact_key),
        "brief_source_path": _artifact_ref(brief_artifact_key),
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
    user_id: str | None = None,
) -> dict[str, str]:
    if not _is_within_primary_project_root(reports_dir):
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
        scope = "inference" if is_inference else "historical"
        artifact_prefix = "overlay_inference" if is_inference else "overlay"
        response_jsonl_artifact_key = (
            _overlay_text_artifact_key(data_source, f"{artifact_prefix}_llm_responses", user_id)
            if user_id
            else overlay_llm_responses_artifact_key(data_source, scope)
        )
        response_summary_artifact_key = (
            _overlay_text_artifact_key(data_source, f"{artifact_prefix}_llm_response_summary", user_id)
            if user_id
            else overlay_llm_response_summary_artifact_key(data_source, scope)
        )

        if response_jsonl_text:
            store.upsert_text(
                artifact_key=response_jsonl_artifact_key,
                data_source=data_source,
                artifact_kind="jsonl",
                content=response_jsonl_text,
                metadata={"path": str(response_jsonl_path), "name": response_jsonl_path.name, "user_id": user_id},
            )
        store.upsert_text(
            artifact_key=response_summary_artifact_key,
            data_source=data_source,
            artifact_kind="markdown",
            content=response_summary_text,
            metadata={"path": str(response_summary_path), "name": response_summary_path.name, "user_id": user_id},
        )

        return {
            "jsonl_path": str(request_jsonl_path),
            "summary_path": str(request_summary_path),
            "response_jsonl_path": str(response_jsonl_path) if response_jsonl_text else "",
            "response_summary_path": str(response_summary_path),
        }

    store = get_dashboard_artifact_store()
    request_jsonl_key = llm_bridge_export_artifact_key(data_source, output_prefix, "requests")
    request_summary_key = llm_bridge_export_artifact_key(data_source, output_prefix, "summary")
    store.upsert_text(
        artifact_key=request_jsonl_key,
        data_source=data_source,
        artifact_kind="jsonl",
        content=request_jsonl_text,
        metadata={"output_prefix": output_prefix},
    )
    store.upsert_text(
        artifact_key=request_summary_key,
        data_source=data_source,
        artifact_kind="markdown",
        content=request_summary_text,
        metadata={"output_prefix": output_prefix},
    )

    is_inference = output_prefix == "overlay_inference_llm"
    scope = "inference" if is_inference else "historical"
    artifact_prefix = "overlay_inference" if is_inference else "overlay"
    response_jsonl_artifact_key = (
        _overlay_text_artifact_key(data_source, f"{artifact_prefix}_llm_responses", user_id)
        if user_id
        else overlay_llm_responses_artifact_key(data_source, scope)
    )
    response_summary_artifact_key = (
        _overlay_text_artifact_key(data_source, f"{artifact_prefix}_llm_response_summary", user_id)
        if user_id
        else overlay_llm_response_summary_artifact_key(data_source, scope)
    )

    if response_jsonl_text:
        store.upsert_text(
            artifact_key=response_jsonl_artifact_key,
            data_source=data_source,
            artifact_kind="jsonl",
            content=response_jsonl_text,
            metadata={"output_prefix": output_prefix, "user_id": user_id},
        )
    store.upsert_text(
        artifact_key=response_summary_artifact_key,
        data_source=data_source,
        artifact_kind="markdown",
        content=response_summary_text,
        metadata={"output_prefix": output_prefix, "user_id": user_id},
    )

    return {
        "jsonl_path": _artifact_ref(request_jsonl_key),
        "summary_path": _artifact_ref(request_summary_key),
        "response_jsonl_path": _artifact_ref(response_jsonl_artifact_key) if response_jsonl_text else "",
        "response_summary_path": _artifact_ref(response_summary_artifact_key),
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


def _watchlist_scope_sql(scope: str) -> str:
    scope_sql = {
        "all": "",
        "holdings": "and item ->> 'entry_group' = '持仓'",
        "focus": "and item ->> 'entry_group' = '重点关注'",
        "overlay": "and coalesce((item ->> 'is_overlay_selected')::boolean, false)",
        "inference": "and coalesce((item ->> 'is_inference_overlay_selected')::boolean, false)",
        "loss": "and coalesce(nullif(item ->> 'unrealized_pnl_pct', '')::double precision, 0) <= -0.1",
    }
    return scope_sql.get(str(scope or "all").strip(), "")


def _watchlist_sort_sql(sort_by: str) -> str:
    sort_sql = {
        "inference_rank": "coalesce(nullif(item ->> 'inference_ensemble_rank', '')::double precision, 'Infinity'::double precision) asc, item ->> 'ts_code' asc",
        "historical_rank": "coalesce(nullif(item ->> 'ensemble_rank', '')::double precision, 'Infinity'::double precision) asc, item ->> 'ts_code' asc",
        "drawdown": "coalesce(nullif(item ->> 'unrealized_pnl_pct', '')::double precision, 'Infinity'::double precision) asc, item ->> 'ts_code' asc",
        "market_value": "coalesce(nullif(item ->> 'market_value', '')::double precision, '-Infinity'::double precision) desc, item ->> 'ts_code' asc",
    }
    return sort_sql.get(str(sort_by or "inference_rank").strip(), sort_sql["inference_rank"])


def _load_watchlist_summary_records_from_database(
    *,
    artifact_key: str,
    field_names: list[str],
    keyword: str,
    scope: str,
    sort_by: str,
    page: int,
    page_size: int,
) -> list[dict[str, Any]]:
    projected_fields = ", ".join(
        f"'{field_name}', item -> '{field_name}'"
        for field_name in field_names
    )
    normalized_keyword = str(keyword or "").strip().lower()
    where_clauses = [
        "artifact_key = %s",
        "payload_json is not null",
        "jsonb_typeof(payload_json) = 'array'",
    ]
    params: list[Any] = [artifact_key]
    if normalized_keyword:
        where_clauses.append(
            "(coalesce(lower(item ->> 'ts_code'), '') like %s or coalesce(lower(item ->> 'name'), '') like %s)"
        )
        keyword_pattern = f"%{normalized_keyword}%"
        params.extend([keyword_pattern, keyword_pattern])

    scope_sql = _watchlist_scope_sql(scope)
    if scope_sql:
        where_clauses.append(scope_sql.removeprefix("and ").strip())

    query = f"""
        select jsonb_build_object({projected_fields}) as projected
        from dashboard_artifacts,
             jsonb_array_elements(payload_json) as item
        where {' and '.join(where_clauses)}
        order by {_watchlist_sort_sql(sort_by)}
        limit %s
        offset %s
    """
    normalized_page_size = max(1, int(page_size))
    normalized_page = max(1, int(page))
    params.extend([normalized_page_size, (normalized_page - 1) * normalized_page_size])
    with connect_database(use_dict_rows=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
        conn.commit()
    return [dict(row.get("projected") or {}) for row in rows]


def _load_watchlist_overview_from_database(*, artifact_key: str) -> dict[str, Any]:
    query = """
        select
            count(*) as total_count,
            sum(case when coalesce((item ->> 'is_overlay_selected')::boolean, false) then 1 else 0 end) as overlay_count,
            sum(case when coalesce((item ->> 'is_inference_overlay_selected')::boolean, false) then 1 else 0 end) as inference_overlay_count,
            sum(coalesce(nullif(item ->> 'market_value', '')::double precision, 0)) as market_value,
            sum(coalesce(nullif(item ->> 'unrealized_pnl', '')::double precision, 0)) as unrealized_pnl
        from dashboard_artifacts,
             jsonb_array_elements(payload_json) as item
        where artifact_key = %s
          and payload_json is not null
          and jsonb_typeof(payload_json) = 'array'
    """
    with connect_database(use_dict_rows=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (artifact_key,))
            row = cur.fetchone()
        conn.commit()
    if not row:
        return {
            "totalCount": 0,
            "overlayCount": 0,
            "inferenceOverlayCount": 0,
            "marketValue": 0.0,
            "unrealizedPnl": 0.0,
        }
    return {
        "totalCount": int(row.get("total_count", 0) or 0),
        "overlayCount": int(row.get("overlay_count", 0) or 0),
        "inferenceOverlayCount": int(row.get("inference_overlay_count", 0) or 0),
        "marketValue": float(row.get("market_value", 0) or 0.0),
        "unrealizedPnl": float(row.get("unrealized_pnl", 0) or 0.0),
    }


def _load_watchlist_filtered_count_from_database(
    *,
    artifact_key: str,
    keyword: str,
    scope: str,
) -> int:
    normalized_keyword = str(keyword or "").strip().lower()
    where_clauses = [
        "artifact_key = %s",
        "payload_json is not null",
        "jsonb_typeof(payload_json) = 'array'",
    ]
    params: list[Any] = [artifact_key]
    if normalized_keyword:
        where_clauses.append(
            "(coalesce(lower(item ->> 'ts_code'), '') like %s or coalesce(lower(item ->> 'name'), '') like %s)"
        )
        keyword_pattern = f"%{normalized_keyword}%"
        params.extend([keyword_pattern, keyword_pattern])

    scope_sql = _watchlist_scope_sql(scope)
    if scope_sql:
        where_clauses.append(scope_sql.removeprefix("and ").strip())

    query = f"""
        select count(*)
        from dashboard_artifacts,
             jsonb_array_elements(payload_json) as item
        where {' and '.join(where_clauses)}
    """
    with connect_database(use_dict_rows=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            row = cur.fetchone()
        conn.commit()
    return int((row or {}).get("count", 0) or 0)


def _filter_watchlist_snapshot_frame(
    frame: pd.DataFrame,
    *,
    keyword: str,
    scope: str,
    sort_by: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame

    filtered = frame.copy()
    normalized_keyword = str(keyword or "").strip().lower()
    if normalized_keyword:
        filtered = filtered.loc[
            filtered["ts_code"].astype(str).str.lower().str.contains(normalized_keyword)
            | filtered["name"].astype(str).str.lower().str.contains(normalized_keyword)
        ].copy()

    if scope == "holdings":
        filtered = filtered.loc[filtered["entry_group"].astype(str) == "持仓"].copy()
    elif scope == "focus":
        filtered = filtered.loc[filtered["entry_group"].astype(str) == "重点关注"].copy()
    elif scope == "overlay":
        filtered = filtered.loc[filtered["is_overlay_selected"].fillna(False)].copy()
    elif scope == "inference":
        filtered = filtered.loc[filtered["is_inference_overlay_selected"].fillna(False)].copy()
    elif scope == "loss":
        filtered = filtered.loc[pd.to_numeric(filtered["unrealized_pnl_pct"], errors="coerce") <= -0.1].copy()

    sort_map = {
        "inference_rank": ("inference_ensemble_rank", True),
        "historical_rank": ("ensemble_rank", True),
        "drawdown": ("unrealized_pnl_pct", True),
        "market_value": ("market_value", False),
    }
    sort_column, ascending = sort_map.get(sort_by, sort_map["inference_rank"])
    if sort_column in filtered.columns:
        filtered = filtered.assign(_sort_value=pd.to_numeric(filtered[sort_column], errors="coerce"))
        filtered = filtered.sort_values("_sort_value", ascending=ascending, na_position="last").drop(columns="_sort_value")
    return filtered.reset_index(drop=True)
