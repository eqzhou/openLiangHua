from __future__ import annotations

from datetime import date, datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.app.repositories.config_repository import load_experiment_config, load_watchlist_config
from src.app.repositories.report_repository import (
    load_daily_bar,
    load_dataset_summary,
    load_diagnostic_table,
    load_feature_importance,
    load_feature_panel,
    load_latest_symbol_markdown,
    load_metrics,
    load_inference_packet,
    load_overlay_brief,
    load_overlay_candidates,
    load_overlay_inference_brief,
    load_overlay_inference_candidates,
    load_overlay_inference_packet,
    load_overlay_packet,
    load_portfolio,
    load_predictions,
    load_stability,
    load_ensemble_weights,
)
from src.app.viewmodels.factor_explorer_vm import build_missing_rate_table, list_numeric_factor_columns
from src.app.services.watchlist_service import build_watchlist_view
from src.db.dashboard_artifact_keys import (
    binary_artifact_key,
    candidate_snapshot_artifact_key,
    config_artifact_key,
    factor_explorer_artifact_key,
    json_artifact_key,
    note_artifact_key,
    overlay_llm_response_summary_artifact_key,
    overlay_llm_responses_artifact_key,
    table_artifact_key,
    text_artifact_key,
    watchlist_artifact_key,
)
from src.db.dashboard_artifact_store import get_dashboard_artifact_store
from src.utils.prediction_snapshot import build_latest_prediction_snapshot
from src.utils.data_source import active_data_source
from src.utils.data_source import source_or_canonical_path
from src.utils.io import project_root

MODEL_NAMES = ("ridge", "lgbm", "ensemble")
PREDICTION_SPLITS = ("valid", "test", "inference")
DIAGNOSTIC_TABLES = ("yearly", "regime")
NOTE_KINDS = ("watch_plan", "action_memo")
@dataclass(frozen=True)
class SyncSummary:
    ok: bool
    synced_items: int
    message: str


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (pd.Timedelta,)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value):
            return None
        return value
    if isinstance(value, str | int | bool) or value is None:
        return value
    if pd.isna(value):
        return None
    return value


def _frame_payload(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    working = frame.copy()
    return [_json_ready(record) for record in working.to_dict(orient="records")]


def _artifact_exists(store, artifact_key: str) -> bool:
    try:
        return store.get_artifact(artifact_key) is not None
    except Exception:
        return False


def _sync_llm_bridge_artifacts(*, store, data_source: str, scope: str, packet: dict[str, Any]) -> int:
    llm_bridge = dict(packet.get("llm_bridge", {}) or {})
    synced_items = 0

    response_path_text = str(llm_bridge.get("response_jsonl_path", "") or "").strip()
    if response_path_text:
        response_path = Path(response_path_text)
        if response_path.exists():
            store.upsert_text(
                artifact_key=overlay_llm_responses_artifact_key(data_source, scope),
                data_source=data_source,
                artifact_kind="jsonl",
                content=response_path.read_text(encoding="utf-8", errors="ignore"),
                metadata={
                    "source_path": str(response_path),
                    "size_bytes": response_path.stat().st_size,
                },
            )
            synced_items += 1

    summary_path_text = str(llm_bridge.get("response_summary_path", "") or "").strip()
    if summary_path_text:
        summary_path = Path(summary_path_text)
        if summary_path.exists():
            store.upsert_text(
                artifact_key=overlay_llm_response_summary_artifact_key(data_source, scope),
                data_source=data_source,
                artifact_kind="markdown",
                content=summary_path.read_text(encoding="utf-8", errors="ignore"),
                metadata={
                    "source_path": str(summary_path),
                    "size_bytes": summary_path.stat().st_size,
                },
            )
            synced_items += 1

    return synced_items


def _build_watchlist_snapshot(
    *,
    root: Path,
    data_source: str,
    watchlist_config: dict[str, Any],
) -> pd.DataFrame:
    return build_watchlist_view(
        root=root,
        data_source=data_source,
        watchlist_config=watchlist_config,
        daily_bar=load_daily_bar(root, data_source=data_source, prefer_database=False),
        ridge_predictions=load_predictions(root, data_source=data_source, model_name="ridge", split_name="test", prefer_database=False),
        lgbm_predictions=load_predictions(root, data_source=data_source, model_name="lgbm", split_name="test", prefer_database=False),
        ensemble_predictions=load_predictions(root, data_source=data_source, model_name="ensemble", split_name="test", prefer_database=False),
        overlay_candidates=load_overlay_candidates(root, data_source=data_source, prefer_database=False),
        ensemble_inference_predictions=load_predictions(root, data_source=data_source, model_name="ensemble", split_name="inference", prefer_database=False),
        overlay_inference_candidates=load_overlay_inference_candidates(root, data_source=data_source, prefer_database=False),
    )


def sync_watchlist_snapshot_artifact(
    *,
    root: Path | None = None,
    data_source: str | None = None,
    watchlist_config: dict[str, Any] | None = None,
    snapshot_frame: pd.DataFrame | None = None,
) -> SyncSummary:
    resolved_root = root or project_root()
    resolved_data_source = data_source or active_data_source()
    resolved_watchlist_config = watchlist_config or load_watchlist_config(resolved_root, prefer_database=False)
    store = get_dashboard_artifact_store()

    try:
        watchlist_snapshot = snapshot_frame
        if watchlist_snapshot is None:
            watchlist_snapshot = _build_watchlist_snapshot(
                root=resolved_root,
                data_source=resolved_data_source,
                watchlist_config=resolved_watchlist_config,
            )
        store.upsert_json(
            artifact_key=watchlist_artifact_key(resolved_data_source),
            data_source=resolved_data_source,
            artifact_kind="table",
            payload=_frame_payload(watchlist_snapshot),
            metadata={"rows": int(len(watchlist_snapshot))},
        )
    except Exception as exc:
        return SyncSummary(ok=False, synced_items=0, message=str(exc))

    return SyncSummary(
        ok=True,
        synced_items=1,
        message=f"Synced watchlist snapshot ({len(watchlist_snapshot)} rows) to Postgres.",
    )


def sync_candidate_snapshot_artifact(
    *,
    model_name: str,
    split_name: str,
    root: Path | None = None,
    data_source: str | None = None,
    predictions: pd.DataFrame | None = None,
    snapshot_frame: pd.DataFrame | None = None,
) -> SyncSummary:
    resolved_root = root or project_root()
    resolved_data_source = data_source or active_data_source()
    store = get_dashboard_artifact_store()

    try:
        working_predictions = predictions
        if working_predictions is None:
            working_predictions = load_predictions(
                resolved_root,
                data_source=resolved_data_source,
                model_name=model_name,
                split_name=split_name,
                prefer_database=False,
            )
        candidate_snapshot = snapshot_frame if snapshot_frame is not None else build_latest_prediction_snapshot(working_predictions)
        store.upsert_json(
            artifact_key=candidate_snapshot_artifact_key(resolved_data_source, model_name, split_name),
            data_source=resolved_data_source,
            artifact_kind="table",
            payload=_frame_payload(candidate_snapshot),
            metadata={
                "model_name": model_name,
                "split_name": split_name,
                "rows": int(len(candidate_snapshot)),
            },
        )
    except Exception as exc:
        return SyncSummary(ok=False, synced_items=0, message=str(exc))

    return SyncSummary(
        ok=True,
        synced_items=1,
        message=f"Synced candidate snapshot ({model_name}/{split_name}, {len(candidate_snapshot)} rows) to Postgres.",
    )


def sync_factor_explorer_snapshot_artifact(
    *,
    root: Path | None = None,
    data_source: str | None = None,
    feature_panel: pd.DataFrame | None = None,
    snapshot_payload: dict[str, Any] | None = None,
) -> SyncSummary:
    resolved_root = root or project_root()
    resolved_data_source = data_source or active_data_source()
    store = get_dashboard_artifact_store()

    try:
        payload = snapshot_payload
        if payload is None:
            working_panel = feature_panel
            if working_panel is None:
                working_panel = load_feature_panel(
                    resolved_root,
                    data_source=resolved_data_source,
                    prefer_database=False,
                )

            numeric_columns = list_numeric_factor_columns(working_panel)
            if not numeric_columns:
                payload = {
                    "available": False,
                    "latestDate": None,
                    "factorOptions": [],
                    "symbolOptions": [],
                    "crossSection": [],
                    "missingRates": [],
                }
            else:
                latest_date = working_panel["trade_date"].max()
                cross_section = working_panel.loc[working_panel["trade_date"] == latest_date].copy()
                payload = {
                    "available": True,
                    "latestDate": _json_ready(latest_date),
                    "factorOptions": [
                        {"key": column, "label": column}
                        for column in numeric_columns
                    ],
                    "symbolOptions": cross_section["ts_code"].sort_values().tolist(),
                    "crossSection": _frame_payload(cross_section),
                    "missingRates": _frame_payload(build_missing_rate_table(working_panel, numeric_columns)),
                }

        store.upsert_json(
            artifact_key=factor_explorer_artifact_key(resolved_data_source),
            data_source=resolved_data_source,
            artifact_kind="json",
            payload=_json_ready(payload),
            metadata={
                "available": bool(payload.get("available")),
                "rows": int(len(payload.get("crossSection", []) or [])),
            },
        )
    except Exception as exc:
        return SyncSummary(ok=False, synced_items=0, message=str(exc))

    return SyncSummary(
        ok=True,
        synced_items=1,
        message=f"Synced factor explorer snapshot ({len(payload.get('crossSection', []) or [])} rows) to Postgres.",
    )


def sync_dataset_summary_artifact(
    *,
    root: Path | None = None,
    data_source: str | None = None,
    summary_payload: dict[str, Any] | None = None,
) -> SyncSummary:
    resolved_root = root or project_root()
    resolved_data_source = data_source or active_data_source()
    store = get_dashboard_artifact_store()

    try:
        payload = summary_payload or load_dataset_summary(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=False,
        )
        store.upsert_json(
            artifact_key=json_artifact_key(resolved_data_source, "dataset_summary"),
            data_source=resolved_data_source,
            artifact_kind="json",
            payload=_json_ready(payload),
            metadata={"artifact_name": "dataset_summary"},
        )
    except Exception as exc:
        return SyncSummary(ok=False, synced_items=0, message=str(exc))

    return SyncSummary(ok=True, synced_items=1, message="Synced dataset summary to Postgres.")


def sync_dashboard_artifacts(*, root: Path | None = None, data_source: str | None = None) -> SyncSummary:
    resolved_root = root or project_root()
    resolved_data_source = data_source or active_data_source()
    store = get_dashboard_artifact_store()
    synced_items = 0

    try:
        experiment_config = load_experiment_config(resolved_root, prefer_database=False)
        store.upsert_json(
            artifact_key=config_artifact_key("experiment"),
            data_source="shared",
            artifact_kind="config",
            payload=_json_ready(experiment_config),
            metadata={"source_path": str(resolved_root / "config" / "experiment.yaml")},
        )
        synced_items += 1

        watchlist_config = load_watchlist_config(resolved_root, prefer_database=False)
        store.upsert_json(
            artifact_key=config_artifact_key("watchlist"),
            data_source="shared",
            artifact_kind="config",
            payload=_json_ready(watchlist_config),
            metadata={"source_path": str(resolved_root / "config" / "watchlist.yaml")},
        )
        synced_items += 1

        for model_name in MODEL_NAMES:
            stability_key = json_artifact_key(resolved_data_source, f"stability:{model_name}")
            stability = {} if _artifact_exists(store, stability_key) else load_stability(
                resolved_root,
                data_source=resolved_data_source,
                model_name=model_name,
                prefer_database=False,
            )
            if stability:
                store.upsert_json(
                    artifact_key=stability_key,
                    data_source=resolved_data_source,
                    artifact_kind="json",
                    payload=_json_ready(stability),
                    metadata={"model_name": model_name},
                )
                synced_items += 1

            importance_key = table_artifact_key(resolved_data_source, f"feature_importance:{model_name}")
            importance = pd.DataFrame() if _artifact_exists(store, importance_key) else load_feature_importance(
                resolved_root,
                data_source=resolved_data_source,
                model_name=model_name,
                prefer_database=False,
            )
            if not importance.empty:
                store.upsert_json(
                    artifact_key=importance_key,
                    data_source=resolved_data_source,
                    artifact_kind="table",
                    payload=_frame_payload(importance),
                    metadata={"model_name": model_name, "rows": int(len(importance))},
                )
                synced_items += 1

            for split_name in PREDICTION_SPLITS:
                prediction_key = table_artifact_key(resolved_data_source, f"predictions:{model_name}:{split_name}")
                predictions = pd.DataFrame() if _artifact_exists(store, prediction_key) else load_predictions(
                    resolved_root,
                    data_source=resolved_data_source,
                    model_name=model_name,
                    split_name=split_name,
                    prefer_database=False,
                )
                if not predictions.empty:
                    store.upsert_json(
                        artifact_key=prediction_key,
                        data_source=resolved_data_source,
                        artifact_kind="table",
                        payload=_frame_payload(predictions),
                        metadata={"model_name": model_name, "split_name": split_name, "rows": int(len(predictions))},
                    )
                    synced_items += 1

                    candidate_summary = sync_candidate_snapshot_artifact(
                        root=resolved_root,
                        data_source=resolved_data_source,
                        model_name=model_name,
                        split_name=split_name,
                        predictions=predictions,
                    )
                    if not candidate_summary.ok:
                        raise RuntimeError(candidate_summary.message)
                    synced_items += candidate_summary.synced_items

                portfolio_key = table_artifact_key(resolved_data_source, f"portfolio:{model_name}:{split_name}")
                portfolio = pd.DataFrame() if _artifact_exists(store, portfolio_key) else load_portfolio(
                    resolved_root,
                    data_source=resolved_data_source,
                    model_name=model_name,
                    split_name=split_name,
                    prefer_database=False,
                )
                if not portfolio.empty:
                    store.upsert_json(
                        artifact_key=portfolio_key,
                        data_source=resolved_data_source,
                        artifact_kind="table",
                        payload=_frame_payload(portfolio),
                        metadata={"model_name": model_name, "split_name": split_name, "rows": int(len(portfolio))},
                    )
                    synced_items += 1

                metrics_key = json_artifact_key(resolved_data_source, f"metrics:{model_name}:{split_name}")
                metrics = {} if _artifact_exists(store, metrics_key) else load_metrics(
                    resolved_root,
                    data_source=resolved_data_source,
                    model_name=model_name,
                    split_name=split_name,
                    prefer_database=False,
                )
                if metrics:
                    store.upsert_json(
                        artifact_key=metrics_key,
                        data_source=resolved_data_source,
                        artifact_kind="json",
                        payload=_json_ready(metrics),
                        metadata={"model_name": model_name, "split_name": split_name},
                    )
                    synced_items += 1

                for table_name in DIAGNOSTIC_TABLES:
                    diagnostic_key = table_artifact_key(resolved_data_source, f"diagnostic:{model_name}:{split_name}:{table_name}")
                    diagnostic = pd.DataFrame() if _artifact_exists(store, diagnostic_key) else load_diagnostic_table(
                        resolved_root,
                        data_source=resolved_data_source,
                        model_name=model_name,
                        split_name=split_name,
                        table_name=table_name,
                        prefer_database=False,
                    )
                    if diagnostic.empty:
                        continue
                    store.upsert_json(
                        artifact_key=diagnostic_key,
                        data_source=resolved_data_source,
                        artifact_kind="table",
                        payload=_frame_payload(diagnostic),
                        metadata={
                            "model_name": model_name,
                            "split_name": split_name,
                            "table_name": table_name,
                            "rows": int(len(diagnostic)),
                        },
                    )
                    synced_items += 1

        overlay_candidates = load_overlay_candidates(resolved_root, data_source=resolved_data_source, prefer_database=False)
        if not overlay_candidates.empty:
            store.upsert_json(
                artifact_key=table_artifact_key(resolved_data_source, "overlay_candidates"),
                data_source=resolved_data_source,
                artifact_kind="table",
                payload=_frame_payload(overlay_candidates),
                metadata={"rows": int(len(overlay_candidates))},
            )
            synced_items += 1

        overlay_packet = load_overlay_packet(resolved_root, data_source=resolved_data_source, prefer_database=False)
        if overlay_packet:
            store.upsert_json(
                artifact_key=json_artifact_key(resolved_data_source, "overlay_packet"),
                data_source=resolved_data_source,
                artifact_kind="json",
                payload=_json_ready(overlay_packet),
                metadata={},
            )
            synced_items += 1
            synced_items += _sync_llm_bridge_artifacts(
                store=store,
                data_source=resolved_data_source,
                scope="historical",
                packet=overlay_packet,
            )

        overlay_brief = load_overlay_brief(resolved_root, data_source=resolved_data_source, prefer_database=False)
        if overlay_brief:
            store.upsert_text(
                artifact_key=text_artifact_key(resolved_data_source, "overlay_brief"),
                data_source=resolved_data_source,
                artifact_kind="text",
                content=overlay_brief,
                metadata={},
            )
            synced_items += 1

        inference_candidates = load_overlay_inference_candidates(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=False,
        )
        if not inference_candidates.empty:
            store.upsert_json(
                artifact_key=table_artifact_key(resolved_data_source, "overlay_inference_candidates"),
                data_source=resolved_data_source,
                artifact_kind="table",
                payload=_frame_payload(inference_candidates),
                metadata={"rows": int(len(inference_candidates))},
            )
            synced_items += 1

        inference_packet = load_overlay_inference_packet(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=False,
        )
        if inference_packet:
            store.upsert_json(
                artifact_key=json_artifact_key(resolved_data_source, "overlay_inference_packet"),
                data_source=resolved_data_source,
                artifact_kind="json",
                payload=_json_ready(inference_packet),
                metadata={},
            )
            synced_items += 1
            synced_items += _sync_llm_bridge_artifacts(
                store=store,
                data_source=resolved_data_source,
                scope="inference",
                packet=inference_packet,
            )

        inference_brief = load_overlay_inference_brief(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=False,
        )
        if inference_brief:
            store.upsert_text(
                artifact_key=text_artifact_key(resolved_data_source, "overlay_inference_brief"),
                data_source=resolved_data_source,
                artifact_kind="text",
                content=inference_brief,
                metadata={},
            )
            synced_items += 1

        inference_packet_key = json_artifact_key(resolved_data_source, "inference_packet")
        inference_packet_payload = {} if _artifact_exists(store, inference_packet_key) else load_inference_packet(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=False,
        )
        if inference_packet_payload:
            store.upsert_json(
                artifact_key=inference_packet_key,
                data_source=resolved_data_source,
                artifact_kind="json",
                payload=_json_ready(inference_packet_payload),
                metadata={},
            )
            synced_items += 1

        ensemble_weights_key = json_artifact_key(resolved_data_source, "ensemble_weights")
        ensemble_weights_payload = {} if _artifact_exists(store, ensemble_weights_key) else load_ensemble_weights(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=False,
        )
        if ensemble_weights_payload:
            store.upsert_json(
                artifact_key=ensemble_weights_key,
                data_source=resolved_data_source,
                artifact_kind="json",
                payload=_json_ready(ensemble_weights_payload),
                metadata={},
            )
            synced_items += 1

        holdings = watchlist_config.get("holdings", []) or []
        focus_pool = watchlist_config.get("focus_pool", []) or []
        symbols = {
            str(item.get("ts_code", "")).strip()
            for item in [*holdings, *focus_pool]
            if str(item.get("ts_code", "")).strip()
        }
        for symbol in sorted(symbols):
            for note_kind in NOTE_KINDS:
                note_payload = load_latest_symbol_markdown(
                    symbol,
                    note_kind,
                    root=resolved_root,
                    data_source=resolved_data_source,
                    prefer_database=False,
                )
                if not note_payload or not note_payload.get("content"):
                    continue
                store.upsert_text(
                    artifact_key=note_artifact_key(resolved_data_source, symbol, note_kind),
                    data_source=resolved_data_source,
                    artifact_kind="note",
                    content=str(note_payload.get("content", "")),
                    metadata={
                        "path": str(note_payload.get("path", "")),
                        "name": str(note_payload.get("name", "")),
                        "plan_date": str(note_payload.get("plan_date", "")),
                    },
                )
                synced_items += 1

    except Exception as exc:
        return SyncSummary(ok=False, synced_items=synced_items, message=str(exc))

    return SyncSummary(
        ok=True,
        synced_items=synced_items,
        message=f"Synced {synced_items} dashboard artifacts to Postgres.",
    )


def run() -> None:
    summary = sync_dashboard_artifacts()
    print(summary.message)
    if not summary.ok:
        raise SystemExit(1)


if __name__ == "__main__":
    run()
