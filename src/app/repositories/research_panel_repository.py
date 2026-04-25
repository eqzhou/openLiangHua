from __future__ import annotations

import json
import uuid
from typing import Any

import pandas as pd

from src.db.connection import connect_database
from src.db.schema import ensure_sql_script


RESEARCH_PANEL_SCHEMA_SCRIPT = "005_research_panel.sql"

RESEARCH_PANEL_COLUMNS = [
    "data_source",
    "trade_date",
    "ts_code",
    "name",
    "industry",
    "index_code",
    "is_current_name_st",
    "is_index_member",
    "days_since_list",
    "pct_chg",
    "ret_1d",
    "mom_5",
    "mom_20",
    "mom_60",
    "mom_120",
    "vol_20",
    "close_to_ma_20",
    "vol_60",
    "close_to_ma_60",
    "amount_20",
    "downside_vol_20",
    "ret_skew_20",
    "drawdown_60",
    "can_enter_next_day",
    "ret_next_1d",
    "label_valid_t5",
    "ret_t1_t5",
    "label_valid_t10",
    "ret_t1_t10",
    "label_valid_t20",
    "ret_t1_t20",
    "run_id",
]

RESEARCH_FEATURE_COLUMNS = [
    "trade_date",
    "ts_code",
    "name",
    "industry",
    "index_code",
    "is_current_name_st",
    "is_index_member",
    "days_since_list",
    "pct_chg",
    "ret_1d",
    "mom_5",
    "mom_20",
    "mom_60",
    "mom_120",
    "vol_20",
    "close_to_ma_20",
    "vol_60",
    "close_to_ma_60",
    "amount_20",
    "downside_vol_20",
    "ret_skew_20",
    "drawdown_60",
]

RESEARCH_LABEL_COLUMNS = [
    "trade_date",
    "ts_code",
    "can_enter_next_day",
    "ret_next_1d",
    "label_valid_t5",
    "ret_t1_t5",
    "label_valid_t10",
    "ret_t1_t10",
    "label_valid_t20",
    "ret_t1_t20",
]


def ensure_research_panel_schema() -> None:
    ensure_sql_script(RESEARCH_PANEL_SCHEMA_SCRIPT)


def merge_feature_and_label_frames(
    *,
    data_source: str,
    run_id: uuid.UUID,
    feature_frame: pd.DataFrame,
    label_frame: pd.DataFrame,
) -> pd.DataFrame:
    merged = feature_frame.copy().merge(label_frame.copy(), on=["trade_date", "ts_code"], how="inner")
    merged["trade_date"] = pd.to_datetime(merged["trade_date"], errors="coerce")
    merged["data_source"] = data_source
    merged["run_id"] = run_id

    for column in RESEARCH_PANEL_COLUMNS:
        if column not in merged.columns:
            merged[column] = pd.NA

    return merged[RESEARCH_PANEL_COLUMNS].sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def build_panel_run_payload(
    *,
    run_id: uuid.UUID,
    data_source: str,
    status: str,
    panel_frame: pd.DataFrame,
    feature_columns: list[str],
    label_columns: list[str],
    message: str = "",
) -> dict[str, Any]:
    working = panel_frame.copy()
    if "trade_date" in working.columns:
        working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")

    date_min = None
    date_max = None
    if not working.empty and "trade_date" in working.columns:
        trade_dates = working["trade_date"].dropna()
        if not trade_dates.empty:
            date_min = str(trade_dates.min().date())
            date_max = str(trade_dates.max().date())

    symbol_count = int(working["ts_code"].nunique()) if not working.empty and "ts_code" in working.columns else 0
    return {
        "run_id": run_id,
        "data_source": data_source,
        "status": status,
        "date_min": date_min,
        "date_max": date_max,
        "row_count": int(len(working)),
        "symbol_count": symbol_count,
        "feature_columns": list(feature_columns),
        "label_columns": list(label_columns),
        "message": message,
    }


def save_panel_run(payload: dict[str, Any]) -> None:
    ensure_research_panel_schema()
    with connect_database() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into research.panel_runs (
                    run_id, data_source, status, date_min, date_max, row_count, symbol_count,
                    feature_columns, label_columns, message, started_at, completed_at, created_at, updated_at
                ) values (
                    %(run_id)s, %(data_source)s, %(status)s, %(date_min)s, %(date_max)s, %(row_count)s, %(symbol_count)s,
                    %(feature_columns)s::jsonb, %(label_columns)s::jsonb, %(message)s, now(),
                    case when %(status)s = 'running' then null else now() end, now(), now()
                )
                on conflict (run_id) do update set
                    status = excluded.status,
                    date_min = excluded.date_min,
                    date_max = excluded.date_max,
                    row_count = excluded.row_count,
                    symbol_count = excluded.symbol_count,
                    feature_columns = excluded.feature_columns,
                    label_columns = excluded.label_columns,
                    message = excluded.message,
                    completed_at = case when excluded.status = 'running' then research.panel_runs.completed_at else now() end,
                    updated_at = now()
                """,
                {
                    **payload,
                    "run_id": str(payload["run_id"]),
                    "feature_columns": json.dumps(payload.get("feature_columns", []), ensure_ascii=False),
                    "label_columns": json.dumps(payload.get("label_columns", []), ensure_ascii=False),
                },
            )
        conn.commit()


def _py_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def save_research_panel(
    *,
    panel_frame: pd.DataFrame,
    chunk_size: int = 10_000,
) -> int:
    ensure_research_panel_schema()
    if panel_frame.empty:
        return 0

    working = panel_frame.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    working = working.dropna(subset=["data_source", "trade_date", "ts_code"]).copy()
    if working.empty:
        return 0

    rows: list[tuple[Any, ...]] = []
    for record in working[RESEARCH_PANEL_COLUMNS].itertuples(index=False, name=None):
        rows.append(tuple(_py_value(value) for value in record))

    insert_sql = """
        insert into research.panel (
            data_source, trade_date, ts_code, name, industry, index_code,
            is_current_name_st, is_index_member, days_since_list,
            pct_chg, ret_1d, mom_5, mom_20, mom_60, mom_120,
            vol_20, close_to_ma_20, vol_60, close_to_ma_60, amount_20,
            downside_vol_20, ret_skew_20, drawdown_60,
            can_enter_next_day, ret_next_1d,
            label_valid_t5, ret_t1_t5, label_valid_t10, ret_t1_t10, label_valid_t20, ret_t1_t20,
            run_id, created_at, updated_at
        ) values (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, now(), now()
        )
        on conflict (data_source, trade_date, ts_code) do update set
            name = excluded.name,
            industry = excluded.industry,
            index_code = excluded.index_code,
            is_current_name_st = excluded.is_current_name_st,
            is_index_member = excluded.is_index_member,
            days_since_list = excluded.days_since_list,
            pct_chg = excluded.pct_chg,
            ret_1d = excluded.ret_1d,
            mom_5 = excluded.mom_5,
            mom_20 = excluded.mom_20,
            mom_60 = excluded.mom_60,
            mom_120 = excluded.mom_120,
            vol_20 = excluded.vol_20,
            close_to_ma_20 = excluded.close_to_ma_20,
            vol_60 = excluded.vol_60,
            close_to_ma_60 = excluded.close_to_ma_60,
            amount_20 = excluded.amount_20,
            downside_vol_20 = excluded.downside_vol_20,
            ret_skew_20 = excluded.ret_skew_20,
            drawdown_60 = excluded.drawdown_60,
            can_enter_next_day = excluded.can_enter_next_day,
            ret_next_1d = excluded.ret_next_1d,
            label_valid_t5 = excluded.label_valid_t5,
            ret_t1_t5 = excluded.ret_t1_t5,
            label_valid_t10 = excluded.label_valid_t10,
            ret_t1_t10 = excluded.ret_t1_t10,
            label_valid_t20 = excluded.label_valid_t20,
            ret_t1_t20 = excluded.ret_t1_t20,
            run_id = excluded.run_id,
            updated_at = now()
    """

    with connect_database() as conn:
        with conn.cursor() as cur:
            for start in range(0, len(rows), chunk_size):
                cur.executemany(insert_sql, rows[start : start + chunk_size])
        conn.commit()
    return len(rows)


def load_research_panel(
    *,
    data_source: str,
    date_from: str | None = None,
    date_to: str | None = None,
    symbols: list[str] | None = None,
    columns: list[str] | None = None,
    index_code: str | None = None,
    require_index_member: bool = False,
    extra_symbols: list[str] | None = None,
) -> pd.DataFrame:
    ensure_research_panel_schema()

    selected_columns = columns or RESEARCH_PANEL_COLUMNS
    query = [
        "select " + ", ".join(selected_columns),
        "from research.panel",
        "where data_source = %s",
    ]
    params: list[Any] = [data_source]
    if date_from:
        query.append("and trade_date >= %s")
        params.append(date_from)
    if date_to:
        query.append("and trade_date <= %s")
        params.append(date_to)
    if symbols:
        query.append("and ts_code = any(%s)")
        params.append(symbols)
    elif require_index_member and index_code:
        normalized_extra_symbols = [str(symbol).strip() for symbol in (extra_symbols or []) if str(symbol).strip()]
        if normalized_extra_symbols:
            query.append("and ((index_code = %s and coalesce(is_index_member, false) = true) or ts_code = any(%s))")
            params.extend([index_code, normalized_extra_symbols])
        else:
            query.append("and index_code = %s and coalesce(is_index_member, false) = true")
            params.append(index_code)
    query.append("order by trade_date, ts_code")

    with connect_database(use_dict_rows=True) as conn:
        with conn.cursor() as cur:
            cur.execute("\n".join(query), tuple(params))
            rows = cur.fetchall()
        conn.commit()

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def load_latest_successful_panel_run(*, data_source: str) -> dict[str, Any]:
    ensure_research_panel_schema()
    with connect_database(use_dict_rows=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id, data_source, status, date_min, date_max, row_count, symbol_count,
                       feature_columns, label_columns, message, started_at, completed_at, updated_at
                from research.panel_runs
                where data_source = %s
                  and status = 'succeeded'
                order by updated_at desc
                limit 1
                """,
                (data_source,),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def load_research_panel_summary(*, data_source: str) -> dict[str, Any]:
    ensure_research_panel_schema()
    latest_run = load_latest_successful_panel_run(data_source=data_source)
    if not latest_run:
        return {}
    return {
        "row_count": int(latest_run.get("row_count", 0) or 0),
        "symbol_count": int(latest_run.get("symbol_count", 0) or 0),
        "date_min": str(pd.Timestamp(latest_run["date_min"]).date()) if latest_run.get("date_min") is not None else None,
        "date_max": str(pd.Timestamp(latest_run["date_max"]).date()) if latest_run.get("date_max") is not None else None,
    }


def load_feature_frame_from_research_panel(*, data_source: str) -> pd.DataFrame:
    frame = load_research_panel(data_source=data_source)
    if frame.empty:
        return frame
    available_columns = [column for column in RESEARCH_FEATURE_COLUMNS if column in frame.columns]
    return frame[available_columns].copy()


def load_label_frame_from_research_panel(*, data_source: str) -> pd.DataFrame:
    frame = load_research_panel(data_source=data_source)
    if frame.empty:
        return frame
    available_columns = [column for column in RESEARCH_LABEL_COLUMNS if column in frame.columns]
    return frame[available_columns].copy()
