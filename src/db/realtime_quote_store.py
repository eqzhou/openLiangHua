from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from typing import Any

import pandas as pd

from src.db.connection import connect_database
from src.db.schema import ensure_sql_script


REALTIME_QUOTE_SCHEMA_SCRIPT = "004_realtime_quote_snapshots.sql"
TIME_COLUMNS = ("realtime_time", "quote_time")


@dataclass(frozen=True)
class RealtimeQuoteSnapshot:
    trade_date: str
    snapshot_bucket: str
    quotes: pd.DataFrame
    status: dict[str, Any]


class RealtimeQuoteStore:
    def __init__(self) -> None:
        self._schema_ready = False

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        ensure_sql_script(REALTIME_QUOTE_SCHEMA_SCRIPT)
        self._schema_ready = True

    def upsert_snapshot(
        self,
        *,
        trade_date: str | date,
        snapshot_bucket: str,
        quotes: pd.DataFrame,
        status: dict[str, Any],
    ) -> None:
        self.ensure_schema()
        normalized_trade_date = str(pd.Timestamp(trade_date).date())
        normalized_status = _json_ready(status)
        fetched_at = _timestamp_or_now(normalized_status.get("fetched_at"))
        failed_symbols = normalized_status.get("failed_symbols", [])
        if not isinstance(failed_symbols, list):
            failed_symbols = [failed_symbols]

        with connect_database() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO realtime_quote_batches (
                        trade_date,
                        snapshot_bucket,
                        source,
                        requested_symbol_count,
                        success_symbol_count,
                        failed_symbols,
                        error_message,
                        fetched_at,
                        status_json,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (trade_date, snapshot_bucket) DO UPDATE
                    SET
                        source = EXCLUDED.source,
                        requested_symbol_count = EXCLUDED.requested_symbol_count,
                        success_symbol_count = EXCLUDED.success_symbol_count,
                        failed_symbols = EXCLUDED.failed_symbols,
                        error_message = EXCLUDED.error_message,
                        fetched_at = EXCLUDED.fetched_at,
                        status_json = EXCLUDED.status_json,
                        updated_at = NOW()
                    """,
                    (
                        normalized_trade_date,
                        snapshot_bucket,
                        str(normalized_status.get("source", "")),
                        int(normalized_status.get("requested_symbol_count", len(quotes))),
                        int(normalized_status.get("success_symbol_count", len(quotes))),
                        json.dumps(failed_symbols, ensure_ascii=False),
                        str(normalized_status.get("error_message", "")),
                        fetched_at,
                        json.dumps(normalized_status, ensure_ascii=False),
                    ),
                )
                cur.execute(
                    """
                    DELETE FROM realtime_quote_rows
                    WHERE trade_date = %s AND snapshot_bucket = %s
                    """,
                    (normalized_trade_date, snapshot_bucket),
                )

                row_params: list[tuple[Any, ...]] = []
                for record in quotes.to_dict(orient="records"):
                    payload = _json_ready(record)
                    row_params.append(
                        (
                            normalized_trade_date,
                            snapshot_bucket,
                            str(payload.get("ts_code", "")),
                            _timestamp_or_none(payload.get("realtime_time") or payload.get("quote_time")),
                            str(payload.get("realtime_quote_source", "")),
                            json.dumps(payload, ensure_ascii=False),
                        )
                    )

                if row_params:
                    cur.executemany(
                        """
                        INSERT INTO realtime_quote_rows (
                            trade_date,
                            snapshot_bucket,
                            ts_code,
                            quote_time,
                            quote_source,
                            payload_json,
                            created_at,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                        """,
                        row_params,
                    )
            conn.commit()

    def get_snapshot(self, *, trade_date: str | date, snapshot_bucket: str) -> RealtimeQuoteSnapshot | None:
        self.ensure_schema()
        normalized_trade_date = str(pd.Timestamp(trade_date).date())
        with connect_database(use_dict_rows=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date, snapshot_bucket, source, requested_symbol_count, success_symbol_count,
                           failed_symbols, error_message, fetched_at, status_json
                    FROM realtime_quote_batches
                    WHERE trade_date = %s AND snapshot_bucket = %s
                    """,
                    (normalized_trade_date, snapshot_bucket),
                )
                batch = cur.fetchone()
                if not batch:
                    conn.commit()
                    return None

                cur.execute(
                    """
                    SELECT payload_json
                    FROM realtime_quote_rows
                    WHERE trade_date = %s AND snapshot_bucket = %s
                    ORDER BY ts_code
                    """,
                    (normalized_trade_date, snapshot_bucket),
                )
                rows = cur.fetchall()
            conn.commit()

        quotes = pd.DataFrame([row["payload_json"] for row in rows]) if rows else pd.DataFrame()
        if not quotes.empty:
            for column in TIME_COLUMNS:
                if column in quotes.columns:
                    quotes[column] = pd.to_datetime(quotes[column], errors="coerce")

        status = dict(batch.get("status_json") or {})
        if not status:
            status = {
                "source": batch.get("source", ""),
                "requested_symbol_count": batch.get("requested_symbol_count", 0),
                "success_symbol_count": batch.get("success_symbol_count", 0),
                "failed_symbols": batch.get("failed_symbols", []),
                "error_message": batch.get("error_message", ""),
                "fetched_at": _json_ready(batch.get("fetched_at")),
            }
        status.setdefault("trade_date", normalized_trade_date)
        status.setdefault("snapshot_bucket", snapshot_bucket)

        return RealtimeQuoteSnapshot(
            trade_date=normalized_trade_date,
            snapshot_bucket=snapshot_bucket,
            quotes=quotes,
            status=status,
        )

    def get_latest_snapshot_summary(self) -> dict[str, Any] | None:
        self.ensure_schema()
        with connect_database(use_dict_rows=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date, snapshot_bucket, source, requested_symbol_count, success_symbol_count,
                           failed_symbols, error_message, fetched_at, status_json
                    FROM realtime_quote_batches
                    ORDER BY
                        trade_date DESC,
                        CASE snapshot_bucket
                            WHEN 'post_close' THEN 0
                            WHEN 'latest' THEN 1
                            ELSE 9
                        END,
                        fetched_at DESC
                    LIMIT 1
                    """
                )
                batch = cur.fetchone()
            conn.commit()

        if not batch:
            return None

        payload = dict(batch.get("status_json") or {})
        payload.update(
            {
                "trade_date": str(pd.Timestamp(batch["trade_date"]).date()),
                "snapshot_bucket": str(batch.get("snapshot_bucket", "")),
                "source": str(batch.get("source", "")),
                "requested_symbol_count": int(batch.get("requested_symbol_count", 0) or 0),
                "success_symbol_count": int(batch.get("success_symbol_count", 0) or 0),
                "failed_symbols": batch.get("failed_symbols", []),
                "error_message": str(batch.get("error_message", "")),
                "fetched_at": _json_ready(batch.get("fetched_at")),
                "served_from": str(payload.get("served_from") or "database"),
            }
        )
        return payload

    def get_latest_snapshot(self) -> RealtimeQuoteSnapshot | None:
        latest = self.get_latest_snapshot_summary()
        if not latest:
            return None
        return self.get_snapshot(
            trade_date=str(latest.get("trade_date", "")),
            snapshot_bucket=str(latest.get("snapshot_bucket", "")),
        )


def _json_ready(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return value
    if pd.isna(value):
        return None
    return value


def _timestamp_or_none(value: Any):
    if value in (None, ""):
        return None
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return None
    return timestamp.to_pydatetime()


def _timestamp_or_now(value: Any):
    timestamp = _timestamp_or_none(value)
    if timestamp is not None:
        return timestamp
    return pd.Timestamp.now(tz="UTC").to_pydatetime()


@lru_cache(maxsize=1)
def get_realtime_quote_store() -> RealtimeQuoteStore:
    return RealtimeQuoteStore()
