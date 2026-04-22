from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import re
from typing import Any
from uuid import UUID

import pandas as pd

from src.db.connection import connect_database
from src.db.schema import ensure_sql_script


ARTIFACT_SCHEMA_SCRIPT = "002_dashboard_artifacts.sql"
ARTIFACT_BYTES_SCHEMA_SCRIPT = "003_dashboard_artifact_bytes.sql"


@dataclass(frozen=True)
class DashboardArtifact:
    artifact_key: str
    data_source: str
    artifact_kind: str
    payload_json: Any
    payload_text: str | None
    payload_bytes: bytes | None
    metadata_json: dict[str, Any]
    updated_at: datetime | None = None


class DashboardArtifactStore:
    def __init__(self) -> None:
        self._schema_ready = False

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        ensure_sql_script(ARTIFACT_SCHEMA_SCRIPT)
        ensure_sql_script(ARTIFACT_BYTES_SCHEMA_SCRIPT)
        self._schema_ready = True

    def upsert_json(
        self,
        *,
        artifact_key: str,
        data_source: str,
        artifact_kind: str,
        payload: Any,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.ensure_schema()
        with connect_database() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dashboard_artifacts (
                        artifact_key,
                        data_source,
                        artifact_kind,
                        payload_json,
                        payload_text,
                        metadata_json,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s::jsonb, NULL, %s::jsonb, NOW())
                    ON CONFLICT (artifact_key) DO UPDATE
                    SET
                        data_source = EXCLUDED.data_source,
                        artifact_kind = EXCLUDED.artifact_kind,
                        payload_json = EXCLUDED.payload_json,
                        payload_text = NULL,
                        payload_bytes = NULL,
                        metadata_json = EXCLUDED.metadata_json,
                        updated_at = NOW()
                    """,
                    (artifact_key, data_source, artifact_kind, _json_dumps(payload), _json_dumps(metadata or {})),
                )
            conn.commit()

    def upsert_text(
        self,
        *,
        artifact_key: str,
        data_source: str,
        artifact_kind: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.ensure_schema()
        with connect_database() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dashboard_artifacts (
                        artifact_key,
                        data_source,
                        artifact_kind,
                        payload_json,
                        payload_text,
                        metadata_json,
                        updated_at
                    )
                    VALUES (%s, %s, %s, NULL, %s, %s::jsonb, NOW())
                    ON CONFLICT (artifact_key) DO UPDATE
                    SET
                        data_source = EXCLUDED.data_source,
                        artifact_kind = EXCLUDED.artifact_kind,
                        payload_json = NULL,
                        payload_text = EXCLUDED.payload_text,
                        payload_bytes = NULL,
                        metadata_json = EXCLUDED.metadata_json,
                        updated_at = NOW()
                    """,
                    (artifact_key, data_source, artifact_kind, content, _json_dumps(metadata or {})),
                )
            conn.commit()

    def upsert_bytes(
        self,
        *,
        artifact_key: str,
        data_source: str,
        artifact_kind: str,
        content: bytes,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.ensure_schema()
        with connect_database() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dashboard_artifacts (
                        artifact_key,
                        data_source,
                        artifact_kind,
                        payload_json,
                        payload_text,
                        payload_bytes,
                        metadata_json,
                        updated_at
                    )
                    VALUES (%s, %s, %s, NULL, NULL, %s, %s::jsonb, NOW())
                    ON CONFLICT (artifact_key) DO UPDATE
                    SET
                        data_source = EXCLUDED.data_source,
                        artifact_kind = EXCLUDED.artifact_kind,
                        payload_json = NULL,
                        payload_text = NULL,
                        payload_bytes = EXCLUDED.payload_bytes,
                        metadata_json = EXCLUDED.metadata_json,
                        updated_at = NOW()
                    """,
                    (artifact_key, data_source, artifact_kind, content, _json_dumps(metadata or {})),
                )
            conn.commit()

    def get_artifact(self, artifact_key: str) -> DashboardArtifact | None:
        self.ensure_schema()
        with connect_database(use_dict_rows=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT artifact_key, data_source, artifact_kind, payload_json, payload_text, payload_bytes, metadata_json, updated_at
                    FROM dashboard_artifacts
                    WHERE artifact_key = %s
                    """,
                    (artifact_key,),
                )
                row = cur.fetchone()
            conn.commit()

        if not row:
            return None

        return DashboardArtifact(
            artifact_key=str(row["artifact_key"]),
            data_source=str(row["data_source"]),
            artifact_kind=str(row["artifact_kind"]),
            payload_json=row.get("payload_json"),
            payload_text=row.get("payload_text"),
            payload_bytes=row.get("payload_bytes"),
            metadata_json=row.get("metadata_json") or {},
            updated_at=row.get("updated_at"),
        )

    def get_json_records_by_field(self, *, artifact_key: str, field_name: str, field_value: str) -> list[dict[str, Any]]:
        self.ensure_schema()
        with connect_database(use_dict_rows=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT item
                    FROM dashboard_artifacts,
                         jsonb_array_elements(payload_json) AS item
                    WHERE artifact_key = %s
                      AND payload_json IS NOT NULL
                      AND jsonb_typeof(payload_json) = 'array'
                      AND item ->> %s = %s
                    """,
                    (artifact_key, field_name, field_value),
                )
                rows = cur.fetchall()
            conn.commit()
        return [dict(row.get("item") or {}) for row in rows]

    def get_projected_json_records(
        self,
        *,
        artifact_key: str,
        field_names: list[str],
        filter_field_name: str | None = None,
        filter_field_value: str | None = None,
        order_by_field: str | None = None,
        descending: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        normalized_fields = _validated_json_field_names(field_names)
        if not normalized_fields:
            return []

        projected_pairs = ", ".join(
            f"'{field_name}', item -> '{field_name}'"
            for field_name in normalized_fields
        )
        query = f"""
            SELECT jsonb_build_object({projected_pairs}) AS projected
            FROM dashboard_artifacts,
                 jsonb_array_elements(payload_json) AS item
            WHERE artifact_key = %s
              AND payload_json IS NOT NULL
              AND jsonb_typeof(payload_json) = 'array'
        """
        params: list[Any] = [artifact_key]
        if filter_field_name and filter_field_value is not None:
            normalized_filter_field = _validated_json_field_name(filter_field_name)
            query += " AND item ->> %s = %s"
            params.extend([normalized_filter_field, filter_field_value])

        if order_by_field:
            normalized_order_field = _validated_json_field_name(order_by_field)
            query += f" ORDER BY item ->> '{normalized_order_field}' {'DESC' if descending else 'ASC'}"

        if limit is not None and limit > 0:
            query += " LIMIT %s"
            params.append(limit)

        with connect_database(use_dict_rows=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                rows = cur.fetchall()
            conn.commit()
        return [dict(row.get("projected") or {}) for row in rows]


def _json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def _validated_json_field_name(field_name: str) -> str:
    normalized = str(field_name or "").strip()
    if not normalized or not re.fullmatch(r"[A-Za-z0-9_]+", normalized):
        raise ValueError(f"Invalid JSON field name: {field_name!r}")
    return normalized


def _validated_json_field_names(field_names: list[str]) -> list[str]:
    return [_validated_json_field_name(field_name) for field_name in field_names]


@lru_cache(maxsize=1)
def get_dashboard_artifact_store() -> DashboardArtifactStore:
    return DashboardArtifactStore()
