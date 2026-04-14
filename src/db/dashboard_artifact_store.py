from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

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
                    SELECT artifact_key, data_source, artifact_kind, payload_json, payload_text, payload_bytes, metadata_json
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
        )


def _json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False)


@lru_cache(maxsize=1)
def get_dashboard_artifact_store() -> DashboardArtifactStore:
    return DashboardArtifactStore()
