from __future__ import annotations

from typing import Any
import psycopg
from psycopg.rows import dict_row

from src.db.schema import ensure_sql_script
from src.web_api.settings import ApiSettings

WATCHLIST_SCHEMA_SCRIPT = "006_watchlist.sql"

class PostgresWatchlistStore:
    def __init__(self, settings: ApiSettings) -> None:
        self.settings = settings
        self._ensure_schema()

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            dbname=self.settings.db_name,
            user=self.settings.db_user,
            password=self.settings.db_password,
            connect_timeout=self.settings.db_connect_timeout,
            options=f"-c search_path={self.settings.db_schema},public",
            row_factory=dict_row,
        )

    def _ensure_schema(self) -> None:
        try:
            ensure_sql_script(WATCHLIST_SCHEMA_SCRIPT)
        except Exception as exc:
            import logging
            logging.getLogger("openlianghua.watchlist").warning(f"Could not initialize watchlist schema: {exc}")

    def load_watchlist(self, user_id: str) -> dict[str, list[dict[str, Any]]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code, name, type, cost, shares, note FROM watchlist_items WHERE user_id = %s ORDER BY created_at ASC",
                    (user_id,)
                )
                rows = cur.fetchall()

        holdings = []
        focus_pool = []
        for row in rows:
            if row["type"] == "holding":
                holdings.append({
                    "ts_code": row["ts_code"],
                    "name": row["name"],
                    "cost": float(row["cost"]) if row["cost"] is not None else None,
                    "shares": row["shares"],
                })
            elif row["type"] == "focus":
                focus_pool.append({
                    "ts_code": row["ts_code"],
                    "name": row["name"],
                    "note": row["note"],
                })

        return {"holdings": holdings, "focus_pool": focus_pool}

    def add_item(self, user_id: str, ts_code: str, name: str, item_type: str, cost: float | None = None, shares: int | None = None, note: str | None = None) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO watchlist_items (user_id, ts_code, name, type, cost, shares, note)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, ts_code, type) 
                    DO UPDATE SET name = EXCLUDED.name, cost = EXCLUDED.cost, shares = EXCLUDED.shares, note = EXCLUDED.note, updated_at = CURRENT_TIMESTAMP
                    """,
                    (user_id, ts_code, name, item_type, cost, shares, note)
                )
            conn.commit()

    def remove_item(self, user_id: str, ts_code: str, item_type: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM watchlist_items WHERE user_id = %s AND ts_code = %s AND type = %s",
                    (user_id, ts_code, item_type)
                )
            conn.commit()
