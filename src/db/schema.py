from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg import sql

from src.db.settings import DatabaseSettings, get_database_settings
from src.db.connection import connect_database
from src.utils.io import project_root


SQL_DIR = project_root() / "db" / "sql"


def ensure_database_exists(settings: DatabaseSettings | None = None) -> None:
    resolved = settings or get_database_settings()
    admin_db_name = str(os.getenv("APP_DB_ADMIN_NAME", "postgres")).strip() or "postgres"

    with psycopg.connect(
        host=resolved.host,
        port=resolved.port,
        dbname=admin_db_name,
        user=resolved.user,
        password=resolved.password,
        connect_timeout=resolved.connect_timeout,
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (resolved.name,))
            exists = cur.fetchone() is not None
            if exists:
                return

            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(resolved.name)))


def ensure_database_schema_exists(settings: DatabaseSettings | None = None) -> None:
    resolved = settings or get_database_settings()
    ensure_database_exists(resolved)

    with psycopg.connect(
        host=resolved.host,
        port=resolved.port,
        dbname=resolved.name,
        user=resolved.user,
        password=resolved.password,
        connect_timeout=resolved.connect_timeout,
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(resolved.schema)))


def run_sql_script(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with connect_database() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def ensure_sql_script(script_name: str) -> None:
    path = SQL_DIR / script_name
    if not path.exists():
        raise FileNotFoundError(f"Missing SQL script: {path}")
    run_sql_script(path)
