from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from src.db.settings import DatabaseSettings, get_database_settings


def connect_database(*, settings: DatabaseSettings | None = None, use_dict_rows: bool = False) -> psycopg.Connection:
    resolved = settings or get_database_settings()
    kwargs: dict[str, object] = {
        "host": resolved.host,
        "port": resolved.port,
        "dbname": resolved.name,
        "user": resolved.user,
        "password": resolved.password,
        "connect_timeout": resolved.connect_timeout,
        "options": f"-c search_path={resolved.schema},public",
    }
    if use_dict_rows:
        kwargs["row_factory"] = dict_row
    return psycopg.connect(**kwargs)
