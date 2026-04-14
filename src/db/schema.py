from __future__ import annotations

from pathlib import Path

from src.db.connection import connect_database
from src.utils.io import project_root


SQL_DIR = project_root() / "db" / "sql"


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
