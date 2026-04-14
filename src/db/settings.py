from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

from src.utils.io import project_root


def _env_text(name: str, default: str) -> str:
    return str(os.getenv(name, default)).strip() or default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    name: str
    user: str
    password: str
    connect_timeout: int


@lru_cache(maxsize=1)
def get_database_settings() -> DatabaseSettings:
    load_dotenv(project_root() / ".env")
    return DatabaseSettings(
        host=_env_text("APP_DB_HOST", "127.0.0.1"),
        port=_env_int("APP_DB_PORT", 5432),
        name=_env_text("APP_DB_NAME", "quant_db"),
        user=_env_text("APP_DB_USER", "postgres"),
        password=_env_text("APP_DB_PASSWORD", "postgres123"),
        connect_timeout=_env_int("APP_DB_CONNECT_TIMEOUT", 5),
    )
