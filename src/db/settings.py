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


def _env_password(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value)


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    name: str
    schema: str
    user: str
    password: str
    connect_timeout: int


@lru_cache(maxsize=1)
def get_database_settings() -> DatabaseSettings:
    load_dotenv(project_root() / ".env")
    return DatabaseSettings(
        host=_env_text("APP_DB_HOST", "localhost"),
        port=_env_int("APP_DB_PORT", 5432),
        name=_env_text("APP_DB_NAME", "replace_with_database_name"),
        schema=_env_text("APP_DB_SCHEMA", "replace_with_database_schema"),
        user=_env_text("APP_DB_USER", "replace_with_database_user"),
        password=_env_password("APP_DB_PASSWORD", ""),
        connect_timeout=_env_int("APP_DB_CONNECT_TIMEOUT", 5),
    )
