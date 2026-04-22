from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

from src.db.settings import get_database_settings
from src.utils.io import project_root


def _env_text(name: str, default: str) -> str:
    return str(os.getenv(name, default)).strip() or default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _env_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return tuple(values) or default


@dataclass(frozen=True)
class ApiSettings:
    db_host: str
    db_port: int
    db_name: str
    db_schema: str
    db_user: str
    db_password: str
    db_connect_timeout: int
    cors_origins: tuple[str, ...]
    auth_cookie_name: str
    auth_session_days: int
    auth_touch_interval_seconds: int
    bootstrap_username: str
    bootstrap_password: str
    bootstrap_display_name: str
    bootstrap_title: str


@lru_cache(maxsize=1)
def get_api_settings() -> ApiSettings:
    load_dotenv(project_root() / ".env")
    database = get_database_settings()
    return ApiSettings(
        db_host=database.host,
        db_port=database.port,
        db_name=database.name,
        db_schema=database.schema,
        db_user=database.user,
        db_password=database.password,
        db_connect_timeout=database.connect_timeout,
        cors_origins=_env_csv(
            "APP_CORS_ORIGINS",
            (
                "http://127.0.0.1:5174",
                "http://localhost:5174",
            ),
        ),
        auth_cookie_name=_env_text("APP_AUTH_COOKIE_NAME", "openlianghua_session"),
        auth_session_days=_env_int("APP_AUTH_SESSION_DAYS", 7),
        auth_touch_interval_seconds=_env_int("APP_AUTH_SESSION_TOUCH_INTERVAL_SECONDS", 300),
        bootstrap_username=_env_text("APP_BOOTSTRAP_USERNAME", "admin"),
        bootstrap_password=_env_text("APP_BOOTSTRAP_PASSWORD", "Openlianghua@2026"),
        bootstrap_display_name=_env_text("APP_BOOTSTRAP_DISPLAY_NAME", "System Admin"),
        bootstrap_title=_env_text("APP_BOOTSTRAP_TITLE", "Research Admin"),
    )
