from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Protocol

import psycopg
from fastapi import Depends, HTTPException, Request, Response, status
from psycopg.rows import dict_row

from src.db.schema import ensure_sql_script
from src.web_api.settings import ApiSettings, get_api_settings

PASSWORD_ITERATIONS = 390_000
AUTH_SCHEMA_SCRIPT = "001_app_auth.sql"


@dataclass(frozen=True)
class AuthenticatedUser:
    user_id: str
    username: str
    display_name: str
    title: str | None = None
    session_expires_at: datetime | None = None

    def to_payload(self) -> dict[str, str | None]:
        return {
            "userId": self.user_id,
            "username": self.username,
            "displayName": self.display_name,
            "title": self.title,
            "sessionExpiresAt": self.session_expires_at.isoformat() if self.session_expires_at else None,
        }


@dataclass(frozen=True)
class LoginResult:
    user: AuthenticatedUser
    session_token: str


class AuthStore(Protocol):
    def get_session_user(self, session_token: str | None) -> AuthenticatedUser | None: ...

    def login(self, username: str, password: str) -> LoginResult | None: ...

    def logout(self, session_token: str | None) -> None: ...

    def change_password(self, user_id: str, old_password: str, new_password: str) -> bool: ...


def _hash_password(password: str, salt_hex: str | None = None) -> tuple[str, str]:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PASSWORD_ITERATIONS)
    return salt.hex(), digest.hex()


def _verify_password(password: str, *, salt_hex: str, digest_hex: str) -> bool:
    _, candidate = _hash_password(password, salt_hex=salt_hex)
    return hmac.compare_digest(candidate, digest_hex)


def _hash_session_token(session_token: str) -> str:
    return hashlib.sha256(session_token.encode("utf-8")).hexdigest()


class PostgresAuthStore:
    def __init__(self, settings: ApiSettings) -> None:
        self.settings = settings
        self._schema_ready = False

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
        if self._schema_ready:
            return

        bootstrap_salt, bootstrap_hash = _hash_password(self.settings.bootstrap_password)
        ensure_sql_script(AUTH_SCHEMA_SCRIPT)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app_users (
                        id,
                        username,
                        password_salt,
                        password_hash,
                        display_name,
                        title,
                        is_active,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
                    ON CONFLICT (username) DO NOTHING
                    """,
                    (
                        "bootstrap-admin",
                        self.settings.bootstrap_username,
                        bootstrap_salt,
                        bootstrap_hash,
                        self.settings.bootstrap_display_name,
                        self.settings.bootstrap_title,
                    ),
                )
            conn.commit()

        self._schema_ready = True

    def hash_password_for_test(self, password: str) -> tuple[str, str]:
        return _hash_password(password)

    def _cleanup_expired_sessions(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_sessions WHERE expires_at <= NOW()")

    def _should_touch_session(self, last_seen_at: datetime | None, *, now: datetime | None = None) -> bool:
        interval_seconds = max(0, int(self.settings.auth_touch_interval_seconds))
        if interval_seconds == 0 or last_seen_at is None:
            return True

        reference_time = now or datetime.now(UTC)
        normalized_last_seen_at = last_seen_at if last_seen_at.tzinfo is not None else last_seen_at.replace(tzinfo=UTC)
        return (reference_time - normalized_last_seen_at) >= timedelta(seconds=interval_seconds)

    def get_session_user(self, session_token: str | None) -> AuthenticatedUser | None:
        if not session_token:
            return None

        self._ensure_schema()
        token_hash = _hash_session_token(session_token)
        with self._connect() as conn:
            self._cleanup_expired_sessions(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        users.id,
                        users.username,
                        users.display_name,
                        users.title,
                        sessions.expires_at,
                        sessions.last_seen_at
                    FROM app_sessions AS sessions
                    INNER JOIN app_users AS users
                        ON users.id = sessions.user_id
                    WHERE sessions.token_hash = %s
                      AND sessions.expires_at > NOW()
                      AND users.is_active = TRUE
                    """,
                    (token_hash,),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return None

                if self._should_touch_session(row.get("last_seen_at")):
                    cur.execute(
                        "UPDATE app_sessions SET last_seen_at = NOW() WHERE token_hash = %s",
                        (token_hash,),
                    )
            conn.commit()

        return AuthenticatedUser(
            user_id=str(row["id"]),
            username=str(row["username"]),
            display_name=str(row["display_name"]),
            title=str(row["title"]).strip() or None if row.get("title") is not None else None,
            session_expires_at=row.get("expires_at"),
        )

    def login(self, username: str, password: str) -> LoginResult | None:
        normalized_username = str(username).strip()
        if not normalized_username or not password:
            return None

        self._ensure_schema()
        with self._connect() as conn:
            self._cleanup_expired_sessions(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, username, password_salt, password_hash, display_name, title
                    FROM app_users
                    WHERE username = %s AND is_active = TRUE
                    """,
                    (normalized_username,),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return None

                if not _verify_password(
                    password,
                    salt_hex=str(row["password_salt"]),
                    digest_hex=str(row["password_hash"]),
                ):
                    conn.commit()
                    return None

                session_token = secrets.token_urlsafe(32)
                expires_at = datetime.now(UTC) + timedelta(days=self.settings.auth_session_days)
                cur.execute(
                    """
                    INSERT INTO app_sessions (token_hash, user_id, expires_at, created_at, last_seen_at)
                    VALUES (%s, %s, %s, NOW(), NOW())
                    """,
                    (_hash_session_token(session_token), str(row["id"]), expires_at),
                )
            conn.commit()

        user = AuthenticatedUser(
            user_id=str(row["id"]),
            username=str(row["username"]),
            display_name=str(row["display_name"]),
            title=str(row["title"]).strip() or None if row.get("title") is not None else None,
            session_expires_at=expires_at,
        )
        return LoginResult(user=user, session_token=session_token)

    def logout(self, session_token: str | None) -> None:
        if not session_token:
            return

        self._ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app_sessions WHERE token_hash = %s",
                    (_hash_session_token(session_token),),
                )
            conn.commit()

    def change_password(self, user_id: str, old_password: str, new_password: str) -> bool:
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id or not old_password or not new_password:
            return False

        self._ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT password_salt, password_hash
                    FROM app_users
                    WHERE id = %s AND is_active = TRUE
                    """,
                    (normalized_user_id,),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return False

                if not _verify_password(
                    old_password,
                    salt_hex=str(row["password_salt"]),
                    digest_hex=str(row["password_hash"]),
                ):
                    conn.commit()
                    return False

                new_salt, new_hash = _hash_password(new_password)
                cur.execute(
                    """
                    UPDATE app_users
                    SET password_salt = %s,
                        password_hash = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (new_salt, new_hash, normalized_user_id),
                )
            conn.commit()
        return True


@lru_cache(maxsize=1)
def _build_auth_store(settings: ApiSettings) -> PostgresAuthStore:
    return PostgresAuthStore(settings)


def get_auth_store(settings: ApiSettings = Depends(get_api_settings)) -> AuthStore:
    return _build_auth_store(settings)


def _raise_auth_unavailable(exc: Exception) -> None:
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Authentication service unavailable.",
    ) from exc


def get_auth_session_token(
    request: Request,
    settings: ApiSettings = Depends(get_api_settings),
) -> str | None:
    return request.cookies.get(settings.auth_cookie_name)


def get_optional_authenticated_user(
    session_token: str | None = Depends(get_auth_session_token),
    auth_store: AuthStore = Depends(get_auth_store),
) -> AuthenticatedUser | None:
    try:
        return auth_store.get_session_user(session_token)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive fallback around driver errors
        _raise_auth_unavailable(exc)


def require_authenticated_user(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> AuthenticatedUser:
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Please log in before performing this action.",
        )
    return user


def set_auth_cookie(response: Response, session_token: str, settings: ApiSettings) -> None:
    response.set_cookie(
        key=settings.auth_cookie_name,
        value=session_token,
        httponly=True,
        samesite="lax",
        secure=settings.auth_cookie_secure,
        max_age=settings.auth_session_days * 24 * 60 * 60,
        path="/",
    )


def clear_auth_cookie(response: Response, settings: ApiSettings) -> None:
    response.delete_cookie(
        key=settings.auth_cookie_name,
        path="/",
        httponly=True,
        samesite="lax",
        secure=settings.auth_cookie_secure,
    )
