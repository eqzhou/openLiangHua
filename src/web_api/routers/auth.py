from typing import Any
from fastapi import APIRouter, Depends, Response, HTTPException

from src.web_api.auth import (
    AuthenticatedUser,
    get_auth_store,
    get_api_settings,
    set_auth_cookie,
    clear_auth_cookie,
    get_auth_session_token,
    get_optional_authenticated_user,
    ApiSettings,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])

def _build_auth_payload(user: AuthenticatedUser | None) -> dict[str, Any]:
    if user is None:
        return {"authenticated": False, "user": None}
    return {"authenticated": True, "user": user.to_payload()}

@router.get("/session")
def get_auth_session(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return _build_auth_payload(user)

@router.post("/login")
def post_auth_login(
    payload: Any, # Use a local schema or Any for now to match app.py context
    response: Response,
    auth_store=Depends(get_auth_store),
    api_settings: ApiSettings = Depends(get_api_settings),
) -> dict[str, Any]:
    # We'll use the same logic as app.py
    try:
        login_result = auth_store.login(payload.username, payload.password)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Authentication service unavailable.") from exc
    if login_result is None:
        raise HTTPException(status_code=401, detail="Invalid username or password.")

    set_auth_cookie(response, login_result.session_token, api_settings)
    return _build_auth_payload(login_result.user)

@router.post("/logout")
def post_auth_logout(
    response: Response,
    session_token: str | None = Depends(get_auth_session_token),
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
    auth_store=Depends(get_auth_store),
    api_settings: ApiSettings = Depends(get_api_settings),
) -> dict[str, Any]:
    if user is not None and session_token:
        try:
            auth_store.logout(session_token)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Authentication service unavailable.") from exc
    clear_auth_cookie(response, api_settings)
    return {"ok": True}
