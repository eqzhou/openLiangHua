from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src.app.facades.dashboard_facade import (
    LABEL_OPTIONS,
    MODEL_NAMES,
    SPLITS,
    get_ai_review_detail_payload,
    clear_cache_payload,
    get_candidate_history_payload,
    generate_action_memo,
    generate_watch_plan,
    get_ai_review_payload,
    get_ai_review_summary_payload,
    get_bootstrap_payload,
    get_candidates_payload,
    get_candidates_summary_payload,
    get_experiment_config_payload,
    get_factor_explorer_detail_payload,
    get_factor_explorer_payload,
    get_factor_explorer_summary_payload,
    get_home_payload,
    get_model_backtest_payload,
    get_overview_payload,
    get_shell_payload,
    get_service_payload,
    get_watchlist_detail_payload,
    get_watchlist_payload,
    get_watchlist_summary_payload,
    run_named_action,
    update_experiment_config_payload,
)
from src.web_api.auth import (
    AuthenticatedUser,
    clear_auth_cookie,
    get_auth_session_token,
    get_optional_authenticated_user,
    get_auth_store,
    require_authenticated_user,
    set_auth_cookie,
)
from src.web_api.settings import ApiSettings, get_api_settings


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=200)


def _build_auth_payload(user: AuthenticatedUser | None) -> dict[str, Any]:
    return {
        "authenticated": user is not None,
        "user": user.to_payload() if user else None,
    }


settings = get_api_settings()
app = FastAPI(title="OpenLianghua Research API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/meta")
def get_meta() -> dict[str, Any]:
    return get_bootstrap_payload()


@app.get("/api/auth/session")
def get_auth_session(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return _build_auth_payload(user)


@app.post("/api/auth/login")
def post_auth_login(
    payload: LoginRequest,
    response: Response,
    auth_store=Depends(get_auth_store),
    api_settings: ApiSettings = Depends(get_api_settings),
) -> dict[str, Any]:
    try:
        login_result = auth_store.login(payload.username, payload.password)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Authentication service unavailable.") from exc
    if login_result is None:
        raise HTTPException(status_code=401, detail="Invalid username or password.")

    set_auth_cookie(response, login_result.session_token, api_settings)
    return _build_auth_payload(login_result.user)


@app.post("/api/auth/logout")
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


@app.get("/api/shell")
def get_shell() -> dict[str, Any]:
    return get_shell_payload()


@app.get("/api/home")
def get_home() -> dict[str, Any]:
    return get_home_payload()


@app.get("/api/config/experiment")
def get_experiment_config() -> dict[str, Any]:
    return get_experiment_config_payload()


@app.put("/api/config/experiment")
def update_experiment_config(
    payload: dict[str, Any],
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    current = get_experiment_config_payload()
    current.update(payload)
    label_col = str(current.get("label_col", "ret_t1_t10"))
    if label_col not in LABEL_OPTIONS:
        raise HTTPException(status_code=400, detail="Invalid label_col")
    return update_experiment_config_payload(current)


@app.post("/api/actions/{action_name}")
def post_action(
    action_name: str,
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    try:
        return run_named_action(action_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown action: {action_name}") from exc


@app.post("/api/actions/watch-plan")
def post_watch_plan(
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return generate_watch_plan()


@app.post("/api/actions/action-memo")
def post_action_memo(
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return generate_action_memo()


@app.post("/api/cache/clear")
def clear_cache(
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return clear_cache_payload()


@app.get("/api/overview")
def get_overview(split_name: str = Query("test")) -> dict[str, Any]:
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_overview_payload(normalized_split)


@app.get("/api/factors")
def get_factors(
    factor_name: str | None = Query(None),
    symbol: str | None = Query(None),
    history_factor: str | None = Query(None),
) -> dict[str, Any]:
    return get_factor_explorer_payload(
        factor_name=factor_name,
        symbol=symbol,
        history_factor=history_factor,
    )


@app.get("/api/factors/summary")
def get_factor_summary(
    factor_name: str | None = Query(None),
    symbol: str | None = Query(None),
    history_factor: str | None = Query(None),
) -> dict[str, Any]:
    return get_factor_explorer_summary_payload(
        factor_name=factor_name,
        symbol=symbol,
        history_factor=history_factor,
    )


@app.get("/api/factors/detail")
def get_factor_detail(
    factor_name: str | None = Query(None),
    symbol: str | None = Query(None),
    history_factor: str | None = Query(None),
) -> dict[str, Any]:
    return get_factor_explorer_detail_payload(
        factor_name=factor_name,
        symbol=symbol,
        history_factor=history_factor,
    )


@app.get("/api/backtests")
def get_backtests(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_model_backtest_payload(model_name=normalized_model, split_name=normalized_split)


@app.get("/api/candidates")
def get_candidates(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
    top_n: int = Query(10, ge=1, le=100),
    symbol: str | None = Query(None),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_candidates_payload(
        model_name=normalized_model,
        split_name=normalized_split,
        top_n=top_n,
        symbol=symbol,
    )


@app.get("/api/candidates/summary")
def get_candidates_summary(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
    top_n: int = Query(10, ge=1, le=100),
    symbol: str | None = Query(None),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_candidates_summary_payload(
        model_name=normalized_model,
        split_name=normalized_split,
        top_n=top_n,
        symbol=symbol,
    )


@app.get("/api/candidates/history")
def get_candidates_history(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
    symbol: str | None = Query(None),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_candidate_history_payload(
        model_name=normalized_model,
        split_name=normalized_split,
        symbol=symbol,
    )


@app.get("/api/watchlist")
def get_watchlist(
    keyword: str = Query(""),
    scope: str = Query("all"),
    sort_by: str = Query("inference_rank"),
    symbol: str | None = Query(None),
    include_realtime: bool = Query(False),
) -> dict[str, Any]:
    return get_watchlist_payload(
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        symbol=symbol,
        include_realtime=include_realtime,
    )


@app.get("/api/watchlist/summary")
def get_watchlist_summary(
    keyword: str = Query(""),
    scope: str = Query("all"),
    sort_by: str = Query("inference_rank"),
    symbol: str | None = Query(None),
    include_realtime: bool = Query(False),
) -> dict[str, Any]:
    return get_watchlist_summary_payload(
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        symbol=symbol,
        include_realtime=include_realtime,
    )


@app.get("/api/watchlist/detail")
def get_watchlist_detail(
    symbol: str | None = Query(None),
    keyword: str = Query(""),
    scope: str = Query("all"),
    sort_by: str = Query("inference_rank"),
    include_realtime: bool = Query(False),
) -> dict[str, Any]:
    return get_watchlist_detail_payload(
        symbol=symbol,
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        include_realtime=include_realtime,
    )


@app.get("/api/ai-review/summary")
def get_ai_review_summary(
    inference_symbol: str | None = Query(None),
    historical_symbol: str | None = Query(None),
) -> dict[str, Any]:
    return get_ai_review_summary_payload(
        inference_symbol=inference_symbol,
        historical_symbol=historical_symbol,
    )


@app.get("/api/ai-review/detail")
def get_ai_review_detail(
    scope: str = Query("inference"),
    symbol: str | None = Query(None),
) -> dict[str, Any]:
    normalized_scope = scope if scope in {"inference", "historical"} else "inference"
    return get_ai_review_detail_payload(scope=normalized_scope, symbol=symbol)


@app.get("/api/ai-review")
def get_ai_review(
    inference_symbol: str | None = Query(None),
    historical_symbol: str | None = Query(None),
) -> dict[str, Any]:
    return get_ai_review_payload(
        inference_symbol=inference_symbol,
        historical_symbol=historical_symbol,
    )


@app.get("/api/service")
def get_service() -> dict[str, Any]:
    return get_service_payload()
