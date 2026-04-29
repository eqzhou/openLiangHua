from __future__ import annotations

import copy
import logging
from time import perf_counter
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.app.facades import (
    LABEL_OPTIONS,
    MODEL_NAMES,
    SPLITS,
    get_ai_review_detail_payload,
    clear_cache_payload,
    get_data_management_payload,
    get_myquant_status_payload,
    get_candidate_history_payload,
    get_candidate_detail_payload,
    generate_action_memo,
    generate_watch_plan,
    get_ai_review_payload,
    get_ai_review_summary_payload,
    get_model_backtest_diagnostics_payload,
    get_model_backtest_portfolio_payload,
    get_model_backtest_summary_payload,
    get_bootstrap_payload,
    get_candidates_payload,
    get_candidates_summary_payload,
    get_experiment_config_payload,
    get_factor_explorer_detail_payload,
    get_factor_explorer_payload,
    get_factor_explorer_summary_payload,
    get_home_ai_review_section_payload,
    get_home_candidates_section_payload,
    get_home_payload,
    get_home_summary_payload,
    get_home_watchlist_section_payload,
    get_model_backtest_payload,
    get_overview_curves_payload,
    get_overview_summary_payload,
    get_overview_payload,
    get_shell_payload,
    get_service_payload,
    get_watchlist_detail_payload,
    get_watchlist_payload,
    get_watchlist_summary_payload,
    run_market_bars_refresh_payload,
    run_myquant_download_payload,
    run_myquant_enrich_payload,
    run_myquant_research_refresh_payload,
    run_watchlist_research_refresh_payload,
    refresh_realtime_payload,
    run_tushare_full_refresh_payload,
    run_tushare_incremental_refresh_payload,
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
from src.web_api.routers import watchlist_router

class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=200)


class DataRefreshRequest(BaseModel):
    target_source: str = Field(default="akshare", min_length=1, max_length=50)
    end_date: str | None = Field(default=None, max_length=20)


class ChangePasswordRequest(BaseModel):
    old_password: str = Field(alias="oldPassword", min_length=1, max_length=200)
    new_password: str = Field(alias="newPassword", min_length=1, max_length=200)


def _build_auth_payload(user: AuthenticatedUser | None) -> dict[str, Any]:
    return {
        "authenticated": user is not None,
        "user": user.to_payload() if user else None,
    }


def _deep_merge_config(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in updates.items():
        current_value = merged.get(key)
        if isinstance(current_value, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_config(current_value, value)
        else:
            merged[key] = value
    return merged


def _validate_new_password(value: str) -> None:
    if len(value) < 8:
        raise HTTPException(status_code=400, detail="New password is too weak.")
    if not any(char.isalpha() for char in value) or not any(char.isdigit() for char in value):
        raise HTTPException(status_code=400, detail="New password is too weak.")


def _is_empty_watchlist_error(exc: RuntimeError) -> bool:
    return "No symbols were provided and no watchlist_items symbols were found" in str(exc)


settings = get_api_settings()
app = FastAPI(title="OpenLianghua Research API", version="0.1.0")
app.include_router(watchlist_router)
logger = logging.getLogger("openlianghua.web_api")

class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message

@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.message}
    )

@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=400,
        content={"detail": str(exc)}
    )

@app.exception_handler(KeyError)
async def key_error_handler(request: Request, exc: KeyError):
    return JSONResponse(
        status_code=404,
        content={"detail": f"Resource not found: {str(exc)}"}
    )

LEGACY_AGGREGATE_PATHS = frozenset(
    {
        "/api/home",
        "/api/overview",
        "/api/backtests",
        "/api/candidates",
    }
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def observe_api_requests(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api/"):
        return await call_next(request)

    started_at = perf_counter()
    response = await call_next(request)
    duration_ms = round((perf_counter() - started_at) * 1000, 2)
    content_length = response.headers.get("content-length", "")
    legacy_aggregate = path in LEGACY_AGGREGATE_PATHS

    response.headers["X-OpenLianghua-Response-Ms"] = str(duration_ms)
    if legacy_aggregate:
        response.headers["X-OpenLianghua-Legacy-Aggregate"] = "true"

    content_length_value = int(content_length) if content_length.isdigit() else -1
    if legacy_aggregate or duration_ms >= 250 or content_length_value >= 50_000:
        logger.info(
            "api_request path=%s method=%s status=%s duration_ms=%.2f content_length=%s legacy_aggregate=%s",
            path,
            request.method,
            response.status_code,
            duration_ms,
            content_length or "unknown",
            legacy_aggregate,
        )

    return response


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


@app.post("/api/auth/change-password")
def post_auth_change_password(
    payload: ChangePasswordRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
    auth_store=Depends(get_auth_store),
) -> dict[str, Any]:
    _validate_new_password(payload.new_password)
    try:
        changed = auth_store.change_password(user.user_id, payload.old_password, payload.new_password)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Authentication service unavailable.") from exc
    if not changed:
        raise HTTPException(status_code=400, detail="Current password is incorrect.")
    return {"ok": True}


@app.get("/api/shell")
def get_shell(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_shell_payload(user_id=user.user_id if user else None)


@app.get("/api/home")
def get_home(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_home_payload(user_id=user.user_id if user else None)


@app.get("/api/home/summary")
def get_home_summary(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_home_summary_payload(user_id=user.user_id if user else None)


@app.get("/api/home/watchlist")
def get_home_watchlist(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_home_watchlist_section_payload(user_id=user.user_id if user else None)


@app.get("/api/home/candidates")
def get_home_candidates() -> dict[str, Any]:
    return get_home_candidates_section_payload()


@app.get("/api/home/ai-review")
def get_home_ai_review(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_home_ai_review_section_payload(user_id=user.user_id if user else None)


@app.get("/api/config/experiment")
def get_experiment_config() -> dict[str, Any]:
    return get_experiment_config_payload()


@app.put("/api/config/experiment")
def update_experiment_config(
    payload: dict[str, Any],
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    current = get_experiment_config_payload()
    merged = _deep_merge_config(current, payload)
    label_col = str(merged.get("label_col", "ret_t1_t10"))
    if label_col not in LABEL_OPTIONS:
        raise HTTPException(status_code=400, detail="Invalid label_col")
    return update_experiment_config_payload(merged)


@app.post("/api/actions/watch-plan")
def post_watch_plan(
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return generate_watch_plan(user_id=user.user_id)


@app.post("/api/actions/action-memo")
def post_action_memo(
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return generate_action_memo(user_id=user.user_id)


@app.post("/api/actions/{action_name}")
def post_action(
    action_name: str,
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return run_named_action(action_name)


@app.post("/api/cache/clear")
def clear_cache(
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return clear_cache_payload()


@app.post("/api/realtime/refresh")
def refresh_realtime(
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return refresh_realtime_payload(user_id=user.user_id)


@app.get("/api/data-management")
def get_data_management(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_data_management_payload(include_sensitive=user is not None)


@app.get("/api/data-management/myquant-status")
def get_myquant_status(
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_myquant_status_payload(include_sensitive=user is not None)


@app.post("/api/data-management/tushare-refresh")
def post_tushare_refresh(
    payload: DataRefreshRequest,
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return run_tushare_incremental_refresh_payload(target_source=payload.target_source, end_date=payload.end_date)


@app.post("/api/data-management/full-refresh")
def post_tushare_full_refresh(
    payload: DataRefreshRequest,
    _: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return run_tushare_full_refresh_payload(target_source=payload.target_source, end_date=payload.end_date)


@app.post("/api/data-management/market-bars-refresh")
def post_market_bars_refresh(
    payload: DataRefreshRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    try:
        return run_market_bars_refresh_payload(user_id=user.user_id, end_date=payload.end_date)
    except RuntimeError as exc:
        if _is_empty_watchlist_error(exc):
            raise HTTPException(status_code=409, detail="观察池为空，请先添加持仓或重点观察标的。") from exc
        raise


@app.post("/api/data-management/watchlist-research-refresh")
def post_watchlist_research_refresh(
    payload: DataRefreshRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return run_watchlist_research_refresh_payload(user_id=user.user_id, target_source=payload.target_source)


@app.post("/api/data-management/myquant-download")
def post_myquant_download(
    payload: DataRefreshRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return run_myquant_download_payload(user_id=user.user_id, end_date=payload.end_date)


@app.post("/api/data-management/myquant-enrich")
def post_myquant_enrich(
    payload: DataRefreshRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return run_myquant_enrich_payload(user_id=user.user_id)


@app.post("/api/data-management/myquant-research-refresh")
def post_myquant_research_refresh(
    payload: DataRefreshRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> dict[str, Any]:
    return run_myquant_research_refresh_payload(user_id=user.user_id)


@app.get("/api/overview")
def get_overview(split_name: str = Query("test")) -> dict[str, Any]:
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_overview_payload(normalized_split)


@app.get("/api/overview/summary")
def get_overview_summary(split_name: str = Query("test")) -> dict[str, Any]:
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_overview_summary_payload(normalized_split)


@app.get("/api/overview/curves")
def get_overview_curves(split_name: str = Query("test")) -> dict[str, Any]:
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_overview_curves_payload(normalized_split)


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


@app.get("/api/backtests/summary")
def get_backtests_summary(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_model_backtest_summary_payload(model_name=normalized_model, split_name=normalized_split)


@app.get("/api/backtests/portfolio")
def get_backtests_portfolio(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_model_backtest_portfolio_payload(model_name=normalized_model, split_name=normalized_split)


@app.get("/api/backtests/diagnostics")
def get_backtests_diagnostics(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_model_backtest_diagnostics_payload(model_name=normalized_model, split_name=normalized_split)


@app.get("/api/candidates")
def get_candidates(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
    top_n: int = Query(30, ge=1, le=100),
    page: int = Query(1, ge=1, le=9999),
    symbol: str | None = Query(None),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_candidates_payload(
        model_name=normalized_model,
        split_name=normalized_split,
        top_n=top_n,
        page=page,
        symbol=symbol,
    )


@app.get("/api/candidates/summary")
def get_candidates_summary(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
    top_n: int = Query(30, ge=1, le=100),
    page: int = Query(1, ge=1, le=9999),
    symbol: str | None = Query(None),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_candidates_summary_payload(
        model_name=normalized_model,
        split_name=normalized_split,
        top_n=top_n,
        page=page,
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


@app.get("/api/candidates/detail")
def get_candidates_detail(
    model_name: str = Query("lgbm"),
    split_name: str = Query("test"),
    symbol: str | None = Query(None),
) -> dict[str, Any]:
    normalized_model = model_name if model_name in MODEL_NAMES else "lgbm"
    normalized_split = split_name if split_name in SPLITS else "test"
    return get_candidate_detail_payload(
        model_name=normalized_model,
        split_name=normalized_split,
        symbol=symbol,
    )


@app.get("/api/watchlist")
def get_watchlist(
    keyword: str = Query(""),
    scope: str = Query("all"),
    sort_by: str = Query("inference_rank"),
    page: int = Query(1, ge=1, le=9999),
    symbol: str | None = Query(None),
    include_realtime: bool = Query(False),
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    if include_realtime and user is None:
        raise HTTPException(status_code=401, detail="Please log in before refreshing realtime data.")
    return get_watchlist_payload(
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        page=page,
        symbol=symbol,
        include_realtime=include_realtime,
        user_id=user.user_id if user else None,
    )


@app.get("/api/watchlist/summary")
def get_watchlist_summary(
    keyword: str = Query(""),
    scope: str = Query("all"),
    sort_by: str = Query("inference_rank"),
    page: int = Query(1, ge=1, le=9999),
    symbol: str | None = Query(None),
    include_realtime: bool = Query(False),
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    if include_realtime and user is None:
        raise HTTPException(status_code=401, detail="Please log in before refreshing realtime data.")
    return get_watchlist_summary_payload(
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        page=page,
        symbol=symbol,
        include_realtime=include_realtime,
        user_id=user.user_id if user else None,
    )


@app.get("/api/watchlist/detail")
def get_watchlist_detail(
    symbol: str | None = Query(None),
    keyword: str = Query(""),
    scope: str = Query("all"),
    sort_by: str = Query("inference_rank"),
    include_realtime: bool = Query(False),
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    if include_realtime and user is None:
        raise HTTPException(status_code=401, detail="Please log in before refreshing realtime data.")
    return get_watchlist_detail_payload(
        symbol=symbol,
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        include_realtime=include_realtime,
        user_id=user.user_id if user else None,
    )


@app.get("/api/ai-review/summary")
def get_ai_review_summary(
    inference_symbol: str | None = Query(None),
    historical_symbol: str | None = Query(None),
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_ai_review_summary_payload(
        inference_symbol=inference_symbol,
        historical_symbol=historical_symbol,
        user_id=user.user_id if user else None,
    )


@app.get("/api/ai-review/detail")
def get_ai_review_detail(
    scope: str = Query("inference"),
    symbol: str | None = Query(None),
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    normalized_scope = scope if scope in {"inference", "historical"} else "inference"
    return get_ai_review_detail_payload(
        scope=normalized_scope,
        symbol=symbol,
        user_id=user.user_id if user else None,
    )


@app.get("/api/ai-review")
def get_ai_review(
    inference_symbol: str | None = Query(None),
    historical_symbol: str | None = Query(None),
    user: AuthenticatedUser | None = Depends(get_optional_authenticated_user),
) -> dict[str, Any]:
    return get_ai_review_payload(
        inference_symbol=inference_symbol,
        historical_symbol=historical_symbol,
        user_id=user.user_id if user else None,
    )


@app.get("/api/service")
def get_service() -> dict[str, Any]:
    return get_service_payload()
