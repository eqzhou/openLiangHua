from typing import Any
from fastapi import APIRouter, Depends, Response, HTTPException
from pydantic import BaseModel

from src.web_api.auth import (
    AuthenticatedUser,
    require_authenticated_user,
    get_api_settings,
    ApiSettings,
)
from src.app.repositories.postgres_watchlist_store import PostgresWatchlistStore
from src.app.facades.base import clear_dashboard_caches

router = APIRouter(prefix="/api/watchlist-config", tags=["watchlist"])

class WatchlistItemRequest(BaseModel):
    ts_code: str
    name: str = ""
    type: str # 'holding' or 'focus'
    cost: float | None = None
    shares: int | None = None
    note: str | None = None

def get_watchlist_store(settings: ApiSettings = Depends(get_api_settings)) -> PostgresWatchlistStore:
    return PostgresWatchlistStore(settings)

@router.post("/items")
def add_watchlist_item(
    payload: WatchlistItemRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
    store: PostgresWatchlistStore = Depends(get_watchlist_store),
) -> dict[str, Any]:
    try:
        # Currently using "system" as user_id for global pool
        store.add_item("system", payload.ts_code, payload.name, payload.type, cost=payload.cost, shares=payload.shares, note=payload.note)
        clear_dashboard_caches()
        return {"ok": True, "message": "Item added successfully"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@router.delete("/items/{ts_code}/{item_type}")
def delete_watchlist_item(
    ts_code: str,
    item_type: str,
    user: AuthenticatedUser = Depends(require_authenticated_user),
    store: PostgresWatchlistStore = Depends(get_watchlist_store),
) -> dict[str, Any]:
    try:
        store.remove_item("system", ts_code, item_type)
        clear_dashboard_caches()
        return {"ok": True, "message": "Item removed successfully"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
