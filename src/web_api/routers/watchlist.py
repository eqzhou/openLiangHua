from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, status
from pydantic import BaseModel, Field

from src.web_api.auth import (
    AuthenticatedUser,
    require_authenticated_user,
    get_api_settings,
    ApiSettings,
)
from src.app.repositories.postgres_watchlist_store import PostgresWatchlistStore
from src.app.facades.base import clear_dashboard_caches
from src.db.dashboard_sync import sync_watchlist_snapshot_artifact
from src.utils.data_source import active_data_source
from src.utils.io import project_root

router = APIRouter(prefix="/api/watchlist-config", tags=["watchlist"])

class WatchlistItemRequest(BaseModel):
    ts_code: str = Field(pattern=r"^\d{6}\.(?:SZ|SH|BJ)$")
    name: str = Field(default="", max_length=100)
    type: Literal["holding", "focus"]
    cost: float | None = Field(default=None, ge=0)
    shares: int | None = Field(default=None, ge=0)
    note: str | None = Field(default=None, max_length=1000)

def get_watchlist_store(settings: ApiSettings = Depends(get_api_settings)) -> PostgresWatchlistStore:
    return PostgresWatchlistStore(settings)


def _sync_user_watchlist_snapshot(store: PostgresWatchlistStore, user_id: str) -> str:
    clear_dashboard_caches()
    summary = sync_watchlist_snapshot_artifact(
        root=project_root(),
        data_source=active_data_source(),
        watchlist_config=store.load_watchlist(user_id),
        user_id=user_id,
    )
    if not summary.ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Watchlist snapshot sync failed.",
        )
    clear_dashboard_caches()
    return summary.message

@router.post("/items")
def add_watchlist_item(
    payload: WatchlistItemRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
    store: PostgresWatchlistStore = Depends(get_watchlist_store),
) -> dict[str, Any]:
    try:
        store.add_item(user.user_id, payload.ts_code, payload.name, payload.type, cost=payload.cost, shares=payload.shares, note=payload.note)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Watchlist configuration store unavailable.",
        ) from exc
    sync_message = _sync_user_watchlist_snapshot(store, user.user_id)
    return {"ok": True, "message": "Item added successfully", "snapshotSynced": True, "snapshotMessage": sync_message}

@router.delete("/items/{ts_code}/{item_type}")
def delete_watchlist_item(
    ts_code: str = Path(pattern=r"^\d{6}\.(?:SZ|SH|BJ)$"),
    item_type: Literal["holding", "focus"] = Path(),
    user: AuthenticatedUser = Depends(require_authenticated_user),
    store: PostgresWatchlistStore = Depends(get_watchlist_store),
) -> dict[str, Any]:
    try:
        store.remove_item(user.user_id, ts_code, item_type)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Watchlist configuration store unavailable.",
        ) from exc
    sync_message = _sync_user_watchlist_snapshot(store, user.user_id)
    return {"ok": True, "message": "Item removed successfully", "snapshotSynced": True, "snapshotMessage": sync_message}
