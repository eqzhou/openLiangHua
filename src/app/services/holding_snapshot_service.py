from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import BDay

from src.app.repositories.holding_repository import (
    load_daily_bar_for_symbols as repo_load_daily_bar_for_symbols,
    load_overlay_symbols as repo_load_overlay_symbols,
    load_prediction_snapshots as repo_load_prediction_snapshots,
    load_trade_dates as repo_load_trade_dates,
    resolve_data_source as repo_resolve_data_source,
)


def resolve_data_source(root: Path | None = None) -> str:
    return repo_resolve_data_source(root)


def latest_prediction_details(snapshot: pd.DataFrame, symbol: str) -> dict[str, object]:
    if snapshot.empty:
        return {}

    row = snapshot.loc[snapshot["ts_code"] == symbol]
    if row.empty:
        return {}

    record = row.iloc[0].to_dict()
    record["signal_date"] = snapshot["trade_date"].iloc[0]
    record["universe_size"] = int(len(snapshot))
    return record


def load_prediction_snapshots(root: Path | None = None, data_source: str | None = None) -> dict[str, pd.DataFrame]:
    return repo_load_prediction_snapshots(root=root, data_source=data_source)


def load_overlay_symbols(
    root: Path | None = None,
    data_source: str | None = None,
    *,
    filename: str = "overlay_latest_candidates.csv",
) -> set[str]:
    return repo_load_overlay_symbols(root=root, data_source=data_source, filename=filename)


def read_daily_bar(root: Path | None = None, data_source: str | None = None, *, symbols: list[str]) -> pd.DataFrame:
    return repo_load_daily_bar_for_symbols(root=root, data_source=data_source, symbols=symbols)


def load_trade_dates(root: Path | None = None, data_source: str | None = None) -> pd.Series:
    return repo_load_trade_dates(root=root, data_source=data_source)


def next_trade_date(trade_dates: pd.Series, as_of: pd.Timestamp | None) -> pd.Timestamp | None:
    if as_of is None or trade_dates.empty:
        return None

    future_dates = trade_dates.loc[trade_dates > as_of.normalize()]
    if future_dates.empty:
        return pd.Timestamp(as_of.normalize() + BDay(1))
    return pd.Timestamp(future_dates.iloc[0])


def latest_symbol_bar(daily_bar: pd.DataFrame, symbol: str) -> tuple[pd.Series | None, pd.Series | None]:
    scoped = daily_bar.loc[daily_bar["ts_code"] == symbol].copy()
    if scoped.empty:
        return None, None

    scoped = scoped.sort_values("trade_date")
    latest_row = scoped.iloc[-1]
    valid_close = scoped.loc[pd.to_numeric(scoped["close"], errors="coerce").notna()]
    latest_valid = valid_close.iloc[-1] if not valid_close.empty else latest_row
    return latest_row, latest_valid


def resolve_mark_context(item: dict, latest_valid: pd.Series | None) -> dict[str, object]:
    price_source = "最新有效收盘价"
    mark_price = None
    mark_date = None

    manual_mark_price = item.get("manual_mark_price")
    if manual_mark_price not in (None, ""):
        mark_price = float(manual_mark_price)
        mark_date = pd.to_datetime(item.get("manual_mark_date"), errors="coerce")
        price_source = str(item.get("manual_mark_note") or "manual_mark_price")
    elif latest_valid is not None:
        latest_close = pd.to_numeric(latest_valid.get("close"), errors="coerce")
        if pd.notna(latest_close):
            mark_price = float(latest_close)
            mark_date = pd.to_datetime(latest_valid.get("trade_date"), errors="coerce")

    return {
        "mark_price": mark_price,
        "mark_date": mark_date,
        "price_source": price_source,
    }


def watch_level_summary(
    ensemble_rank_pct: float | None,
    is_overlay_selected: bool,
    unrealized_pnl_pct: float | None,
) -> tuple[str, str]:
    if is_overlay_selected:
        return (
            "已进入 AI 精选池",
            "量化排名和叠加筛选都已认可，重点观察强弱延续与兑现节奏。",
        )
    if ensemble_rank_pct is not None and ensemble_rank_pct >= 0.9:
        return (
            "已进入高分观察区，接近 Top30，但尚未进入 AI 精选池",
            "系统排序较强，但仍需看盘中承接和分时结构，不宜直接当成中期反转确认。",
        )
    if ensemble_rank_pct is not None and ensemble_rank_pct >= 0.75:
        return (
            "处于中高分观察区",
            "已有修复迹象，但仍属于观察阶段，更适合等更好的确认或减仓节奏。",
        )
    if unrealized_pnl_pct is not None and unrealized_pnl_pct <= -0.15:
        return (
            "弱势风险观察",
            "系统排序不高且浮亏较深，若明天不能延续强势，风控应优先于幻想反转。",
        )
    return (
        "普通观察",
        "当前更适合按关键价位和承接强弱跟踪，不宜过度预设强趋势。",
    )


def price_levels(mark_price: float | None, cost_basis: float | None) -> dict[str, float | tuple[float, float] | None]:
    if mark_price is None:
        return {
            "anchor_price": None,
            "defensive_price": None,
            "first_observe_price": None,
            "breakeven_zone": None,
            "stretch_price": None,
        }

    anchor_price = round(mark_price, 2)
    defensive_price = round(mark_price * 0.9768, 2)

    if cost_basis is None or cost_basis <= 0:
        return {
            "anchor_price": anchor_price,
            "defensive_price": defensive_price,
            "first_observe_price": round(mark_price * 1.03, 2),
            "breakeven_zone": None,
            "stretch_price": round(mark_price * 1.08, 2),
        }

    gap_to_cost = max(cost_basis - mark_price, 0.0)
    first_observe_price = round(mark_price + max(gap_to_cost * 0.4, mark_price * 0.03), 2)
    breakeven_low = round(max(mark_price, cost_basis - 0.05), 2)
    breakeven_high = round(cost_basis, 2)
    stretch_price = round(max(cost_basis * 1.065, breakeven_high * 1.06), 2)
    return {
        "anchor_price": anchor_price,
        "defensive_price": defensive_price,
        "first_observe_price": first_observe_price,
        "breakeven_zone": (breakeven_low, breakeven_high),
        "stretch_price": stretch_price,
    }


def fmt_price(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}f}"


def fmt_pct(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}%}"


def fmt_date(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return str(pd.Timestamp(value).date())


def build_holding_snapshot(
    item: dict,
    *,
    daily_bar: pd.DataFrame,
    prediction_snapshots: dict[str, pd.DataFrame],
    overlay_symbols: set[str],
    trade_dates: pd.Series | None = None,
) -> dict[str, object]:
    symbol = str(item.get("ts_code", "") or "").strip()
    latest_row, latest_valid = latest_symbol_bar(daily_bar, symbol)
    ensemble_info = latest_prediction_details(prediction_snapshots.get("ensemble", pd.DataFrame()), symbol)
    ridge_info = latest_prediction_details(prediction_snapshots.get("ridge", pd.DataFrame()), symbol)
    lgbm_info = latest_prediction_details(prediction_snapshots.get("lgbm", pd.DataFrame()), symbol)
    mark_context = resolve_mark_context(item, latest_valid)

    cost_basis = float(item.get("cost", 0.0) or 0.0)
    shares = int(item.get("shares", 0) or 0)
    mark_price = mark_context["mark_price"]
    market_value = float(mark_price * shares) if mark_price is not None else None
    unrealized_pnl = float((mark_price - cost_basis) * shares) if mark_price is not None else None
    unrealized_pnl_pct = float(mark_price / cost_basis - 1.0) if mark_price is not None and cost_basis else None
    is_overlay_selected = symbol in overlay_symbols
    ensemble_rank_pct = ensemble_info.get("rank_pct")
    watch_level, action_brief = watch_level_summary(ensemble_rank_pct, is_overlay_selected, unrealized_pnl_pct)
    plan_base_date = mark_context["mark_date"] if mark_context["mark_date"] is not None else ensemble_info.get("signal_date")
    next_day = next_trade_date(trade_dates if trade_dates is not None else pd.Series(dtype="datetime64[ns]"), pd.Timestamp(plan_base_date) if plan_base_date is not None else None)
    plan_date = next_day.date().isoformat() if next_day is not None else fmt_date(plan_base_date)

    latest_bar_date = pd.to_datetime(latest_valid.get("trade_date"), errors="coerce") if latest_valid is not None else pd.NaT
    latest_bar_close = (
        float(pd.to_numeric(latest_valid.get("close"), errors="coerce"))
        if latest_valid is not None and pd.notna(pd.to_numeric(latest_valid.get("close"), errors="coerce"))
        else None
    )

    base_name = str(item.get("name") or "")
    if not base_name:
        if latest_valid is not None and pd.notna(latest_valid.get("name")):
            base_name = str(latest_valid.get("name"))
        elif ensemble_info.get("name"):
            base_name = str(ensemble_info.get("name"))
        else:
            base_name = symbol

    return {
        "ts_code": symbol,
        "name": base_name,
        "industry": (
            ensemble_info.get("industry")
            or ridge_info.get("industry")
            or lgbm_info.get("industry")
            or (str(latest_valid.get("industry") or "") if latest_valid is not None else "")
        ),
        "plan_date": plan_date,
        "cost_basis": cost_basis,
        "shares": shares,
        "mark_price": mark_price,
        "mark_date": mark_context["mark_date"],
        "price_source": mark_context["price_source"],
        "market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "unrealized_pnl_pct": unrealized_pnl_pct,
        "signal_date": ensemble_info.get("signal_date") or ridge_info.get("signal_date") or lgbm_info.get("signal_date"),
        "ensemble_rank": ensemble_info.get("rank"),
        "universe_size": ensemble_info.get("universe_size"),
        "ensemble_rank_pct": ensemble_info.get("rank_pct"),
        "ridge_rank_pct": ridge_info.get("rank_pct"),
        "lgbm_rank_pct": lgbm_info.get("rank_pct"),
        "score": ensemble_info.get("score"),
        "score_raw": ensemble_info.get("score_raw"),
        "watch_level": watch_level,
        "action_brief": action_brief,
        "levels": price_levels(mark_price, cost_basis),
        "mom_5": ensemble_info.get("mom_5"),
        "mom_20": ensemble_info.get("mom_20"),
        "mom_60": ensemble_info.get("mom_60"),
        "close_to_ma_20": ensemble_info.get("close_to_ma_20"),
        "close_to_ma_60": ensemble_info.get("close_to_ma_60"),
        "drawdown_60": ensemble_info.get("drawdown_60"),
        "pct_chg": latest_valid.get("pct_chg") if latest_valid is not None else None,
        "latest_bar_date": latest_bar_date,
        "latest_bar_close": latest_bar_close,
        "is_manual_mark": item.get("manual_mark_price") not in (None, ""),
        "is_overlay_selected": is_overlay_selected,
        "manual_mark_note": item.get("manual_mark_note", ""),
        "latest_row": latest_row,
        "latest_valid_row": latest_valid,
    }


# Override polluted UI-facing copy with clean strings.
def resolve_mark_context(item: dict, latest_valid: pd.Series | None) -> dict[str, object]:
    price_source = "最新有效收盘价"
    mark_price = None
    mark_date = None

    manual_mark_price = item.get("manual_mark_price")
    if manual_mark_price not in (None, ""):
        mark_price = float(manual_mark_price)
        mark_date = pd.to_datetime(item.get("manual_mark_date"), errors="coerce")
        price_source = str(item.get("manual_mark_note") or "手工参考价")
    elif latest_valid is not None:
        latest_close = pd.to_numeric(latest_valid.get("close"), errors="coerce")
        if pd.notna(latest_close):
            mark_price = float(latest_close)
            mark_date = pd.to_datetime(latest_valid.get("trade_date"), errors="coerce")

    return {
        "mark_price": mark_price,
        "mark_date": mark_date,
        "price_source": price_source,
    }


def watch_level_summary(
    ensemble_rank_pct: float | None,
    is_overlay_selected: bool,
    unrealized_pnl_pct: float | None,
) -> tuple[str, str]:
    if is_overlay_selected:
        return (
            "已进入 AI 精选池",
            "量化排名和叠加筛选都已认可，重点观察强弱延续与兑现节奏。",
        )
    if ensemble_rank_pct is not None and ensemble_rank_pct >= 0.9:
        return (
            "已进入高分观察区，接近 Top30，但尚未进入 AI 精选池",
            "系统排序较强，但仍需看盘中承接和分时结构，不宜直接当成中期反转确认。",
        )
    if ensemble_rank_pct is not None and ensemble_rank_pct >= 0.75:
        return (
            "处于中高分观察区",
            "已有修复迹象，但仍属于观察阶段，更适合等更好的确认或减仓节奏。",
        )
    if unrealized_pnl_pct is not None and unrealized_pnl_pct <= -0.15:
        return (
            "弱势风险观察",
            "系统排序不高且浮亏较深，如果明天不能延续强势，风控应优先于幻想反转。",
        )
    return (
        "普通观察",
        "当前更适合按关键价位和承接强弱跟踪，不宜过度预设强趋势。",
    )
