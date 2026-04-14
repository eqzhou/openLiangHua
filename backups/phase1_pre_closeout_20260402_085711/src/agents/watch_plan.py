from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pandas.tseries.offsets import BDay

from src.app.services.holding_snapshot_service import (
    build_holding_snapshot as build_shared_holding_snapshot,
    load_overlay_symbols as load_shared_overlay_symbols,
    load_prediction_snapshots as load_shared_prediction_snapshots,
    load_trade_dates as load_shared_trade_dates,
    read_daily_bar as read_shared_daily_bar,
    resolve_data_source as resolve_shared_data_source,
)
from src.utils.data_source import normalize_data_source, source_or_canonical_path
from src.utils.io import ensure_dir, load_yaml, project_root, save_text
from src.utils.logger import configure_logging

logger = configure_logging()

PREDICTION_CHUNK_SIZE = 200_000
WATCH_PLAN_FACTOR_COLUMNS = [
    "mom_5",
    "mom_20",
    "mom_60",
    "close_to_ma_20",
    "close_to_ma_60",
    "drawdown_60",
]


def _resolve_data_source(root: Path) -> str:
    universe = load_yaml(root / "config" / "universe.yaml")
    return normalize_data_source(universe.get("data_source", "akshare"))


def _read_prediction_latest_snapshot(path: Path, usecols: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=usecols)

    available_columns = pd.read_csv(path, nrows=0).columns.tolist()
    scoped_columns = [column for column in usecols if column in available_columns]
    required_columns = {"trade_date", "ts_code", "score"}
    if not required_columns.issubset(scoped_columns):
        return pd.DataFrame(columns=usecols)

    latest_date: pd.Timestamp | None = None
    latest_frames: list[pd.DataFrame] = []

    for chunk in pd.read_csv(path, usecols=scoped_columns, chunksize=PREDICTION_CHUNK_SIZE):
        chunk["trade_date"] = pd.to_datetime(chunk["trade_date"], errors="coerce")
        chunk = chunk.loc[chunk["trade_date"].notna()].copy()
        if chunk.empty:
            continue

        chunk_max = chunk["trade_date"].max()
        chunk_latest = chunk.loc[chunk["trade_date"] == chunk_max].copy()

        if latest_date is None or chunk_max > latest_date:
            latest_date = chunk_max
            latest_frames = [chunk_latest]
        elif chunk_max == latest_date:
            latest_frames.append(chunk_latest)

    if latest_date is None or not latest_frames:
        return pd.DataFrame(columns=usecols)

    snapshot = pd.concat(latest_frames, ignore_index=True)
    for column in usecols:
        if column not in snapshot.columns:
            snapshot[column] = pd.NA
    snapshot = snapshot[usecols]
    snapshot = snapshot.sort_values("score", ascending=False).reset_index(drop=True)
    snapshot["rank"] = snapshot.index + 1
    snapshot["rank_pct"] = snapshot["score"].rank(pct=True, ascending=True)
    return snapshot


def _latest_prediction_details(snapshot: pd.DataFrame, symbol: str) -> dict[str, object]:
    if snapshot.empty:
        return {}

    row = snapshot.loc[snapshot["ts_code"] == symbol]
    if row.empty:
        return {}

    record = row.iloc[0].to_dict()
    record["signal_date"] = snapshot["trade_date"].iloc[0]
    record["universe_size"] = int(len(snapshot))
    return record


def _load_prediction_snapshots(root: Path, data_source: str) -> dict[str, pd.DataFrame]:
    reports_dir = root / "reports" / "weekly"
    base_cols = ["trade_date", "ts_code", "name", "score"]
    snapshots = {
        "ridge": _read_prediction_latest_snapshot(
            source_or_canonical_path(reports_dir, "ridge_test_predictions.csv", data_source),
            usecols=base_cols,
        ),
        "lgbm": _read_prediction_latest_snapshot(
            source_or_canonical_path(reports_dir, "lgbm_test_predictions.csv", data_source),
            usecols=base_cols,
        ),
        "ensemble": _read_prediction_latest_snapshot(
            source_or_canonical_path(reports_dir, "ensemble_test_predictions.csv", data_source),
            usecols=base_cols + WATCH_PLAN_FACTOR_COLUMNS,
        ),
    }
    return snapshots


def _load_overlay_symbols(root: Path, data_source: str) -> set[str]:
    reports_dir = root / "reports" / "weekly"
    path = source_or_canonical_path(reports_dir, "overlay_latest_candidates.csv", data_source)
    if not path.exists():
        return set()
    frame = pd.read_csv(path, usecols=["ts_code"])
    return set(frame["ts_code"].astype(str))


def _read_daily_bar(root: Path, data_source: str, symbols: list[str]) -> pd.DataFrame:
    path = source_or_canonical_path(root / "data" / "staging", "daily_bar.parquet", data_source)
    if not path.exists() or not symbols:
        return pd.DataFrame()

    columns = ["trade_date", "ts_code", "name", "close", "open", "high", "low", "pct_chg", "amount", "industry"]
    table = pq.read_table(path, columns=columns, filters=[("ts_code", "in", symbols)])
    frame = table.to_pandas()
    if frame.empty:
        return frame
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)


def _load_trade_dates(root: Path, data_source: str) -> pd.Series:
    paths: list[Path] = []
    source_path = source_or_canonical_path(root / "data" / "staging", "trade_calendar.parquet", data_source)
    if source_path.exists():
        paths.append(source_path)

    canonical_path = root / "data" / "staging" / "trade_calendar.parquet"
    if canonical_path.exists() and canonical_path not in paths:
        paths.append(canonical_path)

    frames: list[pd.DataFrame] = []
    for path in paths:
        frame = pd.read_parquet(path)
        if "trade_date" not in frame.columns:
            continue
        frames.append(frame[["trade_date"]].copy())

    if not frames:
        return pd.Series(dtype="datetime64[ns]")

    combined = pd.concat(frames, ignore_index=True)
    combined["trade_date"] = pd.to_datetime(combined["trade_date"], errors="coerce")
    combined = combined.loc[combined["trade_date"].notna()].drop_duplicates().sort_values("trade_date")
    return combined["trade_date"].reset_index(drop=True)


def _next_trade_date(trade_dates: pd.Series, as_of: pd.Timestamp | None) -> pd.Timestamp | None:
    if as_of is None or trade_dates.empty:
        return None

    future_dates = trade_dates.loc[trade_dates > as_of.normalize()]
    if future_dates.empty:
        return pd.Timestamp(as_of.normalize() + BDay(1))
    return pd.Timestamp(future_dates.iloc[0])


def _latest_symbol_bar(daily_bar: pd.DataFrame, symbol: str) -> tuple[pd.Series | None, pd.Series | None]:
    scoped = daily_bar.loc[daily_bar["ts_code"] == symbol].copy()
    if scoped.empty:
        return None, None

    scoped = scoped.sort_values("trade_date")
    latest_row = scoped.iloc[-1]
    valid_close = scoped.loc[pd.to_numeric(scoped["close"], errors="coerce").notna()]
    latest_valid = valid_close.iloc[-1] if not valid_close.empty else latest_row
    return latest_row, latest_valid


def _resolve_mark_context(item: dict, latest_valid: pd.Series | None) -> dict[str, object]:
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


def _watch_level_summary(ensemble_rank_pct: float | None, is_overlay_selected: bool, unrealized_pnl_pct: float | None) -> tuple[str, str]:
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


def _price_levels(mark_price: float | None, cost_basis: float | None) -> dict[str, float | tuple[float, float] | None]:
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


def _fmt_price(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}f}"


def _fmt_pct(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}%}"


def _fmt_date(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return str(pd.Timestamp(value).date())


def _compose_watch_plan(snapshot: dict[str, object]) -> str:
    levels = snapshot["levels"]
    breakeven_zone = levels.get("breakeven_zone")
    breakeven_text = (
        f"{_fmt_price(breakeven_zone[0])}-{_fmt_price(breakeven_zone[1])}" if isinstance(breakeven_zone, tuple) else "-"
    )
    factor_notes: list[str] = []
    if pd.notna(snapshot.get("mom_20")):
        factor_notes.append(f"- 20日动量：{_fmt_pct(snapshot.get('mom_20'))}")
    if pd.notna(snapshot.get("mom_60")):
        factor_notes.append(f"- 60日动量：{_fmt_pct(snapshot.get('mom_60'))}")
    if pd.notna(snapshot.get("close_to_ma_20")):
        factor_notes.append(f"- 相对20日线：{_fmt_pct(snapshot.get('close_to_ma_20'))}")
    if pd.notna(snapshot.get("close_to_ma_60")):
        factor_notes.append(f"- 相对60日线：{_fmt_pct(snapshot.get('close_to_ma_60'))}")
    if pd.notna(snapshot.get("drawdown_60")):
        factor_notes.append(f"- 近60日回撤：{_fmt_pct(snapshot.get('drawdown_60'))}")
    factor_section = factor_notes if factor_notes else ["- 当前没有可用的因子快照。"]

    lines = [
        f"# {snapshot['name']}盘中盯盘清单",
        "",
        f"日期：{snapshot['plan_date']}",
        f"股票：{snapshot['ts_code']} {snapshot['name']}",
        "",
        "## 持仓信息",
        "",
        f"- 成本价：{_fmt_price(snapshot.get('cost_basis'), digits=3)}",
        f"- 持股数量：{int(snapshot.get('shares') or 0)}",
        f"- 参考价格：{_fmt_price(snapshot.get('mark_price'))}",
        f"- 参考价格日期：{_fmt_date(snapshot.get('mark_date'))}",
        f"- 价格来源：{snapshot.get('price_source', '-')}",
        f"- 参考浮动盈亏：{int(round(float(snapshot.get('unrealized_pnl') or 0.0))):,} 元" if snapshot.get("unrealized_pnl") is not None else "- 参考浮动盈亏：-",
        f"- 参考浮亏比例：{_fmt_pct(snapshot.get('unrealized_pnl_pct'))}",
        "",
        "## 量化位置",
        "",
        f"- 融合策略最近信号日：{_fmt_date(snapshot.get('signal_date'))}",
        f"- 融合策略排名：{snapshot.get('ensemble_rank', '-')}"
        + (f" / {snapshot.get('universe_size', '-')}" if snapshot.get("ensemble_rank") is not None else ""),
        f"- 融合策略分位：{_fmt_pct(snapshot.get('ensemble_rank_pct'))}",
        f"- 岭回归分位：{_fmt_pct(snapshot.get('ridge_rank_pct'))}",
        f"- 梯度提升树分位：{_fmt_pct(snapshot.get('lgbm_rank_pct'))}",
        f"- 当前状态：{snapshot['watch_level']}",
        "",
        "## 当前技术侧备注",
        "",
        *factor_section,
        "",
        "## 关键价位",
        "",
        f"- {_fmt_price(levels.get('anchor_price'))}：参考价/强弱分界",
        f"- {_fmt_price(levels.get('defensive_price'))}：近端防守位，跌回其下说明强度明显减弱",
        f"- {_fmt_price(levels.get('first_observe_price'))}：延续上攻的第一观察位",
        f"- {breakeven_text}：解套区和核心压力区",
        f"- {_fmt_price(levels.get('stretch_price'))}：明显超预期时才看这一档",
        "",
        "## 盘中应对表",
        "",
        "### 1. 强势剧本",
        "",
        "条件：",
        "",
        f"- 高开后围绕 {_fmt_price(levels.get('anchor_price'))} 上方震荡",
        "- 冲高回落后仍能快速收回",
        "- 成交放大但不是单边砸盘",
        "",
        "观察重点：",
        "",
        f"- 是否始终守住 {_fmt_price(levels.get('defensive_price'))} 以上",
        "- 分时回封/回拉速度是否快",
        "- 尾盘是否还能维持主动承接",
        "",
        "含义：",
        "",
        f"- 有望继续向 {_fmt_price(levels.get('first_observe_price'))} 推进",
        f"- 若情绪继续发酵，才有机会挑战 {breakeven_text}",
        "",
        "### 2. 中性剧本",
        "",
        "条件：",
        "",
        "- 冲高但反复开板/回落",
        f"- 回落后仍能守住 {_fmt_price(levels.get('defensive_price'))} 附近",
        "",
        "观察重点：",
        "",
        "- 是否出现放量换手而不是直接跳水",
        f"- 午后能否重新回到 {_fmt_price(levels.get('anchor_price'))} 上方",
        "",
        "含义：",
        "",
        "- 更像修复行情延续",
        "- 仍然有靠近解套区的机会，但节奏会反复",
        "",
        "### 3. 偏弱剧本",
        "",
        "条件：",
        "",
        "- 高开低走",
        f"- 跌回 {_fmt_price(levels.get('defensive_price'))} 下方且站不回去",
        "- 尾盘承接明显走弱",
        "",
        "观察重点：",
        "",
        "- 是否放量下杀",
        "- 是否跌破日内关键均价",
        "",
        "含义：",
        "",
        "- 更像情绪脉冲后的兑现",
        "- 这时不要把它当成中期趋势反转票",
        "",
        "## 当前一句话判断",
        "",
        str(snapshot["action_brief"]),
    ]

    if isinstance(breakeven_zone, tuple):
        lines.extend(
            [
                "",
                "## 持仓视角",
                "",
                f"- {_fmt_price(levels.get('first_observe_price'))} 时，参考浮亏约 {(float(levels['first_observe_price']) - float(snapshot.get('cost_basis') or 0.0)) * int(snapshot.get('shares') or 0):,.0f} 元",
                f"- {breakeven_text} 区域属于接近回本/解套区",
                f"- {_fmt_price(levels.get('stretch_price'))} 时，参考浮盈约 {(float(levels['stretch_price']) - float(snapshot.get('cost_basis') or 0.0)) * int(snapshot.get('shares') or 0):,.0f} 元",
            ]
        )

    if snapshot.get("signal_date") and snapshot.get("mark_date"):
        lines.extend(
            [
                "",
                "## 使用说明",
                "",
                "- 本清单基于最新保存的量化信号和当前持仓参考价自动生成。",
                "- 量化信号日期可能早于参考价格日期，这是因为当前标签周期使用 T+1 持有期回报，最新信号会滞后于原始行情数据。",
            ]
        )

    return "\n".join(lines).strip() + "\n"


def _build_holding_snapshot(
    item: dict,
    daily_bar: pd.DataFrame,
    prediction_snapshots: dict[str, pd.DataFrame],
    overlay_symbols: set[str],
    trade_dates: pd.Series,
) -> dict[str, object]:
    symbol = str(item.get("ts_code", "") or "").strip()
    latest_row, latest_valid = _latest_symbol_bar(daily_bar, symbol)
    ensemble_info = _latest_prediction_details(prediction_snapshots.get("ensemble", pd.DataFrame()), symbol)
    ridge_info = _latest_prediction_details(prediction_snapshots.get("ridge", pd.DataFrame()), symbol)
    lgbm_info = _latest_prediction_details(prediction_snapshots.get("lgbm", pd.DataFrame()), symbol)
    mark_context = _resolve_mark_context(item, latest_valid)

    cost_basis = float(item.get("cost", 0.0) or 0.0)
    shares = int(item.get("shares", 0) or 0)
    mark_price = mark_context["mark_price"]
    market_value = float(mark_price * shares) if mark_price is not None else None
    unrealized_pnl = float((mark_price - cost_basis) * shares) if mark_price is not None else None
    unrealized_pnl_pct = float(mark_price / cost_basis - 1.0) if mark_price is not None and cost_basis else None
    is_overlay_selected = symbol in overlay_symbols
    ensemble_rank_pct = ensemble_info.get("rank_pct")
    watch_level, action_brief = _watch_level_summary(ensemble_rank_pct, is_overlay_selected, unrealized_pnl_pct)
    plan_base_date = mark_context["mark_date"] if mark_context["mark_date"] is not None else ensemble_info.get("signal_date")
    next_trade_day = _next_trade_date(trade_dates, pd.Timestamp(plan_base_date) if plan_base_date is not None else None)
    plan_date = next_trade_day.date().isoformat() if next_trade_day is not None else _fmt_date(plan_base_date)

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
        "watch_level": watch_level,
        "action_brief": action_brief,
        "levels": _price_levels(mark_price, cost_basis),
        "mom_5": ensemble_info.get("mom_5"),
        "mom_20": ensemble_info.get("mom_20"),
        "mom_60": ensemble_info.get("mom_60"),
        "close_to_ma_20": ensemble_info.get("close_to_ma_20"),
        "close_to_ma_60": ensemble_info.get("close_to_ma_60"),
        "drawdown_60": ensemble_info.get("drawdown_60"),
        "is_overlay_selected": is_overlay_selected,
    }


def generate_watch_plans(root: Path | None = None) -> list[Path]:
    resolved_root = root or project_root()
    watchlist = load_yaml(resolved_root / "config" / "watchlist.yaml").get("holdings", []) or []
    if not watchlist:
        logger.info("No holdings were found in config/watchlist.yaml.")
        return []

    data_source = resolve_shared_data_source(resolved_root)
    symbols = [str(item.get("ts_code", "") or "").strip() for item in watchlist if str(item.get("ts_code", "") or "").strip()]
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    daily_bar = read_shared_daily_bar(resolved_root, data_source, symbols=symbols)
    trade_dates = load_shared_trade_dates(resolved_root, data_source)
    prediction_snapshots = load_shared_prediction_snapshots(resolved_root, data_source)
    overlay_symbols = load_shared_overlay_symbols(resolved_root, data_source)

    generated_paths: list[Path] = []
    for item in watchlist:
        symbol = str(item.get("ts_code", "") or "").strip()
        if not symbol:
            continue

        snapshot = build_shared_holding_snapshot(
            item=item,
            daily_bar=daily_bar,
            prediction_snapshots=prediction_snapshots,
            overlay_symbols=overlay_symbols,
            trade_dates=trade_dates,
        )
        plan_date = snapshot.get("plan_date") or pd.Timestamp.today().date().isoformat()
        base_code = symbol.split(".")[0]
        output_path = reports_dir / f"{base_code}_watch_plan_{plan_date}.md"
        save_text(_compose_watch_plan(snapshot), output_path)
        generated_paths.append(output_path)
        logger.info(f"Generated watch plan: {output_path}")

    return generated_paths


def run() -> None:
    generated_paths = generate_watch_plans()
    if not generated_paths:
        print("No watch plans generated.")
        return
    for path in generated_paths:
        print(path)


if __name__ == "__main__":
    run()
