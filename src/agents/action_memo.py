from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from src.app.repositories.config_repository import load_watchlist_config
from src.app.repositories.report_repository import save_symbol_note as repo_save_symbol_note
from src.app.services.holding_snapshot_service import (
    build_holding_snapshot,
    fmt_date,
    fmt_pct,
    fmt_price,
    load_overlay_symbols,
    load_prediction_snapshots,
    load_trade_dates,
    read_daily_bar,
    resolve_data_source,
)
from src.utils.holding_marks import describe_price_reference
from src.utils.io import project_root
from src.utils.llm_discussion import load_symbol_discussion_snapshot
from src.utils.logger import configure_logging
from src.utils.premarket_plan import build_premarket_plan

logger = configure_logging()


def _positioning_view(snapshot: dict[str, object]) -> list[str]:
    lines = [
        f"- 融合策略最近信号日：{fmt_date(snapshot.get('signal_date'))}",
        f"- 融合策略排名：{snapshot.get('ensemble_rank', '-')}" + (f" / {snapshot.get('universe_size', '-')}" if snapshot.get("ensemble_rank") is not None else ""),
        f"- 融合策略分位：{fmt_pct(snapshot.get('ensemble_rank_pct'))}",
        f"- 岭回归分位：{fmt_pct(snapshot.get('ridge_rank_pct'))}",
        f"- 梯度提升树分位：{fmt_pct(snapshot.get('lgbm_rank_pct'))}",
        f"- AI 精选池：{'是' if snapshot.get('is_overlay_selected') else '否'}",
        f"- 当前状态：{snapshot.get('watch_level', '-')}",
    ]
    return lines


def _factor_view(snapshot: dict[str, object]) -> list[str]:
    labels = [
        ("mom_5", "5日动量"),
        ("mom_20", "20日动量"),
        ("mom_60", "60日动量"),
        ("close_to_ma_20", "相对20日线"),
        ("close_to_ma_60", "相对60日线"),
        ("drawdown_60", "近60日回撤"),
    ]
    notes = [f"- {label}：{fmt_pct(snapshot.get(key))}" for key, label in labels if pd.notna(snapshot.get(key))]
    return notes or ["- 当前没有可用的因子快照。"]


def _price_levels_view(snapshot: dict[str, object]) -> list[str]:
    levels = snapshot["levels"]
    breakeven_zone = levels.get("breakeven_zone")
    breakeven_text = (
        f"{fmt_price(breakeven_zone[0])}-{fmt_price(breakeven_zone[1])}" if isinstance(breakeven_zone, tuple) else "-"
    )
    return [
        f"- {fmt_price(levels.get('anchor_price'))}：参考价 / 强弱分界",
        f"- {fmt_price(levels.get('defensive_price'))}：近端防守位",
        f"- {fmt_price(levels.get('first_observe_price'))}：第一观察位",
        f"- {breakeven_text}：解套区和核心压力区",
        f"- {fmt_price(levels.get('stretch_price'))}：超预期目标位",
    ]


def _memo_conclusion(snapshot: dict[str, object]) -> str:
    if snapshot.get("is_overlay_selected"):
        return "当前持仓既有量化排序支持，也进入了 AI 精选池，更适合顺着强弱确认去处理，不要在第一笔冲高时过度主观。"
    ensemble_rank_pct = snapshot.get("ensemble_rank_pct")
    if ensemble_rank_pct is not None and float(ensemble_rank_pct) >= 0.9:
        return "当前更像高分修复观察标的，适合围绕关键价位执行，不适合把一次强修复直接理解成中期主升确认。"
    if ensemble_rank_pct is not None and float(ensemble_rank_pct) >= 0.75:
        return "当前属于中高分观察，但没到最强区，更适合等待确认或优化减仓节奏。"
    return "当前系统排序不强，执行上应该优先看风控和承接，不宜给过高预期。"


def _memo_scenarios(snapshot: dict[str, object]) -> list[str]:
    levels = snapshot["levels"]
    first_price = fmt_price(levels.get("first_observe_price"))
    defensive_price = fmt_price(levels.get("defensive_price"))
    anchor_price = fmt_price(levels.get("anchor_price"))
    breakeven_zone = levels.get("breakeven_zone")
    breakeven_text = (
        f"{fmt_price(breakeven_zone[0])}-{fmt_price(breakeven_zone[1])}" if isinstance(breakeven_zone, tuple) else "-"
    )
    return [
        "### 1. 强势剧本",
        "",
        f"- 开盘后能继续围绕 {anchor_price} 上方运行",
        "- 冲高回落后仍能快速收回",
        "- 量能放大但不是单边砸盘",
        "",
        f"- 先看是否有继续向 {first_price} 推进的能力",
        f"- 如果已经接近 {breakeven_text}，更适合主动优化卖点，而不是继续抬高预期",
        "",
        "### 2. 中性剧本",
        "",
        f"- 冲高后反复开板或回落，但大部分时间还能守住 {defensive_price}",
        "- 午后仍有反抽，但没有形成持续加速",
        "",
        "- 这更像修复行情延续，不代表趋势已经彻底反转",
        f"- 如果始终过不去 {first_price}，可以把它当成优化仓位结构的窗口",
        "",
        "### 3. 偏弱剧本",
        "",
        f"- 高开低走，跌回 {defensive_price} 下方且站不回去",
        "- 尾盘承接明显走弱，或回封速度明显变慢",
        "",
        "- 这时不要再把它当成强修复延续",
        "- 如果分时持续弱，风控优先级要明显抬高",
    ]


def _discussion_view(snapshot: dict[str, object]) -> list[str]:
    discussion_snapshot = snapshot.get("llm_discussion_snapshot") or {}
    rounds = discussion_snapshot.get("rounds", []) or []
    if not rounds:
        return ["- 当前还没有可用的外部模型研讨产物。"]

    lines = [f"- 总览：{discussion_snapshot.get('overview', '-')}"]
    for round_info in rounds:
        lines.append(
            f"- {round_info.get('round_label', '-')}"
            f" | 截面：{round_info.get('latest_date', '-')}"
            f" | 状态：{round_info.get('display_status', '-')}"
        )
        summary_text = str(round_info.get("summary_preview", "") or "").strip()
        if summary_text:
            lines.append(f"  结论：{summary_text}")
    return lines


def _compose_action_memo(snapshot: dict[str, object]) -> str:
    latest_bar_date = snapshot.get("latest_bar_date")
    price_reference = describe_price_reference(
        is_manual_mark=bool(snapshot.get("is_manual_mark")),
        mark_date=snapshot.get("mark_date"),
        latest_bar_date=latest_bar_date,
    )
    unrealized_line = (
        f"- 参考浮盈亏：约 `{int(round(float(snapshot.get('unrealized_pnl') or 0.0))):,} 元`"
        if snapshot.get("unrealized_pnl") is not None
        else "- 参考浮盈亏：`-`"
    )

    lines = [
        f"# {snapshot['ts_code']} {snapshot['name']} {snapshot['plan_date']} 操作备忘",
        "",
        "## 1. 用途说明",
        "",
        "这是一份根据本地量化结果和持仓参考价自动生成的盘前执行备忘，不是自动交易指令。",
        "真正执行前，仍需以盘中价格、量能、盘口承接和最新公告为准。",
        "",
        "## 2. 当前持仓快照",
        "",
        f"- 股票：`{snapshot['ts_code']} {snapshot['name']}`",
        f"- 持仓成本：`{fmt_price(snapshot.get('cost_basis'), digits=3)}`",
        f"- 持股数量：`{int(snapshot.get('shares') or 0)}`",
        f"- 当前参考价：`{fmt_price(snapshot.get('mark_price'))}`",
        f"- 参考日期：`{fmt_date(snapshot.get('mark_date'))}`",
        f"- 价格来源：`{snapshot.get('price_source', '-')}`",
        f"- 价格状态：`{price_reference.get('mark_status', '-')}`",
        f"- 最新落库日线日期：`{fmt_date(latest_bar_date)}`",
        f"- 最新落库收盘价：`{fmt_price(snapshot.get('latest_bar_close'))}`",
        unrealized_line,
        f"- 参考浮盈比例：约 `{fmt_pct(snapshot.get('unrealized_pnl_pct'))}`",
        "",
        "说明：",
        "",
        f"- {price_reference.get('mark_status_note', '当前持仓估值使用最新参考价。')}",
        "",
        "## 3. 项目内量化结论",
        "",
        "### 3.1 最新量化位置",
        "",
        *_positioning_view(snapshot),
        "",
        "### 3.2 最新因子状态",
        "",
        *_factor_view(snapshot),
        "",
        "### 3.3 当前一句话判断",
        "",
        str(snapshot.get("premarket_plan") or snapshot.get("action_brief") or _memo_conclusion(snapshot)),
        "",
        "## 4. 多轮 AI 研讨回写",
        "",
        *_discussion_view(snapshot),
        "",
        "## 5. 关键价位",
        "",
        *_price_levels_view(snapshot),
        "",
        "## 6. 明日执行剧本",
        "",
        *_memo_scenarios(snapshot),
        "",
        "## 7. 结论",
        "",
        _memo_conclusion(snapshot),
        "",
        "## 8. 盘前执行清单",
        "",
        "- 先确认是否有新的公司公告、监管问询或板块级突发消息。",
        "- 盘中优先看强弱分界和防守位，不要只看第一笔冲高。",
        "- 如果是手工参考价，记得把它和最新落库日线收盘价区分开来看。",
    ]
    return "\n".join(lines).strip() + "\n"


def _build_action_snapshot(
    item: dict,
    root: Path,
    data_source: str,
    daily_bar: pd.DataFrame,
    prediction_snapshots: dict[str, pd.DataFrame],
    overlay_symbols: set[str],
    trade_dates: pd.Series,
) -> dict[str, object]:
    snapshot = build_holding_snapshot(
        item=item,
        daily_bar=daily_bar,
        prediction_snapshots=prediction_snapshots,
        overlay_symbols=overlay_symbols,
        trade_dates=trade_dates,
    )
    symbol = str(item.get("ts_code", "") or "").strip()
    snapshot["llm_discussion_snapshot"] = load_symbol_discussion_snapshot(
        root=root,
        data_source=data_source,
        symbol=symbol,
    )
    premarket_plan_payload = build_premarket_plan(
        discussion_snapshot=snapshot["llm_discussion_snapshot"],
        action_brief=str(snapshot.get("action_brief", "") or ""),
        anchor_price=float(snapshot["levels"].get("anchor_price")) if snapshot.get("levels", {}).get("anchor_price") is not None else None,
        defensive_price=float(snapshot["levels"].get("defensive_price")) if snapshot.get("levels", {}).get("defensive_price") is not None else None,
        breakeven_price=float(snapshot.get("cost_basis") or 0.0) if snapshot.get("cost_basis") else None,
    )
    snapshot.update(premarket_plan_payload)
    return snapshot


def generate_action_memos(root: Path | None = None, *, user_id: str | None = None) -> list[Path]:
    resolved_root = root or project_root()
    watchlist = load_watchlist_config(resolved_root, prefer_database=True, user_id=user_id).get("holdings", []) or []
    if not watchlist:
        logger.info("No holdings were found in the database watchlist configuration.")
        return []

    data_source = resolve_data_source(resolved_root)
    symbols = [str(item.get("ts_code", "") or "").strip() for item in watchlist if str(item.get("ts_code", "") or "").strip()]
    daily_bar = read_daily_bar(resolved_root, data_source, symbols=symbols)
    trade_dates = load_trade_dates(resolved_root, data_source)
    prediction_snapshots = load_prediction_snapshots(resolved_root, data_source)
    overlay_symbols = load_overlay_symbols(resolved_root, data_source)

    generated_paths: list[Path] = []
    for item in watchlist:
        symbol = str(item.get("ts_code", "") or "").strip()
        if not symbol:
            continue

        snapshot = _build_action_snapshot(
            item=item,
            root=resolved_root,
            data_source=data_source,
            daily_bar=daily_bar,
            prediction_snapshots=prediction_snapshots,
            overlay_symbols=overlay_symbols,
            trade_dates=trade_dates,
        )
        plan_date = snapshot.get("plan_date") or pd.Timestamp.today().date().isoformat()
        output_path = repo_save_symbol_note(
            resolved_root,
            data_source=data_source,
            symbol=symbol,
            note_kind="action_memo",
            plan_date=str(plan_date),
            content=_compose_action_memo(snapshot),
            user_id=user_id,
        )
        generated_paths.append(output_path)
        logger.info(f"Generated action memo: {output_path}")

    return generated_paths


def run() -> None:
    user_id = os.environ.get("OPENLIANGHUA_USER_ID") or None
    generated_paths = generate_action_memos(user_id=user_id)
    if not generated_paths:
        print("No action memos generated.")
        return
    from src.db.dashboard_sync import sync_dashboard_artifacts

    summary = sync_dashboard_artifacts()
    for path in generated_paths:
        print(path)
    print(summary.message)


if __name__ == "__main__":
    run()
