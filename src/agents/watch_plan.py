from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.app.services.holding_snapshot_service import (
    build_holding_snapshot,
    fmt_date,
    fmt_pct,
    fmt_price,
    load_overlay_symbols,
    load_prediction_snapshots,
    load_trade_dates,
    next_trade_date as shared_next_trade_date,
    read_daily_bar,
    resolve_data_source,
)
from src.utils.io import ensure_dir, load_yaml, project_root, save_text
from src.utils.logger import configure_logging

logger = configure_logging()


def _next_trade_date(trade_dates: pd.Series, as_of: pd.Timestamp | None) -> pd.Timestamp | None:
    return shared_next_trade_date(trade_dates, as_of)


def _compose_watch_plan(snapshot: dict[str, object]) -> str:
    levels = snapshot["levels"]
    breakeven_zone = levels.get("breakeven_zone")
    breakeven_text = (
        f"{fmt_price(breakeven_zone[0])}-{fmt_price(breakeven_zone[1])}" if isinstance(breakeven_zone, tuple) else "-"
    )

    factor_notes: list[str] = []
    if pd.notna(snapshot.get("mom_20")):
        factor_notes.append(f"- 20日动量：{fmt_pct(snapshot.get('mom_20'))}")
    if pd.notna(snapshot.get("mom_60")):
        factor_notes.append(f"- 60日动量：{fmt_pct(snapshot.get('mom_60'))}")
    if pd.notna(snapshot.get("close_to_ma_20")):
        factor_notes.append(f"- 相对20日线：{fmt_pct(snapshot.get('close_to_ma_20'))}")
    if pd.notna(snapshot.get("close_to_ma_60")):
        factor_notes.append(f"- 相对60日线：{fmt_pct(snapshot.get('close_to_ma_60'))}")
    if pd.notna(snapshot.get("drawdown_60")):
        factor_notes.append(f"- 近60日回撤：{fmt_pct(snapshot.get('drawdown_60'))}")
    if not factor_notes:
        factor_notes = ["- 当前没有可用的因子快照。"]

    unrealized_line = (
        f"- 参考浮动盈亏：{int(round(float(snapshot.get('unrealized_pnl') or 0.0))):,} 元"
        if snapshot.get("unrealized_pnl") is not None
        else "- 参考浮动盈亏：-"
    )

    lines = [
        f"# {snapshot['name']} 盘中盯盘清单",
        "",
        f"日期：{snapshot['plan_date']}",
        f"股票：{snapshot['ts_code']} {snapshot['name']}",
        "",
        "## 持仓信息",
        "",
        f"- 成本价：{fmt_price(snapshot.get('cost_basis'), digits=3)}",
        f"- 持股数量：{int(snapshot.get('shares') or 0)}",
        f"- 参考价格：{fmt_price(snapshot.get('mark_price'))}",
        f"- 参考价格日期：{fmt_date(snapshot.get('mark_date'))}",
        f"- 价格来源：{snapshot.get('price_source', '-')}",
        unrealized_line,
        f"- 参考浮盈比例：{fmt_pct(snapshot.get('unrealized_pnl_pct'))}",
        "",
        "## 量化位置",
        "",
        f"- 融合策略最近信号日：{fmt_date(snapshot.get('signal_date'))}",
        f"- 融合策略排名：{snapshot.get('ensemble_rank', '-')}" + (f" / {snapshot.get('universe_size', '-')}" if snapshot.get("ensemble_rank") is not None else ""),
        f"- 融合策略分位：{fmt_pct(snapshot.get('ensemble_rank_pct'))}",
        f"- 岭回归分位：{fmt_pct(snapshot.get('ridge_rank_pct'))}",
        f"- 梯度提升树分位：{fmt_pct(snapshot.get('lgbm_rank_pct'))}",
        f"- 当前状态：{snapshot['watch_level']}",
        "",
        "## 当前技术侧备注",
        "",
        *factor_notes,
        "",
        "## 关键价位",
        "",
        f"- {fmt_price(levels.get('anchor_price'))}：参考价 / 强弱分界",
        f"- {fmt_price(levels.get('defensive_price'))}：近端防守位，跌回其下说明强度明显减弱",
        f"- {fmt_price(levels.get('first_observe_price'))}：第一观察位",
        f"- {breakeven_text}：解套区和核心压力区",
        f"- {fmt_price(levels.get('stretch_price'))}：超预期目标位",
        "",
        "## 盘中应对表",
        "",
        "### 1. 强势剧本",
        "",
        f"- 开盘后围绕 {fmt_price(levels.get('anchor_price'))} 上方震荡",
        "- 冲高回落后仍能快速收回",
        "- 成交放大但不是单边砸盘",
        "",
        "观察重点：",
        "",
        f"- 是否始终守住 {fmt_price(levels.get('defensive_price'))}",
        "- 分时回封 / 回拉速度是否够快",
        "- 尾盘是否仍有主动承接",
        "",
        "### 2. 中性剧本",
        "",
        "- 冲高后反复开板或回落",
        f"- 回落后仍能守住 {fmt_price(levels.get('defensive_price'))} 附近",
        f"- 午后若能回到 {fmt_price(levels.get('anchor_price'))} 上方，说明修复仍在延续",
        "",
        "### 3. 偏弱剧本",
        "",
        "- 高开低走",
        f"- 跌回 {fmt_price(levels.get('defensive_price'))} 下方且站不回去",
        "- 尾盘承接明显走弱",
        "",
        "## 当前一句话判断",
        "",
        str(snapshot["action_brief"]),
        "",
    ]

    if isinstance(breakeven_zone, tuple):
        lines.extend(
            [
                "## 持仓视角",
                "",
                f"- {fmt_price(levels.get('first_observe_price'))} 时，参考浮盈亏约 {(float(levels['first_observe_price']) - float(snapshot.get('cost_basis') or 0.0)) * int(snapshot.get('shares') or 0):,.0f} 元",
                f"- {breakeven_text} 属于接近回本 / 解套区域",
                f"- {fmt_price(levels.get('stretch_price'))} 时，参考浮盈约 {(float(levels['stretch_price']) - float(snapshot.get('cost_basis') or 0.0)) * int(snapshot.get('shares') or 0):,.0f} 元",
                "",
            ]
        )

    if snapshot.get("signal_date") and snapshot.get("mark_date"):
        lines.extend(
            [
                "## 使用说明",
                "",
                "- 本清单基于最新保存的量化信号和当前持仓参考价格自动生成。",
                "- 量化信号日期可能早于参考价格日期，因为当前标签周期使用 T+1 持有期回报，最新信号会滞后于原始行情数据。",
                "",
            ]
        )

    return "\n".join(lines).strip() + "\n"


def generate_watch_plans(root: Path | None = None) -> list[Path]:
    resolved_root = root or project_root()
    watchlist = load_yaml(resolved_root / "config" / "watchlist.yaml").get("holdings", []) or []
    if not watchlist:
        logger.info("No holdings were found in config/watchlist.yaml.")
        return []

    data_source = resolve_data_source(resolved_root)
    symbols = [str(item.get("ts_code", "") or "").strip() for item in watchlist if str(item.get("ts_code", "") or "").strip()]
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    daily_bar = read_daily_bar(resolved_root, data_source, symbols=symbols)
    trade_dates = load_trade_dates(resolved_root, data_source)
    prediction_snapshots = load_prediction_snapshots(resolved_root, data_source)
    overlay_symbols = load_overlay_symbols(resolved_root, data_source)

    generated_paths: list[Path] = []
    for item in watchlist:
        symbol = str(item.get("ts_code", "") or "").strip()
        if not symbol:
            continue

        snapshot = build_holding_snapshot(
            item=item,
            daily_bar=daily_bar,
            prediction_snapshots=prediction_snapshots,
            overlay_symbols=overlay_symbols,
            trade_dates=trade_dates,
        )
        plan_date = snapshot.get("plan_date") or pd.Timestamp.today().date().isoformat()
        output_path = reports_dir / f"{symbol.split('.')[0]}_watch_plan_{plan_date}.md"
        save_text(_compose_watch_plan(snapshot), output_path)
        generated_paths.append(output_path)
        logger.info(f"Generated watch plan: {output_path}")

    return generated_paths


def run() -> None:
    generated_paths = generate_watch_plans()
    if not generated_paths:
        print("No watch plans generated.")
        return

    from src.db.dashboard_sync import sync_dashboard_artifacts

    summary = sync_dashboard_artifacts()
    for path in generated_paths:
        print(path)
    print(summary.message)


if __name__ == "__main__":
    run()
