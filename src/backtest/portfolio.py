from __future__ import annotations

import pandas as pd

from src.backtest.costs import net_return_after_cost


def rank_candidates(
    frame: pd.DataFrame,
    score_col: str,
    group_col: str | None = None,
    max_per_group: int | None = None,
) -> pd.DataFrame:
    ranked = frame.sort_values(["trade_date", score_col], ascending=[True, False]).copy()
    if group_col and max_per_group:
        ranked["_group_key"] = ranked[group_col].fillna("未知行业")
        ranked["_group_rank"] = ranked.groupby(["trade_date", "_group_key"]).cumcount() + 1
        ranked = ranked.loc[ranked["_group_rank"] <= int(max_per_group)].copy()
    return ranked


def select_top_n(
    frame: pd.DataFrame,
    score_col: str,
    top_n: int,
    group_col: str | None = None,
    max_per_group: int | None = None,
) -> pd.DataFrame:
    ranked = rank_candidates(
        frame=frame,
        score_col=score_col,
        group_col=group_col,
        max_per_group=max_per_group,
    )
    return ranked.groupby("trade_date", group_keys=False).head(top_n).copy()


def top_n_daily_portfolio(
    frame: pd.DataFrame,
    score_col: str,
    return_col: str = "ret_next_1d",
    top_n: int = 10,
    cost_bps: float = 0.0,
    group_col: str | None = None,
    max_per_group: int | None = None,
) -> pd.DataFrame:
    selected = select_top_n(
        frame=frame,
        score_col=score_col,
        top_n=top_n,
        group_col=group_col,
        max_per_group=max_per_group,
    )
    portfolio = (
        selected.groupby("trade_date")[return_col]
        .mean()
        .rename("gross_return")
        .reset_index()
        .sort_values("trade_date")
    )
    portfolio["turnover_ratio"] = 1.0
    portfolio["selected_count"] = int(top_n)
    portfolio["overlap_count"] = 0
    portfolio["net_return"] = portfolio.apply(
        lambda row: net_return_after_cost(float(row["gross_return"]), cost_bps * float(row["turnover_ratio"])),
        axis=1,
    )
    portfolio["equity_curve"] = (1.0 + portfolio["net_return"].fillna(0.0)).cumprod()
    return portfolio


def top_n_period_portfolio(
    frame: pd.DataFrame,
    score_col: str,
    return_col: str,
    top_n: int,
    holding_period_days: int,
    cost_bps: float = 0.0,
    group_col: str | None = None,
    max_per_group: int | None = None,
    hold_buffer: int = 0,
) -> pd.DataFrame:
    ranked = rank_candidates(
        frame=frame,
        score_col=score_col,
        group_col=group_col,
        max_per_group=max_per_group,
    )
    selected_dates = ranked["trade_date"].drop_duplicates().sort_values().reset_index(drop=True)
    if selected_dates.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "gross_return",
                "net_return",
                "equity_curve",
                "holding_period_days",
                "turnover_ratio",
                "selected_count",
                "overlap_count",
                "retained_count",
            ]
        )

    rebalance_dates = set(selected_dates.iloc[:: max(1, int(holding_period_days))].tolist())
    rebalanced = ranked.loc[ranked["trade_date"].isin(rebalance_dates)].copy()

    portfolio_rows: list[dict[str, object]] = []
    previous_holdings: list[str] = []
    candidate_limit = max(int(top_n), int(top_n) + max(0, int(hold_buffer)))

    for trade_date, group in rebalanced.groupby("trade_date", sort=True):
        candidates = group.sort_values(score_col, ascending=False).head(candidate_limit).copy()
        if candidates.empty:
            continue

        retained: list[str] = []
        if previous_holdings and hold_buffer > 0:
            candidate_codes = set(candidates["ts_code"].astype(str))
            retained = [code for code in previous_holdings if code in candidate_codes]

        chosen_codes = retained.copy()
        for code in candidates["ts_code"].astype(str).tolist():
            if code in chosen_codes:
                continue
            chosen_codes.append(code)
            if len(chosen_codes) >= int(top_n):
                break

        chosen = candidates.loc[candidates["ts_code"].astype(str).isin(chosen_codes)].copy()
        if chosen.empty:
            continue

        overlap_count = len(set(previous_holdings) & set(chosen["ts_code"].astype(str)))
        selected_count = int(len(chosen))
        turnover_ratio = 1.0 if not previous_holdings else max(0.0, 1.0 - overlap_count / max(selected_count, 1))
        gross_return = float(pd.to_numeric(chosen[return_col], errors="coerce").mean())
        net_return = net_return_after_cost(gross_return, cost_bps * turnover_ratio)

        portfolio_rows.append(
            {
                "trade_date": pd.Timestamp(trade_date),
                "gross_return": gross_return,
                "net_return": net_return,
                "holding_period_days": max(1, int(holding_period_days)),
                "turnover_ratio": turnover_ratio,
                "selected_count": selected_count,
                "overlap_count": overlap_count,
                "retained_count": len(retained),
            }
        )
        previous_holdings = chosen["ts_code"].astype(str).tolist()

    portfolio = pd.DataFrame(portfolio_rows).sort_values("trade_date").reset_index(drop=True)
    portfolio["equity_curve"] = (1.0 + portfolio["net_return"].fillna(0.0)).cumprod()
    return portfolio
