from __future__ import annotations

import pandas as pd

from src.backtest.costs import round_trip_cost_bps
from src.backtest.metrics import annualized_return, max_drawdown, sharpe_ratio
from src.backtest.risk_filter import apply_trend_filter
from src.backtest.portfolio import select_top_n, top_n_daily_portfolio, top_n_period_portfolio


def infer_holding_period(label_col: str) -> int:
    if label_col.startswith("ret_t1_t"):
        suffix = label_col.removeprefix("ret_t1_t")
        if suffix.isdigit():
            return max(1, int(suffix))
    return 1


def rank_ic_by_date(frame: pd.DataFrame, score_col: str, label_col: str) -> pd.Series:
    return (
        frame.groupby("trade_date")[[score_col, label_col]]
        .apply(lambda group: group[score_col].corr(group[label_col], method="spearman"))
        .dropna()
        .rename("rank_ic")
    )


def top_n_forward_return_by_date(
    frame: pd.DataFrame,
    score_col: str,
    label_col: str,
    top_n: int,
    group_col: str | None = None,
    max_per_group: int | None = None,
) -> pd.Series:
    selected = select_top_n(
        frame=frame,
        score_col=score_col,
        top_n=top_n,
        group_col=group_col,
        max_per_group=max_per_group,
    )
    return selected.groupby("trade_date")[label_col].mean().rename("top_n_forward_return")


def _group_performance_summary(frame: pd.DataFrame, periods_per_year: int) -> dict[str, float]:
    clean_returns = frame["net_return"].fillna(0.0)
    summary = {
        "periods": float(len(frame)),
        "total_return": float((1.0 + clean_returns).prod() - 1.0),
        "annualized_return": float(annualized_return(clean_returns, periods_per_year=periods_per_year)),
        "sharpe": float(sharpe_ratio(clean_returns, periods_per_year=periods_per_year)),
        "max_drawdown": float(max_drawdown(clean_returns)),
        "win_rate": float((clean_returns > 0.0).mean()),
        "avg_net_return": float(clean_returns.mean()),
    }
    if "gross_return_unfiltered" in frame.columns:
        gross_unfiltered = frame["gross_return_unfiltered"].fillna(0.0)
        summary["unfiltered_total_return"] = float((1.0 + gross_unfiltered).prod() - 1.0)
        summary["unfiltered_avg_return"] = float(gross_unfiltered.mean())
    if "benchmark_proxy_period_return" in frame.columns:
        benchmark_returns = frame["benchmark_proxy_period_return"].fillna(0.0)
        summary["benchmark_total_return"] = float((1.0 + benchmark_returns).prod() - 1.0)
        summary["benchmark_avg_return"] = float(benchmark_returns.mean())
    if "risk_on" in frame.columns:
        summary["risk_on_ratio"] = float(frame["risk_on"].fillna(True).mean())
    return summary


def yearly_performance_summary(portfolio: pd.DataFrame, periods_per_year: int) -> pd.DataFrame:
    if portfolio.empty:
        return pd.DataFrame()

    working = portfolio.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"])
    working["year"] = working["trade_date"].dt.year

    rows: list[dict[str, float | int]] = []
    for year, group in working.groupby("year", sort=True):
        row = {"year": int(year)}
        row.update(_group_performance_summary(group, periods_per_year))
        rows.append(row)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def regime_performance_summary(portfolio: pd.DataFrame, periods_per_year: int) -> pd.DataFrame:
    if portfolio.empty or "risk_on" not in portfolio.columns:
        return pd.DataFrame()

    working = portfolio.copy()
    working["regime"] = working["risk_on"].map({True: "trend_on", False: "trend_off"})

    rows: list[dict[str, float | str]] = []
    for regime, group in working.groupby("regime", sort=False):
        row = {"regime": str(regime)}
        row.update(_group_performance_summary(group, periods_per_year))
        rows.append(row)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).reset_index(drop=True)


def build_performance_diagnostics(portfolio: pd.DataFrame, label_col: str) -> dict[str, pd.DataFrame]:
    holding_period = infer_holding_period(label_col)
    periods_per_year = 252 if holding_period <= 1 else max(1, int(round(252 / holding_period)))
    return {
        "yearly": yearly_performance_summary(portfolio, periods_per_year=periods_per_year),
        "regime": regime_performance_summary(portfolio, periods_per_year=periods_per_year),
    }


def summarize_predictions(
    frame: pd.DataFrame,
    score_col: str,
    label_col: str,
    top_n: int,
    daily_return_col: str | None = None,
    group_col: str | None = None,
    max_per_group: int | None = None,
    benchmark_proxy: pd.DataFrame | None = None,
    experiment: dict | None = None,
) -> tuple[dict[str, float], pd.DataFrame]:
    holding_period = infer_holding_period(label_col)
    strategy_return_col = daily_return_col or label_col
    rank_ic = rank_ic_by_date(frame, score_col=score_col, label_col=label_col)
    top_n_forward = top_n_forward_return_by_date(
        frame=frame,
        score_col=score_col,
        label_col=label_col,
        top_n=top_n,
        group_col=group_col,
        max_per_group=max_per_group,
    )
    if holding_period <= 1 and strategy_return_col == "ret_next_1d":
        daily_portfolio = top_n_daily_portfolio(
            frame=frame,
            score_col=score_col,
            return_col=strategy_return_col,
            top_n=top_n,
            cost_bps=round_trip_cost_bps(),
            group_col=group_col,
            max_per_group=max_per_group,
        )
        periods_per_year = 252
    else:
        daily_portfolio = top_n_period_portfolio(
            frame=frame,
            score_col=score_col,
            return_col=strategy_return_col,
            top_n=top_n,
            holding_period_days=holding_period,
            cost_bps=round_trip_cost_bps(),
            group_col=group_col,
            max_per_group=max_per_group,
            hold_buffer=int((experiment or {}).get("portfolio", {}).get("hold_buffer", 0) or 0),
        )
        periods_per_year = max(1, int(round(252 / holding_period)))

    if benchmark_proxy is not None and experiment is not None:
        daily_portfolio = apply_trend_filter(
            portfolio=daily_portfolio,
            benchmark_proxy=benchmark_proxy,
            experiment=experiment,
        )

    summary = {
        "observations": float(len(frame)),
        "dates": float(frame["trade_date"].nunique()),
        "holding_period_days": float(holding_period),
        "rank_ic_mean": float(rank_ic.mean()),
        "rank_ic_std": float(rank_ic.std()),
        "top_n_forward_mean": float(top_n_forward.mean()),
        "top_n_hit_rate": float((top_n_forward > 0).mean()),
        "daily_portfolio_annualized_return": float(
            annualized_return(daily_portfolio["net_return"], periods_per_year=periods_per_year)
        ),
        "daily_portfolio_sharpe": float(sharpe_ratio(daily_portfolio["net_return"], periods_per_year=periods_per_year)),
        "daily_portfolio_max_drawdown": float(max_drawdown(daily_portfolio["net_return"])),
    }
    if "risk_on" in daily_portfolio.columns:
        summary["risk_filter_active_ratio"] = float(daily_portfolio["risk_on"].fillna(True).mean())
        summary["risk_filter_filtered_periods"] = float((~daily_portfolio["risk_on"].fillna(True)).sum())
    if "benchmark_proxy_period_return" in daily_portfolio.columns:
        summary["benchmark_proxy_total_return"] = float(
            (1.0 + daily_portfolio["benchmark_proxy_period_return"].fillna(0.0)).prod() - 1.0
        )
    if "turnover_ratio" in daily_portfolio.columns:
        summary["avg_turnover_ratio"] = float(daily_portfolio["turnover_ratio"].fillna(0.0).mean())
        summary["max_turnover_ratio"] = float(daily_portfolio["turnover_ratio"].fillna(0.0).max())
    if "selected_count" in daily_portfolio.columns:
        summary["avg_selected_count"] = float(daily_portfolio["selected_count"].fillna(0.0).mean())
    return summary, daily_portfolio
