from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd


def _safe_return(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    valid = numerator.notna() & denominator.notna() & (numerator > 0) & (denominator > 0)
    result = pd.Series(np.nan, index=denominator.index, dtype="float64")
    result.loc[valid] = numerator.loc[valid] / denominator.loc[valid] - 1.0
    return result


def add_forward_returns(frame: pd.DataFrame, horizons: Iterable[int] = (5, 10, 20)) -> pd.DataFrame:
    labeled = frame.copy().sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    grouped = labeled.groupby("ts_code", group_keys=False)
    close_group = grouped["close_adj"]

    entry_proxy = close_group.shift(-1)
    next_is_st = grouped["is_st"].shift(-1).where(grouped["is_st"].shift(-1).notna(), True).astype(bool)
    next_is_suspend = grouped["is_suspend"].shift(-1).where(
        grouped["is_suspend"].shift(-1).notna(),
        True,
    ).astype(bool)
    next_is_buy_locked = grouped["is_buy_locked"].shift(-1).where(
        grouped["is_buy_locked"].shift(-1).notna(),
        True,
    ).astype(bool)

    labeled["can_enter_next_day"] = ~(next_is_st | next_is_suspend | next_is_buy_locked)
    labeled["ret_next_1d"] = _safe_return(entry_proxy, labeled["close_adj"]).where(labeled["can_enter_next_day"])

    for horizon in horizons:
        exit_proxy = close_group.shift(-horizon)
        exit_is_suspend = grouped["is_suspend"].shift(-horizon).where(
            grouped["is_suspend"].shift(-horizon).notna(),
            True,
        ).astype(bool)
        exit_is_sell_locked = grouped["is_sell_locked"].shift(-horizon).where(
            grouped["is_sell_locked"].shift(-horizon).notna(),
            True,
        ).astype(bool)
        valid_label = labeled["can_enter_next_day"] & ~exit_is_suspend & ~exit_is_sell_locked

        labeled[f"label_valid_t{horizon}"] = valid_label
        labeled[f"ret_t1_t{horizon}"] = _safe_return(exit_proxy, entry_proxy).where(valid_label)

    return labeled
