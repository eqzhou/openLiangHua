from __future__ import annotations

import numpy as np
import pandas as pd


def add_valuation_factors(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    created_columns: list[str] = []

    if "pe_ttm" in enriched.columns:
        enriched["earnings_yield"] = 1.0 / enriched["pe_ttm"].replace(0, np.nan)
        created_columns.append("earnings_yield")

    if "pb" in enriched.columns:
        enriched["book_to_price"] = 1.0 / enriched["pb"].replace(0, np.nan)
        created_columns.append("book_to_price")

    if "ps_ttm" in enriched.columns:
        enriched["sales_yield"] = 1.0 / enriched["ps_ttm"].replace(0, np.nan)
        created_columns.append("sales_yield")

    if "total_mv" in enriched.columns:
        enriched["log_total_mv"] = np.log(enriched["total_mv"].where(enriched["total_mv"] > 0))
        created_columns.append("log_total_mv")

    for column in created_columns:
        enriched[f"{column}_cs_rank"] = enriched.groupby("trade_date")[column].rank(pct=True)

    return enriched
