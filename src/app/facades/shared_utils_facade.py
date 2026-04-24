from __future__ import annotations

import pandas as pd
from typing import Any

from src.app.facades.base import _json_ready

def _best_comparison_record(comparison: list[dict[str, Any]], field: str, *, mode: str = "max") -> dict[str, Any]:
    if not comparison:
        return {}

    frame = pd.DataFrame(comparison)
    if frame.empty or field not in frame.columns:
        return {}

    numeric_series = pd.to_numeric(frame[field], errors="coerce")
    scoped = frame.loc[numeric_series.notna()].copy()
    if scoped.empty:
        return {}

    scoped[field] = numeric_series.loc[numeric_series.notna()].astype(float)
    ascending = mode == "min"
    return _json_ready(scoped.sort_values(field, ascending=ascending).iloc[0].to_dict())
