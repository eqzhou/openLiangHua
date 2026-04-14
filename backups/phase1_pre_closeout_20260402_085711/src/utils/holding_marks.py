from __future__ import annotations

import pandas as pd


def _normalize_timestamp(value: object) -> pd.Timestamp | None:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp).normalize()


def describe_price_reference(
    *,
    is_manual_mark: bool,
    mark_date: object,
    latest_bar_date: object,
) -> dict[str, object]:
    normalized_mark_date = _normalize_timestamp(mark_date)
    normalized_latest_bar_date = _normalize_timestamp(latest_bar_date)
    mark_vs_latest_bar_days: int | None = None

    if normalized_mark_date is not None and normalized_latest_bar_date is not None:
        mark_vs_latest_bar_days = int((normalized_mark_date - normalized_latest_bar_date).days)

    if is_manual_mark:
        if normalized_latest_bar_date is None:
            return {
                "mark_status": "手工参考价",
                "mark_status_note": "当前持仓估值使用手工记录价格；本地日线中暂无可用收盘价。",
                "mark_vs_latest_bar_days": mark_vs_latest_bar_days,
            }
        if mark_vs_latest_bar_days is not None and mark_vs_latest_bar_days > 0:
            note = (
                f"本地日线最新只到 {normalized_latest_bar_date.date()}，"
                f"当前参考价 {normalized_mark_date.date() if normalized_mark_date is not None else '-'} 为手工记录。"
            )
            return {
                "mark_status": "手工参考价(日线未到)",
                "mark_status_note": note,
                "mark_vs_latest_bar_days": mark_vs_latest_bar_days,
            }
        if mark_vs_latest_bar_days is not None and mark_vs_latest_bar_days < 0:
            note = (
                f"当前手工参考价日期 {normalized_mark_date.date() if normalized_mark_date is not None else '-'} "
                f"早于本地最新日线 {normalized_latest_bar_date.date()}。"
            )
            return {
                "mark_status": "手工参考价(早于日线)",
                "mark_status_note": note,
                "mark_vs_latest_bar_days": mark_vs_latest_bar_days,
            }
        return {
            "mark_status": "手工参考价",
            "mark_status_note": "当前持仓估值使用手工记录价格。",
            "mark_vs_latest_bar_days": mark_vs_latest_bar_days,
        }

    if normalized_latest_bar_date is not None:
        note = f"当前持仓估值使用本地最新落库日线 {normalized_latest_bar_date.date()} 的收盘价。"
    else:
        note = "当前持仓估值使用本地最新落库收盘价。"
    return {
        "mark_status": "最新日线收盘价",
        "mark_status_note": note,
        "mark_vs_latest_bar_days": mark_vs_latest_bar_days,
    }
