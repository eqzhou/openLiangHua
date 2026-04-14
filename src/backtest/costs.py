from __future__ import annotations


def round_trip_cost_bps(
    buy_commission_bps: float = 3.0,
    sell_commission_bps: float = 3.0,
    sell_tax_bps: float = 10.0,
    slippage_bps: float = 5.0,
) -> float:
    return buy_commission_bps + sell_commission_bps + sell_tax_bps + 2 * slippage_bps


def net_return_after_cost(gross_return: float, cost_bps: float) -> float:
    return gross_return - cost_bps / 10000.0
