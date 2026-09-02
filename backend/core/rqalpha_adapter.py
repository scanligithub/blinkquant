"""RQAlpha adapter — runs RQAlpha and returns comparable DataFrames."""

import datetime
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import polars as pl


@dataclass
class RQAlphaResult:
    """RQAlpha output in BlinkQuant-compatible format."""
    trades: pl.DataFrame
    equity_curve: pl.DataFrame
    positions_daily: pl.DataFrame
    account_events: List[Dict[str, Any]] = field(default_factory=list)


def _to_rqalpha_code(code: str) -> str:
    """Convert sh.600000 → 600000.XSHG format."""
    prefix, num = code.split(".")
    exchange = {"sh": "XSHG", "sz": "XSHE", "bj": "XBJE"}.get(prefix, "XSHG")
    return f"{num}.{exchange}"


def _from_rqalpha_code(code: str) -> str:
    """Convert 600000.XSHG → sh.600000 format."""
    num, exchange = code.split(".")
    prefix = {"XSHG": "sh", "XSHE": "sz", "XBJE": "bj"}.get(exchange, "sh")
    return f"{prefix}.{num}"


def run_rqalpha(
    codes: List[str],
    start_date: datetime.date,
    end_date: datetime.date,
    initial_cash: float,
    hf_repo: Optional[str] = None,
    hf_token: Optional[str] = None,
) -> RQAlphaResult:
    """Run RQAlpha with a simple MA20 strategy and return comparable DataFrames."""
    from rqalpha import run_func
    from rqalpha.apis import history_bars, update_universe
    from rqalpha.environment import Environment

    rqalpha_codes = [_to_rqalpha_code(c) for c in codes]

    config = {
        "base": {
            "start_date": start_date,
            "end_date": end_date,
            "initial_cash": initial_cash,
            "benchmark": None,
            "accounts": {"stock": initial_cash},
            "capital_gain_tax_rate": 0,
        },
        "mod": {
            "sys_accounts": {"enabled": True},
        },
    }

    trade_log: List[Dict[str, Any]] = []
    equity_log: List[Dict[str, Any]] = []
    position_log: List[Dict[str, Any]] = []

    def init(context):
        context.codes = rqalpha_codes
        context.holdings: Dict[str, int] = {}
        # Register universe so bar_dict contains our codes
        update_universe(rqalpha_codes)

    def handle_bar(context, bar_dict):
        today = context.now.date() if hasattr(context.now, 'date') else context.now
        total_value = context.portfolio.total_value
        cash = context.portfolio.cash

        equity_log.append({
            "date": today,
            "total_value": total_value,
            "cash": cash,
            "market_value": context.portfolio.market_value,
        })

        for code in context.codes:
            pos = context.portfolio.positions.get(code)
            if pos and pos.quantity > 0:
                position_log.append({
                    "date": today,
                    "code": _from_rqalpha_code(code),
                    "quantity": pos.quantity,
                    "avg_cost": getattr(pos, 'avg_cost', getattr(pos, 'average_cost', 0)),
                    "market_value": pos.market_value,
                })

        for code in context.codes:
            try:
                bar = bar_dict[code]
            except (KeyError, Exception):
                continue
            if bar is None or bar.close != bar.close:  # NaN check
                continue
            prices = history_bars(code, 20, "1d", "close")
            if prices is None or len(prices) < 20:
                continue
            ma20 = float(prices.mean())
            current_price = float(bar.close)
            held = context.holdings.get(code, 0)

            if current_price > ma20 and held == 0:
                weight = 1.0 / len(context.codes)
                target_value = total_value * weight
                shares = int(target_value / current_price / 100) * 100
                if shares >= 100 and shares * current_price <= cash:
                    from rqalpha.apis import order
                    order(code, shares)
                    context.holdings[code] = context.holdings.get(code, 0) + shares
                    trade_log.append({
                        "date": today,
                        "code": _from_rqalpha_code(code),
                        "side": "buy",
                        "quantity": shares,
                        "price": current_price,
                        "cost": shares * current_price,
                    })

            elif current_price < ma20 and held > 0:
                from rqalpha.apis import order
                order(code, -held)
                trade_log.append({
                    "date": today,
                    "code": _from_rqalpha_code(code),
                    "side": "sell",
                    "quantity": held,
                    "price": current_price,
                    "cost": -held * current_price,
                })
                context.holdings[code] = 0

    run_func(config=config, init=init, handle_bar=handle_bar)

    trades_df = pl.DataFrame(trade_log) if trade_log else pl.DataFrame(
        {"date": [], "code": [], "side": [], "quantity": [], "price": [], "cost": []}
    )
    equity_df = pl.DataFrame(equity_log) if equity_log else pl.DataFrame(
        {"date": [], "total_value": [], "cash": [], "market_value": []}
    )
    positions_df = pl.DataFrame(position_log) if position_log else pl.DataFrame(
        {"date": [], "code": [], "quantity": [], "avg_cost": [], "market_value": []}
    )

    return RQAlphaResult(
        trades=trades_df,
        equity_curve=equity_df,
        positions_daily=positions_df,
    )
