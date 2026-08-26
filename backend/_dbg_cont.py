# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '.')
import datetime
import tempfile
import polars as pl
from tests.test_backtest_continuity import (
    _weekdays, _fixture, _install, _run_segment)
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
from core.data_manager import data_manager
from core.engine import selection_engine

days = _weekdays(10)
split_idx = 6
df = _fixture(days)
_install(df)
try:
    with tempfile.TemporaryDirectory() as tmp:
        df.write_parquet(f"{tmp}/stock_kline_{days[0].year}.parquet")

        segA_end_signal = days[split_idx - 1]

        def eng():
            cal = TradingCalendar(); cal.set_trade_dates(days)
            return BacktestEngine(
                calendar=cal, selection_engine=selection_engine,
                raw_price_store=RawPriceStore(tmp),
                fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
                allocator=equal_weight_allocator)

        eA = eng()
        resA = eA.run(formula="CLOSE > 10", start_date=days[0],
                      end_signal_date=segA_end_signal, initial_cash=1_000_000)
        state = eA.export_state()
        import json
        state = json.loads(json.dumps(
            {**state["portfolio"], "last_close": state["last_close"]},
            default=str))

        c1 = _run_segment(tmp, days, "CLOSE > 10",
                          start=days[0], end=days[-2])
        b = _run_segment(tmp, days, "CLOSE > 10",
                         start=days[split_idx], end=days[-2], state=state)

        tail = set(days[split_idx + 1:])
        t1 = c1.trades.filter(pl.col("execution_date").is_in(tail))
        t2 = b.trades
        print("C1 tail trades:", t1.height, " B trades:", t2.height)
        d1 = t1.to_dicts(); d2 = t2.to_dicts()
        key = lambda r: (r["execution_date"], r["code"], r["side"], r["signal_date"], r["qty"])
        s1 = sorted(d1, key=key); s2 = sorted(d2, key=key)
        i = 0
        while i < min(len(s1), len(s2)) and key(s1[i]) == key(s2[i]):
            i += 1
        for j in range(max(0, i - 2), min(i + 3, max(len(s1), len(s2)))):
            mark = "<<<" if (j >= len(s1) or j >= len(s2) or key(s1[j]) != key(s2[j])) else ""
            left = s1[j] if j < len(s1) else None
            right = s2[j] if j < len(s2) else None
            print(j, "C1:", left and {k: left[k] for k in ('signal_date','execution_date','code','side','qty')},
                  "B:", right and {k: right[k] for k in ('signal_date','execution_date','code','side','qty')}, mark)

        eq1 = c1.equity_curve.filter(pl.col("date").is_in(tail)).sort("date").to_dicts()
        eq2 = b.equity_curve.sort("date").to_dicts()
        for x, y in zip(eq1, eq2):
            if abs(x["equity"] - y["equity"]) > 1e-9:
                print("EQ DIVERGE", x["date"], x["equity"], y["equity"])
finally:
    data_manager.df_daily = None
    data_manager.df_weekly = None
    data_manager.df_monthly = None
