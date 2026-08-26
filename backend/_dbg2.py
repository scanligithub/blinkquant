# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '.')
import json
import tempfile
import polars as pl
from tests.test_backtest_continuity import (
    _weekdays, _fixture, _install, _run_segment, _engine_with)
from core.data_manager import data_manager

days = _weekdays(10)
a_last = days[6]
b_first = days[7]
df = _fixture(days)
_install(df)
try:
    with tempfile.TemporaryDirectory() as tmp:
        df.write_parquet(f"{tmp}/stock_kline_{days[0].year}.parquet")
        c1 = _run_segment(tmp, days, "CLOSE > 10", start=days[0], end=a_last)

        eng_a = _engine_with(tmp, days)
        res_a = eng_a.run(formula="CLOSE > 10", start_date=days[0],
                          end_signal_date=a_last, initial_cash=1_000_000)
        st = eng_a.export_state()
        state = {"cash": st["portfolio"]["cash"],
                 "positions": st["portfolio"]["positions"],
                 "last_close": st["last_close"],
                 "thru_thaw": st["thru_thaw"].isoformat() if st["thru_thaw"] else None,
                 "pending": st["pending"],
                 "selected_thru": st["selected_thru"].isoformat() if st["selected_thru"] else None}
        print("exported state:", {k: v for k, v in state.items() if k != 'positions'})
        print("positions:", state["positions"])

        b = _run_segment(tmp, days, "CLOSE > 10",
                         start=b_first, end=days[-2], state=state)
        print("B trades:", b.trades.height)
        print(b.trades)
        print("B equity:")
        print(b.equity_curve)
finally:
    data_manager.df_daily = None
    data_manager.df_weekly = None
    data_manager.df_monthly = None