# -*- coding: utf-8 -*-
"""Quick pipeline test: single segment, single ranking."""
import sys
import time
import datetime
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parents[2]))

import polars as pl
from backtest_quality_2024_2025 import build_df_daily
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.metrics import compute_metrics
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG
from research.ranking.ranking import code_asc_ranking, strength_desc_ranking, strength_asc_ranking
import core.data_manager as dmm
from core.engine import selection_engine

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
CASH = 10_000_000

print("loading data...", flush=True)
dm = build_df_daily(2025)
dates_all = (dm.df_daily.select(pl.col("date")).unique()
             .sort("date").to_series().to_list())
print(f"data ready: {len(dates_all)} dates, "
      f"codes={dm.df_daily['code'].n_unique()}", flush=True)

saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
dmm.data_manager.df_daily = dm.df_daily
dmm.data_manager.df_weekly = dm.df_weekly
dmm.data_manager.df_monthly = dm.df_monthly
dmm.data_manager.df_mapping = dm.df_mapping

test_cases = [
    ("code_asc", code_asc_ranking, 20),
    ("strength_desc", strength_desc_ranking, 20),
]

for rname, rfn, n in test_cases:
    cal = TradingCalendar()
    cal.set_trade_dates(dates_all)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)
    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=None,
    )
    t0 = time.time()
    result = engine.run(
        formula="CLOSE > MA(CLOSE, 20)",
        start_date=datetime.date(2025, 7, 1),
        end_signal_date=datetime.date(2025, 12, 31),
        initial_cash=CASH, rebalance_freq="weekly",
        ranking_fn=rfn, top_n=n,
    )
    elapsed = time.time() - t0
    m = compute_metrics(result, initial_cash=CASH)
    flat = m.to_flat_dict()
    ret = flat.get("performance.total_return", 0)
    dd = flat.get("performance.max_drawdown", 0)
    tc = flat.get("trading.trade_count", 0)
    sd_count = len(result.equity_curve.filter(
        pl.col("signal_date").is_not_null())["signal_date"].unique())
    print(f"  {rname:>16} N={n}: ret={ret:+.1%} dd={dd:.1%} "
          f"trades={tc} sigDates={sd_count} elapsed={elapsed:.1f}s",
          flush=True)

dmm.data_manager.df_daily, dmm.data_manager.df_weekly, dmm.data_manager.df_monthly, dmm.data_manager.df_mapping = saved
print("done", flush=True)
