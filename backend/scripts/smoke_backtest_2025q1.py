"""Layer 2 真实历史 Smoke Test：2025-Q1 · CLOSE > MA(CLOSE,20) · 等权日频。

用法（token 走环境变量，严禁写入本文件；国内网络需镜像）：
    $env:HF_TOKEN = "<token>"
    $env:HF_ENDPOINT = "https://hf-mirror.com"   # 可选，直连超时时使用
    python scripts/smoke_backtest_2025q1.py

检查项（不看 Sharpe）：
    trades > 0；equity 全程 > 0；cash >= 0；
    每笔成交 price > 0；每个 execution_date 有 raw open；
    每个 valuation_date 有 raw close（严格估值强制）。
"""
import os
import sys
import time
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
from huggingface_hub import hf_hub_download

from core.data_manager import DataManager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator

TOKEN = os.getenv("HF_TOKEN")
if not TOKEN:
    print("ERROR: HF_TOKEN not set"); sys.exit(1)

REPO = "scanli/stocka-data"
START = datetime.date(2025, 1, 2)
END_SIGNAL = datetime.date(2025, 3, 31)
NEEDED_YEARS = [2024, 2025]          # MA20 需要窗口前置历史

KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def build_df_daily() -> pl.DataFrame:
    """复用生产变换管线：下载(缓存) → 分片 → 列裁剪 → limit flags → 前复权 → 重采样。"""
    dm = DataManager()
    parts = []
    for year in NEEDED_YEARS:
        log(f"downloading stock_kline_{year}.parquet ...")
        p = hf_hub_download(repo_id=REPO, filename=f"stock_kline_{year}.parquet",
                            repo_type="dataset", token=TOKEN)
        df = pl.read_parquet(p, columns=KEEP_COLS if all(
            c in pl.scan_parquet(p).collect_schema().names() for c in KEEP_COLS
        ) else None)
        # 与生产一致的分片语义（取 node 0），提前裁列控内存
        df = df.filter((df["code"].hash() % dm.total_nodes) == 0)
        keep = [c for c in KEEP_COLS if c in df.columns]
        parts.append(df.select(keep))
        del df
        log(f"  {year} sharded rows={parts[-1].height}")
    df = pl.concat(parts, how="diagonal")
    del parts
    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
    dm.df_daily = df.sort(["code", "date"])
    log(f"df_daily rows={dm.df_daily.height}, codes={dm.df_daily['code'].n_unique()}, "
        f"range={dm.df_daily['date'].min()}..{dm.df_daily['date'].max()}")

    dm._compute_limit_flags()
    dm._apply_forward_adjustment()
    dm._append_prev_close()
    dm._optimize_memory(dm.df_daily, "df_daily")
    dm._resample_all()
    return dm


def main():
    t0 = time.time()
    dm = build_df_daily()
    data_manager_ref = dm

    trade_dates = (dm.df_daily.select(pl.col("date")).unique()
                   .sort("date").to_series().to_list())
    calendar = TradingCalendar()
    calendar.set_trade_dates(trade_dates)

    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)
    log(f"raw source: {store.source_type}")

    engine = BacktestEngine(
        calendar=calendar,
        selection_engine=selection_engine,
        raw_price_store=store,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )

    # 让 SelectionEngine 使用本地构建的内存表
    import core.data_manager as dmm
    saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
             dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
    dmm.data_manager.df_daily = dm.df_daily
    dmm.data_manager.df_weekly = dm.df_weekly
    dmm.data_manager.df_monthly = dm.df_monthly
    dmm.data_manager.df_mapping = dm.df_mapping

    try:
        log("running backtest 2025-01-02 .. 2025-03-31 (daily) ...")
        result = engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=START,
            end_signal_date=END_SIGNAL,
            initial_cash=1_000_000,
        )
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved

    ec = result.equity_curve
    tr = result.trades
    pd_ = result.positions_daily

    # ---- 检查项 ----
    n_days = ec.height
    min_equity = float(ec["equity"].min()) if n_days else 0.0
    min_cash = float(ec["cash"].min()) if n_days else 0.0
    n_trades = tr.height
    bad_price = tr.filter(pl.col("price") <= 0).height if n_trades else 0
    missing_open = 0
    if n_trades:
        exec_dates = tr["execution_date"].unique().to_list()
        px = store.load_execution_prices(exec_dates)
        have = set(zip(px["code"].to_list(), px["date"].to_list()))
        missing_open = sum(
            1 for r in tr.iter_rows(named=True)
            if (r["code"], r["execution_date"]) not in have
        )

    log("========== SMOKE RESULT ==========")
    log(f"valuation days      : {n_days}")
    log(f"curve range         : {ec['date'].min()} .. {ec['date'].max()}")
    log(f"equity min/max/end  : {min_equity:,.2f} / {float(ec['equity'].max()):,.2f} "
        f"/ {float(ec.tail(1)['equity'][0]):,.2f}")
    log(f"cash min            : {min_cash:,.2f}")
    log(f"trades              : {n_trades} (bad_price={bad_price}, missing_open={missing_open})")
    log(f"buy/sell            : {tr.filter(pl.col('side')=='BUY').height}/{tr.filter(pl.col('side')=='SELL').height}")
    log(f"positions_daily     : {pd_.height} rows")
    log(f"metrics             : {result.metrics}")
    log(f"elapsed             : {time.time()-t0:.1f}s")

    ok = True
    def chk(name, cond):
        nonlocal ok
        log(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        ok = ok and cond

    chk("trades > 0", n_trades > 0)
    chk("equity > 0 全程", n_days > 0 and min_equity > 0)
    chk("cash >= 0 全程", n_days > 0 and min_cash >= -1e-6)
    chk("无零价成交", bad_price == 0)
    chk("每笔成交有 raw open", missing_open == 0)

    print("\nSMOKE:", "ALL PASS" if ok else "FAILED", flush=True)
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()