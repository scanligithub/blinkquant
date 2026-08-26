"""Layer 2+ 长窗口质量验收：2024 全年 / 2025 全年 分开跑，输出数据/交易/组合/效率四层指标。

用法：
    $env:HF_TOKEN = "<token>"
    $env:HF_ENDPOINT = "https://hf-mirror.com"
    python scripts/backtest_quality_2024_2025.py

插桩全部在脚本层完成（wrapper 包装），不改动核心代码。
"""
import gc
import os
import sys
import time
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
from huggingface_hub import hf_hub_download

from core.data_manager import DataManager
import core.data_manager as dmm
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.portfolio import Portfolio
from core.execution import ExecutionEngine
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
LOT = 100
EPS = 1e-6


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def build_df_daily(year: int) -> DataManager:
    dm = DataManager()
    parts = []
    for y in (year - 1, year):                      # MA20 需上一年暖机
        p = hf_hub_download(repo_id=REPO, filename=f"stock_kline_{y}.parquet",
                            repo_type="dataset", token=TOKEN)
        df = pl.read_parquet(p)
        df = df.filter((df["code"].hash() % dm.total_nodes) == 0)
        keep = [c for c in ["date", "code", "open", "high", "low", "close",
                            "volume", "amount", "adjustFactor", "pctChg", "isST"]
                if c in df.columns]
        parts.append(df.select(keep))
        del df
    df = pl.concat(parts, how="diagonal"); del parts; gc.collect()
    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
    dm.df_daily = df.sort(["code", "date"])
    dm._compute_limit_flags()
    dm._apply_forward_adjustment()
    dm._append_prev_close()
    dm._optimize_memory(dm.df_daily, "df_daily")
    dm._resample_all()
    return dm


def run_year(year: int) -> dict:
    t0 = time.time()
    R = {"year": year}
    dm = build_df_daily(year)
    R["prep_s"] = round(time.time() - t0, 1)
    R["codes"] = dm.df_daily["code"].n_unique()
    R["range"] = f"{dm.df_daily['date'].min()}..{dm.df_daily['date'].max()}"

    trade_dates = (dm.df_daily.select(pl.col("date")).unique()
                   .sort("date").to_series().to_list())
    sig_dates = [d for d in trade_dates
                 if datetime.date(year, 1, 1) <= d <= datetime.date(year, 12, 31)]
    R["signal_days"] = len(sig_dates)

    cal = TradingCalendar(); cal.set_trade_dates(trade_dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    # ---------- 脚本层插桩 ----------
    M = {k: 0 for k in [
        "store_calls", "store_misses", "hf_resolves",
        "intents_buy", "intents_sell", "fills_buy", "fills_sell",
        "partial_fill", "rejected", "zero_price", "non_lot_buy",
        "t1_violation", "limitup_buy_try", "limitdown_sell_try",
        "derived_suspension", "sel_calls", "sel_time"]}

    _scan = RawPriceStore.scan_window
    def scan_wrap(self, s, e):
        M["store_calls"] += 1
        key = (s.isoformat(), e.isoformat())
        if key not in self._scan_cache:
            M["store_misses"] += 1
        return _scan(self, s, e)
    RawPriceStore.scan_window = scan_wrap

    for be in (_HF := __import__("core.raw_price_store", fromlist=["_HFParquetBackend"])._HFParquetBackend,):
        _res = be.resolve_year_file
        def res_wrap(self, y, _r=_res):
            M["hf_resolves"] += 1
            return _r(self, y)
        be.resolve_year_file = res_wrap

    _flags = dmm.data_manager.get_limit_flags
    def flags_wrap(date, codes):
        fl = _flags(date, codes)
        M["derived_suspension"] += sum(1 for v in fl.values() if v.get("is_suspended"))
        return fl
    dmm.data_manager.get_limit_flags = flags_wrap

    _exec = ExecutionEngine.execute
    def exec_wrap(self, execution_date, intents, positions, raw_prices, cash, limit_flags=None):
        ib = [i for i in intents if i.side == "BUY"]
        isl = [i for i in intents if i.side == "SELL"]
        M["intents_buy"] += len(ib); M["intents_sell"] += len(isl)
        for i in ib:
            fl = (limit_flags or {}).get(i.code, {})
            if fl.get("is_limit_up"): M["limitup_buy_try"] += 1
        for i in isl:
            fl = (limit_flags or {}).get(i.code, {})
            if fl.get("is_limit_down"): M["limitdown_sell_try"] += 1
        fills = _exec(self, execution_date, intents, positions, raw_prices, cash, limit_flags)
        fb = [f for f in fills if f.side == "BUY"]; fs = [f for f in fills if f.side == "SELL"]
        M["fills_buy"] += len(fb); M["fills_sell"] += len(fs)
        for f in fills:
            if f.price <= 0: M["zero_price"] += 1
            if f.side == "BUY" and f.qty % LOT != 0: M["non_lot_buy"] += 1
        # rejected：意图无任何对应成交
        got = {(f.code, f.side) for f in fills}
        want = [(i.code, i.side) for i in intents]
        M["rejected"] += sum(1 for w in want if w not in got)
        # partial：有成交但数量低于意图目标
        fmap = {(f.code, f.side): f.qty for f in fills}
        for i in intents:
            q = fmap.get((i.code, i.side))
            if q is not None and q < i.target_qty:
                M["partial_fill"] += 1
        return fills
    ExecutionEngine.execute = exec_wrap

    _apply = Portfolio.apply_fills
    def apply_wrap(self, fills, execution_date, raw_prices):
        avail_before = {c: p.available_qty for c, p in self.positions.items()}
        viol = 0
        for f in fills:
            if f.side == "SELL":
                if f.qty > avail_before.get(f.code, 0):
                    viol += 1
        out = _apply(self, fills, execution_date, raw_prices)
        M["t1_violation"] += viol
        return out
    Portfolio.apply_fills = apply_wrap

    _sel = type(selection_engine).execute_selector
    sel_stats = {"n": 0, "t": 0.0}
    def sel_wrap(self, formula, timeframe, bg, target_date=None):
        s = time.time()
        r = _sel(self, formula, timeframe, bg, target_date=target_date)
        sel_stats["n"] += 1; sel_stats["t"] += time.time() - s
        return r
    type(selection_engine).execute_selector = sel_wrap
    # ------------------

    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=equal_weight_allocator,
    )

    saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
             dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
    dmm.data_manager.df_daily = dm.df_daily
    dmm.data_manager.df_weekly = dm.df_weekly
    dmm.data_manager.df_monthly = dm.df_monthly
    dmm.data_manager.df_mapping = dm.df_mapping

    try:
        bt0 = time.time()
        result = engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=sig_dates[0], end_signal_date=sig_dates[-1],
            initial_cash=1_000_000,
        )
        R["bt_s"] = round(time.time() - bt0, 1)
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved

    ec, tr, pdl = result.equity_curve, result.trades, result.positions_daily

    # carried valuation：估值日持仓中当日无价、靠 carry 的次数（用 store 实价集回查）
    px_by_date = {}
    for d in ec["date"].unique().to_list():
        px_by_date[d] = set(
            store.scan_window(d, d).select("code").collect()["code"].to_list()
        )
    carried = 0
    for r in pdl.iter_rows(named=True):
        if r["code"] not in px_by_date.get(r["date"], set()):
            carried += 1
    R["carried_valuation"] = carried

    daily_pos = pdl.group_by("date").agg(
        pl.col("market_value").sum().alias("pv"),
        pl.len().alias("npos"),
    )
    join = ec.join(daily_pos, on="date", how="left").with_columns(
        (pl.col("pv") / pl.col("equity")).alias("gross"))
    R.update({
        "valuation_days": ec.height,
        "curve_range": f"{ec['date'].min()}..{ec['date'].max()}",
        "trades": tr.height,
        "buy": tr.filter(pl.col("side") == "BUY").height,
        "sell": tr.filter(pl.col("side") == "SELL").height,
        "zero_price": M["zero_price"], "non_lot_buy": M["non_lot_buy"],
        "t1_violation": M["t1_violation"],
        "min_cash": float(ec["cash"].min()),
        "max_gross": float(join["gross"].max()) if join["gross"].max() is not None else 0.0,
        "min_equity": float(ec["equity"].min()),
        "max_positions": int(join["npos"].max()) if join["npos"].max() is not None else 0,
        "end_equity": float(ec.tail(1)["equity"][0]),
        "total_return": result.metrics.get("total_return"),
        "max_drawdown": result.metrics.get("max_drawdown"),
        "sel_calls": sel_stats["n"], "sel_time_s": round(sel_stats["t"], 1),
        "M": M,
    })
    return R


def report(R):
    M = R["M"]
    print(f"\n================ {R['year']} 全年质量报告 ================", flush=True)
    print(f"[数据层] 信号日={R['signal_days']} 估值日={R['valuation_days']} "
          f"股票数={R['codes']} 区间={R['range']}")
    print(f"[数据层] derived_suspension(意图口径)={M['derived_suspension']} "
          f"carried_valuation(持仓口径)={R['carried_valuation']}")
    print(f"[交易层] trades={R['trades']} BUY={R['buy']} SELL={R['sell']}")
    print(f"[交易层] partial_fill={M['partial_fill']} rejected={M['rejected']} "
          f"zero_price={R['zero_price']} non_lot_buy={M['non_lot_buy']} "
          f"T1违规={R['t1_violation']}")
    print(f"[交易层] 涨停买尝试={M['limitup_buy_try']} 跌停卖尝试={M['limitdown_sell_try']}")
    print(f"[组合层] min_cash={R['min_cash']:,.2f} max_gross={R['max_gross']:.2%} "
          f"min_equity={R['min_equity']:,.2f} end_equity={R['end_equity']:,.2f} "
          f"max_positions={R['max_positions']}")
    print(f"[组合层] 总收益={R['total_return']:.2%} 最大回撤={R['max_drawdown']:.2%}")
    print(f"[效率层] prep={R['prep_s']}s 回测={R['bt_s']}s "
          f"(选股调用 {R['sel_calls']} 次/{R['sel_time_s']}s)")
    print(f"[效率层] scan calls={M['store_calls']} miss={M['store_misses']} "
          f"HF resolve={M['hf_resolves']}")
    print(flush=True)


if __name__ == "__main__":
    all_ok = True
    for y in (2024, 2025):
        R = run_year(y)
        report(R)
        hard_fail = (R["zero_price"] or R["non_lot_buy"] or R["t1_violation"]
                     or R["min_cash"] < -EPS or R["min_equity"] <= 0)
        if hard_fail:
            all_ok = False
        gc.collect()
    print("QUALITY:", "HARD-CHECK ALL PASS" if all_ok else "HARD-CHECK FAILURES PRESENT")