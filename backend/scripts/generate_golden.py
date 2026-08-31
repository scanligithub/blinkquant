"""Generate golden regression-test artifacts for BlinkQuant.

Produces the canonical backtest output (equity curve, trades, positions,
metrics, diagnostics) and writes them alongside metadata.json so that the
determinism test suite can compare against a known-good baseline.

Usage:
    cd backend
    HF_TOKEN=<token> python scripts/generate_golden.py [--start DATE] [--end DATE] ...

Environment variables:
    HF_TOKEN      – HuggingFace API token (required)
    HF_ENDPOINT   – HuggingFace mirror endpoint (optional, for China)
"""
import argparse
import datetime
import hashlib
import json
import os
import sys
import time
from pathlib import Path

# Ensure backend root is on sys.path so that `core.*` imports work.
_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)

import polars as pl
from huggingface_hub import hf_hub_download

from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate golden regression-test artifacts for BlinkQuant.")
    p.add_argument("--start", type=str, default="2024-01-02",
                   help="Start date (default: 2024-01-02)")
    p.add_argument("--end", type=str, default="2024-03-29",
                   help="End signal date (default: 2024-03-29)")
    p.add_argument("--output", type=str, default=None,
                   help="Output directory (default: tests/golden/{year}Q{quarter} derived from --start)")
    p.add_argument("--hf-repo", type=str, default="scanli/stocka-data",
                   help="HuggingFace repo ID (default: scanli/stocka-data)")
    p.add_argument("--cash", type=float, default=10_000_000,
                   help="Initial cash (default: 10000000)")
    p.add_argument("--top-n", type=int, default=20,
                   help="Top N stocks (default: 20)")
    p.add_argument("--formula", type=str, default="CLOSE > MA(CLOSE, 20)",
                   help='Selection formula (default: "CLOSE > MA(CLOSE, 20)")')
    p.add_argument("--rebalance", type=str, default="weekly",
                   choices=["daily", "weekly"],
                   help="Rebalance frequency (default: weekly)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]


def _load_df_daily(
    start_date: datetime.date,
    end_signal_date: datetime.date,
    hf_repo: str,
    token: str,
    dm: DataManager,
) -> None:
    """Download year-sharded kline parquets from HF, build df_daily in-memory.

    Reproduces the production transform pipeline:
    download → shard (node 0) → column prune → limit flags → forward-adjust → resample.
    """
    # Determine which years are needed (MA20 needs ~40 trading days of history before start).
    history_start = start_date - datetime.timedelta(days=60)
    needed_years = list(range(history_start.year, end_signal_date.year + 1))

    parts: list[pl.DataFrame] = []
    for year in needed_years:
        _log(f"downloading {hf_repo}/stock_kline_{year}.parquet ...")
        p = hf_hub_download(
            repo_id=hf_repo,
            filename=f"stock_kline_{year}.parquet",
            repo_type="dataset",
            token=token,
        )
        # Read with column pruning where possible.
        scan = pl.scan_parquet(p)
        available = scan.collect_schema().names()
        use_cols = [c for c in KEEP_COLS if c in available]
        df = scan.select(use_cols).collect()

        # Shard to node 0 (consistent with production single-node mode).
        df = df.filter((df["code"].hash() % dm.total_nodes) == 0)
        parts.append(df)
        _log(f"  {year}: {df.height} rows, {df['code'].n_unique()} codes")
        del df

    df = pl.concat(parts, how="diagonal")
    del parts
    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=True))

    dm.df_daily = df.sort(["code", "date"])
    _log(f"df_daily: {dm.df_daily.height} rows, "
         f"{dm.df_daily['code'].n_unique()} codes, "
         f"range {dm.df_daily['date'].min()}..{dm.df_daily['date'].max()}")

    dm._compute_limit_flags()
    dm._apply_forward_adjustment()
    dm._append_prev_close()
    dm._optimize_memory(dm.df_daily, "df_daily")
    dm._resample_all()


def _install_dm(dm: DataManager) -> None:
    """Wire the local DataManager into the global singletons."""
    import core.data_manager as _mod
    _mod.data_manager.df_daily = dm.df_daily
    _mod.data_manager.df_weekly = dm.df_weekly
    _mod.data_manager.df_monthly = dm.df_monthly
    _mod.data_manager.df_mapping = None
    _mod.data_manager._asof_frame_cache.clear()
    selection_engine._set_cache.clear()


def _cleanup_dm() -> None:
    """Release memory held by global singletons."""
    import core.data_manager as _mod
    _mod.data_manager.df_daily = None
    _mod.data_manager.df_weekly = None
    _mod.data_manager.df_monthly = None
    _mod.data_manager._asof_frame_cache.clear()


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------

def _compute_fingerprint(
    formula: str, rebalance_freq: str, top_n: int,
    allocator_name: str, initial_cash: float,
) -> str:
    """SHA-256 of strategy config (deterministic, JSON sorted)."""
    cfg = {
        "formula": formula,
        "rebalance_freq": rebalance_freq,
        "top_n": top_n,
        "allocator": allocator_name,
        "initial_cash": initial_cash,
    }
    return hashlib.sha256(
        json.dumps(cfg, sort_keys=True).encode()
    ).hexdigest()


# ---------------------------------------------------------------------------
# JSON safety
# ---------------------------------------------------------------------------

def _json_safe(obj):
    """Recursively convert date/datetime keys and values to strings for JSON."""
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    elif isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    return obj


# ---------------------------------------------------------------------------
# Metrics (inline from contract formulas)
# ---------------------------------------------------------------------------

def _compute_metrics_json(ec: pl.DataFrame) -> dict:
    """Compute the 5 contract metrics from equity curve."""
    if ec.is_empty():
        return {"total_return": 0, "cagr": 0, "sharpe": 0,
                "max_drawdown": 0, "total_days": 0}
    eq = ec["equity"]
    first = float(eq.head(1)[0])
    last = float(eq.tail(1)[0])
    n = ec.height
    total_return = (last / first) - 1 if first > 0 else 0.0
    years = n / 252
    cagr = ((1 + total_return) ** (1 / years) - 1) if years > 0 else 0.0

    returns = eq.pct_change().drop_nulls()
    std = float(returns.std()) if len(returns) > 0 else 0.0
    sharpe = (float(returns.mean()) / std * (252 ** 0.5)) if std > 0 else 0.0

    peak = eq.cum_max()
    drawdown = (eq - peak) / peak
    max_dd = float(drawdown.min()) if len(returns) > 0 else 0.0

    return {
        "total_return": round(total_return, 10),
        "cagr": round(cagr, 10),
        "sharpe": round(sharpe, 10),
        "max_drawdown": round(max_dd, 10),
        "total_days": n,
    }


# ---------------------------------------------------------------------------
# HF snapshot helper
# ---------------------------------------------------------------------------

def _get_hf_snapshot(hf_repo: str, token: str) -> str | None:
    """Try to fetch the current commit SHA of the HF dataset; return None on failure."""
    try:
        from huggingface_hub import HfApi
        api = HfApi()
        info = api.repo_info(hf_repo, repo_type="dataset", token=token)
        return info.sha
    except Exception as e:
        _log(f"Warning: could not fetch HF snapshot: {e}")
        return None


# ---------------------------------------------------------------------------
# Engine version
# ---------------------------------------------------------------------------

def _get_engine_version() -> str:
    """Read the latest git tag/describe output."""
    try:
        import subprocess
        out = subprocess.check_output(
            ["git", "describe", "--tags", "--always"],
            cwd=_BACKEND_ROOT, stderr=subprocess.DEVNULL,
        ).decode().strip()
        return out
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    start_date = datetime.date.fromisoformat(args.start)
    end_signal_date = datetime.date.fromisoformat(args.end)
    # Default output: ../tests/golden/{year}Q{quarter} (relative to backend/ where this script runs).
    if args.output:
        output_dir = Path(args.output)
    else:
        qtr = (start_date.month - 1) // 3 + 1
        output_dir = Path(_BACKEND_ROOT).parent / "tests" / "golden" / f"{start_date.year}q{qtr}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- env -----------------------------------------------------------
    token = os.getenv("HF_TOKEN")
    if not token:
        print("ERROR: HF_TOKEN environment variable not set.", file=sys.stderr)
        sys.exit(1)

    # Optional HF_ENDPOINT mirror (picked up automatically by hf_hub_download).
    hf_endpoint = os.getenv("HF_ENDPOINT")
    if hf_endpoint:
        os.environ["HF_ENDPOINT"] = hf_endpoint
        _log(f"Using HF mirror: {hf_endpoint}")

    # --- load data -----------------------------------------------------
    _log(f"Loading data from {args.hf_repo} ...")
    dm = DataManager()
    dm.repo_id = args.hf_repo

    _load_df_daily(start_date, end_signal_date, args.hf_repo, token, dm)

    # Extract trading dates for the calendar.
    trade_dates = sorted(
        dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list()
    )
    calendar = TradingCalendar()
    calendar.set_trade_dates(trade_dates)
    _log(f"Calendar: {len(trade_dates)} trade dates "
         f"({trade_dates[0]} .. {trade_dates[-1]})")

    # --- wire globals --------------------------------------------------
    _install_dm(dm)

    # --- build engine components ---------------------------------------
    raw_store = RawPriceStore(hf_repo_id=args.hf_repo, hf_token=token)

    fee_schedule_path = Path(_BACKEND_ROOT).parent / "config" / "fee_schedule.yaml"
    if not fee_schedule_path.exists():
        fee_schedule_path = Path(_BACKEND_ROOT) / "config" / "fee_schedule.yaml"
    fee_schedule = load_fee_schedule(fee_schedule_path)

    allocator = top_n_equal_weight_allocator(args.top_n)

    engine = BacktestEngine(
        calendar=calendar,
        selection_engine=selection_engine,
        raw_price_store=raw_store,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=allocator,
    )

    # --- run backtest --------------------------------------------------
    _log(f"Running backtest: {args.formula} | {args.rebalance} | "
         f"top_n={args.top_n} | cash={args.cash:,.0f}")
    _log(f"  date range: {start_date} .. {end_signal_date}")

    try:
        result = engine.run(
            formula=args.formula,
            start_date=start_date,
            end_signal_date=end_signal_date,
            initial_cash=args.cash,
            rebalance_freq=args.rebalance,
            top_n=args.top_n,
            fee_schedule=fee_schedule,
        )
    finally:
        _cleanup_dm()

    # --- compute derived artifacts --------------------------------------
    metrics_json = _compute_metrics_json(result.equity_curve)

    diagnostics_json = result.execution_diagnostics or {}

    # --- write parquet artifacts ----------------------------------------
    ec_path = output_dir / "equity_curve.parquet"
    result.equity_curve.write_parquet(ec_path)
    _log(f"wrote {ec_path} ({result.equity_curve.height} rows)")

    tr_path = output_dir / "trades.parquet"
    result.trades.write_parquet(tr_path)
    _log(f"wrote {tr_path} ({result.trades.height} rows)")

    pd_path = output_dir / "positions_daily.parquet"
    result.positions_daily.write_parquet(pd_path)
    _log(f"wrote {pd_path} ({result.positions_daily.height} rows)")

    # --- write json artifacts -------------------------------------------
    m_path = output_dir / "metrics.json"
    with open(m_path, "w", encoding="utf-8") as f:
        json.dump(metrics_json, f, indent=2)
    _log(f"wrote {m_path}")

    d_path = output_dir / "diagnostics.json"
    with open(d_path, "w", encoding="utf-8") as f:
        json.dump(_json_safe(diagnostics_json), f, indent=2)
    _log(f"wrote {d_path}")

    # --- update metadata.json -------------------------------------------
    fingerprint = _compute_fingerprint(
        args.formula, args.rebalance, args.top_n,
        "top_n_equal_weight", args.cash,
    )
    engine_version = _get_engine_version()
    hf_snapshot = _get_hf_snapshot(args.hf_repo, token)

    # Read existing metadata to preserve artifact schema info.
    meta_path = output_dir / "metadata.json"
    if meta_path.exists():
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    else:
        meta = {}

    meta["schema_version"] = "1.0.0"
    meta["engine_version"] = engine_version
    meta["generated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    meta["strategy"] = {
        "name": "MA20_CSI300_TOP20",
        "formula": args.formula,
        "rebalance_freq": args.rebalance,
        "top_n": args.top_n,
        "allocator": "top_n_equal_weight",
        "initial_cash": args.cash,
        "fingerprint": fingerprint,
    }

    meta["data"] = {
        "source": "huggingface",
        "hf_repo": args.hf_repo,
        "snapshot": hf_snapshot,
        "universe": "csi300",
        "start_date": args.start,
        "end_signal_date": args.end,
    }

    # Extract effective rates from the loaded fee schedule for the start date.
    eff_fc = fee_schedule.get_fee_config(start_date)
    meta["fee_schedule"] = {
        "path": "config/fee_schedule.yaml",
        "effective_rates": {
            "commission_rate": eff_fc.commission_rate,
            "commission_min": eff_fc.commission_min,
            "stamp_tax_rate": eff_fc.stamp_tax_rate,
            "transfer_fee_rate": eff_fc.transfer_fee_rate,
        },
    }

    meta["reproducibility"] = {
        "script": "backend/scripts/generate_golden.py",
        "env_vars": ["HF_TOKEN", "HF_ENDPOINT"],
        "command": "cd backend && python scripts/generate_golden.py",
    }

    meta["artifacts"] = {
        "equity_curve": {
            "file": "equity_curve.parquet",
            "columns": ["date", "equity", "cash", "positions_value", "signal_date"],
            "types": ["Date", "Float64", "Float64", "Float64", "Date"],
        },
        "trades": {
            "file": "trades.parquet",
            "columns": ["signal_date", "execution_date", "code", "side",
                         "qty", "price", "fee"],
            "types": ["Date", "Date", "Utf8", "Utf8", "Int64", "Float64", "Float64"],
        },
        "positions_daily": {
            "file": "positions_daily.parquet",
            "columns": ["date", "code", "qty", "cost", "market_value"],
            "types": ["Date", "Utf8", "Int64", "Float64", "Float64"],
        },
        "metrics": {
            "file": "metrics.json",
            "required_keys": ["total_return", "cagr", "sharpe",
                              "max_drawdown", "total_days"],
        },
        "diagnostics": {
            "file": "diagnostics.json",
            "required_keys": ["rej_counters", "intents_total",
                              "partial_fill_count", "carried_events"],
        },
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    _log(f"wrote {meta_path}")

    # --- summary -------------------------------------------------------
    _log("=" * 60)
    _log("GOLDEN ARTIFACTS GENERATED SUCCESSFULLY")
    _log(f"  output dir       : {output_dir}")
    _log(f"  equity curve     : {result.equity_curve.height} rows, "
         f"{ec_path.stat().st_size / 1024:.1f} KB")
    _log(f"  trades           : {result.trades.height} rows, "
         f"{tr_path.stat().st_size / 1024:.1f} KB")
    _log(f"  positions_daily  : {result.positions_daily.height} rows, "
         f"{pd_path.stat().st_size / 1024:.1f} KB")
    _log(f"  metrics          : {metrics_json}")
    _log(f"  diagnostics      : intents={diagnostics_json.get('intents_total', 0)}, "
         f"rej={sum(diagnostics_json.get('rej_counters', {}).values())}")
    _log(f"  fingerprint      : {fingerprint[:16]}...")
    _log(f"  engine_version   : {engine_version}")
    _log(f"  hf_snapshot      : {(hf_snapshot or 'N/A')[:16]}...")
    _log("=" * 60)


if __name__ == "__main__":
    main()
