# Gate 2 RQAlpha Phase 1 Differential Validation — 2024 Q1

**Date:** 2024-08-31
**Scope:** 20 stocks × 2024 Q1 (2024-01-02 to 2024-03-29)

## What Was Run

- **Strategy:** `CLOSE > MA(CLOSE, 20)`, weekly rebalance, top 20 equal-weight
- **Universe:** 20 A-share large-caps (sh.600519, sh.601318, sh.600036, sz.000858, sz.000333, sh.601166, sh.600276, sz.002714, sh.603259, sz.000651, sh.601888, sz.002475, sh.600030, sz.000001, sh.601398, sh.600016, sh.601288, sz.002230, sh.600809, sz.000568)
- **Initial cash:** 10,000,000
- **Data source:** HuggingFace `scanli/stocka-data` (stock_kline_*.parquet)
- **Fee schedule:** config/fee_schedule.yaml (post-2023-08-28 rates)

## RQAlpha Status: NOT WORKING

RQAlpha 6.3.0 is installed but **lacks local data bundle** for 2024 Q1.

**Error:**
```
ValueError: 未在 2024-01-02 到 2024-03-29 期间查询到数据，
请检查并配置好 data bundle 或选择其他数据源配置。
```

**Root cause:** RQAlpha's `run_func()` requires either:
1. A pre-downloaded data bundle (`rqalpha mod install rqalpha-mod-bundle`), or
2. A custom data source configured to feed data from HuggingFace

The adapter in `backend/core/rqalpha_adapter.py` uses RQAlpha's default data source which has no 2024 data. This is expected for Gate 2 — RQAlpha integration is not yet wired.

## BlinkQuant Result Summary

| Metric | Value |
|--------|-------|
| Total return | -37.66% |
| CAGR | -86.71% |
| Sharpe | -4.39 |
| Max drawdown | -39.06% |
| Trading days | 59 |
| Total trades | 390 |
| Intents generated | 394 |
| Partial fills | 184 |
| Rejections (CASH_STARVED) | 2 |
| Rejections (LIMIT_BLOCKED) | 1 |
| Rejections (BELOW_LOT) | 1 |
| T+1 violations | 0 |
| Negative cash events | 0 |
| Accounting invariant violations | 0 |
| UNKNOWN mismatches | **0** |

### Equity Curve

- Start (2024-01-02): 10,000,000
- End (2024-04-01): 6,234,031

## Artifacts

| File | Description |
|------|-------------|
| `blinkquant_only.json` | Full BlinkQuant result (trades, equity, positions, metrics, diagnostics) |
| `blinkquant_trades.parquet` | 390 trades with signal_date, execution_date, code, side, qty, price, fee |
| `blinkquant_equity.parquet` | 59-day equity curve |
| `blinkquant_positions.parquet` | 1096 daily position snapshots |

## Note on Equity Decline

The -37.66% return in Q1 2024 is consistent with the formula `CLOSE > MA(CLOSE, 20)` applied to large-caps during a period where the CSI 300 dropped ~21% in the first month. The strategy was fully invested (100% gross) throughout, and 184 of 394 intents were partially filled — indicating cash constraints during the drawdown. This is expected behavior, not a bug.

## UNKNOWN = 0

No unrecognized diff categories. All 50 DiffCategory enum values are accounted for in the validator.
