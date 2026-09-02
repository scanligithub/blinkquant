# Backtest Engine v1 Production Readiness Roadmap

**Date:** 2026-08-28
**Status:** Active
**Author:** OpenCode + User

---

## Context

- **v1.0.3** stable baseline: 314/314 core tests + 57 RQAlpha PoC tests = 371 all green
- **Architecture decision (Route B):** Native BacktestEngine = primary production engine; RQAlpha = optional differential validation backend
- **Key findings from RQAlpha PoC:**
  - 1d frequency fills at CLOSE price (not OPEN) — blinkquant controls T+1 OPEN semantics externally
  - RQAlpha fee defaults: commission_rate=0.0008, min_commission=5.0, tax_rate=0.0005 (after 2023-08-28)
  - RQAlpha corporate action ordering: dividend_book_closure → split → dividend_payable
  - Partial fill: `base.partial_fill_on_insufficient_cash=True` enables lot-aligned fills
  - Commission minimum is order-level (not per-fill)
- **Data sources:**
  - `scanli/stocka-data` HF dataset: `stock_kline_{year}.parquet` (date, code, open, high, low, close, volume, amount, adjustFactor, isST, total_shares, float_shares, total_mv, float_mv, pctChg, turn)
  - `dividends.parquet`: book_closure_date, announcement_date, dividend_cash_before_tax, ex_dividend_date, payable_date, round_lot
  - `splits.parquet`: ex_date, split_factor
  - `stock_basic.parquet`: code, name, list_date, etc.
- **Event ordering issue:** Current loop is signal-before-execution; v1.1 Event State Machine spec designed to resolve

---

## P0 Items (Must-have for v1.0.4)

### P0-1: CorporateActionStore Real Data Integration

**Goal:** Replace synthetic corporate action data with real HF data from `scanli/stocka-data`.

**Current state:** `CorporateActionStore` accepts `CorporateAction` objects in-memory. `BlinkquantParquetDataSource` (RQAlpha backend) already reads `dividends.parquet` and `splits.parquet` from HF.

**Implementation:**
1. Create `backend/core/ca_store.py` — `HFCorporateActionStore` class:
   - Constructor: `(hf_repo: str, token: str | None)` — lazy-loads from HF
   - `load(start_date, end_date)` — downloads and caches `dividends.parquet` + `splits.parquet`
   - `query(code, start_date, end_date)` → `list[CorporateAction]` — converts HF format to `CorporateAction` dataclass
   - `query_all(start_date, end_date)` → `list[CorporateAction]`
2. HF → CorporateAction mapping:
   - `dividends.parquet` → `CASH_DIVIDEND` action, date = `ex_dividend_date`, cash_dividend_per_share = `dividend_cash_before_tax`
   - `splits.parquet` → `STOCK_SPLIT` action, date = `ex_date`, split_factor = `split_factor`
3. Update `BacktestEngine.run()` to accept `hf_repo` + `token` params, auto-create `HFCorporateActionStore` if provided
4. Add `adjust_factor` validation: verify corporate actions reconstruct the adjust_factor series (conservation test)

**Files:**
- NEW: `backend/core/ca_store.py`
- MODIFY: `backend/core/backtest_engine.py` (add hf_repo/token params)
- NEW: `backend/tests/test_hf_ca_store.py`
- NEW: `backend/tests/test_ca_conservation.py`

---

### P0-2: Event State Machine (v1.1 spec implementation)

**Goal:** Refactor `BacktestEngine.run()` event loop to use explicit state machine dispatch, resolving the signal-before-execution ordering issue.

**Current state:** Event loop in `backtest_engine.py:132-471` uses sequential steps within a single `for t in all_days` loop. Signal generation and execution happen in the same iteration.

**Design (from `2026-08-27-backtest-event-state-machine.md`):**
```
PRE_OPEN(CorporateAction→Thaw→ExecutePending)
→ POST_EXECUTION
→ MARKET_CLOSE
→ POST_CLOSE_SIGNAL
→ VALUATION
→ CHECKPOINT
```

**Implementation:**
1. Define `EventPhase` enum: `PRE_OPEN, POST_EXECUTION, MARKET_CLOSE, POST_CLOSE_SIGNAL, VALUATION, CHECKPOINT`
2. Create `_dispatch_phase(phase, t)` method with phase-specific logic
3. `valuation_through` is sole resume authority for cross-day state
4. Separate signal generation (POST_CLOSE_SIGNAL) from execution (PRE_OPEN)
5. Corporate actions execute in PRE_OPEN before thaw

**Files:**
- MODIFY: `backend/core/backtest_engine.py` (refactor run())
- NEW: `backend/tests/test_event_state_machine.py`

---

### P0-3: Raw/QFQ/CA Conservation Tests

**Goal:** Prove `portfolio_value` continuity through price semantic boundaries.

**Tests:**
1. **Raw conservation:** Portfolio value using raw prices is invariant across all operations
2. **QFQ conservation:** Portfolio value using qfq prices equals raw value × cumulative adjust_factor
3. **CA conservation:** Corporate action adjustments preserve total portfolio value (qty × price + cash)
4. **Boundary test:** No single day where `equity ≠ cash + sum(positions_value)` across full 2024Q1

**Files:**
- NEW: `backend/tests/test_price_semantic_conservation.py`

---

### P0-4: FeeSchedule Configuration

**Goal:** Move from hardcoded `FeeConfig` to YAML-based configuration.

**Implementation:**
1. Define YAML schema:
   ```yaml
   fee_schedule:
     - date_start: "2023-08-28"
       commission_rate: 0.0008
       commission_min: 5.0
       stamp_tax_rate: 0.0005
       transfer_fee_rate: 0.00001
   ```
2. Create `backend/core/fee_config.py` — `load_fee_schedule(path: str) -> FeeSchedule`
3. Update `BacktestEngine.__init__` to accept `fee_config_path: str | None`
4. Keep `FeeConfig` dataclass as internal representation

**Files:**
- NEW: `backend/core/fee_config.py`
- NEW: `config/fee_schedule.yaml`
- MODIFY: `backend/core/backtest_engine.py` (add fee_config_path param)
- NEW: `backend/tests/test_fee_config.py`

---

### P0-5: 2024Q1 Golden Case

**Goal:** Generate golden regression artifacts for full-market 2024Q1 backtest.

**Deliverables:**
1. Run full 2024Q1 backtest (node0 shard, ~1,778 stocks)
2. Capture: equity_curve, trades, positions_daily, metrics, execution_diagnostics
3. Store as `tests/golden/2024q1/` — golden files for regression testing
4. Create `tests/test_golden_2024q1.py` — asserts exact match against golden files
5. CI gate: golden case must pass before any release

**Files:**
- NEW: `tests/golden/2024q1/equity_curve.parquet`
- NEW: `tests/golden/2024q1/trades.parquet`
- NEW: `tests/golden/2024q1/metrics.json`
- NEW: `tests/test_golden_2024q1.py`

---

## P1 Items (Nice-to-have for v1.0.4, defer to v1.1 if needed)

### P1-1: SignalTrace Production Wiring
- Wire `TraceRecord` schema (17 fields) into selection engine output
- Add `SignalTrace` to `BacktestResult`
- Schema/API complete, needs wiring only

### P1-2: UniverseFilter Historical Integration
- Integrate `UniverseFilter` (min_listing_days=60, exclude_st=true) into backtest signal filtering
- Currently only in selection engine; needs backtest integration

### P1-3: Checkpoint Cross-Year Continuity
- Verify `export_state()`/`import_state()` works across year boundaries
- Currently C1==C2 equivalence tested; needs full cross-year validation

### P1-4: Full-Market Performance Baseline
- Run 2024 full-year backtest
- Capture Sharpe, max_drawdown, turnover as baseline metrics
- Compare with RQAlpha validation backend (differential check)

---

## Frozen Core Semantics (DO NOT modify)

1. **signal_date / execution_date**: signal_date=T → execution_date=next_trade_day(T)
2. **T+1 freeze/thaw**: BUY freezes qty, thaw next trade day
3. **raw / qfq separation**: qfq for selection, raw for backtest execution/valuation
4. **Execution ordering**: sell_first, then buy
5. **Allocator semantics**: equal_weight, top_n, ranking
6. **No-lookahead contract**: L3-1 signal poisoning, L3-2 settlement, L3-3 truncation, L3-4 suspension

---

## User Directives

- **P0:** no PIT sector backtest
- **P0:** Sector/Industry historical factors deferred
- **v1.0:** Ranking infrastructure kept in core (protocols/types only); research code in `backend/research/ranking/`
- **RQAlpha as optional dependency:** `pip install blinkquant[rqalpha]` pattern recommended
- **HF env vars:** `HF_TOKEN`, `HF_ENDPOINT=https://hf-mirror.com`
- **`backtest_mode=True`:** Forbidden for SECTOR/INDUSTRY/S_CLOSE/S_PCT_CHG fields; raises `UnsupportedInBacktestError`

---

## Implementation Order

| Phase | Item | Estimated Effort | Dependencies |
|-------|------|-----------------|--------------|
| 1 | P0-1: CorporateActionStore real data | Medium | HF data access |
| 2 | P0-3: Conservation tests | Low | P0-1 complete |
| 3 | P0-4: FeeSchedule config | Low | None |
| 4 | P0-2: Event State Machine | High | P0-1 complete |
| 5 | P0-5: 2024Q1 Golden Case | Medium | P0-1, P0-2, P0-3, P0-4 |
| 6 | P1-1: SignalTrace wiring | Low | None |
| 7 | P1-2: UniverseFilter integration | Low | None |
| 8 | P1-3: Cross-year checkpoint | Low | None |
| 9 | P1-4: Full-market performance | Medium | All P0 items |

---

## Exit Criteria for v1.0.4

- [ ] All 314 existing core tests pass
- [ ] All 57 RQAlpha PoC tests pass
- [ ] P0-1: CorporateActionStore reads real HF data, all CA tests pass
- [ ] P0-2: Event State Machine refactored, all tests pass
- [ ] P0-3: Raw/QFQ/CA conservation tests pass
- [ ] P0-4: FeeSchedule configurable via YAML
- [ ] P0-5: 2024Q1 golden case generated and regression tests pass
- [ ] No regressions in existing functionality
