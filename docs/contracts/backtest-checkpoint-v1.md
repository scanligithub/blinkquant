# BlinkQuant BacktestCheckpoint Contract v1.0

## Purpose

Define the canonical, versioned, portable checkpoint format for BacktestEngine.

**Design principles:**

- No pickle, no Python object graph
- JSON for metadata, Parquet for columnar data
- Forward/backward compatible within major version
- Deterministic serialization (sorted keys, canonical ordering)

---

## File Layout

```
checkpoint/
├── meta.json              # schema_version, engine_version, timestamp, date, phase
├── portfolio.json         # cash + positions (columnar-friendly)
├── pending.json           # in-flight intents (signal→execution pipeline)
├── corporate_actions.json # processed CA cursor
├── diagnostics.json       # accumulated diagnostics
├── last_close.parquet     # code, close (carry-forward valuation)
└── engine_state.json      # thru_thaw, selected_thru, random_seed
```

---

## Schema Definitions

### 1. meta.json

```json
{
  "schema_version": "1.0.0",
  "engine_version": "v1.0.3-9-g43547cc",
  "created_at": "2024-02-15T10:30:00Z",
  "current_date": "2024-02-15",
  "phase": "CHECKPOINT",
  "description": "Checkpoint at end of 2024-02-15"
}
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| schema_version | string | ✅ | Contract version (semver) |
| engine_version | string | ✅ | Git describe output |
| created_at | string (ISO8601) | ✅ | UTC timestamp |
| current_date | string (YYYY-MM-DD) | ✅ | Trading date of this checkpoint |
| phase | string | ✅ | EventPhase enum value |
| description | string | ❌ | Human-readable note |

---

### 2. portfolio.json

```json
{
  "cash": 123456.78,
  "positions": [
    {
      "code": "sh.600000",
      "total_qty": 10000,
      "available_qty": 9800,
      "frozen_qty": 200,
      "avg_cost": 12.34,
      "market_value": 123400.0
    }
  ]
}
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| cash | float | ✅ | Available cash |
| positions[].code | string | ✅ | Stock code |
| positions[].total_qty | int64 | ✅ | Total shares held |
| positions[].available_qty | int64 | ✅ | Shares available for SELL (T+1) |
| positions[].frozen_qty | int64 | ✅ | Shares frozen (T+1 BUY) |
| positions[].avg_cost | float | ✅ | Average cost per share |
| positions[].market_value | float | ❌ | Current market value (recomputed on restore) |

**Constraints:**

- `available_qty + frozen_qty == total_qty`
- `avg_cost >= 0`
- `cash >= -1e-6`

---

### 3. pending.json

```json
{
  "signal_date": "2024-02-15",
  "execution_date": "2024-02-19",
  "intents": [
    {"code": "sh.600000", "side": "BUY", "target_qty": 5000, "target_weight": 0.05}
  ],
  "prices": {
    "sh.600000": {"open": 12.50, "close": 12.60}
  }
}
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| signal_date | string (YYYY-MM-DD) | ❌ | Signal generation date |
| execution_date | string (YYYY-MM-DD) | ❌ | Scheduled execution date (T+1) |
| intents[].code | string | ✅ | Stock code |
| intents[].side | string | ✅ | "BUY" or "SELL" |
| intents[].target_qty | int64 | ✅ | Target quantity (pre lot-size) |
| intents[].target_weight | float | ✅ | Target portfolio weight |
| prices.{code}.open | float | ✅ | Execution date open price |
| prices.{code}.close | float | ✅ | Execution date close price |

**Constraints:**

- If `signal_date` is null, all other fields must be null/empty
- `execution_date > signal_date` (T+1)
- `intents` sorted by `(code, side)` for determinism

---

### 4. corporate_actions.json

```json
{
  "processed_dividends": [
    {"code": "sh.600000", "ex_date": "2024-02-01", "record_date": "2024-01-31", "pay_date": "2024-02-15", "amount": 0.5}
  ],
  "processed_splits": [
    {"code": "sh.600000", "ex_date": "2024-02-01", "ratio": 2.0}
  ],
  "cursor_date": "2024-02-15"
}
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| processed_dividends[].code | string | ✅ | Stock code |
| processed_dividends[].ex_date | string (YYYY-MM-DD) | ✅ | Ex-dividend date |
| processed_dividends[].record_date | string (YYYY-MM-DD) | ✅ | Record date |
| processed_dividends[].pay_date | string (YYYY-MM-DD) | ✅ | Payment date |
| processed_dividends[].amount | float | ✅ | Dividend per share |
| processed_splits[].code | string | ✅ | Stock code |
| processed_splits[].ex_date | string (YYYY-MM-DD) | ✅ | Ex-split date |
| processed_splits[].ratio | float | ✅ | Split ratio (e.g., 2.0 for 2-for-1) |
| cursor_date | string (YYYY-MM-DD) | ✅ | All CA before this date have been processed |

**Purpose:** Prevent duplicate CA processing on resume.

---

### 5. diagnostics.json

```json
{
  "rej_counters": {"CASH_STARVED": 2, "LIMIT_BLOCKED": 1},
  "intents_total": 156,
  "partial_fill_count": 12,
  "carried_events": 0,
  "zero_price_trade_count": 0,
  "t1_violation_count": 0,
  "negative_cash_count": 0,
  "accounting_invariant_violations": 0,
  "target_gross_by_date": {"2024-02-15": 1.0, "2024-02-16": 1.0}
}
```

**Fields:** All accumulated scalar diagnostics from `diag` dict.

---

### 6. last_close.parquet

| Column | Type | Description |
|--------|------|-------------|
| code | Utf8 | Stock code |
| close | Float64 | Last known raw close price |

Used for carry-forward valuation of suspended stocks.

---

### 7. engine_state.json

```json
{
  "thru_thaw": "2024-02-14",
  "selected_thru": "2024-02-15",
  "random_seed": 42
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| thru_thaw | string (YYYY-MM-DD) | ❌ | Last date where thaw was processed |
| selected_thru | string (YYYY-MM-DD) | ❌ | Last date where selection ran |
| random_seed | int64 | ❌ | Deterministic seed (currently unused, reserved) |

---

## Serialization Rules

1. **JSON**: `json.dumps(obj, sort_keys=True, separators=(',', ':'), default=str)`
2. **Dates**: ISO8601 `YYYY-MM-DD` strings
3. **Parquet**: `pl.DataFrame.write_parquet(compression="zstd")`
3. **Ordering**: All arrays sorted by natural key (`code`, then `side`, then `date`)
4. **Floats**: Round to 10 decimal places for determinism

---

## Restore Rules

1. Validate `schema_version` compatibility (major must match)
2. Reconstruct `Portfolio` via `import_state`
3. Reconstruct `last_close` dict from parquet
4. Reconstruct `pending` intents as `OrderIntent` objects
5. Set engine cursors (`_thru_thaw`, `_selected_thru`, `_pend_*`)
6. Set CA cursor from `corporate_actions.json`
7. Restore `diag` accumulators
8. Continue main loop from `current_date + 1 day`

---

## Version Compatibility

| Checkpoint schema | Engine schema | Compatible |
|-------------------|---------------|------------|
| 1.x | 1.x | ✅ Yes |
| 1.x | 2.x | ❌ No (major mismatch) |
| 2.x | 1.x | ❌ No |

Minor/patch version changes must be additive only (new optional fields).

---

## Implementation Files

- `backend/core/checkpoint.py` — `BacktestCheckpoint` dataclass + `save/load`
- `backend/core/backtest_engine.py` — `export_checkpoint/load_checkpoint` methods
- `backend/tests/test_checkpoint_determinism.py` — regression tests