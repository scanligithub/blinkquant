# Golden Artifact Contract Specification

> **Version:** 1.0.0
> **Date:** 2026-08-28
> **Status:** Active
> **Supersedes:** None

## 1. Purpose

This document defines the golden artifact contract for BlinkQuant's 2024Q1 regression test suite. Golden artifacts serve as the v1.0 release gate — proving the backtest engine produces correct, reproducible results on real market data.

## 2. Schema Versioning

### 2.1 Semantic Versioning

The `schema_version` field follows Semantic Versioning 2.0.0:

| Change Type | Version Bump | Example |
|-------------|--------------|---------|
| **Breaking change** | Major (X.0.0) | Column removed, column type changed, key structure changed |
| **Backward-compatible addition** | Minor (0.X.0) | New column added, new optional key in JSON |
| **Documentation/metadata only** | Patch (0.0.X) | Description updated, example corrected |

### 2.2 Current Schema Version

```
schema_version: "1.0.0"
```

## 3. Artifact Contract

### 3.1 Directory Structure

```
tests/golden/2024q1/
├── metadata.json                 # Strategy params, data version, generation timestamp
├── equity_curve.parquet          # Daily equity curve
├── trades.parquet                # All executed trades
├── positions_daily.parquet       # Daily position snapshots
├── metrics.json                  # Summary performance metrics
└── diagnostics.json              # Execution diagnostics
```

### 3.2 metadata.json Schema

```json
{
  "schema_version": "string (semver)",
  "engine_version": "string (git tag)",
  "generated_at": "string (ISO 8601) | null",
  "strategy": {
    "name": "string",
    "formula": "string (BlinkQuant DSL)",
    "rebalance_freq": "string (daily | weekly)",
    "top_n": "integer",
    "allocator": "string (function name)",
    "initial_cash": "number (CNY)",
    "fingerprint": "string (SHA-256 of strategy config)"
  },
  "data": {
    "source": "string (huggingface | local)",
    "hf_repo": "string (HF dataset repo ID)",
    "snapshot": "string | null (HF dataset version/commit hash)",
    "universe": "string (csi300 | all)",
    "start_date": "string (ISO date YYYY-MM-DD)",
    "end_signal_date": "string (ISO date YYYY-MM-DD)"
  },
  "fee_schedule": {
    "path": "string (relative path to fee config)",
    "effective_rates": {
      "commission_rate": "number",
      "commission_min": "number",
      "stamp_tax_rate": "number",
      "transfer_fee_rate": "number"
    }
  },
  "reproducibility": {
    "script": "string (relative path to generation script)",
    "env_vars": ["array of required environment variables"],
    "command": "string (shell command to regenerate)"
  },
  "artifacts": {
    "<artifact_name>": {
      "file": "string (filename)",
      "columns": ["array of column names"],
      "types": ["array of Polars type strings"]
    }
  }
}
```

**Required Fields:**
- `schema_version`
- `engine_version`
- `generated_at`
- `strategy`
- `data`
- `fee_schedule`
- `reproducibility`
- `artifacts`

### 3.3 equity_curve.parquet

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `date` | Date | No | Trading date (execution date) |
| `equity` | Float64 | No | Total portfolio equity (cash + positions) |
| `cash` | Float64 | No | Cash balance |
| `positions_value` | Float64 | No | Total market value of positions |
| `signal_date` | Date | Yes | Signal date that triggered trades on this execution date; null if no trades |

**Invariants:**
- `equity = cash + positions_value` (within 1e-9 tolerance)
- `cash >= 0` (always, or engine throws BacktestLedgerError)
- Rows sorted by `date` ascending
- One row per trading day in `[start_date, last_execution_date]`

### 3.4 trades.parquet

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `signal_date` | Date | No | Date the selection signal was generated |
| `execution_date` | Date | No | Date the trade was executed (T+1 from signal) |
| `code` | Utf8 | No | Stock code (e.g., "sh.600000") |
| `side` | Utf8 | No | "BUY" or "SELL" |
| `qty` | Int64 | No | Executed quantity (shares); BUY qty % 100 == 0 |
| `price` | Float64 | No | Execution price (raw open price) |
| `fee` | Float64 | No | Total fee (commission + stamp tax + transfer fee) |

**Invariants:**
- `execution_date > signal_date` (T+1 constraint)
- BUY trades: `qty % 100 == 0` (lot size compliance)
- `fee >= 0`
- `price > 0`
- Sorted by `(execution_date, code, side)` for deterministic comparison

### 3.5 positions_daily.parquet

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `date` | Date | No | Trading date |
| `code` | Utf8 | No | Stock code |
| `qty` | Int64 | No | Total quantity held (total_qty) |
| `cost` | Float64 | No | Average cost per share (avg_cost) |
| `market_value` | Float64 | No | Current market value |

**Invariants:**
- `qty > 0` (only non-zero positions recorded)
- `market_value = qty * close_price` (for that day)
- Sorted by `(date, code)` for deterministic comparison

### 3.6 metrics.json

```json
{
  "total_return": "number (float)",
  "cagr": "number (float)",
  "sharpe": "number (float)",
  "max_drawdown": "number (float, negative)",
  "total_days": "integer"
}
```

**Formulas:**
- `total_return = (last_equity / first_equity) - 1`
- `cagr = (1 + total_return) ^ (252 / total_days) - 1`
- `sharpe = mean(daily_returns) / std(daily_returns) * sqrt(252)`
- `max_drawdown = min((equity - peak) / peak)`
- `total_days = len(equity_curve)`

### 3.7 diagnostics.json

```json
{
  "rej_counters": {
    "<reason>": "integer (count)"
  },
  "intents_total": "integer",
  "partial_fill_count": "integer",
  "carried_events": "integer",
  "zero_price_trade_count": "integer",
  "t1_violation_count": "integer",
  "negative_cash_count": "integer (must be 0)",
  "accounting_invariant_violations": "integer (must be 0)",
  "target_gross_by_date": {
    "<execution_date>": "number (sum of target weights)"
  }
}
```

**Invariants:**
- `negative_cash_count == 0` (engine throws if violated)
- `accounting_invariant_violations == 0` (engine throws if violated)

## 4. Comparison Rules

### 4.1 Float Tolerance

| Artifact | Field | Tolerance | Rationale |
|----------|-------|-----------|-----------|
| equity_curve | equity, cash, positions_value | 1e-6 | Floating point accumulation over ~60 days |
| trades | price | 1e-6 | Raw open price, minimal precision loss |
| trades | fee | 0.01 (1 cent) | Fee calculation involves multiple rate multiplications |
| positions_daily | cost | 1e-6 | Average cost accumulation |
| positions_daily | market_value | 1e-6 | Derived from qty * price |
| metrics | all numeric | 1e-6 | Summary statistics |

### 4.2 Exact Comparison

| Artifact | Field | Comparison | Rationale |
|----------|-------|------------|-----------|
| trades | qty | Exact (Int64) | Share count must be exact |
| trades | signal_date, execution_date | Exact (Date) | Date semantics must match |
| trades | code, side | Exact (Utf8) | String identity |
| positions_daily | date, code | Exact | Composite key |
| positions_daily | qty | Exact (Int64) | Share count |
| equity_curve | date | Exact (Date) | Row identity |

### 4.3 Trade Comparison Keys

Trades are compared using the composite key: `(execution_date, code, side)`.

This means:
- Two trades on the same date, same stock, same side are considered the same trade
- If the engine produces a different number of trades for the same key, the test fails
- Trade ordering within the same key is not compared (sorted before comparison)

### 4.4 Position Comparison Keys

Positions are compared using the composite key: `(date, code)`.

### 4.5 Equity Curve Comparison

Equity curves are compared row-by-row after sorting by `date`. All numeric columns use float tolerance.

## 5. Skip Behavior

### 5.1 Missing Artifacts

When golden artifact files are missing (not generated yet, or not committed to git), tests MUST use `pytest.skip()` with a descriptive message:

```python
def _load_golden(name: str):
    path = GOLDEN_DIR / name
    if not path.exists():
        pytest.skip(f"Golden artifact not found: {path}")
    # ... load and return
```

### 5.2 Missing HF_TOKEN

When `HF_TOKEN` environment variable is not set, determinism tests that require re-running the backtest MUST skip:

```python
token = os.getenv("HF_TOKEN")
if not token:
    pytest.skip("HF_TOKEN not set")
```

### 5.3 Rationale

Golden artifacts are stored in git (small files, ~200KB total). However, the generation script requires HF data access. Skip behavior allows:
- CI to run golden tests even without HF access (artifact validation only)
- Local development to skip generation when not needed
- Full determinism testing when HF_TOKEN is available

## 6. Reproducibility

### 6.1 Requirements

Any person with `HF_TOKEN` access to `scanli/stocka-data` must be able to regenerate identical artifacts by running:

```bash
cd backend
HF_TOKEN=<token> python scripts/generate_golden.py
```

### 6.2 Determinism Guarantees

- **Strategy:** Deterministic (MA20 crossover, weekly rebalance, top-20 equal weight)
- **Allocation:** Deterministic (code ascending sort, equal weight)
- **Execution:** Deterministic (open price, sell-first, same-cycle cash reinvestment)
- **Data:** Point-in-time safe (HF dataset snapshot)

### 6.3 Fingerprint

The `strategy.fingerprint` field is set to the literal string `"RECOMPUTE_ON_GENERATION"`. The actual fingerprint (SHA-256 hash of strategy config: formula, rebalance_freq, top_n, allocator, initial_cash) is recomputed at generation time and validated at test time. This allows quick comparison of whether two metadata files describe the same strategy without comparing all fields.

## 7. Data Versioning

### 7.1 HF Snapshot

The `data.snapshot` field should contain the HF dataset commit hash or version tag. This ensures:
- Identical data is used for regeneration
- Data updates trigger artifact regeneration
- Audit trail for which data version produced which results

### 7.2 Engine Version

The `engine_version` field links artifacts to the code that produced them. When engine behavior changes:
1. Bump engine version (git tag)
2. Regenerate golden artifacts
3. Update `engine_version` in metadata.json
4. Commit new artifacts

## 8. Schema Evolution

### 8.1 Adding a Column

When adding a column to an existing artifact:
1. Bump `schema_version` minor version (e.g., 1.0.0 → 1.1.0)
2. Update `artifacts.<name>.columns` and `artifacts.<name>.types`
3. Regenerate golden artifacts
4. Update comparison code to handle new column

### 8.2 Removing a Column

When removing a column from an existing artifact:
1. Bump `schema_version` major version (e.g., 1.0.0 → 2.0.0)
2. Update `artifacts.<name>.columns` and `artifacts.<name>.types`
3. Regenerate golden artifacts
4. Update comparison code to remove column handling

### 8.3 Changing Column Type

When changing a column type (e.g., Int64 → Float64):
1. Bump `schema_version` major version (e.g., 1.0.0 → 2.0.0)
2. Update `artifacts.<name>.types`
3. Regenerate golden artifacts
4. Update comparison code for new type

### 8.4 Adding a New Artifact

When adding a new artifact file:
1. Bump `schema_version` minor version (e.g., 1.0.0 → 1.1.0)
2. Add new entry in `artifacts` section
3. Regenerate golden artifacts
4. Add comparison code for new artifact

## 9. Implementation Notes

### 9.1 File Sizes (Expected)

| Artifact | Rows | Size |
|----------|------|------|
| equity_curve.parquet | ~57 | ~5 KB |
| trades.parquet | ~200-500 | ~50 KB |
| positions_daily.parquet | ~1000-2000 | ~100 KB |
| metrics.json | 1 | ~0.2 KB |
| diagnostics.json | 1 | ~1 KB |
| metadata.json | 1 | ~1 KB |
| **Total** | | **~160 KB** |

### 9.2 Git Commit

Golden artifacts are committed to git. The total size (~160 KB) is small enough for version control. Artifacts should be committed:
- After initial generation
- After any strategy change
- After any engine change that affects results

### 9.3 CI Integration

Golden regression tests run in CI with:
- Artifact validation (column presence, type checks, invariant checks)
- Determinism testing (when HF_TOKEN is available)
- Skip behavior (when artifacts are missing)

## 10. Metadata.json Formal Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Golden Artifact Metadata",
  "description": "Metadata for BlinkQuant golden regression test artifacts",
  "type": "object",
  "required": [
    "schema_version",
    "engine_version",
    "generated_at",
    "strategy",
    "data",
    "fee_schedule",
    "reproducibility",
    "artifacts"
  ],
  "properties": {
    "schema_version": {
      "type": "string",
      "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$",
      "description": "Semantic version of the artifact schema"
    },
    "engine_version": {
      "type": "string",
      "description": "BlinkQuant engine version (git tag)"
    },
    "generated_at": {
      "type": ["string", "null"],
      "format": "date-time",
      "description": "ISO 8601 timestamp when artifacts were generated; null if not yet generated"
    },
    "strategy": {
      "type": "object",
      "required": ["name", "formula", "rebalance_freq", "top_n", "allocator", "initial_cash", "fingerprint"],
      "properties": {
        "name": { "type": "string" },
        "formula": { "type": "string" },
        "rebalance_freq": { "type": "string", "enum": ["daily", "weekly"] },
        "top_n": { "type": "integer", "minimum": 1 },
        "allocator": { "type": "string" },
        "initial_cash": { "type": "number", "minimum": 0 },
        "fingerprint": { "type": "string", "description": "SHA-256 hash of strategy config" }
      }
    },
    "data": {
      "type": "object",
      "required": ["source", "hf_repo", "snapshot", "universe", "start_date", "end_signal_date"],
      "properties": {
        "source": { "type": "string", "enum": ["huggingface", "local"] },
        "hf_repo": { "type": "string" },
        "snapshot": { "type": ["string", "null"] },
        "universe": { "type": "string", "enum": ["csi300", "all"] },
        "start_date": { "type": "string", "format": "date" },
        "end_signal_date": { "type": "string", "format": "date" }
      }
    },
    "fee_schedule": {
      "type": "object",
      "required": ["path", "effective_rates"],
      "properties": {
        "path": { "type": "string" },
        "effective_rates": {
          "type": "object",
          "required": ["commission_rate", "commission_min", "stamp_tax_rate", "transfer_fee_rate"],
          "properties": {
            "commission_rate": { "type": "number" },
            "commission_min": { "type": "number" },
            "stamp_tax_rate": { "type": "number" },
            "transfer_fee_rate": { "type": "number" }
          }
        }
      }
    },
    "reproducibility": {
      "type": "object",
      "required": ["script", "env_vars", "command"],
      "properties": {
        "script": { "type": "string" },
        "env_vars": { "type": "array", "items": { "type": "string" } },
        "command": { "type": "string" }
      }
    },
    "artifacts": {
      "type": "object",
      "propertyNames": {
        "enum": ["equity_curve", "trades", "positions_daily", "metrics", "diagnostics"]
      },
      "additionalProperties": {
        "anyOf": [
          {
            "description": "Parquet artifact",
            "type": "object",
            "required": ["file", "columns", "types"],
            "properties": {
              "file": { "type": "string", "pattern": "\\.parquet$" },
              "columns": { "type": "array", "items": { "type": "string" } },
              "types": { "type": "array", "items": { "type": "string" } }
            }
          },
          {
            "description": "JSON artifact",
            "type": "object",
            "required": ["file", "required_keys"],
            "properties": {
              "file": { "type": "string", "pattern": "\\.json$" },
              "required_keys": { "type": "array", "items": { "type": "string" } }
            }
          }
        ]
      }
    }
  }
}
```

## 11. Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-08-28 | Initial release |

---

*This document is the single source of truth for the golden artifact contract. All implementation code must conform to this specification.*
