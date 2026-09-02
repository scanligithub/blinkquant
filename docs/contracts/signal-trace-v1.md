# BlinkQuant SignalTrace Contract v1.0

## Purpose

Define the canonical trace format linking **Formula Atom Evaluation** → **SelectionResult** → **Execution**.

This enables answering: *"Why did this stock get traded on this date?"* with full atomic-level evidence.

---

## Design Principles

1. **Single Source of Truth** — SignalTrace is generated during SelectionEngine evaluation, not reconstructed post-hoc
2. **Atomic Granularity** — Each indicator/window/field evaluation is one record
3. **PIT Compliant** — All values reflect data available at `target_date` (no lookahead)
4. **Deterministic** — Same input + formula = identical trace (sorted by code, then atom order)
5. **Portable** — Parquet + JSON, schema versioned

---

## Data Model

### SignalTrace (per signal date)

```json
{
  "schema_version": "1.0.0",
  "engine_version": "v1.0.3-9-g43547cc",
  "signal_date": "2024-01-05",
  "formula": "CLOSE > MA(CLOSE, 20)",
  "traces": [
    {
      "code": "sh.600000",
      "passed": true,
      "atoms": [
        {
          "atom_id": "CLOSE",
          "field": "close",
          "window": null,
          "value": 17.5,
          "operator": ">",
          "threshold": 16.8,
          "passed": true
        },
        {
          "atom_id": "MA_CLOSE_20",
          "field": "close",
          "window": 20,
          "value": 16.8,
          "operator": ">",
          "threshold": 16.8,
          "passed": true
        }
      ],
      "execution": {
        "execution_date": "2024-01-08",
        "price": 17.08,
        "side": "BUY",
        "qty": 5000
      }
    }
  ]
}
```

---

## Schema Definitions

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| schema_version | string | ✅ | Contract version |
| engine_version | string | ✅ | Git describe |
| signal_date | string (YYYY-MM-DD) | ✅ | Signal generation date |
| formula | string | ✅ | Original formula string |
| traces | array | ✅ | One entry per selected code |

### Trace Entry

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| code | string | ✅ | Stock code |
| passed | bool | ✅ | Overall formula result |
| atoms | array | ✅ | Per-atom evaluation records |
| execution | object | ❌ | Filled after execution phase |

### Atom Record

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| atom_id | string | ✅ | Unique atom identifier (e.g., `MA_CLOSE_20`) |
| field | string | ✅ | Raw field name (`close`, `volume`, `s_close`) |
| window | int/string/null | ✅ | Window size (e.g., `20`) or `null` for spot fields |
| value | float | ✅ | Computed value at `signal_date` (PIT) |
| operator | string/null | ✅ | `>`, `<`, `>=`, `<=`, `==`, `!=`, `cross_up`, `cross_down`, `null` |
| threshold | float/null | ✅ | Comparison threshold (for boolean atoms) |
| passed | bool | ✅ | Atom evaluation result |

### Execution Record (optional, filled post-execution)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| execution_date | string (YYYY-MM-DD) | ✅ | T+1 execution date |
| price | float | ✅ | Fill price (open) |
| side | string | ✅ | "BUY" / "SELL" |
| qty | int64 | ✅ | Fill quantity |
| fee | float | ❌ | Transaction fee |

---

## File Layout

```
signal_trace/
├── meta.json              # schema_version, engine_version, formula, signal_date
├── traces.parquet         # Columnar trace data (one row per code per signal_date)
└── atoms.parquet          # Normalized atom evaluations (one row per atom per code)
```

### traces.parquet

| Column | Type | Description |
|--------|------|-------------|
| signal_date | Date | Signal date |
| code | Utf8 | Stock code |
| passed | Boolean | Overall formula result |
| formula | Utf8 | Formula string |
| execution_date | Date | T+1 execution date (null if not executed) |
| exec_price | Float64 | Fill price (null if not executed) |
| exec_side | Utf8 | "BUY"/"SELL" (null if not executed) |
| exec_qty | Int64 | Fill quantity (null if not executed) |

### atoms.parquet

| Column | Type | Description |
|--------|------|-------------|
| signal_date | Date | Signal date |
| code | Utf8 | Stock code |
| atom_id | Utf8 | Unique atom identifier |
| field | Utf8 | Raw field name |
| window | Utf8 | Window spec (e.g., "20" or "") |
| value | Float64 | Computed value |
| operator | Utf8 | Comparison operator (or "") |
| threshold | Float64 | Threshold (or NaN) |
| passed | Boolean | Atom result |

---

## Generation Rules

1. **During Selection**: Each code's formula evaluation produces a trace entry
2. **Only Eligible Codes**: Only codes passing universe filter are traced
3. **Atom Order**: Atoms sorted by formula parse order (deterministic)
4. **No Lookahead**: All values from `build_asof_frame(target_date=signal_date)`
5. **PIT Fields**: Sector/Industry fields rejected in backtest mode (raise error)
6. **Null Handling**: Missing data → atom `passed=false`, `value=NaN`

---

## Version Compatibility

| Trace schema | Engine | Compatible |
|--------------|--------|------------|
| 1.x | 1.x | ✅ |
| 1.x | 2.x | ❌ |

---

## Implementation Files

- `backend/core/signal_trace.py` — `SignalTrace`, `AtomTrace` dataclasses + serialization
- `backend/core/selection_engine.py` — Trace generation during `execute_selector`
- `backend/core/backtest_engine.py` — Trace collection + execution enrichment
- `backend/tests/test_signal_trace.py` — Regression tests

---

## Future Extensions (v2+)

- Multi-formula traces (ensemble strategies)
- Risk model traces (factor exposures)
- Alternative data traces
- Explanation/attribution scores