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
| passed | bool | ✅ | Overall formula result; formula-level only |
| triggered | bool | ✅ | Whether the configured Entry trigger fired |
| targeted | bool | ✅ | Whether the code entered the final target portfolio |
| target_weight | float/null | ✅ | Target weight when targeted; otherwise null |
| selection_reason | string | ✅ | FORMULA_REJECTED / TRIGGER_NOT_FIRED / TOP_N_EXCLUDED / TARGET_SELECTED |
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
├── atoms.parquet          # Normalized atom evaluations (one row per atom per code)
├── executions.parquet     # IA5.6.1: all execution records per code
└── decisions.parquet      # IA5.6.1: complete order-intent decisions and outcomes
```

### traces.parquet

| Column | Type | Description |
|--------|------|-------------|
| signal_date | Date | Signal date |
| code | Utf8 | Stock code |
| passed | Boolean | Overall formula result; formula-level only |
| formula | Utf8 | Formula string |
| triggered | Boolean | Entry trigger fired for this candidate |
| targeted | Boolean | Candidate entered final target portfolio |
| target_weight | Float64 | Target weight when targeted; null otherwise |
| selection_reason | Utf8 | Downstream selection outcome |
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

## IA5.6.1 Additive Persistence Extension

The original traces.parquet + atoms.parquet layout remains readable through to_parquet()
for backward compatibility. Complete IA5.6.1 artifacts additionally persist:

- executions.parquet: every execution record in CodeTrace.executions, preserving multiple
  executions for the same code/date while retaining the legacy CodeTrace.execution projection.
- decisions.parquet: every DecisionTrace, including BUY/SELL, FILLED/PARTIAL/REJECTED,
  target quantities/weights, execution outcome and rejection reason.

Readers treat both sidecar files as optional so existing v1 artifacts remain readable.
## IA5.6.2 Candidate Semantics

A traced signal covers the complete PIT candidate universe supplied to the selection
engine, not only the final selected codes. Candidates that fail the formula remain in
the trace with `passed=false` and their atom evaluations retain the observed PIT
values and comparison outcomes.

Ranking, Top-N allocation, and event-trigger post-filtering are downstream selection
semantics. They do not redefine `CodeTrace.passed`. The candidate universe is resolved
as-of the signal date and must not include future universe membership.


## IA5.6.3 Selection / Allocation Provenance

For each traced PIT candidate, downstream strategy selection is recorded separately from formula evaluation:

- `passed` remains the formula-level result and is never changed by trigger or allocation.
- `triggered` indicates whether the configured Entry trigger fired for that traced candidate.
- `targeted` indicates whether the candidate entered the final target portfolio produced by the sizing allocator.
- `target_weight` records the resulting target weight when `targeted=true`; otherwise it is null.
- `selection_reason` is one of `FORMULA_REJECTED`, `TRIGGER_NOT_FIRED`, `TOP_N_EXCLUDED`, or `TARGET_SELECTED`.

This makes a complete explanation possible without conflating formula truth with downstream trigger or Top-N allocation semantics.

```
# Candidate formula false -> FORMULA_REJECTED
# Formula true + cross condition not fired -> TRIGGER_NOT_FIRED
# Formula true + trigger fired but outside Top-N -> TOP_N_EXCLUDED
# Formula true + trigger fired + allocated -> TARGET_SELECTED
```

The fields are additive and older artifacts without them remain readable with their default values.
## Generation Rules

1. **During Selection**: Each PIT candidate code's formula evaluation produces a trace entry
2. **Candidate Universe**: Candidates are the codes eligible for the signal as-of date; formula failures remain traceable
3. **Selection Result**: Final selected codes are a subset of candidates; `CodeTrace.passed` describes formula evaluation, not Top-N/ranking/trigger outcome
4. **Atom Order**: Atoms sorted by formula parse order (deterministic)
5. **No Lookahead**: All values from `build_asof_frame(target_date=signal_date)`
6. **PIT Fields**: Sector/Industry fields rejected in backtest mode (raise error)
7. **Null Handling**: Missing data → atom `passed=false`, `value=NaN`

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