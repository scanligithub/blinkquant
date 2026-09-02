# BlinkQuant Differential Validator Contract v1.0

## Purpose

Validate BlinkQuant execution model against RQAlpha as a **differential validator**, not a replication target.

**Core principle:** We compare models, not raw outputs. Known semantic differences (price, fee, CA ordering) are classified and excluded from "mismatch" counts.

---

## Scope (Phase 1)

| Dimension | Value |
|-----------|-------|
| Universe | 20-50 stocks (subset of CSI300) |
| Period | 2024 Q1 (2024-01-02 to 2024-03-29) |
| Strategy | `CLOSE > MA(CLOSE, 20)` weekly rebalance Top20 |
| Initial Cash | 10,000,000 |
| Rebalance | Weekly |

---

## Comparison Levels

### Level 1: Signal Generation
| Field | Tolerance | Notes |
|-------|-----------|-------|
| Signal dates | Exact | Same trading calendar |
| Eligible codes per date | Exact | Same universe filter |
| Ranking scores | 1e-6 | Same ranking function |
| Top-N codes | Exact | Tie-break by code asc |

### Level 2: Order Intent
| Field | Tolerance | Notes |
|-------|-----------|-------|
| Signal date | Exact | Same selection |
| Execution date | Exact | Both T+1 |
| Code | Exact | |
| Side | Exact | BUY/SELL |
| Target quantity | Exact | Pre lot-size/funds |
| Target weight | 1e-6 | |

### Level 3: Execution / Trade
| Field | Tolerance | Notes |
|-------|-----------|-------|
| Execution date | Exact | T+1 |
| Code | Exact | |
| Side | Exact | |
| Fill quantity | Exact | Lot-size applied |
| Fill price | **Classified** | See Price Semantic below |
| Fee | **Classified** | See Fee below |

### Level 4: Account State (Daily)
| Field | Tolerance | Notes |
|-------|-----------|-------|
| Cash | 0.01 | Rounded |
| Position value | 0.01 | |
| Equity | 0.01 | |
| Positions (code/qty) | Exact | |

---

## Differential Classification

Every mismatch is classified into exactly one category:

| Category | Description | BlinkQuant | RQAlpha | Action |
|----------|-------------|------------|---------|--------|
| **PRICE_SEMANTIC** | Fill price source | T+1 OPEN | Configurable (default CLOSE) | **Excluded** from mismatch count |
| **FEE_MODEL** | Fee calculation | Commission min per order, exact | May differ (per-trade vs per-share) | Classify |
| **CA_ORDERING** | Corporate action timing | Dividend book → split → pay | May differ | Classify |
| **LOT_SIZE** | Rounding | BUY floor to 100 | May differ | Classify |
| **T_PLUS_ONE** | Execution timing | T signal → T+1 open | May differ (some use T close) | **Excluded** |
| **PARTIAL_FILL** | Cash handling | Keep cash, no redistribution | May differ | Classify |
| **DATA_MISSING** | Price/data gaps | Fail-fast / carry-forward | May fill NaN | Investigate |
| **ROUNDING** | Float precision | 2-decimal fee rounding | May differ | Classify if > 0.01 |
| **TRUE_MISMATCH** | Unexplained | — | — | **Flag for investigation** |

---

## Output: DifferentialReport

```json
{
  "schema_version": "1.0.0",
  "generated_at": "2024-08-28T10:00:00Z",
  "engine_versions": {
    "blinkquant": "v1.0.3-9-g43547cc",
    "rqalpha": "0.x.x"
  },
  "scope": {
    "universe": ["code1", "code2", ...],
    "start_date": "2024-01-02",
    "end_signal_date": "2024-03-29",
    "strategy": "CLOSE > MA(CLOSE, 20)",
    "rebalance": "weekly",
    "top_n": 20,
    "initial_cash": 10000000
  },
  "summary": {
    "total_signals_compared": 0,
    "total_orders_compared": 0,
    "total_trades_compared": 0,
    "total_account_days_compared": 0,
    "mismatches_by_category": {
      "PRICE_SEMANTIC": 0,
      "FEE_MODEL": 0,
      "CA_ORDERING": 0,
      "LOT_SIZE": 0,
      "T_PLUS_ONE": 0,
      "PARTIAL_FILL": 0,
      "DATA_MISSING": 0,
      "ROUNDING": 0,
      "TRUE_MISMATCH": 0
    },
    "match_rate": 0.0
  },
  "details": [
    {
      "level": "TRADE",
      "category": "FEE_MODEL",
      "date": "2024-01-08",
      "code": "600000",
      "side": "BUY",
      "blinkquant": {"price": 10.5, "fee": 5.0, "qty": 10000},
      "rqalpha": {"price": 10.5, "fee": 5.25, "qty": 10000},
      "diff": {"fee": 0.25}
    }
  ]
}
```

---

## Acceptance Criteria (Phase 1)

| Metric | Target |
|--------|--------|
| Signal match rate | 100% |
| Order intent match rate | 100% |
| Trade match rate (excl. PRICE_SEMANTIC) | ≥ 95% |
| Account state match rate (excl. PRICE_SEMANTIC) | ≥ 95% |
| TRUE_MISMATCH count | 0 |

---

## Implementation Files

- `backend/core/differential_validator.py` — Main validator
- `backend/scripts/run_differential.py` — CLI runner
- `backend/tests/test_differential.py` — Regression tests

---

## Future Phases

| Phase | Expansion |
|-------|-----------|
| Phase 2 | Full CSI300, multiple strategies |
| Phase 3 | Multi-year, stress scenarios |
| Phase 4 | Performance regression tracking |