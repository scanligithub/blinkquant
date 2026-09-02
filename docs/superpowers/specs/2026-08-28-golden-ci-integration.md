# Golden Tests CI Integration Specification

> **Version:** 1.0.0
> **Date:** 2026-08-28
> **Status:** Active
> **Supersedes:** None

## 1. Purpose

This document defines how golden regression tests integrate into BlinkQuant's CI/CD pipeline. Golden tests serve as a merge gate, ensuring backtest engine changes don't break reproducibility or violate invariants.

## 2. Current CI State

### 2.1 Existing Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `deploy_backend.yml` | Push to `main` with `backend/**` changes | Deploys backend to 3 HF Spaces |
| `daily_cron.yml` | Daily 03:00 CST (cron) + manual | Restarts cluster nodes |

### 2.2 Missing CI Components

- **No test workflow** — tests are not run automatically
- **No Makefile** — no unified test runner script
- **No pytest configuration** — tests run via ad-hoc `pytest` commands

## 3. Golden Test Architecture

### 3.1 Test Structure

```
tests/golden/2024q1/
├── metadata.json              # Strategy params, data version, generation timestamp
├── equity_curve.parquet       # Daily equity curve (~5 KB)
├── trades.parquet             # All executed trades (~50 KB)
├── positions_daily.parquet    # Daily position snapshots (~100 KB)
├── metrics.json               # Summary performance metrics (~0.2 KB)
└── diagnostics.json           # Execution diagnostics (~1 KB)
```

**Total size:** ~160 KB (committed to git)

### 3.2 Test Classes (16 tests)

| Class | Tests | What it validates |
|-------|-------|-------------------|
| `TestGoldenEquityCurve` | 5 | Row count, monotonicity, first row, cash non-negative, equity invariant |
| `TestGoldenTrades` | 5 | Column presence, T+1 ordering, lot size compliance, positive fees, no same-day buy+sell |
| `TestGoldenPositions` | 2 | Column presence, positive quantities |
| `TestGoldenMetrics` | 2 | Required keys, total days ≥ 50 |
| `TestGoldenDiagnostics` | 2 | Required keys, zero invariant violations |

### 3.3 Skip Behavior

Golden tests **skip** (not fail) when:

1. **Artifact files missing** — `_load_golden()` calls `pytest.skip()` if file doesn't exist
2. **HF_TOKEN not set** — determinism tests skip (future implementation)

This allows CI to run artifact validation even without HF data access.

## 4. CI Integration Design

### 4.1 Proposed Workflow: `golden-tests.yml`

```yaml
name: Golden Regression Tests

on:
  pull_request:
    paths:
      - 'backend/**'
      - 'tests/golden/**'
      - 'config/**'
  push:
    branches: [main]
    paths:
      - 'backend/**'
      - 'tests/golden/**'

jobs:
  golden-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          
      - name: Install dependencies
        run: |
          cd backend
          pip install -r requirements.txt
          pip install pytest
          
      - name: Run golden tests
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
        run: |
          cd backend
          pytest tests/test_golden_2024q1.py -v
          
      - name: Run all backend tests
        run: |
          cd backend
          pytest tests/ -v --ignore=tests/test_golden_2024q1.py
```

### 4.2 Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Skip on missing artifacts** | CI can validate test structure without HF data |
| **HF_TOKEN as secret** | Never expose tokens in logs; skip determinism tests if missing |
| **Run golden tests separately** | Faster feedback; golden tests are more expensive |
| **Trigger on backend changes** | Golden tests validate engine behavior |
| **Trigger on golden artifact changes** | Ensure new artifacts pass validation |

### 4.3 Merge Gate Rules

1. **All golden tests must pass** (or skip due to missing artifacts)
2. **All backend tests must pass**
3. **No new golden artifacts without explicit approval** — regeneration requires HF_TOKEN

## 5. Manual Regeneration Workflow

### 5.1 Prerequisites

- `HF_TOKEN` with access to `scanli/stocka-data`
- Python 3.11+ with `backend/requirements.txt` installed
- Network access to HuggingFace API

### 5.2 Step-by-Step

```bash
# 1. Navigate to backend directory
cd backend

# 2. Set HF_TOKEN (required)
export HF_TOKEN="hf_your_token_here"

# 3. Run generation script (defaults: 2024-01-02 to 2024-03-29)
python scripts/generate_golden.py

# 4. Verify artifacts were created
ls -la ../tests/golden/2024q1/

# 5. Run golden tests to validate
pytest tests/test_golden_2024q1.py -v

# 6. Commit new artifacts
cd ..
git add tests/golden/2024q1/
git commit -m "Regenerate golden artifacts for 2024Q1"
```

### 5.3 Custom Parameters

```bash
# Generate for different date range
python scripts/generate_golden.py --start 2024-04-01 --end 2024-06-30

# Generate with custom output directory
python scripts/generate_golden.py --output ../tests/golden/2024q2

# Generate with different strategy parameters
python scripts/generate_golden.py --top-n 10 --rebalance daily
```

### 5.4 Verification Checklist

After regeneration:

1. **Check metadata.json** — ensure `generated_at` is updated, `fingerprint` is recomputed
2. **Check file sizes** — parquet files should be ~160KB total
3. **Run golden tests** — all 16 tests must pass
4. **Review diff** — ensure changes are expected (not random variation)
5. **Update engine_version** — if engine code changed, bump version tag

## 6. HF_TOKEN Handling

### 6.1 CI Environment

| Scenario | HF_TOKEN | Behavior |
|----------|----------|----------|
| PR from fork | Not available | Golden tests skip; artifact validation only |
| PR from same repo | Available | Full golden test suite runs |
| Push to main | Available | Full golden test suite runs |
| Manual trigger | Available | Full golden test suite runs |

### 6.2 Local Development

| Scenario | HF_TOKEN | Behavior |
|----------|----------|----------|
| Developer has token | Set | Full test suite runs |
| Developer without token | Not set | Golden tests skip; other tests run |

### 6.3 Security

- **Never commit HF_TOKEN** to git
- **Never log HF_TOKEN** in CI output
- **Use GitHub Secrets** for CI storage
- **Rotate tokens** if compromised

## 7. When to Regenerate Golden Artifacts

### 7.1 Required Regeneration

| Trigger | Reason |
|---------|--------|
| Engine behavior change | Any change affecting backtest results |
| Strategy parameter change | Formula, rebalance frequency, top_n, etc. |
| Data version change | HF dataset update |
| Fee schedule change | Commission rates, tax rates, etc. |
| Execution logic change | Order matching, lot sizing, etc. |

### 7.2 Optional Regeneration

| Trigger | Reason |
|---------|--------|
| Engine version bump | Tagging a new release |
| Performance optimization | Ensure no behavioral change |
| Bug fix | Ensure fix doesn't break existing behavior |

### 7.3 No Regeneration Needed

| Trigger | Reason |
|---------|--------|
| Documentation changes | No engine impact |
| Test-only changes | No engine impact |
| CI configuration changes | No engine impact |
| Frontend changes | No engine impact |

## 8. PR Workflow

### 8.1 Developer Workflow

1. **Make engine changes** in `backend/`
2. **Run tests locally** — `pytest tests/test_golden_2024q1.py -v`
3. **If golden tests fail:**
   - Investigate if failure is expected (engine change)
   - If expected: regenerate artifacts, commit new parquet files
   - If unexpected: fix engine bug
4. **Push changes** — CI runs golden tests automatically
5. **Review PR** — ensure golden test results are as expected

### 8.2 Reviewer Checklist

- [ ] All golden tests pass (or skip due to missing artifacts)
- [ ] If golden artifacts changed, reason is documented in PR
- [ ] No unexpected invariant violations
- [ ] metadata.json updated if applicable
- [ ] Engine version bumped if engine changed

### 8.3 Merge Requirements

1. **All CI checks pass** (golden tests + other tests)
2. **Golden artifact changes reviewed** — must be intentional
3. **No regression** — existing tests don't break

## 9. Makefile Proposal

### 9.1 Purpose

Provide unified test runner commands for developers.

### 9.2 Proposed Commands

```makefile
# Run all tests
test:
	cd backend && pytest tests/ -v

# Run golden tests only
test-golden:
	cd backend && pytest tests/test_golden_2024q1.py -v

# Run all tests except golden
test-unit:
	cd backend && pytest tests/ -v --ignore=tests/test_golden_2024q1.py

# Regenerate golden artifacts
generate-golden:
	cd backend && HF_TOKEN=$(HF_TOKEN) python scripts/generate_golden.py

# Validate golden artifacts
validate-golden:
	cd backend && python -c "import json; from pathlib import Path; meta=json.load(open('../tests/golden/2024q1/metadata.json')); print('Schema:', meta['schema_version']); print('Artifacts:', list(meta['artifacts'].keys()))"

# Check test coverage
coverage:
	cd backend && pytest tests/ --cov=core --cov-report=term-missing
```

### 9.3 Usage

```bash
# Run golden tests
make test-golden

# Regenerate artifacts
HF_TOKEN=hf_xxx make generate-golden

# Run all tests
make test
```

## 10. Monitoring and Alerting

### 10.1 CI Metrics to Track

| Metric | Threshold | Action |
|--------|-----------|--------|
| Golden test pass rate | 100% | Investigate failures |
| Test execution time | < 5 minutes | Optimize if slow |
| Artifact size | < 200 KB | Review if growing |

### 10.2 Failure Response

1. **Golden tests fail unexpectedly:**
   - Check if engine changed intentionally
   - If yes: regenerate artifacts
   - If no: investigate regression

2. **Golden tests skip (missing artifacts):**
   - Regenerate artifacts before merge
   - Ensure HF_TOKEN is available

3. **CI timeout:**
   - Optimize test execution
   - Consider parallel test runs

## 11. Future Enhancements

### 11.1 Determinism Tests

Add tests that re-run backtest and compare against golden artifacts:

```python
def test_determinism():
    token = os.getenv("HF_TOKEN")
    if not token:
        pytest.skip("HF_TOKEN not set")
    # Re-run backtest and compare results
```

### 11.2 Multiple Golden Sets

Support golden artifacts for different date ranges:

```
tests/golden/
├── 2024q1/
├── 2024q2/
└── 2024q3/
```

### 11.3 Artifact Diff Tool

Create tool to compare golden artifacts across versions:

```bash
python scripts/diff_golden.py tests/golden/2024q1 tests/golden/2024q2
```

## 12. Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-08-28 | Initial release |

---

*This document defines how golden tests integrate into BlinkQuant's CI/CD pipeline. All CI configuration must conform to this specification.*