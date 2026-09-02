# Gate 3B: HF Space Production Validation

## Deployment Steps

### 1. Push to HF Space
```bash
# From repository root
git add backend/
git commit -m "feat: Gate 3B HF Space validation - benchmark API + V4 lazy path"
git push
```

### 2. Verify Service Health
```bash
curl https://<your-username>-blinkquant.hf.space/api/v1/health
# Expected: {"status": "healthy", "build_id": "..."}
```

### 3. Run Benchmarks Sequentially

**B1 (Smoke Test)**
```bash
curl -X POST https://<your-username>-blinkquant.hf.space/api/v1/benchmark \
  -H "Content-Type: application/json" \
  -d '{"benchmark": "B1"}'
# Expected: trades=395, equity=6,791,917.78
```

**B2 (Full Year)**
```bash
curl -X POST https://<your-username>-blinkquant.hf.space/api/v1/benchmark \
  -H "Content-Type: application/json" \
  -d '{"benchmark": "B2"}'
```

**B3 (Multi-year)**
```bash
curl -X POST https://<your-username>-blinkquant.hf.space/api/v1/benchmark \
  -H "Content-Type: application/json" \
  -d '{"benchmark": "B3"}'
```

**B4 (Full History)**
```bash
curl -X POST https://<your-username>-blinkquant.hf.space/api/v1/benchmark \
  -H "Content-Type: application/json" \
  -d '{"benchmark": "B4"}'
# Expected: trades=20,678, equity=3,371,518.82
```

### 4. Record Results

Save each benchmark result to `benchmarks/hf_space/`:
```json
{
  "engine": "selection-v4",
  "environment": "hf-space",
  "golden_equivalence": true,
  "oom": false,
  "timeout": false,
  "max_peak_rss_mb": 0,
  "benchmarks": {
    "B1": "PASS",
    "B2": "PASS",
    "B3": "PASS",
    "B4": "PASS"
  }
}
```

## Expected Results

| Benchmark | Period | Trades | Final Equity | Peak RSS |
|-----------|--------|--------|--------------|----------|
| B1 | 2024 Q1 | 395 | 6,791,917.78 | < 1.5 GB |
| B2 | 2024 | — | — | < 1.5 GB |
| B3 | 2019-2024 | — | — | < 1.5 GB |
| B4 | 2010-2024 | 20,678 | 3,371,518.82 | < 2 GB |

## Validation Criteria

For each benchmark:
- [ ] No OOM
- [ ] No timeout (> 1 hour)
- [ ] trades count matches golden
- [ ] final_equity matches golden (within $0.01)
- [ ] No negative cash
- [ ] No accounting violations
- [ ] Peak RSS < 7 GB (HF Space per-node limit)

## Troubleshooting

### OOM
- Check `POLARS_MAX_THREADS=2` in Dockerfile
- Consider reducing data range

### Timeout
- Check HF Space CPU allocation (2 vCPU default)
- B4 may take 20-30 minutes on HF Space

### Data Access
- Ensure HF_TOKEN is set in Space secrets
- Verify `scanli/stocka-data` dataset is accessible

## References

- Golden B4: `benchmarks/B4_V4_golden/`
- Golden B1: `tests/golden/2024q1/`
- Local validation: `backend/scripts/gate3b_b1_local_validate.py`
- Full validation: `backend/scripts/gate3b_hf_validate.py`
