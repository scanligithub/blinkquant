# RQAlpha Backend Experiment

## Status
- **ADR-001**: Accepted - RQAlpha as Execution Kernel
- **PoC-0**: Timing verification (T signal → T+1 open execution)
- **Bundle Download**: In progress (slow)

## Branch Structure
```
experiment/rqalpha-backend/
├── backends/
│   └── rqalpha/
│       ├── adapter.py          # SelectionResult → RQAlpha orders
│       ├── datasource.py       # BlinkquantParquetDataSource
│       ├── mapper.py           # Trade/Fill ↔ Fill/Trade
│       ├── result.py           # Normalized BacktestResult
│       ├── config.py           # RQAlpha config builder
│       └── requirements.txt
├── tests/
│   └── rqalpha/
│       ├── test_t1_timing.py           # PoC-0: T+1 timing
│       ├── test_t1_invariant.py        # PoC-1: T+1 freeze
│       ├── test_fee_mapping.py         # PoC-2: Fee mapping
│       ├── test_corporate_action.py    # PoC-3: CA mapping
│       ├── test_result_mapping.py      # PoC-4: Result mapping
│       ├── test_minimal_datasource.py  # Minimal DS test (WIP)
│       └── conftest.py
└── README.md
```

## Current Status

### Completed
- ✅ ADR-001 documented
- ✅ Adapter skeleton (`backends/rqalpha/adapter.py`)
- ✅ Custom DataSource skeleton (`backends/rqalpha/datasource.py`)
- ✅ Test structure created

### In Progress
- 🔄 Bundle download (slow, in background)
- 🔄 Minimal DataSource test (blocked by bundle)
- 🔄 PoC-0 timing verification

### Blocked
- RQAlpha bundle download extremely slow
- Custom DataSource needs bundle files

## Next Steps
1. Wait for bundle download or find alternative
2. Implement `BlinkquantParquetDataSource` fully
3. Run PoC-0 timing verification
4. Implement PoC-1 through PoC-4

## Quick Start (once bundle ready)
```bash
cd experiment/rqalpha-backend
pip install -r backends/rqalpha/requirements.txt
pytest tests/rqalpha/ -v
```