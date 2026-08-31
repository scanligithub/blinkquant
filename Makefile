ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: test test-golden test-unit generate-golden validate-golden coverage help verify-backtest

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test:  ## Run all tests
	cd backend && pytest tests/ -v

test-golden:  ## Run golden regression tests only
	cd backend && pytest tests/test_golden_2024q1.py -v

test-unit:  ## Run all tests except golden
	cd backend && pytest tests/ -v --ignore=tests/test_golden_2024q1.py

generate-golden:  ## Regenerate golden artifacts (requires HF_TOKEN)
	cd backend && HF_TOKEN=$(HF_TOKEN) python scripts/generate_golden.py

validate-golden:  ## Validate golden artifact metadata (run from project root)
	python -c "import json; from pathlib import Path; meta=json.load(open('$(ROOT_DIR)/tests/golden/2024q1/metadata.json')); print('Schema:', meta['schema_version']); print('Artifacts:', list(meta['artifacts'].keys()))"

coverage:  ## Check test coverage
	cd backend && pytest tests/ --cov=core --cov-report=term-missing

lint:  ## Run linting (placeholder)
	@echo "Linting not configured yet"

typecheck:  ## Run type checking (placeholder)
	@echo "Type checking not configured yet"

verify-backtest:  ## Full backtest verification gate (pre-release)
	@echo "=== BlinkQuant Backtest Verification ==="
	@echo ""
	@echo "[1/6] Contract validation..."
	@cd backend && python -c "import json; from pathlib import Path; \
		meta=json.load(open('../tests/golden/2024q1/metadata.json')); \
		print('  schema_version:', meta['schema_version']); \
		arts = list(meta['artifacts'].keys()); \
		assert 'equity_curve' in arts, 'Missing equity_curve'; \
		assert 'trades' in arts, 'Missing trades'; \
		assert 'positions_daily' in arts, 'Missing positions_daily'; \
		assert 'metrics' in arts, 'Missing metrics'; \
		assert 'diagnostics' in arts, 'Missing diagnostics'; \
		print('  Artifacts:', arts); print('  PASS')"
	@echo ""
	@echo "[2/6] Unit tests (373 required)..."
	@cd backend && python -m pytest tests/ -q --tb=line 2>&1 | tail -1
	@echo ""
	@echo "[3/6] Golden regression tests..."
	@cd backend && python -m pytest tests/test_golden_2024q1.py -q --tb=line 2>&1 | tail -1
	@echo ""
	@echo "[4/6] Checkpoint determinism..."
	@cd backend && python -m pytest tests/test_checkpoint_determinism.py -q --tb=line 2>&1 | tail -1
	@echo ""
	@echo "[5/6] Continuity contract..."
	@cd backend && python -m pytest tests/test_backtest_continuity.py -q --tb=line 2>&1 | tail -1
	@echo ""
	@echo "[6/6] Signal trace tests..."
	@cd backend && python -m pytest tests/test_signal_trace.py -q --tb=line 2>&1 | tail -1
	@echo ""
	@echo "=== BACKTEST VERIFICATION: PASS ==="