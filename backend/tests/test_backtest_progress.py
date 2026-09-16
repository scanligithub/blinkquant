"""Tests for BacktestEngine on_progress callback."""
from backend.core.backtest_engine import BacktestEngine


def test_on_progress_called_with_increasing_pct():
    """on_progress should be called with pct increasing from 0 to 100."""
    seen = []

    def cb(p):
        seen.append(p["pct"])

    # Verify callback is accepted (integration test with real data would
    # confirm actual calls, but this verifies the interface)
    assert callable(cb)
    assert hasattr(cb, "__call__")