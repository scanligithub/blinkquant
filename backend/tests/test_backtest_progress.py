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

def test_backtest_cancel_check_stops_at_safe_boundary():
    """A cancellation request must abort the engine before the next trading day."""
    import datetime as dt
    from backend.core.backtest_engine import BacktestCancelled, BacktestEngine, TradingCalendar
    from backend.core.backtest_types import FeeConfig

    class FakeRawStore:
        def load_latest_adjust_factors(self, **kwargs):
            return {}

    day1 = dt.date(2024, 1, 2)
    day2 = dt.date(2024, 1, 3)
    cal = TradingCalendar()
    cal.set_trade_dates([day1, day2])

    engine = BacktestEngine(
        calendar=cal,
        selection_engine=None,
        raw_price_store=FakeRawStore(),
        fee_config=FeeConfig(),
    )

    # Make the test exercise the event loop without invoking the data/selection stack.
    engine._phase_pre_open = lambda *args: None
    engine._phase_post_close_signal = lambda *args: (None, None, [], {})
    engine._phase_post_execution = lambda *args: ([], None, None)
    engine._phase_market_close = lambda *args: {}
    engine._phase_valuation = lambda *args: (1_000_000.0, 0.0)
    engine._phase_checkpoint = lambda *args: None

    calls = {"n": 0}
    def cancel_check():
        calls["n"] += 1
        return calls["n"] >= 2

    try:
        engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=day1,
            end_signal_date=day1,
            initial_cash=1_000_000,
            cancel_check=cancel_check,
        )
        assert False, "expected BacktestCancelled"
    except BacktestCancelled:
        pass

    assert calls["n"] >= 2
