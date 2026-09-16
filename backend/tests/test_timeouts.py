"""Tests for compute_task_timeout_sec dynamic window timeout."""
from backend.scheduler.config import (
    compute_task_timeout_sec,
    TASK_RUNNING_TIMEOUT_SEC,
    TASK_TIMEOUT_BASE_SEC,
    TASK_TIMEOUT_MIN_SEC,
    TASK_TIMEOUT_MAX_SEC,
    SELECTION_RUNNING_TIMEOUT_SEC,
)


def test_selection_returns_fixed_timeout():
    payload = {"start_date": "2024-01-01", "end_date": "2024-12-31"}
    assert compute_task_timeout_sec("selection", payload) == SELECTION_RUNNING_TIMEOUT_SEC


def test_no_payload_returns_fallback():
    assert compute_task_timeout_sec("backtest", None) == TASK_RUNNING_TIMEOUT_SEC


def test_invalid_dates_returns_fallback():
    payload = {"start_date": "not-a-date", "end_date": "also-bad"}
    assert compute_task_timeout_sec("backtest", payload) == TASK_RUNNING_TIMEOUT_SEC


def test_one_month_window_returns_min():
    payload = {"start_date": "2024-11-01", "end_date": "2024-11-01"}
    result = compute_task_timeout_sec("backtest", payload)
    assert result == TASK_TIMEOUT_MIN_SEC


def test_half_year_window():
    payload = {"start_date": "2024-01-01", "end_date": "2024-06-30"}
    result = compute_task_timeout_sec("backtest", payload)
    # ~0.5 years → base(15min) + 0.5*20min = 25min = 1500s
    assert 1400 <= result <= 1600


def test_five_year_window_hits_cap():
    payload = {"start_date": "2019-01-01", "end_date": "2024-07-01"}
    result = compute_task_timeout_sec("backtest", payload)
    # ~5.5 years → 15min + 5.5*20min = 125min → capped at 120min
    assert result == TASK_TIMEOUT_MAX_SEC


def test_alternative_date_keys():
    """Payload may use start/end or signal_end_date."""
    payload = {"start": "2024-01-01", "signal_end_date": "2024-12-31"}
    result = compute_task_timeout_sec("backtest", payload)
    # ~1 year → 15min + 20min = 35min = 2100s
    assert 2000 <= result <= 2200
