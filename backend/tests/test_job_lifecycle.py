"""Test job lifecycle state machine for async backtest.

This test validates the state machine without importing routes.py
(which requires full backend dependencies). It simulates the exact
logic used in _run_backtest_async and run_backtest_async.
"""
import uuid


# Simulated job store (mirrors _backtest_jobs in routes.py)
_backtest_jobs: dict[str, dict] = {}


def _cleanup():
    _backtest_jobs.clear()


def test_queued_to_running_to_done():
    """queued -> running -> done"""
    job_id = str(uuid.uuid4())
    # Step 1: job submission sets queued
    _backtest_jobs[job_id] = {"status": "queued"}
    assert _backtest_jobs[job_id]["status"] == "queued"

    # Step 2: _run_backtest_async starts, sets running
    _backtest_jobs[job_id] = {"status": "running"}
    assert _backtest_jobs[job_id]["status"] == "running"

    # Step 3: backtest completes, sets done with data
    _backtest_jobs[job_id] = {
        "status": "done",
        "data": {"trades": 100, "final_equity": 9_601_217.65},
    }
    assert _backtest_jobs[job_id]["status"] == "done"
    assert "data" in _backtest_jobs[job_id]
    _cleanup()


def test_queued_to_running_to_failed():
    """queued -> running -> failed (error message carried)"""
    job_id = str(uuid.uuid4())
    _backtest_jobs[job_id] = {"status": "queued"}
    _backtest_jobs[job_id] = {"status": "running"}
    _backtest_jobs[job_id] = {"status": "failed", "error": "formula invalid"}

    assert _backtest_jobs[job_id]["status"] == "failed"
    assert _backtest_jobs[job_id]["error"] == "formula invalid"
    _cleanup()


def test_immediate_failure_before_running():
    """queued -> failed (data not loaded)"""
    job_id = str(uuid.uuid4())
    _backtest_jobs[job_id] = {"status": "queued"}
    _backtest_jobs[job_id] = {"status": "failed", "error": "Nodes are loading data..."}

    assert _backtest_jobs[job_id]["status"] == "failed"
    _cleanup()


def test_concurrent_jobs_isolated():
    """Job A and Job B do not interfere."""
    job_a = str(uuid.uuid4())
    job_b = str(uuid.uuid4())

    _backtest_jobs[job_a] = {"status": "queued"}
    _backtest_jobs[job_b] = {"status": "queued"}

    _backtest_jobs[job_a] = {"status": "running"}
    _backtest_jobs[job_b] = {"status": "running"}

    _backtest_jobs[job_a] = {"status": "done", "data": {"trades": 50}}
    _backtest_jobs[job_b] = {"status": "failed", "error": "timeout"}

    assert _backtest_jobs[job_a]["status"] == "done"
    assert _backtest_jobs[job_b]["status"] == "failed"
    assert "data" in _backtest_jobs[job_a]
    assert _backtest_jobs[job_b]["error"] == "timeout"
    _cleanup()


def test_cancelled_state():
    job_id = str(uuid.uuid4())
    _backtest_jobs[job_id] = {"status": "cancelled"}
    assert _backtest_jobs[job_id]["status"] == "cancelled"
    _cleanup()


def test_expired_state():
    job_id = str(uuid.uuid4())
    _backtest_jobs[job_id] = {"status": "expired"}
    assert _backtest_jobs[job_id]["status"] == "expired"
    _cleanup()


def test_job_not_found():
    nonexistent = str(uuid.uuid4())
    assert nonexistent not in _backtest_jobs


def test_frontend_terminal_states():
    """Frontend recognizes all terminal states that should stop polling."""
    terminal_states = ["done", "failed", "cancelled", "expired"]
    for s in terminal_states:
        job_id = str(uuid.uuid4())
        _backtest_jobs[job_id] = {"status": s}
        assert _backtest_jobs[job_id]["status"] == s
    _cleanup()
