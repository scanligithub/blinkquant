"""Tests for result_store: summary + Parquet persistence."""
import json
import os
import tempfile
from backend.scheduler.result_store import (
    build_result_summary, persist, load_part, load_as_legacy_json,
)


def _make_sample_data():
    return {
        "formula": "CLOSE > MA(CLOSE, 20)",
        "start_date": "2024-01-02",
        "signal_end_date": "2024-01-31",
        "initial_cash": 10_000_000,
        "equity_curve": [
            {"date": "2024-01-02", "equity": 10_000_000, "cash": 10_000_000, "positions_value": 0},
            {"date": "2024-01-31", "equity": 10_636_942, "cash": 500_000, "positions_value": 10_136_942},
        ],
        "trades": [
            {"date": "2024-01-03", "code": "sh.600000", "side": "BUY", "qty": 1000, "price": 10.0, "fee": 5.0},
            {"date": "2024-01-15", "code": "sh.600000", "side": "SELL", "qty": 1000, "price": 11.0, "fee": 5.5},
        ],
        "positions_daily": [],
        "metrics": {
            "total_return": 0.0637,
            "max_drawdown": -0.0662,
            "trade_count": 2,
        },
    }


def test_build_result_summary():
    data = _make_sample_data()
    summary = build_result_summary(data)
    assert summary["final_equity"] == 10_636_942
    assert summary["total_return"] == 0.0637
    assert summary["max_drawdown"] == -0.0662
    assert summary["n_trades"] == 2
    assert summary["n_equity_points"] == 2
    assert summary["initial_cash"] == 10_000_000


def test_build_result_summary_empty():
    summary = build_result_summary({})
    assert summary["final_equity"] is None
    assert summary["n_trades"] == 0


def test_persist_creates_files():
    data = _make_sample_data()
    with tempfile.TemporaryDirectory() as tmpdir:
        summary, uri = persist(42, data, tmpdir)
        assert uri == "task_42"
        assert os.path.exists(os.path.join(tmpdir, "task_42", "meta.json"))
        assert os.path.exists(os.path.join(tmpdir, "task_42", "equity_curve.parquet"))
        assert os.path.exists(os.path.join(tmpdir, "task_42", "trades.parquet"))
        assert summary["final_equity"] == 10_636_942


def test_load_part_roundtrip():
    data = _make_sample_data()
    with tempfile.TemporaryDirectory() as tmpdir:
        _, uri = persist(1, data, tmpdir)
        df = load_part(uri, "trades", tmpdir)
        assert df is not None
        assert df.height == 2
        assert "code" in df.columns


def test_load_as_legacy_json():
    data = _make_sample_data()
    with tempfile.TemporaryDirectory() as tmpdir:
        _, uri = persist(2, data, tmpdir)
        legacy = load_as_legacy_json(uri, tmpdir)
        assert "equity_curve" in legacy
        assert "trades" in legacy
        assert len(legacy["trades"]) == 2
        assert legacy["formula"] == "CLOSE > MA(CLOSE, 20)"


def test_load_part_unknown_name():
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            load_part("task_1", "unknown", tmpdir)
            assert False, "Should raise ValueError"
        except ValueError:
            pass
