"""Tests for result_store v2: summary without list + persist_frames."""
import json
import os
import tempfile
import polars as pl
from backend.scheduler.result_store import (
    build_result_summary, persist_frames, load_part, load_as_legacy_json,
)


def _make_sample_frames():
    ec = pl.DataFrame({
        "date": ["2024-01-02", "2024-01-31"],
        "equity": [10_000_000.0, 10_636_942.0],
        "cash": [10_000_000.0, 500_000.0],
        "positions_value": [0.0, 10_136_942.0],
    }).cast({"date": pl.Date})
    trades = pl.DataFrame({
        "signal_date": ["2024-01-03"],
        "execution_date": ["2024-01-03"],
        "code": ["sh.600000"],
        "side": ["BUY"],
        "qty": [1000],
        "price": [10.0],
        "fee": [5.0],
    }).cast({"signal_date": pl.Date, "execution_date": pl.Date})
    return ec, trades


def test_build_result_summary_from_dict():
    data = {"metrics": {"total_return": 0.06, "max_drawdown": -0.03}, "equity_curve": [{"equity": 100}], "trades": 5}
    s = build_result_summary(data)
    assert s["n_trades"] == 5
    assert s["n_equity_points"] == 1


def test_build_result_summary_from_count():
    data = {"metrics": {"trade_count": 42}, "equity_curve": {"n": 1000, "final_equity": 999}, "trades": 42}
    s = build_result_summary(data)
    assert s["n_trades"] == 42
    assert s["final_equity"] == 999


def test_persist_frames_creates_parquet():
    ec, trades = _make_sample_frames()
    meta = {"formula": "CLOSE > MA(CLOSE, 20)", "metrics": {"total_return": 0.06}}
    with tempfile.TemporaryDirectory() as tmpdir:
        summary, uri, _nbytes = persist_frames(
            42, user_id="test-user", result_dir=tmpdir, meta=meta,
            equity_curve=ec, trades=trades, positions_daily=None,
        )
        assert uri == "by_user/test-user/task_42"
        assert os.path.exists(os.path.join(tmpdir, "by_user/test-user/task_42", "meta.json"))
        assert os.path.exists(os.path.join(tmpdir, "by_user/test-user/task_42", "equity_curve.parquet"))
        assert os.path.exists(os.path.join(tmpdir, "by_user/test-user/task_42", "trades.parquet"))
        assert summary["n_trades"] == 1
        assert summary["final_equity"] == 10_636_942.0


def test_persist_frames_roundtrip():
    ec, trades = _make_sample_frames()
    meta = {"formula": "test"}
    with tempfile.TemporaryDirectory() as tmpdir:
        _, uri, _ = persist_frames(1, user_id="test-user", result_dir=tmpdir, meta=meta, equity_curve=ec, trades=trades)
        df = load_part(uri, "trades", tmpdir)
        assert df is not None
        assert df.height == 1
        legacy = load_as_legacy_json(uri, tmpdir)
        assert "equity_curve" in legacy
        assert len(legacy["equity_curve"]) == 2