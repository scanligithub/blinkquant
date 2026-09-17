import os
import tempfile
import polars as pl
import pytest
from backend.scheduler.result_store import (
    make_result_uri, assert_safe_user_id, persist, persist_frames,
    load_part, delete_result_dir, dir_size,
)

UID = "11111111-1111-1111-1111-111111111111"


def test_make_result_uri():
    assert make_result_uri(UID, 3) == f"by_user/{UID}/task_3"


def test_reject_unsafe_user_id():
    with pytest.raises(ValueError):
        assert_safe_user_id("../etc")
    with pytest.raises(ValueError):
        assert_safe_user_id("a/b")


def test_persist_path_and_bytes():
    data = {
        "metrics": {"total_return": 0.1, "max_drawdown": -0.05, "trade_count": 1},
        "equity_curve": [{"date": "2024-01-02", "equity": 1e7}],
        "trades": [{"code": "sh.600000", "side": "BUY", "qty": 100, "price": 10.0, "fee": 5.0,
                    "signal_date": "2024-01-02", "execution_date": "2024-01-03"}],
    }
    with tempfile.TemporaryDirectory() as d:
        summary, uri, nbytes = persist(7, data, d, user_id=UID)
        assert uri == f"by_user/{UID}/task_7"
        assert os.path.isdir(os.path.join(d, uri))
        assert os.path.isfile(os.path.join(d, uri, "trades.parquet"))
        assert nbytes == dir_size(os.path.join(d, uri))
        assert nbytes > 0
        df = load_part(uri, "trades", d)
        assert df is not None and df.height == 1
        assert delete_result_dir(uri, d) is True
        assert not os.path.isdir(os.path.join(d, uri))


def test_delete_refuses_flat_uri():
    with tempfile.TemporaryDirectory() as d:
        os.makedirs(os.path.join(d, "task_1"), exist_ok=True)
        assert delete_result_dir("task_1", d) is False
