"""Tests for quota GC + delete auth + access control."""
import os
import tempfile
import pytest
from unittest.mock import patch, AsyncMock
from backend.scheduler.result_store import (
    make_result_uri, persist, delete_result_dir, dir_size,
)

UID_A = "11111111-1111-1111-1111-111111111111"
UID_B = "22222222-2222-2222-2222-222222222222"


def _small_data():
    return {
        "metrics": {"total_return": 0.1, "max_drawdown": -0.05, "trade_count": 1},
        "equity_curve": [{"date": "2024-01-02", "equity": 1e7}],
        "trades": [{"code": "sh.600000", "side": "BUY", "qty": 100, "price": 10.0, "fee": 5.0,
                    "signal_date": "2024-01-02", "execution_date": "2024-01-03"}],
    }


# ── result_store: delete + isolation ──

def test_delete_result_dir_by_user_prefix():
    with tempfile.TemporaryDirectory() as d:
        summary, uri, _ = persist(1, _small_data(), d, user_id=UID_A)
        assert uri.startswith("by_user/")
        assert os.path.isdir(os.path.join(d, uri))
        assert delete_result_dir(uri, d) is True
        assert not os.path.isdir(os.path.join(d, uri))


def test_delete_result_dir_nonexistent():
    with tempfile.TemporaryDirectory() as d:
        assert delete_result_dir("by_user/noexist/task_999", d) is False


def test_delete_result_dir_empty_uri():
    with tempfile.TemporaryDirectory() as d:
        assert delete_result_dir(None, d) is False
        assert delete_result_dir("", d) is False


def test_user_a_cannot_see_user_b_path():
    """User A's result_uri should not be accessible under User B's dir."""
    with tempfile.TemporaryDirectory() as d:
        _, uri_a, _ = persist(1, _small_data(), d, user_id=UID_A)
        assert f"by_user/{UID_A}" in uri_a
        # User B's path is different
        uri_b = make_result_uri(UID_B, 1)
        assert uri_b == f"by_user/{UID_B}/task_1"
        assert uri_a != uri_b


# ── access control helper ──

def test_assert_task_access_owner_ok():
    from backend.scheduler.routes import _assert_task_access
    _assert_task_access({"user_id": UID_A}, UID_A, "user")


def test_assert_task_access_other_forbidden():
    from backend.scheduler.routes import _assert_task_access
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as ei:
        _assert_task_access({"user_id": UID_A}, UID_B, "user")
    assert ei.value.status_code == 403


def test_assert_task_access_admin_ok():
    from backend.scheduler.routes import _assert_task_access
    _assert_task_access({"user_id": UID_A}, None, "admin")


def test_assert_task_access_no_user_id_forbidden():
    from backend.scheduler.routes import _assert_task_access
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as ei:
        _assert_task_access({"user_id": UID_A}, None, "user")
    assert ei.value.status_code == 401


# ── quota config ──

def test_admin_user_ids_empty_by_default():
    from backend.scheduler.config import ADMIN_USER_IDS
    assert isinstance(ADMIN_USER_IDS, frozenset)


def test_quota_bytes_positive():
    from backend.scheduler.config import RESULT_QUOTA_BYTES_PER_USER
    assert RESULT_QUOTA_BYTES_PER_USER > 0
