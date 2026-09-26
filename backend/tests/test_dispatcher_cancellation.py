"""Regression tests for scheduler backtest preemption semantics."""

# These tests intentionally distinguish worker termination from mere DB state.
# Dedicated CI step runs these without requiring pytest-asyncio.
import asyncio

from backend.scheduler import dispatcher


class _Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Client:
    def __init__(self, statuses):
        self.statuses = iter(statuses)

    async def post(self, *args, **kwargs):
        return _Response(200, {"ok": True, "status": "cancelling"})

    async def get(self, *args, **kwargs):
        return _Response(200, {"status": next(self.statuses)})


def test_cancel_task_requires_actual_cancelled_state(monkeypatch):
    client = _Client(["running", "done"])
    monkeypatch.setattr(dispatcher, "get_client", lambda: _async_return(client))

    result = asyncio.run(dispatcher.cancel_task("node1", "job-1", timeout=1))

    assert result is False


def test_cancel_task_succeeds_after_worker_reports_cancelled(monkeypatch):
    client = _Client(["running", "cancelling", "cancelled"])
    monkeypatch.setattr(dispatcher, "get_client", lambda: _async_return(client))

    result = asyncio.run(dispatcher.cancel_task("node1", "job-2", timeout=1))

    assert result is True


async def _async_return(value):
    return value
