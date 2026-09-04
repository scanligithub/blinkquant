# backend/scheduler/dispatcher.py
import asyncio
import httpx
from typing import Optional

from .config import HF_NODES, DISPATCH_TIMEOUT_SEC, TaskType

_client: httpx.AsyncClient | None = None

async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=300.0, write=30.0, pool=5.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
    return _client

async def close_client() -> None:
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None

async def dispatch_backtest(node_id: str, payload: dict, timeout: int = 240) -> dict:
    url = f"{HF_NODES[node_id]}/api/v1/backtest/async"
    client = await get_client()
    try:
        resp = await asyncio.wait_for(
            _client.post(
                f"{HF_NODES[node_id]}/api/v1/backtest/async",
                json=payload,
                headers={"Content-Type": "application/json"},
            ),
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"job_id": data["job_id"], "status": data.get("status", "queued")}
    except Exception as e:
        raise RuntimeError(f"dispatch_backtest({node_id}) failed: {e}")

async def dispatch_selection(payload: dict, timeout: int = 60) -> dict:
    async def call_one(node_id: str, url: str) -> tuple[str, dict]:
        client = await get_client()
        resp = await asyncio.wait_for(
            client.post(
                f"{url}/api/v1/select",
                json=payload,
                headers={"Content-Type": "application/json"},
            ),
            timeout=timeout,
        )
        resp.raise_for_status()
        return node_id, resp.json()
    
    from .config import HF_NODES
    tasks = [call_one(nid, ep) for nid, ep in HF_NODES.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    success = {}
    for res in results:
        if isinstance(res, Exception):
            pass
        else:
            node_id, data = res
            success[node_id] = data
    
    if not success:
        raise RuntimeError("All selection nodes failed")
    
    return {"nodes": success}

async def cancel_task(node_id: str, job_id: str, reason: str = "preempted_by_selection") -> bool:
    from .config import HF_NODES
    client = await get_client()
    try:
        resp = await asyncio.wait_for(
            _client.post(
                f"{HF_NODES[node_id]}/api/v1/backtest/cancel",
                json={"job_id": job_id, "reason": reason},
            ),
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False