# backend/scheduler/dispatcher.py
import asyncio
import httpx
import json
from typing import Optional

from .config import HF_NODES, DISPATCH_TIMEOUT_SEC, POLL_JOB_TIMEOUT_SEC

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
    """向单节点提交 backtest，返回 {job_id, status}"""
    client = await get_client()
    try:
        resp = await asyncio.wait_for(
            client.post(
                f"{HF_NODES[node_id]}/api/v1/backtest/async",
                json=payload,
                headers={"Content-Type": "application/json"},
            ),
            timeout=timeout,
        )
        if resp.status_code == 422:
            detail = await resp.text()
            raise RuntimeError(f"dispatch_backtest({node_id}) 422: {detail} | payload={payload}")
        resp.raise_for_status()
        data = resp.json()
        return {"job_id": data["job_id"], "status": data.get("status", "queued")}
    except Exception as e:
        raise RuntimeError(f"dispatch_backtest({node_id}) failed: {e}")

async def dispatch_selection(payload: dict, timeout: int = 60) -> dict:
    """并行向 3 节点发起 selection，返回各节点结果"""
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
    
    # 聚合逻辑：取 3 节点结果的并集（选股分片并行，结果取并集）
    all_codes = set()
    for node_data in success.values():
        codes = node_data.get("codes", [])
        all_codes.update(codes)
    
    return {"nodes": success, "codes": list(all_codes)}

async def cancel_task(node_id: str, job_id: str, reason: str = "preempted_by_selection") -> bool:
    """协作式取消：POST /api/v1/backtest/cancel (需节点实现)"""
    client = await get_client()
    try:
        resp = await asyncio.wait_for(
            client.post(
                f"{HF_NODES[node_id]}/api/v1/backtest/cancel",
                json={"job_id": job_id, "reason": reason},
            ),
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False

async def poll_backtest_job(node_id: str, job_id: str, timeout: int = 60) -> dict:
    """轮询 HF 节点的 job 状态"""
    client = await get_client()
    try:
        resp = await asyncio.wait_for(
            client.get(
                f"{HF_NODES[node_id]}/api/v1/backtest/async/{job_id}",
                headers={"Content-Type": "application/json"},
            ),
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"poll_backtest_job({node_id}, {job_id}) failed: {e}")