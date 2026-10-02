# backend/scheduler/dispatcher.py
import asyncio
import httpx
import json
from typing import Optional, Union

from .config import HF_NODES, DISPATCH_TIMEOUT_SEC, POLL_JOB_TIMEOUT_SEC, PREEMPT_CANCEL_TIMEOUT_SEC

_client: httpx.AsyncClient | None = None

def _ensure_dict(payload: Union[dict, str]) -> dict:
    """确保 payload 是 dict；若是 JSON 字符串则解析"""
    if isinstance(payload, str):
        return json.loads(payload)
    return payload

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

async def dispatch_backtest(node_id: str, payload: Union[dict, str], timeout: int = 240) -> dict:
    """向单节点提交 backtest，返回 {job_id, status}"""
    payload = _ensure_dict(payload)
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
            detail = resp.text
            raise RuntimeError(f"dispatch_backtest({node_id}) 422: {detail} | payload={payload}")
        resp.raise_for_status()
        data = resp.json()
        return {"job_id": data["job_id"], "status": data.get("status", "queued")}
    except Exception as e:
        raise RuntimeError(f"dispatch_backtest({node_id}) failed: {e}")

async def dispatch_selection(payload: Union[dict, str], timeout: int = 60) -> dict:
    """并行向 3 节点发起 selection，返回各节点结果"""
    payload = _ensure_dict(payload)
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

    # IA5.6.6: selection SignalTrace follows the same shard-union
    # semantics as the selected codes. Keep per-node results intact while
    # exposing one deterministic cluster-level trace artifact.
    trace_by_code = {}
    trace_meta = None
    any_trace = False
    for node_id in sorted(success):
        node_trace = success[node_id].get("signal_trace")
        if not isinstance(node_trace, dict):
            continue
        any_trace = True
        if trace_meta is None:
            trace_meta = {
                key: node_trace.get(key)
                for key in ("schema_version", "engine_version", "signal_date", "formula")
                if node_trace.get(key) is not None
            }
        node_traces = node_trace.get("traces")
        if not isinstance(node_traces, list):
            continue
        for code_trace in node_traces:
            if not isinstance(code_trace, dict) or not code_trace.get("code"):
                continue
            trace_by_code.setdefault(str(code_trace["code"]), code_trace)

    result = {"nodes": success, "codes": sorted(all_codes)}
    if any_trace:
        signal_trace = dict(trace_meta or {})
        signal_trace["traces"] = [trace_by_code[code] for code in sorted(trace_by_code)]
        signal_trace.setdefault("decisions", [])
        result["signal_trace"] = signal_trace

    return result

async def cancel_task(
    node_id: str,
    job_id: str,
    reason: str = "preempted_by_selection",
    timeout: int = PREEMPT_CANCEL_TIMEOUT_SEC,
) -> bool:
    """Request cancellation and wait until the node confirms the worker stopped.

    A 200 from /cancel only means the cancellation signal was accepted. The
    scheduler must not release the worker slot until the async job reaches a
    terminal state, otherwise selection could overlap the old backtest.
    """
    client = await get_client()
    try:
        resp = await asyncio.wait_for(
            client.post(
                f"{HF_NODES[node_id]}/api/v1/backtest/cancel",
                json={"job_id": job_id, "reason": reason},
            ),
            timeout=10,
        )
        if resp.status_code != 200:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            try:
                status_resp = await asyncio.wait_for(
                    client.get(f"{HF_NODES[node_id]}/api/v1/backtest/async/{job_id}"),
                    timeout=3,
                )
                if status_resp.status_code == 404:
                    return False
                status_resp.raise_for_status()
                status = status_resp.json().get("status")
                if status == "cancelled":
                    return True
                if status in ("failed", "done"):
                    # The backtest reached a terminal state without being
                    # cooperatively cancelled. It must not be requeued as a
                    # selection preemption, otherwise a naturally completed
                    # task could be executed a second time.
                    return False
            except Exception:
                pass
            await asyncio.sleep(0.25)
        return False
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


async def fetch_backtest_artifact(node_id: str, job_id: str, name: str, timeout: int = 120) -> bytes:
    """从节点下载 Parquet artifact。"""
    client = await get_client()
    resp = await asyncio.wait_for(
        client.get(
            f"{HF_NODES[node_id]}/api/v1/backtest/async/{job_id}/artifact/{name}",
        ),
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.content