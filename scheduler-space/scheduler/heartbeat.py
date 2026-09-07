# backend/scheduler/heartbeat.py
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from scheduler.db import execute

router = APIRouter(prefix="/internal", tags=["internal"])

class HeartbeatPayload(BaseModel):
    node_id: str
    status: str
    task_id: Optional[int] = None
    load: float = 0.0
    metrics: dict = {}

@router.post("/heartbeat")
async def receive_heartbeat(
    payload: HeartbeatPayload,
    authorization: Optional[str] = Header(None),
):
    expected_token = "internal-secret-change-me"
    if authorization != f"Bearer {expected_token}":
        raise HTTPException(401, "Invalid token")

    now = datetime.utcnow()
    # 只更新运行时状态，不覆盖 endpoint/name/weight 等静态字段
    from scheduler.db import execute
    await execute("""
        UPDATE cluster_nodes
        SET status = $2,
            current_task_id = $3,
            task_type = $4,
            heartbeat_at = now(),
            last_error = $5,
            updated_at = now()
        WHERE node_id = $1
    """,
        payload.node_id,
        payload.status,
        payload.task_id,
        "backtest" if payload.task_id else None,
        None,
    )

    # 记录心跳历史
    from scheduler.db import execute
    await execute("""
        INSERT INTO node_heartbeats (node_id, status, task_id, load, metrics, reported_at)
        VALUES ($1, $2, $3, $4, $5, now())
    """, payload.node_id, payload.status, payload.task_id, payload.load, payload.metrics)

    return {"ok": True}