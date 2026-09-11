# backend/scheduler/heartbeat.py
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from .db import execute
from .config import INTERNAL_TOKEN
import json

router = APIRouter(prefix="/internal", tags=["internal"])

class HeartbeatPayload(BaseModel):
    node_id: str
    status: str
    task_id: Optional[int] = None
    load: float = 0.0
    metrics: dict = {}
    generation: Optional[int] = None

@router.post("/heartbeat")
async def receive_heartbeat(
    payload: HeartbeatPayload,
    authorization: Optional[str] = Header(None),
):
    if authorization != f"Bearer {INTERNAL_TOKEN}":
        raise HTTPException(401, "Invalid token")

    # 只更新运行时状态，不覆盖 endpoint/name/weight/generation 等静态字段
    # 心跳不应覆盖 generation（由调度器管理），只更新状态相关字段
    await execute("""
        UPDATE cluster_nodes
        SET status = ?,
            current_task_id = ?,
            task_type = ?,
            heartbeat_at = datetime('now'),
            last_error = ?,
            updated_at = datetime('now')
        WHERE node_id = ?
    """,
        payload.status,
        payload.task_id,
        "backtest" if payload.task_id else None,
        None,
        payload.node_id,
    )

    # 记录心跳历史
    await execute("""
        INSERT INTO node_heartbeats (node_id, status, task_id, load, metrics, reported_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    """, payload.node_id, payload.status, payload.task_id, payload.load, json.dumps(payload.metrics))

    return {"ok": True}