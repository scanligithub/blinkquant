# backend/scheduler/heartbeat.py
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from .db import execute

router = APIRouter(prefix="/internal", tags=["internal"])

class HeartbeatPayload(BaseModel):
    node_id: str
    status: str
    task_id: Optional[int] = None
    load: float = 0.0
    metrics: dict = {}

router = APIRouter(prefix="/internal", tags=["internal"])

@router.post("/heartbeat")
async def receive_heartbeat(
    payload: HeartbeatPayload,
    authorization: Optional[str] = Header(None),
    expected_token: str = "internal-secret-change-me",
):
    if authorization != f"Bearer internal-secret-change-me":
        raise HTTPException(401, "Invalid token")

    now = datetime.utcnow()
    await execute("""
        INSERT INTO cluster_nodes (node_id, name, endpoint, weight, status,
                                   current_task_id, task_type, heartbeat_at, last_error, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())
        ON CONFLICT (node_id) DO UPDATE SET
            status = EXCLUDED.status,
            current_task_id = EXCLUDED.current_task_id,
            task_type = EXCLUDED.task_type,
            heartbeat_at = EXCLUDED.heartbeat_at,
            last_error = EXCLUDED.last_error,
            updated_at = now()
    """,
        payload.node_id,
        payload.node_id,
        "",
        1,
        payload.status,
        payload.task_id,
        "backtest" if payload.task_id else None,
        datetime.utcnow(),
        None,
    )

    await execute("""
        INSERT INTO node_heartbeats (node_id, status, task_id, load, metrics, reported_at)
        VALUES ($1, $2, $3, $4, $5, now())
    """, payload.node_id, payload.status, payload.task_id, payload.load, payload.metrics)

    return {"ok": True}