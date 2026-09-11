# backend/scheduler/routes.py
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import Optional, List
import json

from .db import acquire, execute, fetch, fetchrow
from .config import INTERNAL_TOKEN
from .scheduler import ClusterScheduler
from .models import TaskRow

router = APIRouter(prefix="/internal", tags=["internal"])


# ──────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────

async def verify_internal_token(authorization: Optional[str] = Header(None)) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    token = authorization[7:]
    if token != INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid internal token")


# ──────────────────────────────────────────────
# Schemas
# ──────────────────────────────────────────────

class TaskCreate(BaseModel):
    user_id: str
    task_type: str  # "selection" | "backtest"
    payload: dict
    priority: int = 0


class TaskResponse(BaseModel):
    id: int
    user_id: str
    task_type: str
    payload: dict
    priority: int
    status: str
    assigned_node: Optional[str]
    cluster_job_id: Optional[str]
    result: Optional[dict]
    error: Optional[str]
    created_at: str
    queued_at: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
    retry_count: int
    max_retries: int
    preempted_by: Optional[int]
    generation: int


class TaskListResponse(BaseModel):
    tasks: List[TaskResponse]


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _row_to_task(row: dict) -> TaskResponse:
    return TaskResponse(
        id=row["id"],
        user_id=row["user_id"],
        task_type=row["task_type"],
        payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
        priority=row["priority"],
        status=row["status"],
        assigned_node=row["assigned_node"],
        cluster_job_id=row["cluster_job_id"],
        result=json.loads(row["result"]) if row["result"] else None,
        error=row["error"],
        created_at=row["created_at"],
        queued_at=row["queued_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        retry_count=row["retry_count"],
        max_retries=row["max_retries"],
        preempted_by=row["preempted_by"],
        generation=row["generation"],
    )


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────

@router.post("/tasks", dependencies=[Depends(verify_internal_token)])
async def create_task(task: TaskCreate) -> dict:
    """创建任务，返回 task_id"""
    async with acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO task_queue (user_id, task_type, payload, priority, status)
            VALUES (?, ?, ?, ?, 'pending')
            RETURNING id, status, created_at
        """, task.user_id, task.task_type, json.dumps(task.payload), task.priority)
    return {"task_id": row["id"], "status": row["status"], "created_at": row["created_at"]}


@router.get("/tasks", dependencies=[Depends(verify_internal_token)])
async def list_tasks(user_id: Optional[str] = None, status: Optional[str] = None) -> TaskListResponse:
    """列出任务（可选按 user_id/status 过滤），默认不返回 result 全文"""
    query = "SELECT * FROM task_queue WHERE 1=1"
    params = []
    if user_id:
        query += " AND user_id = ?"
        params.append(user_id)
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT 100"
    
    rows = await fetch(query, *params)
    return TaskListResponse(tasks=[_row_to_task(r) for r in rows])


@router.get("/tasks/{task_id}", dependencies=[Depends(verify_internal_token)])
async def get_task(task_id: int) -> TaskResponse:
    """获取单个任务详情（含 result）"""
    row = await fetchrow("SELECT * FROM task_queue WHERE id = ?", task_id)
    if not row:
        raise HTTPException(404, "Task not found")
    return _row_to_task(row)


@router.post("/tasks/{task_id}/cancel", dependencies=[Depends(verify_internal_token)])
async def cancel_task(task_id: int) -> dict:
    """取消任务：pending/queued → cancelled；running → 触发抢占/取消流程"""
    row = await fetchrow("SELECT status, task_type, cluster_job_id, assigned_node FROM task_queue WHERE id = ?", task_id)
    if not row:
        raise HTTPException(404, "Task not found")
    
    if row["status"] in ("pending", "queued"):
        async with acquire() as conn:
            await conn.execute("UPDATE task_queue SET status = 'cancelled', finished_at = datetime('now') WHERE id = ?", task_id)
        return {"ok": True, "message": "Task cancelled"}
    
    elif row["status"] == "running":
        # 运行中任务：标记为 preempted，由调度器下一轮回收节点
        async with acquire() as conn:
            await conn.execute("""
                UPDATE task_queue
                SET status = 'preempted', finished_at = datetime('now'), error = 'cancelled by user',
                    preempted_by = NULL, retry_count = 0
                WHERE id = ? AND status = 'running'
            """, task_id)
            # 释放节点
            if row["assigned_node"]:
                await conn.execute("""
                    UPDATE cluster_nodes
                    SET status = 'idle', current_task_id = NULL, task_type = NULL,
                        generation = generation + 1, updated_at = datetime('now')
                    WHERE node_id = ?
                """, row["assigned_node"])
        return {"ok": True, "message": "Running task marked for cancellation"}
    
    else:
        return {"ok": False, "message": f"Task already in terminal state: {row['status']}"}


# ──────────────────────────────────────────────
# Scheduler control (for debugging)
# ──────────────────────────────────────────────

@router.post("/scheduler/trigger-cycle", dependencies=[Depends(verify_internal_token)])
async def trigger_cycle() -> dict:
    """手动触发一次调度周期（调试用）"""
    from .scheduler import ClusterScheduler
    s = ClusterScheduler()
    # Don't start full scheduler loop, just run one cycle
    await s._schedule_cycle()
    return {"ok": True, "message": "Schedule cycle triggered"}