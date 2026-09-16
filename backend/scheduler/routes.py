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

# 列表查询：禁止 SELECT *，不读 result 大字段
LIST_COLS = (
    "id, user_id, task_type, payload, priority, status, "
    "assigned_node, cluster_job_id, error, result_summary, result_uri, "
    "created_at, queued_at, started_at, finished_at, "
    "retry_count, max_retries, preempted_by, generation, timeout_sec"
)


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
    result_summary: Optional[dict] = None
    result_uri: Optional[str] = None
    error: Optional[str]
    created_at: str
    queued_at: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
    retry_count: int
    max_retries: int
    preempted_by: Optional[int]
    generation: int
    timeout_sec: Optional[int] = None


class TaskListResponse(BaseModel):
    tasks: List[TaskResponse]


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _row_to_task(row: dict, slim: bool = False) -> TaskResponse:
    """将 DB 行转为 TaskResponse。slim=True 时不解析 result（列表用）。"""
    result = None
    if not slim and row.get("result"):
        try:
            result = json.loads(row["result"])
        except (json.JSONDecodeError, TypeError):
            result = None

    result_summary = None
    if row.get("result_summary"):
        try:
            result_summary = json.loads(row["result_summary"])
        except (json.JSONDecodeError, TypeError):
            result_summary = None

    return TaskResponse(
        id=row["id"],
        user_id=row["user_id"],
        task_type=row["task_type"],
        payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
        priority=row["priority"],
        status=row["status"],
        assigned_node=row.get("assigned_node"),
        cluster_job_id=row.get("cluster_job_id"),
        result=result,
        result_summary=result_summary,
        result_uri=row.get("result_uri"),
        error=row.get("error"),
        created_at=row["created_at"],
        queued_at=row.get("queued_at"),
        started_at=row.get("started_at"),
        finished_at=row.get("finished_at"),
        retry_count=row["retry_count"],
        max_retries=row["max_retries"],
        preempted_by=row.get("preempted_by"),
        generation=row["generation"],
        timeout_sec=row.get("timeout_sec"),
    )


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────

@router.post("/tasks", dependencies=[Depends(verify_internal_token)])
async def create_task(task: TaskCreate) -> dict:
    """创建任务，返回 task_id"""
    from .config import compute_task_timeout_sec
    timeout_sec = compute_task_timeout_sec(task.task_type, task.payload)
    async with acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO task_queue (user_id, task_type, payload, priority, status, timeout_sec)
            VALUES (?, ?, ?, ?, 'pending', ?)
            RETURNING id, status, created_at
        """, task.user_id, task.task_type, json.dumps(task.payload), task.priority, timeout_sec)
    return {"task_id": row["id"], "status": row["status"], "created_at": row["created_at"]}


@router.get("/tasks", dependencies=[Depends(verify_internal_token)])
async def list_tasks(user_id: Optional[str] = None, status: Optional[str] = None) -> TaskListResponse:
    """列出任务（可选按 user_id/status 过滤），不返回 result 全文"""
    query = f"SELECT {LIST_COLS} FROM task_queue WHERE 1=1"
    params = []
    if user_id:
        query += " AND user_id = ?"
        params.append(user_id)
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT 100"

    rows = await fetch(query, *params)
    return TaskListResponse(tasks=[_row_to_task(r, slim=True) for r in rows])


@router.get("/tasks/{task_id}", dependencies=[Depends(verify_internal_token)])
async def get_task(task_id: int) -> TaskResponse:
    """获取单个任务详情（含 result）"""
    row = await fetchrow("SELECT * FROM task_queue WHERE id = ?", task_id)
    if not row:
        raise HTTPException(404, "Task not found")
    return _row_to_task(row)


@router.get("/tasks/{task_id}/artifact", dependencies=[Depends(verify_internal_token)])
async def get_task_artifact(task_id: int, name: str = "equity_curve", fmt: str = "json"):
    """按需加载回测产物：equity_curve / trades / positions_daily。"""
    from .config import RESULT_DIR
    from .result_store import load_part, load_as_legacy_json, ARTIFACT_NAMES

    row = await fetchrow("SELECT result_uri, result FROM task_queue WHERE id = ?", task_id)
    if not row:
        raise HTTPException(404, "Task not found")

    # 旧任务：没有 result_uri 但有 result JSON → 直接返回
    if not row.get("result_uri") and row.get("result"):
        data = json.loads(row["result"])
        if name in data:
            return data[name]
        raise HTTPException(404, f"Artifact '{name}' not found in legacy result")

    if not row.get("result_uri"):
        raise HTTPException(404, "No result available for this task")

    if fmt == "legacy":
        full = load_as_legacy_json(row["result_uri"], RESULT_DIR)
        return full.get(name, [])

    if name not in ARTIFACT_NAMES:
        raise HTTPException(400, f"Unknown artifact: {name}. Valid: {ARTIFACT_NAMES}")

    df = load_part(row["result_uri"], name, RESULT_DIR)
    if df is None:
        raise HTTPException(404, f"Artifact '{name}' not found")

    import io
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)
    from fastapi.responses import Response
    return Response(content=buffer.getvalue(), media_type="application/octet-stream")


@router.post("/tasks/{task_id}/cancel", dependencies=[Depends(verify_internal_token)])
async def cancel_task(task_id: int, user_id: Optional[str] = None) -> dict:
    """取消任务：pending/queued → cancelled；running → 触发抢占/取消流程
    
    如果提供 user_id，会校验任务归属，防止越权取消。
    """
    row = await fetchrow("SELECT status, task_type, cluster_job_id, assigned_node, user_id FROM task_queue WHERE id = ?", task_id)
    if not row:
        raise HTTPException(404, "Task not found")
    
    # 校验用户归属
    if user_id and row["user_id"] != user_id:
        raise HTTPException(403, "Task belongs to another user")
    
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


@router.get("/cluster/status", dependencies=[Depends(verify_internal_token)])
async def get_cluster_status() -> dict:
    """获取集群状态：节点列表 + 队列统计"""
    nodes = await fetch("""
        SELECT node_id, name, endpoint, weight, status, current_task_id,
               task_type, heartbeat_at, last_error
        FROM cluster_nodes
        ORDER BY node_id
    """)

    stats = await fetchrow("""
        SELECT
            SUM(CASE WHEN status = 'pending' AND task_type = 'selection' THEN 1 ELSE 0 END) AS pending_selection,
            SUM(CASE WHEN status = 'pending' AND task_type = 'backtest' THEN 1 ELSE 0 END) AS pending_backtest,
            SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running
        FROM task_queue
    """)

    return {
        "nodes": [
            {
                "node_id": n["node_id"],
                "name": n["name"],
                "status": n["status"],
                "current_task_id": n["current_task_id"],
                "task_type": n["task_type"],
                "heartbeat_at": n["heartbeat_at"],
                "last_error": n["last_error"],
            }
            for n in nodes
        ],
        "queueStats": {
            "pending_selection": int(stats["pending_selection"] or 0) if stats else 0,
            "pending_backtest": int(stats["pending_backtest"] or 0) if stats else 0,
            "running": int(stats["running"] or 0) if stats else 0,
        },
    }


@router.post("/admin/unstick", dependencies=[Depends(verify_internal_token)])
async def admin_unstick() -> dict:
    """人工解卡：将所有僵尸占用/卡住的 running 任务与节点就地收尾。

    用于当前 task 挂在 node 这类场景：节点 job 已在节点内存 dict 丢失
    （进程重启/OOM），调度侧仍显示 running。一次性把超时 running 的
    backtest 标记失败，节点回 idle，不依赖心跳。
    """
    mark = await execute("""
        UPDATE task_queue
        SET status = 'failed', finished_at = datetime('now'),
            error = COALESCE(error, 'unstick: manual recovery'),
            assigned_node = NULL, cluster_job_id = NULL,
            generation = generation + 1
        WHERE status = 'running' AND task_type = 'backtest'
          AND started_at < datetime('now', '-30 minutes')
    """)

    release = await execute("""
        UPDATE cluster_nodes
        SET status = 'idle', current_task_id = NULL, task_type = NULL,
            generation = generation + 1, updated_at = datetime('now')
        WHERE current_task_id IN (
            SELECT id FROM task_queue
            WHERE status = 'failed' AND error LIKE 'unstick%'
        )
    """)

    idle = await execute("""
        UPDATE cluster_nodes
        SET status = 'idle', current_task_id = NULL, task_type = NULL,
            generation = generation + 1, updated_at = datetime('now')
        WHERE status <> 'idle'
          AND (current_task_id IS NULL
               OR current_task_id NOT IN (
                   SELECT id FROM task_queue WHERE status = 'running'
               ))
    """)

    return {"ok": True, "marked_failed": mark, "released_nodes": release,
            "idled_nonidle": idle}