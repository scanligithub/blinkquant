# backend/scheduler/routes.py
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import Optional, List
import json

from .db import acquire, execute, fetch, fetchrow, fetchval
from .config import INTERNAL_TOKEN
from .scheduler import ClusterScheduler
from .models import TaskRow

router = APIRouter(prefix="/internal", tags=["internal"])

# 列表查询：禁止 SELECT *，不读 result 大字段
LIST_COLS = (
    "id, user_id, task_type, payload, priority, status, "
    "assigned_node, cluster_job_id, error, result_summary, result_uri, result_bytes, "
    "strategy_template_id, strategy_template_name, strategy_template_updated_at, strategy_template_version, source_task_id, "
    "progress_pct, progress_json, "
    "(SELECT id FROM artifacts a WHERE a.task_id = task_queue.id) AS artifact_id, "
    "created_at, queued_at, started_at, finished_at, "
    "retry_count, max_retries, preempted_by, generation, timeout_sec"
)


# ──────────────────────────────────────────────
# Auth & Access Control
# ──────────────────────────────────────────────

async def verify_internal_token(authorization: Optional[str] = Header(None)) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    token = authorization[7:]
    if token != INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid internal token")


def _is_admin(role: str | None) -> bool:
    return (role or "").lower() == "admin"


def _assert_task_access(row: dict, user_id: str | None, role: str | None) -> None:
    """非 admin 必须提供 user_id 且与任务一致。"""
    if _is_admin(role):
        return
    if not user_id:
        raise HTTPException(401, "user_id required")
    if str(row.get("user_id")) != str(user_id):
        raise HTTPException(403, "Task belongs to another user")


# ──────────────────────────────────────────────
# Schemas
# ──────────────────────────────────────────────

class TaskCreate(BaseModel):
    user_id: str
    task_type: str  # "selection" | "backtest"
    payload: dict
    priority: int = 0
    strategy_template_id: Optional[int] = None


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
    result_bytes: Optional[int] = None
    strategy_template_id: Optional[int] = None
    strategy_template_name: Optional[str] = None
    strategy_template_updated_at: Optional[str] = None
    strategy_template_version: Optional[int] = None
    source_task_id: Optional[int] = None
    artifact_id: Optional[int] = None
    progress_pct: Optional[float] = None
    progress: Optional[dict] = None
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

    progress = None
    if row.get("progress_json"):
        try:
            progress = json.loads(row["progress_json"])
        except (json.JSONDecodeError, TypeError):
            progress = None

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
        result_bytes=row.get("result_bytes"),
        strategy_template_id=row.get("strategy_template_id"),
        strategy_template_name=row.get("strategy_template_name"),
        strategy_template_updated_at=row.get("strategy_template_updated_at"),
        strategy_template_version=row.get("strategy_template_version"),
        source_task_id=row.get("source_task_id"),
        artifact_id=row.get("artifact_id"),
        progress_pct=row.get("progress_pct"),
        progress=progress,
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
# Strategy template binding
# ──────────────────────────────────────────────

def _task_template_config(payload: dict) -> dict | None:
    if not isinstance(payload, dict) or not isinstance(payload.get("strategy"), dict):
        return None
    candidate = {
        "strategy": payload.get("strategy"),
        "fee_policy": payload.get("fee_policy") or {"mode": "historical"},
        "benchmark": payload.get("benchmark") or {"enabled": True, "type": "index", "index_id": "000300"},
        "min_listing_days": payload.get("min_listing_days", 0),
        "exclude_st": payload.get("exclude_st", False),
    }
    if payload.get("source_selection_strategy") is not None:
        candidate["source_selection_strategy"] = payload.get("source_selection_strategy")
    return candidate


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


async def _resolve_strategy_template(task: TaskCreate) -> tuple[int | None, str | None, str | None, int | None]:
    """Validate optional template binding and return an immutable task-time snapshot."""
    template_id = task.strategy_template_id
    if template_id is None:
        return None, None, None, None
    if task.task_type != "backtest":
        raise HTTPException(400, "strategy_template_id is only valid for backtest tasks")

    candidate = _task_template_config(task.payload)
    if candidate is None:
        # Backward-compatible task payloads may not contain the canonical strategy.
        return None, None, None, None

    row = await fetchrow(
        "SELECT id, name, updated_at, config, "
        "(SELECT MAX(version_no) FROM backtest_strategy_versions WHERE strategy_template_id = backtest_strategy_templates.id) AS version_no "
        "FROM backtest_strategy_templates WHERE id = ? AND user_id = ?",
        template_id, task.user_id,
    )
    if not row:
        raise HTTPException(400, "strategy template not found for user")

    try:
        template_config = json.loads(row["config"])
    except (TypeError, json.JSONDecodeError):
        raise HTTPException(500, "stored strategy template config is invalid")

    if _canonical_json(candidate) != _canonical_json(template_config):
        # Do not reject the backtest: the user may have edited the panel after selecting
        # a template. Simply avoid a false historical attribution.
        return None, None, None, None

    return int(row["id"]), str(row["name"]), row["updated_at"], int(row["version_no"] or 1)


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────

@router.post("/tasks", dependencies=[Depends(verify_internal_token)])
async def create_task(task: TaskCreate) -> dict:
    """创建任务，返回 task_id"""
    from .config import compute_task_timeout_sec
    template_id, template_name, template_updated_at, template_version = await _resolve_strategy_template(task)
    timeout_sec = compute_task_timeout_sec(task.task_type, task.payload)
    async with acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO task_queue (
                user_id, task_type, payload, priority, status, timeout_sec,
                strategy_template_id, strategy_template_name, strategy_template_updated_at, strategy_template_version
            )
            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)
            RETURNING id, status, created_at, strategy_template_id,
                      strategy_template_name, strategy_template_updated_at, strategy_template_version
        """,
        task.user_id, task.task_type, json.dumps(task.payload), task.priority, timeout_sec,
        template_id, template_name, template_updated_at, template_version)
    return {
        "task_id": row["id"],
        "status": row["status"],
        "created_at": row["created_at"],
        "strategy_template_id": row["strategy_template_id"],
        "strategy_template_name": row["strategy_template_name"],
        "strategy_template_updated_at": row["strategy_template_updated_at"],
        "strategy_template_version": row["strategy_template_version"],
    }


@router.get("/tasks", dependencies=[Depends(verify_internal_token)])
async def list_tasks(
    user_id: Optional[str] = None,
    status: Optional[str] = None,
    role: Optional[str] = None,
) -> TaskListResponse:
    query = f"SELECT {LIST_COLS} FROM task_queue WHERE 1=1"
    params: list = []
    if not _is_admin(role):
        if not user_id:
            raise HTTPException(401, "user_id required")
        query += " AND user_id = ?"
        params.append(user_id)
    elif user_id:
        query += " AND user_id = ?"
        params.append(user_id)
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT 100"
    rows = await fetch(query, *params)
    return TaskListResponse(tasks=[_row_to_task(r, slim=True) for r in rows])


@router.get("/tasks/{task_id}", dependencies=[Depends(verify_internal_token)])
async def get_task(
    task_id: int,
    user_id: Optional[str] = None,
    role: Optional[str] = None,
) -> TaskResponse:
    row = await fetchrow("""
        SELECT tq.*, a.id AS artifact_id
        FROM task_queue tq
        LEFT JOIN artifacts a ON a.task_id = tq.id
        WHERE tq.id = ?
    """, task_id)
    if not row:
        raise HTTPException(404, "Task not found")
    _assert_task_access(row, user_id, role)
    return _row_to_task(row)



@router.post("/tasks/{task_id}/rerun", dependencies=[Depends(verify_internal_token)])
async def rerun_task(
    task_id: int,
    user_id: Optional[str] = None,
    role: Optional[str] = None,
) -> dict:
    """从历史 terminal backtest task 精确创建一个新的 pending task。"""
    from .config import compute_task_timeout_sec

    row = await fetchrow(
        "SELECT id, user_id, task_type, payload, priority, status, "
        "strategy_template_id FROM task_queue WHERE id = ?",
        task_id,
    )
    if not row:
        raise HTTPException(404, "Task not found")
    _assert_task_access(row, user_id, role)

    if row["task_type"] != "backtest":
        raise HTTPException(400, "Only backtest tasks can be rerun")
    if row["status"] not in ("done", "failed", "cancelled", "preempted"):
        raise HTTPException(409, f"Task is not terminal: {row['status']}")

    try:
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    except (TypeError, json.JSONDecodeError):
        raise HTTPException(500, "Stored task payload is invalid")
    if not isinstance(payload, dict):
        raise HTTPException(500, "Stored task payload is invalid")

    candidate_template_id = row.get("strategy_template_id")
    if candidate_template_id is not None:
        current = await fetchrow(
            "SELECT id FROM backtest_strategy_templates WHERE id = ? AND user_id = ?",
            candidate_template_id, row["user_id"],
        )
        if not current:
            candidate_template_id = None

    task = TaskCreate(
        user_id=str(row["user_id"]),
        task_type="backtest",
        payload=payload,
        priority=int(row.get("priority") or 0),
        strategy_template_id=candidate_template_id,
    )
    template_id, template_name, template_updated_at, template_version = await _resolve_strategy_template(task)
    timeout_sec = compute_task_timeout_sec(task.task_type, task.payload)

    async with acquire() as conn:
        new_row = await conn.fetchrow("""
            INSERT INTO task_queue (
                user_id, task_type, payload, priority, status, timeout_sec,
                strategy_template_id, strategy_template_name, strategy_template_updated_at, strategy_template_version,
                source_task_id
            )
            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
            RETURNING id, status, created_at, strategy_template_id,
                      strategy_template_name, strategy_template_updated_at, strategy_template_version,
                      source_task_id
        """,
        task.user_id, task.task_type, json.dumps(task.payload), task.priority, timeout_sec,
        template_id, template_name, template_updated_at, template_version, task_id)

    return {
        "task_id": new_row["id"],
        "status": new_row["status"],
        "created_at": new_row["created_at"],
        "strategy_template_id": new_row["strategy_template_id"],
        "strategy_template_name": new_row["strategy_template_name"],
        "strategy_template_updated_at": new_row["strategy_template_updated_at"],
        "strategy_template_version": new_row["strategy_template_version"],
        "source_task_id": new_row["source_task_id"],
    }


@router.get("/artifacts", dependencies=[Depends(verify_internal_token)])
async def list_artifacts(
    artifact_type: Optional[str] = None,
    user_id: Optional[str] = None,
    role: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    if artifact_type not in (None, "selection", "backtest"):
        raise HTTPException(400, "Invalid artifact_type")
    if not _is_admin(role) and not user_id:
        raise HTTPException(401, "user_id required")
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    where = ["1=1"]
    params: list = []
    if not _is_admin(role):
        where.append("user_id = ?")
        params.append(user_id)
    elif user_id:
        where.append("user_id = ?")
        params.append(user_id)
    if artifact_type:
        where.append("artifact_type = ?")
        params.append(artifact_type)

    where_sql = " AND ".join(where)
    total = int(await fetchval(f"SELECT COUNT(*) FROM artifacts WHERE {where_sql}") or 0)

    query = f"""SELECT id, user_id, artifact_type, task_id, title, status, metadata,
                       summary, result_json, result_uri, result_bytes, created_at, finished_at,
                       EXISTS(SELECT 1 FROM task_queue tq WHERE tq.id = artifacts.task_id) AS task_exists
                FROM artifacts WHERE {where_sql}
                ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"""
    page_params = [*params, limit, offset]
    rows = await fetch(query, *page_params)
    items = []
    for row in rows:
        try: metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        except (TypeError, json.JSONDecodeError): metadata = {}
        try: summary = json.loads(row["summary"]) if row["summary"] else None
        except (TypeError, json.JSONDecodeError): summary = None
        try: result = json.loads(row["result_json"]) if row["result_json"] else None
        except (TypeError, json.JSONDecodeError): result = None
        task_exists = bool(row["task_exists"])
        payload = metadata.get("payload") if isinstance(metadata, dict) else {}
        items.append({
            "id": row["id"], "artifact_id": row["id"], "task_id": row["task_id"],
            "user_id": row["user_id"], "artifact_type": row["artifact_type"],
            "task_type": row["artifact_type"], "title": row["title"],
            "status": "done", "payload": payload, "result": result,
            "result_summary": summary, "summary": summary, "result_uri": row["result_uri"],
            "result_bytes": row["result_bytes"] or 0, "metadata": metadata,
            "created_at": row["created_at"], "finished_at": row["finished_at"],
            "task_exists": bool(task_exists),
            "strategy_template_id": metadata.get("strategy_template_id"),
            "strategy_template_name": metadata.get("strategy_template_name"),
            "strategy_template_updated_at": metadata.get("strategy_template_updated_at"),
            "strategy_template_version": metadata.get("strategy_template_version"),
            "source_task_id": metadata.get("source_task_id"),
        })
    return {"artifacts": items, "total": total, "limit": limit, "offset": offset}

@router.get("/artifacts/{artifact_id}", dependencies=[Depends(verify_internal_token)])
async def get_artifact(artifact_id: int, user_id: Optional[str] = None, role: Optional[str] = None) -> dict:
    row = await fetchrow("SELECT * FROM artifacts WHERE id = ?", artifact_id)
    if not row: raise HTTPException(404, "Artifact not found")
    _assert_task_access(row, user_id, role)
    try: metadata = json.loads(row["metadata"]) if row["metadata"] else {}
    except (TypeError, json.JSONDecodeError): metadata = {}
    try: summary = json.loads(row["summary"]) if row["summary"] else None
    except (TypeError, json.JSONDecodeError): summary = None
    try: result = json.loads(row["result_json"]) if row["result_json"] else None
    except (TypeError, json.JSONDecodeError): result = None
    task_exists = await fetchval("SELECT 1 FROM task_queue WHERE id = ?", row["task_id"])
    return {
        "id": row["id"], "artifact_id": row["id"], "task_id": row["task_id"],
        "user_id": row["user_id"], "artifact_type": row["artifact_type"],
        "task_type": row["artifact_type"], "title": row["title"], "status": "done",
        "payload": metadata.get("payload") or {}, "result": result,
        "result_summary": summary, "summary": summary, "result_uri": row["result_uri"],
        "result_bytes": row["result_bytes"] or 0, "metadata": metadata,
        "created_at": row["created_at"], "finished_at": row["finished_at"],
        "task_exists": bool(task_exists), "source_task_id": row["task_id"],
        "strategy_template_id": metadata.get("strategy_template_id"),
        "strategy_template_name": metadata.get("strategy_template_name"),
        "strategy_template_updated_at": metadata.get("strategy_template_updated_at"),
        "strategy_template_version": metadata.get("strategy_template_version"),
    }

@router.get("/artifacts/{artifact_id}/part", dependencies=[Depends(verify_internal_token)])
async def get_artifact_part(
    artifact_id: int, name: str = "equity_curve", fmt: str = "parquet",
    limit: int | None = None, offset: int = 0, code: str | None = None,
    side: str | None = None, date_from: str | None = None, date_to: str | None = None,
    user_id: str | None = None, role: str | None = None,
):
    from .config import RESULT_DIR
    from .result_store import ARTIFACT_NAMES, load_part
    row = await fetchrow("SELECT result_uri, result_json, user_id, artifact_type FROM artifacts WHERE id = ?", artifact_id)
    if not row: raise HTTPException(404, "Artifact not found")
    _assert_task_access(row, user_id, role)
    if row["artifact_type"] == "selection":
        if name not in ("result", "selection_result"): raise HTTPException(400, "Selection Artifact only supports result")
        try: return json.loads(row["result_json"]) if row["result_json"] else {}
        except (TypeError, json.JSONDecodeError): raise HTTPException(500, "Stored selection result is invalid")
    if not row["result_uri"]: raise HTTPException(404, "Artifact result files are unavailable")
    if name not in ARTIFACT_NAMES: raise HTTPException(400, f"Unknown artifact: {name}")
    df = load_part(row["result_uri"], name, RESULT_DIR)
    if df is None: raise HTTPException(404, f"Artifact '{name}' not found")
    import polars as pl
    if code and "code" in df.columns: df = df.filter(pl.col("code").cast(pl.Utf8) == code)
    if side and "side" in df.columns: df = df.filter(pl.col("side").cast(pl.Utf8).str.to_uppercase() == side.upper())
    date_col = "execution_date" if "execution_date" in df.columns else ("date" if "date" in df.columns else None)
    if date_col and date_from: df = df.filter(pl.col(date_col).cast(pl.Utf8) >= date_from)
    if date_col and date_to: df = df.filter(pl.col(date_col).cast(pl.Utf8) <= date_to)
    total = df.height; off = max(0, int(offset or 0))
    if limit is not None: lim = max(1, min(int(limit), 5000)); df = df.slice(off, lim)
    else: lim = total
    if fmt == "json": return {"name": name, "total": total, "offset": off, "limit": lim, "rows": df.to_dicts()}
    import io
    from fastapi.responses import Response
    buffer = io.BytesIO(); df.write_parquet(buffer, compression="zstd"); buffer.seek(0)
    return Response(content=buffer.getvalue(), media_type="application/octet-stream", headers={
        "Content-Disposition": f'attachment; filename="artifact_{artifact_id}_{name}.parquet"',
        "X-Result-Total-Rows": str(total), "X-Result-Offset": str(off), "X-Result-Limit": str(lim),
    })
@router.get("/tasks/{task_id}/artifact", dependencies=[Depends(verify_internal_token)])
async def get_task_artifact(
    task_id: int,
    name: str = "equity_curve",
    fmt: str = "parquet",
    limit: int | None = None,
    offset: int = 0,
    code: str | None = None,
    side: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    user_id: str | None = None,
    role: str | None = None,
):
    from .config import RESULT_DIR
    from .result_store import load_part, load_as_legacy_json, ARTIFACT_NAMES

    row = await fetchrow("SELECT result_uri, result, user_id FROM task_queue WHERE id = ?", task_id)
    if not row:
        raise HTTPException(404, "Task not found")
    _assert_task_access(row, user_id, role)

    if not row.get("result_uri") and row.get("result"):
        data = json.loads(row["result"])
        if name in data:
            if fmt == "json" and isinstance(data[name], list):
                items = data[name]
                total = len(items)
                off = max(0, offset)
                if limit is not None:
                    lim = max(1, min(int(limit), 5000))
                    items = items[off:off + lim]
                else:
                    lim = total
                return {"name": name, "total": total, "offset": off, "limit": lim, "rows": items}
            return data[name]
        raise HTTPException(404, f"Artifact '{name}' not found in legacy result")

    if not row.get("result_uri"):
        raise HTTPException(
            404,
            "结果文件已清理，仅保留摘要。Result files were purged; only summary remains.",
        )

    if fmt == "legacy":
        full = load_as_legacy_json(row["result_uri"], RESULT_DIR)
        return full.get(name, [])

    if name not in ARTIFACT_NAMES:
        raise HTTPException(400, f"Unknown artifact: {name}. Valid: {ARTIFACT_NAMES}")

    df = load_part(row["result_uri"], name, RESULT_DIR)
    if df is None:
        raise HTTPException(404, f"Artifact '{name}' not found")

    import polars as pl

    if code and "code" in df.columns:
        df = df.filter(pl.col("code").cast(pl.Utf8) == code)
    if side and "side" in df.columns:
        df = df.filter(pl.col("side").cast(pl.Utf8).str.to_uppercase() == side.upper())
    date_col = "execution_date" if "execution_date" in df.columns else (
        "date" if "date" in df.columns else None
    )
    if date_col and date_from:
        df = df.filter(pl.col(date_col).cast(pl.Utf8) >= date_from)
    if date_col and date_to:
        df = df.filter(pl.col(date_col).cast(pl.Utf8) <= date_to)

    total = df.height
    off = max(0, int(offset or 0))
    if limit is not None:
        lim = max(1, min(int(limit), 5000))
        df = df.slice(off, lim)
    else:
        lim = total

    if fmt == "json":
        return {
            "name": name,
            "total": total,
            "offset": off,
            "limit": lim if lim is not None else total,
            "rows": df.to_dicts(),
        }

    import io
    from fastapi.responses import Response

    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)
    filename = f"task_{task_id}_{name}.parquet"
    return Response(
        content=buffer.getvalue(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Result-Total-Rows": str(total),
            "X-Result-Offset": str(off),
            "X-Result-Limit": str(lim if lim is not None else total),
        },
    )


@router.post("/tasks/{task_id}/cancel", dependencies=[Depends(verify_internal_token)])
async def cancel_task(
    task_id: int,
    user_id: Optional[str] = None,
    role: Optional[str] = None,
) -> dict:
    row = await fetchrow("SELECT status, task_type, cluster_job_id, assigned_node, user_id FROM task_queue WHERE id = ?", task_id)
    if not row:
        raise HTTPException(404, "Task not found")
    _assert_task_access(row, user_id, role)

    if row["status"] in ("pending", "queued"):
        async with acquire() as conn:
            await conn.execute("UPDATE task_queue SET status = 'cancelled', finished_at = datetime('now') WHERE id = ?", task_id)
        return {"ok": True, "message": "Task cancelled"}

    elif row["status"] == "running":
        async with acquire() as conn:
            await conn.execute("""
                UPDATE task_queue
                SET status = 'preempted', finished_at = datetime('now'), error = 'cancelled by user',
                    preempted_by = NULL, retry_count = 0
                WHERE id = ? AND status = 'running'
            """, task_id)
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


@router.delete("/tasks/{task_id}", dependencies=[Depends(verify_internal_token)])
async def delete_task(
    task_id: int,
    user_id: Optional[str] = None,
    role: Optional[str] = None,
) -> dict:
    """真删除：先 cancel（若非终态）→ 删磁盘 → 删 DB 行。"""
    from .config import RESULT_DIR
    from .result_store import delete_result_dir

    row = await fetchrow(
        "SELECT id, status, user_id, result_uri, assigned_node, cluster_job_id, task_type "
        "FROM task_queue WHERE id = ?",
        task_id,
    )
    if not row:
        raise HTTPException(404, "Task not found")
    _assert_task_access(row, user_id, role)

    # 非终态：先 cancel
    if row["status"] in ("pending", "queued", "running"):
        await cancel_task(task_id, user_id=user_id, role=role)

    row2 = await fetchrow(
        "SELECT result_uri, user_id FROM task_queue WHERE id = ?", task_id
    )
    if not row2:
        return {"ok": True, "message": "Task already removed"}

    delete_result_dir(row2.get("result_uri"), RESULT_DIR)

    async with acquire() as conn:
        if _is_admin(role):
            await conn.execute("DELETE FROM task_queue WHERE id = ?", task_id)
        else:
            await conn.execute(
                "DELETE FROM task_queue WHERE id = ? AND user_id = ?",
                task_id, user_id,
            )
    return {
        "ok": True,
        "artifact_id": artifact_id,
        "message": "Task removed; historical artifact preserved" if artifact_id is not None else "Task and result files deleted",
    }


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


TASK_TYPE_ZH = {"selection": "选股", "backtest": "回测"}
NODE_STATUS_ZH = {"idle": "空闲", "running": "运行中", "draining": "排空中", "unhealthy": "异常", "maintenance": "维护"}


def _node_display_label(status: str, task_type: str | None, task_id: int | None) -> str:
    st = (status or "").lower()
    if st == "idle":
        return "空闲"
    if st == "running":
        zh = TASK_TYPE_ZH.get(task_type or "", task_type or "任务")
        return f"运行中  {zh} #{task_id}" if task_id is not None else f"运行中  {zh}"
    base = NODE_STATUS_ZH.get(st, st or "未知")
    if task_id is not None and task_type:
        return f"{base}  {TASK_TYPE_ZH.get(task_type, task_type)} #{task_id}"
    return base


def _task_brief(row: dict) -> dict:
    payload = row.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    payload = payload or {}
    formula = (payload.get("formula") or "")[:48]
    start = payload.get("start_date") or payload.get("start")
    end = payload.get("end_signal_date") or payload.get("end_date") or payload.get("signal_end_date")
    range_s = f"{str(start)[:10]}→{str(end)[:10]}" if start and end else None
    return {
        "id": row["id"],
        "task_type": row["task_type"],
        "task_type_zh": TASK_TYPE_ZH.get(row["task_type"], row["task_type"]),
        "status": row["status"],
        "user_id": row.get("user_id"),
        "assigned_node": row.get("assigned_node"),
        "priority": row.get("priority"),
        "progress_pct": row.get("progress_pct"),
        "formula_preview": formula or None,
        "date_range": range_s,
        "created_at": row.get("created_at"),
        "queued_at": row.get("queued_at"),
        "started_at": row.get("started_at"),
    }


@router.get("/cluster/status", dependencies=[Depends(verify_internal_token)])
async def get_cluster_status(queue_limit: int = 8) -> dict:
    limit = max(1, min(int(queue_limit), 30))

    nodes = await fetch("""
        SELECT node_id, name, endpoint, weight, status, current_task_id,
               task_type, heartbeat_at, last_error, generation, updated_at
        FROM cluster_nodes ORDER BY node_id
    """)

    stats = await fetchrow("""
        SELECT
            SUM(CASE WHEN status IN ('pending','queued') AND task_type='selection' THEN 1 ELSE 0 END) AS sel_q,
            SUM(CASE WHEN status IN ('pending','queued') AND task_type='backtest' THEN 1 ELSE 0 END) AS bt_q,
            SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running
        FROM task_queue
    """)

    sel_rows = await fetch("""
        SELECT id, user_id, task_type, payload, priority, status,
               assigned_node, progress_pct, created_at, queued_at, started_at
        FROM task_queue
        WHERE status IN ('pending','queued') AND task_type = 'selection'
        ORDER BY priority DESC, created_at ASC LIMIT ?
    """, limit)

    bt_rows = await fetch("""
        SELECT id, user_id, task_type, payload, priority, status,
               assigned_node, progress_pct, created_at, queued_at, started_at
        FROM task_queue
        WHERE status IN ('pending','queued') AND task_type = 'backtest'
        ORDER BY priority DESC, created_at ASC LIMIT ?
    """, limit)

    run_rows = await fetch("""
        SELECT id, user_id, task_type, payload, priority, status,
               assigned_node, progress_pct, created_at, queued_at, started_at
        FROM task_queue
        WHERE status = 'running'
        ORDER BY started_at ASC LIMIT ?
    """, limit)

    node_list = []
    for n in nodes:
        st = n["status"] or "idle"
        tid = n["current_task_id"]
        tt = n["task_type"]
        node_list.append({
            "node_id": n["node_id"],
            "name": n["name"],
            "status": st,
            "status_zh": NODE_STATUS_ZH.get(st, st),
            "current_task_id": tid,
            "task_type": tt,
            "task_type_zh": TASK_TYPE_ZH.get(tt or "", tt),
            "display_label": _node_display_label(st, tt, tid),
            "heartbeat_at": n["heartbeat_at"],
            "last_error": n["last_error"],
            "generation": n.get("generation"),
            "updated_at": n.get("updated_at"),
        })

    return {
        "nodes": node_list,
        "queueStats": {
            "pending_selection": int(stats["sel_q"] or 0) if stats else 0,
            "pending_backtest": int(stats["bt_q"] or 0) if stats else 0,
            "running": int(stats["running"] or 0) if stats else 0,
        },
        "queues": {
            "selection": {
                "title": "选股队列",
                "count": int(stats["sel_q"] or 0) if stats else 0,
                "items": [_task_brief(r) for r in sel_rows],
            },
            "backtest": {
                "title": "回测队列",
                "count": int(stats["bt_q"] or 0) if stats else 0,
                "items": [_task_brief(r) for r in bt_rows],
            },
            "running": {
                "title": "运行中",
                "count": int(stats["running"] or 0) if stats else 0,
                "items": [_task_brief(r) for r in run_rows],
            },
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