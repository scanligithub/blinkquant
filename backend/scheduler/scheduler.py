# backend/scheduler/scheduler.py
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from .config import (
    SCHEDULE_INTERVAL_SEC,
    HEARTBEAT_TIMEOUT_SEC,
    TASK_RUNNING_TIMEOUT_SEC,
    MAX_RETRIES_DEFAULT,
    PREEMPT_CANCEL_TIMEOUT_SEC,
    TaskType, TaskStatus, NodeStatus,
    HF_NODES,
)
from .db import acquire, execute, fetch, fetchrow
from .models import NodeRow, TaskRow
from .dispatcher import (
    dispatch_backtest, dispatch_selection, cancel_task,
)
from .state import can_transition_node, can_transition_task

log = logging.getLogger("scheduler")

class ClusterScheduler:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        log.info("Scheduler started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("Scheduler stopped")

    async def _run_loop(self) -> None:
        from .config import SCHEDULE_INTERVAL_SEC
        while True:
            try:
                await self._schedule_cycle()
            except Exception as e:
                logging.exception("Scheduler cycle error: %s", e)
            await asyncio.sleep(1)

    async def _schedule_cycle(self) -> None:
        async with acquire() as conn:
            await self._recover_stuck(conn)

            nodes = await self._get_nodes(conn)
            idle_nodes = [n for n in nodes if n.is_idle]
            running_nodes = [n for n in nodes if n.status == "running"]

            if not idle_nodes and not any(n.status == "draining" for n in nodes):
                return

            if len(idle_nodes) == 3:
                task = await self._pop_task(conn, "selection")
                if task:
                    await self._dispatch_selection(conn, task)
                    return

            for node in idle_nodes:
                task = await self._pop_task(conn, "backtest")
                if not task:
                    break
                await self._dispatch_backtest(conn, task, node.node_id)

    async def _get_nodes(self, conn) -> list[NodeRow]:
        rows = await conn.fetch("""
            SELECT node_id, name, endpoint, weight, status,
                   current_task_id, task_type, heartbeat_at, last_error
            FROM cluster_nodes
            ORDER BY node_id
        """)
        return [NodeRow(**dict(r)) for r in rows]

    async def _pop_task(self, conn, task_type: str) -> Optional[dict]:
        row = await conn.fetchrow("""
            UPDATE task_queue
            SET status = 'queued', queued_at = now()
            WHERE id = (
                SELECT id FROM task_queue
                WHERE status = 'pending' AND task_type = $1
                ORDER BY priority DESC, created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, user_id, task_type, payload, priority,
                      status, assigned_node, cluster_job_id, result,
                      error, created_at, queued_at, started_at,
                      finished_at, retry_count, max_retries, preempted_by
        """, task_type)
        return dict(row) if row else None

    async def _dispatch_selection(self, conn, task: dict) -> None:
        task_id = task["id"]
        payload = task["payload"]
        node_ids = ["node1", "node2", "node3"]

        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = now(),
                assigned_node = $1
            WHERE id = $2
        """, "node1,node2,node3", task["id"])

        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = $1, task_type = 'selection',
                updated_at = now()
            WHERE node_id = ANY($2)
        """, task["id"], node_ids)

        # 实际应用中这里应并行 POST /api/v1/select 到 3 个节点
        # 结果由节点回调或轮询更新

    async def _dispatch_backtest(self, conn, task: dict, node_id: str) -> None:
        from .dispatcher import dispatch_backtest
        from .config import HF_NODES

        try:
            result = await dispatch_backtest(node_id, task["payload"])
            job_id = result["job_id"]
            await conn.execute("""
                UPDATE task_queue
                SET status = 'running', started_at = now(),
                    assigned_node = $1, cluster_job_id = $2
                WHERE id = $3
            """, node_id, result["job_id"], task["id"])

            await conn.execute("""
                UPDATE cluster_nodes
                SET status = 'running', current_task_id = $1, task_type = 'backtest',
                    updated_at = now()
                WHERE node_id = $2
            """, task["id"], node_id)

        except Exception as e:
            await self._mark_task_failed(conn, task["id"], str(e))

    async def _mark_task_failed(self, conn, task_id: int, error: str) -> None:
        await conn.execute("""
            UPDATE task_queue
            SET status = 'failed', finished_at = now(), error = $1
            WHERE id = $2
        """, error, task_id)

    async def preempt_backtests_for_selection(self, conn, selection_task_id: int) -> list[int]:
        rows = await conn.fetch("""
            SELECT id, assigned_node, cluster_job_id
            FROM task_queue
            WHERE status = 'running' AND task_type = 'backtest'
        """)
        preempted = []
        for row in rows:
            await conn.execute("""
                UPDATE task_queue
                SET status = 'preempted', finished_at = now(),
                    error = 'preempted by selection #' || $1,
                    preempted_by = $1
                WHERE id = $2
            """, row["id"], row["id"])

            if row["cluster_job_id"]:
                from .dispatcher import cancel_task
                await cancel_task(row["assigned_node"], row["cluster_job_id"])

            await conn.execute("""
                UPDATE cluster_nodes
                SET status = 'draining', current_task_id = NULL, task_type = NULL
                WHERE node_id = $1
            """, row["assigned_node"])

            preempted.append(row["id"])
        return preempted

    async def _recover_stuck(self, conn) -> None:
        await conn.execute("""
            UPDATE task_queue t
            SET status = 'pending', assigned_node = NULL,
                retry_count = retry_count + 1
            FROM cluster_nodes n
            WHERE t.status = 'running'
              AND t.assigned_node = n.node_id
              AND n.heartbeat_at < now() - interval '30 minutes'
              AND t.retry_count < t.max_retries
        """)

        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'unhealthy', last_error = 'heartbeat timeout'
            WHERE heartbeat_at < now() - interval '60 seconds'
              AND status IN ('idle', 'running')
        """)

    async def run_forever(self) -> None:
        await self.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await self.stop()