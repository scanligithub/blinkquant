# backend/scheduler/scheduler.py
from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, List

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
    dispatch_backtest, dispatch_selection, cancel_task, poll_backtest_job,
)
from .state import can_transition_node, can_transition_task

log = logging.getLogger("scheduler")

# ════════════════════════════════════════════════════════════
# 调度器主类
# ════════════════════════════════════════════════════════════

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

    # ═══════════════════════════════════════════════════════════
    # 主循环
    # ═══════════════════════════════════════════════════════════

    async def _run_loop(self) -> None:
        from .config import SCHEDULE_INTERVAL_SEC
        while self._running:
            try:
                await self._schedule_cycle()
            except Exception as e:
                logging.exception("Scheduler cycle error: %s", e)
            await asyncio.sleep(SCHEDULE_INTERVAL_SEC)

    # ═══════════════════════════════════════════════════════════
    # 单次调度周期：核心策略 = 选股优先 + 抢占回测
    # ═══════════════════════════════════════════════════════════

    async def _schedule_cycle(self) -> None:
        async with acquire() as conn:
            # 1. 清理超时/心跳丢失
            await self._recover_stuck(conn)

            # 2. 获取集群快照
            nodes = await self._get_nodes(conn)
            idle_nodes = [n for n in nodes if n.is_idle]
            running_nodes = [n for n in nodes if n.status == "running"]
            
            # 3. 检查是否有 pending selection（最高优先级）
            pending_selection = await conn.fetchrow("""
                SELECT id, payload FROM task_queue
                WHERE status IN ('pending', 'queued') AND task_type = 'selection'
                ORDER BY priority DESC, created_at
                LIMIT 1
            """)
            
            if pending_selection:
                # 有选股任务等待，必须确保 3 节点全可用
                await self._ensure_nodes_for_selection(conn, idle_nodes, running_nodes, pending_selection["id"], pending_selection["payload"])
                # 本轮只处理选股调度
                # 注意：不 return，继续轮询已运行的回测，释放已完成节点
            
            # 5. 无选股排队，调度 backtest 到空闲节点
            if not idle_nodes:
                for node in idle_nodes:
                    task = await self._pop_task(conn, "backtest")
                    if not task:
                        break
                    await self._dispatch_backtest(conn, task, node.node_id)

            # 6. 轮询正在运行的 backtest 任务（检查完成/失败）
            await self._poll_running_backtests(conn)

    async def _ensure_nodes_for_selection(self, conn, idle_nodes: list, running_nodes: list, selection_task_id: int, selection_payload: dict) -> None:
        """确保 3 节点可用于 selection：抢占占用的节点"""
        needed = 3 - len(idle_nodes)
        if needed <= 0:
            # 已经有 3 个空闲节点，直接派发
            await self._dispatch_selection_now(conn, selection_task_id, selection_payload)
            return

        # 需要抢占 running backtest
        # 优先抢占最早开始的 backtest（公平）
        victims = await conn.fetch("""
            SELECT id, assigned_node, cluster_job_id
            FROM task_queue
            WHERE status = 'running' AND task_type = 'backtest'
            ORDER BY started_at ASC
            LIMIT $1
        """, needed)

        for victim in victims:
            await self._preempt_backtest(conn, victim["id"], victim["cluster_job_id"], victim["assigned_node"], selection_task_id)

        # 等待节点变 idle（下一轮调度会处理）
        # 或者立即查询并派发
        updated_idle = await conn.fetch("""
            SELECT node_id FROM cluster_nodes
            WHERE status = 'idle' AND node_id = ANY($1)
        """, [v["assigned_node"] for v in victims] + [n.node_id for n in idle_nodes])

        if len(updated_idle) >= 3:
            await self._dispatch_selection_now(conn, selection_task_id, selection_payload)

    async def _dispatch_selection_now(self, conn, selection_task_id: int, selection_payload: dict) -> None:
        """派发选股任务到 3 个节点"""
        # 更新任务状态
        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = now(),
                assigned_node = 'node1,node2,node3'
            WHERE id = $1
        """, selection_task_id)

        # 标记 3 节点为 running
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = $1, task_type = 'selection',
                updated_at = now()
            WHERE node_id IN ('node1', 'node2', 'node3')
        """, selection_task_id)

        # 并行发起 selection 请求（异步）
        asyncio.create_task(self._execute_selection(selection_payload, selection_task_id))

    async def _execute_selection(self, payload: dict, task_id: int) -> None:
        """实际发送 selection 请求，聚合 3 节点结果"""
        from .dispatcher import dispatch_selection
        from .db import execute
        try:
            result = await dispatch_selection(payload)
            # 更新任务完成状态
            await execute("""
                UPDATE task_queue
                SET status = 'done', finished_at = now(), result = $1
                WHERE id = $2
            """, json.dumps(result), task_id)
            
            # 释放 3 节点
            await execute("""
                UPDATE cluster_nodes
                SET status = 'idle', current_task_id = NULL, task_type = NULL, updated_at = now()
                WHERE node_id IN ('node1', 'node2', 'node3')
            """)
        except Exception as e:
            await self._mark_task_failed(task_id, str(e))

    async def _dispatch_backtest(self, conn, task: dict, node_id: str) -> None:
        """单节点派发 backtest，事务内只写状态"""
        task_id = task["id"]
        payload = task["payload"]

        # 先在事务内标记
        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = now()
            WHERE id = $1
        """, task_id)

        # 事务外发起 HTTP
        asyncio.create_task(self._execute_backtest(node_id, payload, task_id))

    async def _execute_backtest(self, node_id: str, payload: dict, task_id: int) -> None:
        """事务外执行 HTTP，完成后更新状态"""
        from .dispatcher import dispatch_backtest
        from .db import execute
        try:
            result = await dispatch_backtest(node_id, payload)
            job_id = result["job_id"]
            
            await execute("""
                UPDATE task_queue
                SET cluster_job_id = $1
                WHERE id = $2
            """, job_id, task_id)

            await execute("""
                UPDATE cluster_nodes
                SET status = 'running', current_task_id = $1, task_type = 'backtest',
                    updated_at = now()
                WHERE node_id = $2
            """, task_id, node_id)
        except Exception as e:
            await execute("""
                UPDATE task_queue SET status = 'failed', finished_at = now(), error = $1 WHERE id = $2
            """, str(e), task_id)
            await execute("""
                UPDATE cluster_nodes SET status = 'idle', current_task_id = NULL, task_type = NULL WHERE node_id = $1
            """, node_id)

    # ═══════════════════════════════════════════════════════════
    # 抢占与恢复
    # ═══════════════════════════════════════════════════════════

    async def _preempt_backtest(self, conn, task_id: int, job_id: str, node_id: str, preempted_by: int) -> None:
        """抢占单个 backtest：协作取消 + 标记 preempted + 自动重入队"""
        # 1. 标记 preempted
        await conn.execute("""
            UPDATE task_queue
            SET status = 'preempted', finished_at = now(),
                error = 'preempted by selection #' || $1,
                preempted_by = $1,
                retry_count = retry_count + 1
            WHERE id = $2
        """, preempted_by, task_id)

        # 2. 协作式取消 HF job（异步）
        if job_id:
            from .dispatcher import cancel_task
            asyncio.create_task(cancel_task(node_id, job_id, "preempted_by_selection"))

        # 3. 释放节点（设为 draining，取消完成后回 idle）
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'draining', current_task_id = NULL, task_type = NULL
            WHERE node_id = $1
        """, node_id)

        # 4. 重新入队（自动重试）
        await conn.execute("""
            UPDATE task_queue
            SET status = 'pending', assigned_node = NULL, cluster_job_id = NULL,
                queued_at = NULL, started_at = NULL, finished_at = NULL
            WHERE id = $1
        """, task_id)

    async def _recover_stuck(self, conn) -> None:
        """回收超时任务 & 心跳丢失节点"""
        # 1. running 超过 30min 无心跳 → 重置 pending
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

        # 2. 节点心跳超时 → unhealthy
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'unhealthy', last_error = 'heartbeat timeout'
            WHERE heartbeat_at < now() - interval '60 seconds'
              AND status IN ('idle', 'running')
        """)

        # 3. draining 超时（取消超时）→ idle
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL
            WHERE status = 'draining'
              AND updated_at < now() - interval '30 seconds'
        """)

    # ═══════════════════════════════════════════════════════════
    # 轮询正在运行的 backtest（检查完成/失败）
    # ═══════════════════════════════════════════════════════════

    async def _poll_running_backtests(self, conn) -> None:
        """轮询 running backtest 的 HF job 状态"""
        rows = await conn.fetch("""
            SELECT id, assigned_node, cluster_job_id
            FROM task_queue
            WHERE status = 'running' AND task_type = 'backtest'
        """)

        for row in rows:
            job_id = row["cluster_job_id"]
            node_id = row["assigned_node"]
            if not job_id:
                continue
            
            try:
                from .dispatcher import poll_backtest_job
                result = await poll_backtest_job(node_id, job_id)
                status = result.get("status")
                
                if status == "done":
                    await self._complete_backtest(row["id"], result.get("data"))
                elif status in ("failed", "cancelled", "expired"):
                    await self._fail_backtest(row["id"], result.get("error", status))
            except Exception as e:
                log.warning("Poll job %s failed: %s", row["id"], e)

    async def _complete_backtest(self, task_id: int, data: dict) -> None:
        await execute("""
            UPDATE task_queue
            SET status = 'done', finished_at = now(), result = $1
            WHERE id = $2
        """, json.dumps(data), task_id)
        
        await execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, updated_at = now()
            WHERE current_task_id = $1
        """, task_id)

    async def _fail_backtest(self, task_id: int, error: str) -> None:
        await execute("""
            UPDATE task_queue
            SET status = 'failed', finished_at = now(), error = $1
            WHERE id = $2
        """, error, task_id)
        
        await execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, updated_at = now()
            WHERE current_task_id = $1
        """, task_id)

    async def _mark_task_failed(self, task_id: int, error: str) -> None:
        await execute("""
            UPDATE task_queue
            SET status = 'failed', finished_at = now(), error = $1
            WHERE id = $2
        """, error, task_id)

    async def run_forever(self) -> None:
        await self.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await self.stop()