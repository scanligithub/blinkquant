# backend/scheduler/scheduler.py
from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime
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
        # 启动时先清理一次残留状态
        try:
            async with acquire() as conn:
                await self._recover_stuck(conn)
        except Exception as e:
            log.warning("Startup recover_stuck failed: %s", e)
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
            
            # 3. 统计资源
            idle_nodes = [n for n in nodes if n.is_idle]
            running_nodes = [n for n in nodes if n.status == "running"]
            
            # 4. 检查是否有 pending/queued selection（最高优先级）
            pending_selection = await conn.fetchrow("""
                SELECT id, payload, generation FROM task_queue
                WHERE status IN ('pending', 'queued') AND task_type = 'selection'
                ORDER BY priority DESC, created_at
                LIMIT 1
            """)
            
            if pending_selection:
                # Selection Barrier：有选股任务时，禁止派发新 backtest
                # 直接抢占并同轮派发选股
                await self._ensure_nodes_for_selection(conn, idle_nodes, running_nodes, 
                                                       pending_selection["id"], 
                                                       pending_selection["payload"],
                                                       pending_selection["generation"])
                # 本轮只处理选股调度，不派发 backtest
                # 注意：继续轮询已运行的 backtest，释放已完成节点
            else:
                # 5. 无选股排队，调度 backtest 到空闲节点（优先 node2/3，避免堵 node1 调度进程）
                backtest_idle = sorted(idle_nodes, key=lambda n: (n.node_id == 'node1', n.node_id))
                for node in backtest_idle:
                    task = await self._pop_task(conn, "backtest")
                    if not task:
                        break
                    await self._dispatch_backtest(conn, task, node.node_id)

            # 6. 轮询正在运行的 backtest 任务（并发 + 短 timeout）
            await self._poll_running_backtests(conn)

    async def _ensure_nodes_for_selection(self, conn, idle_nodes: List[NodeRow], running_nodes: List[NodeRow], 
                                          selection_task_id: int, selection_payload: dict, 
                                          selection_generation: int) -> None:
        """确保 3 节点可用于 selection：抢占所有占用节点的 backtest，同轮派发"""
        # 1. 抢占所有 running backtest（选股要三节点，不留尾巴）
        victims = await conn.fetch("""
            SELECT id, assigned_node, cluster_job_id, generation
            FROM task_queue
            WHERE status = 'running' AND task_type = 'backtest'
            ORDER BY started_at ASC
        """)
        
        for victim in victims:
            await self._preempt_backtest(conn, victim["id"], victim["cluster_job_id"], 
                                         victim["assigned_node"], selection_task_id,
                                         victim["generation"])

        # 2. 兜底：节点表仍非 idle 的强制清空（含 selection 残留、假占用）
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle',
                current_task_id = NULL,
                task_type = NULL,
                generation = generation + 1,
                updated_at = now()
            WHERE node_id IN ('node1', 'node2', 'node3')
              AND status <> 'idle'
        """)

        # 3. 同一轮立刻派选股（不要等下一轮 / draining）
        await self._dispatch_selection_now(conn, selection_task_id, selection_payload, selection_generation)

    async def _dispatch_selection_now(self, conn, selection_task_id: int, selection_payload: dict, 
                                      selection_generation: int) -> None:
        """派发选股任务到 3 个节点"""
        new_generation = selection_generation + 1
        
        # 更新任务状态
        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = now(),
                assigned_node = 'node1,node2,node3',
                generation = $1
            WHERE id = $2
        """, new_generation, selection_task_id)

        # 标记 3 节点为 running，更新 generation
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = $1, task_type = 'selection',
                generation = $2, updated_at = now()
            WHERE node_id IN ('node1', 'node2', 'node3')
        """, selection_task_id, new_generation)

        # 并行发起 selection 请求（异步）
        asyncio.create_task(self._execute_selection(selection_payload, selection_task_id, new_generation))

    # ═══════════════════════════════════════════════════════════
    # 数据库查询封装
    # ═══════════════════════════════════════════════════════════

    async def _get_nodes(self, conn) -> list[NodeRow]:
        rows = await conn.fetch("""
            SELECT node_id, name, endpoint, weight, status,
                   current_task_id, task_type, heartbeat_at, last_error, generation
            FROM cluster_nodes
            ORDER BY node_id
        """)
        return [NodeRow(**dict(r)) for r in rows]

    async def _pop_task(self, conn, task_type: str) -> Optional[dict]:
        """原子性抢占任务"""
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
                      finished_at, retry_count, max_retries, preempted_by, generation
        """, task_type)
        return dict(row) if row else None

    # ═══════════════════════════════════════════════════════════
    # 派发实现（事务内只写状态，事务外发 HTTP）
    # ═══════════════════════════════════════════════════════════

    async def _dispatch_selection(self, conn, task: dict) -> None:
        """在事务内标记状态，事务外发起 HTTP"""
        task_id = task["id"]
        payload = task["payload"]
        generation = task.get("generation", 0) + 1
        node_ids = ["node1", "node2", "node3"]

        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = now(),
                assigned_node = 'node1,node2,node3',
                generation = $1
            WHERE id = $2
        """, generation, task_id)

        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = $1, task_type = 'selection',
                generation = $2, updated_at = now()
            WHERE node_id = ANY($3)
        """, task_id, generation, node_ids)

        # 事务提交后异步执行
        asyncio.create_task(self._execute_selection(payload, task_id, generation))

    async def _execute_selection(self, payload: dict, task_id: int, generation: int) -> None:
        """实际发送 selection 请求，聚合 3 节点结果"""
        from .dispatcher import dispatch_selection
        from .db import execute
        try:
            result = await dispatch_selection(payload)
            # 更新任务完成状态（带 generation 防止覆盖）
            await execute("""
                UPDATE task_queue
                SET status = 'done', finished_at = now(), result = $1
                WHERE id = $2 AND generation = $3
            """, json.dumps(result), task_id, generation)
            
            # 释放 3 节点
            await self._release_selection_nodes(generation)
        except Exception as e:
            await self._mark_task_failed(task_id, str(e))
            # 失败也要释放节点，防止假忙死锁
            await self._release_selection_nodes(generation)

    async def _release_selection_nodes(self, generation: int) -> None:
        from .db import execute
        await execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                generation = $1, updated_at = now()
            WHERE node_id IN ('node1', 'node2', 'node3')
              AND generation = $1
        """, generation)

    async def _dispatch_backtest(self, conn, task: dict, node_id: str) -> None:
        """单节点派发 backtest：事务内标记 task + 占用节点，事务外发 HTTP"""
        task_id = task["id"]
        payload = task["payload"]
        generation = task.get("generation", 0) + 1

        # 同一事务：任务 running + 节点 running（避免竞态）
        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = now(),
                generation = $1
            WHERE id = $2
        """, generation, task_id)

        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = $1, task_type = 'backtest',
                generation = $2, updated_at = now()
            WHERE node_id = $3
        """, task_id, generation, node_id)

        # 事务外发起 HTTP
        asyncio.create_task(self._execute_backtest(node_id, payload, task_id, generation))

    async def _execute_backtest(self, node_id: str, payload: dict, task_id: int, generation: int) -> None:
        """事务外执行 HTTP，成功写 job_id + assigned_node，失败回滚节点状态"""
        from .dispatcher import dispatch_backtest
        from .db import execute
        try:
            result = await dispatch_backtest(node_id, payload)
            job_id = result["job_id"]
            
            await execute("""
                UPDATE task_queue
                SET cluster_job_id = $1, assigned_node = $2, generation = $3
                WHERE id = $4 AND generation = $3
            """, job_id, node_id, generation, task_id)
        except Exception as e:
            await execute("""
                UPDATE task_queue SET status = 'failed', finished_at = now(), error = $1 WHERE id = $2 AND generation = $3
            """, str(e), task_id, generation)
            await execute("""
                UPDATE cluster_nodes SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                    generation = $1, updated_at = now() WHERE node_id = $2 AND generation = $1
            """, generation, node_id)

    # ═══════════════════════════════════════════════════════════
    # 抢占与恢复
    # ═══════════════════════════════════════════════════════════

    async def _preempt_backtest(self, conn, task_id: int, job_id: str, node_id: str, 
                                preempted_by: int, old_generation: int) -> None:
        """抢占单个 backtest：协作取消 + 标记 preempted + 自动重入队
        关键：节点立刻 idle（不 draining），任务回 pending，DB 先提交"""
        # 1. 标记 preempted + 重入队（原子操作，用 task 的 generation 校验）
        result = await conn.execute("""
            UPDATE task_queue
            SET status = 'pending', 
                finished_at = now(),
                error = 'preempted by selection #' || $1,
                preempted_by = $1,
                retry_count = retry_count + 1,
                assigned_node = NULL,
                cluster_job_id = NULL,
                queued_at = NULL,
                started_at = NULL,
                finished_at = NULL,
                generation = generation + 1
            WHERE id = $2 AND generation = $3 AND status = 'running'
        """, preempted_by, task_id, old_generation)
        log.info("Preempted task %s (gen=%s) by selection #%s, rows=%s", task_id, old_generation, preempted_by, result)

        # 2. 协作式取消 HF job（异步，不阻塞调度）
        if job_id and node_id:
            from .dispatcher import cancel_task
            asyncio.create_task(cancel_task(node_id, job_id, "preempted_by_selection"))

        # 3. 立刻释放节点（关键：idle，不是 draining），按 node_id 更新，不依赖 task 的 generation
        if node_id:
            await conn.execute("""
                UPDATE cluster_nodes
                SET status = 'idle',
                    current_task_id = NULL,
                    task_type = NULL,
                    generation = generation + 1,
                    updated_at = now()
                WHERE node_id = $1
            """, node_id)
            log.info("Released node %s for selection preemption", node_id)

    async def _recover_stuck(self, conn) -> None:
        """回收超时任务 & 心跳丢失节点 & 任务终态但节点未释放"""
        # 1. running 超过 30min 无心跳 → 重置 pending
        await conn.execute("""
            UPDATE task_queue t
            SET status = 'pending', assigned_node = NULL,
                retry_count = t.retry_count + 1,
                generation = t.generation + 1
            FROM cluster_nodes n
            WHERE t.status = 'running'
              AND t.assigned_node = n.node_id
              AND n.heartbeat_at < now() - interval '30 minutes'
              AND t.retry_count < t.max_retries
        """)

        # 2. 节点心跳超时 → unhealthy
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'unhealthy', last_error = 'heartbeat timeout',
                generation = generation + 1
            WHERE heartbeat_at < now() - interval '60 seconds'
              AND status IN ('idle', 'running')
        """)

        # 3. draining 超时（取消超时）→ idle
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL,
                generation = generation + 1
            WHERE status = 'draining'
              AND updated_at < now() - interval '30 seconds'
        """)

        # 4. 任务已终态但节点仍指向它 → 强制释放节点
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                generation = generation + 1, updated_at = now()
            WHERE current_task_id IS NOT NULL
              AND current_task_id IN (
                  SELECT id FROM task_queue
                  WHERE status IN ('done', 'failed', 'cancelled', 'preempted')
              )
        """)

    # ═══════════════════════════════════════════════════════════
    # 轮询正在运行的 backtest（并发 + 短 timeout）
    # ═══════════════════════════════════════════════════════════

    async def _poll_running_backtests(self, conn) -> None:
        """并发轮询 running backtest 的 HF job 状态，单节点 5s timeout"""
        rows = await conn.fetch("""
            SELECT id, assigned_node, cluster_job_id, generation
            FROM task_queue
            WHERE status = 'running' AND task_type = 'backtest'
        """)

        if not rows:
            return

        async def poll_one(row):
            job_id = row["cluster_job_id"]
            node_id = row["assigned_node"]
            task_id = row["id"]
            generation = row["generation"]
            if not job_id or not node_id:
                return
            
            try:
                from .dispatcher import poll_backtest_job
                # 5s timeout，防止单慢节点拖住整轮调度
                result = await asyncio.wait_for(poll_backtest_job(node_id, job_id), timeout=5.0)
                status = result.get("status")
                
                if status == "done":
                    await self._complete_backtest(task_id, result.get("data"), generation)
                elif status in ("failed", "cancelled", "expired"):
                    await self._fail_backtest(task_id, result.get("error", status), generation)
            except asyncio.TimeoutError:
                log.warning("Poll job %s timeout (5s)", task_id)
            except Exception as e:
                log.warning("Poll job %s failed: %s", task_id, e)

        # 并发轮询所有 running backtest
        await asyncio.gather(*[poll_one(row) for row in rows], return_exceptions=True)

    async def _complete_backtest(self, task_id: int, data: dict, generation: int) -> None:
        from .db import execute
        await execute("""
            UPDATE task_queue
            SET status = 'done', finished_at = now(), result = $1
            WHERE id = $2 AND generation = $3
        """, json.dumps(data), task_id, generation)
        
        await execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                generation = $1, updated_at = now()
            WHERE current_task_id = $2 AND generation = $1
        """, generation, task_id)

    async def _fail_backtest(self, task_id: int, error: str, generation: int) -> None:
        from .db import execute
        await execute("""
            UPDATE task_queue
            SET status = 'failed', finished_at = now(), error = $1
            WHERE id = $2 AND generation = $3
        """, error, task_id, generation)
        
        await execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                generation = $1, updated_at = now()
            WHERE current_task_id = $2 AND generation = $1
        """, generation, task_id)

    async def _mark_task_failed(self, task_id: int, error: str) -> None:
        from .db import execute
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