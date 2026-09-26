# backend/scheduler/scheduler.py
from __future__ import annotations
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional, List
from httpx import HTTPStatusError

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
    fetch_backtest_artifact,
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


    async def recover_stale_on_boot(self) -> None:
        """启动时恢复：复用 _recover_stuck 逻辑"""
        try:
            async with acquire() as conn:
                await self._recover_stuck(conn)
        except Exception as e:
            log.warning("recover_stale_on_boot failed: %s", e)

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
        
        # Actual preemption is a two-phase operation:
        # 1) signal the node and wait until the worker really stops;
        # 2) only then requeue the task and release the node.
        # Never force an occupied node to idle while its compute thread may
        # still be running.
        for victim in victims:
            stopped = await self._preempt_backtest(
                conn,
                victim["id"],
                victim["cluster_job_id"],
                victim["assigned_node"],
                selection_task_id,
                victim["generation"],
            )
            if not stopped:
                log.warning(
                    "Selection #%s waiting for backtest %s to stop before dispatch",
                    selection_task_id,
                    victim["id"],
                )
                return

        # All backtest workers have now confirmed termination.
        await self._dispatch_selection_now(
            conn, selection_task_id, selection_payload, selection_generation
        )

    async def _dispatch_selection_now(self, conn, selection_task_id: int, selection_payload: dict, 
                                      selection_generation: int) -> None:
        """派发选股任务到 3 个节点"""
        new_generation = selection_generation + 1
        
        # 更新任务状态
        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = datetime('now'),
                assigned_node = 'node1,node2,node3',
                generation = ?
            WHERE id = ?
        """, new_generation, selection_task_id)

        # 标记 3 节点为 running，更新 generation
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = ?, task_type = 'selection',
                generation = ?, updated_at = datetime('now')
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
        """原子性抢占任务（SQLite 兼容：CTE + 事务锁）"""
        row = await conn.fetchrow("""
            WITH picked AS (
                SELECT id FROM task_queue
                WHERE status = 'pending' AND task_type = ?
                ORDER BY priority DESC, created_at
                LIMIT 1
            )
            UPDATE task_queue
            SET status = 'queued', queued_at = datetime('now')
            WHERE id IN (SELECT id FROM picked)
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
            SET status = 'running', started_at = datetime('now'),
                assigned_node = 'node1,node2,node3',
                generation = ?
            WHERE id = ?
        """, generation, task_id)

        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = ?, task_type = 'selection',
                generation = ?, updated_at = datetime('now')
            WHERE node_id IN (?,?,?)
        """, task_id, generation, *node_ids)

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
                SET status = 'done', finished_at = datetime('now'), result = ?
                WHERE id = ? AND generation = ?
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
                generation = ?, updated_at = datetime('now')
            WHERE node_id IN ('node1', 'node2', 'node3')
              AND generation = ?
        """, generation, generation)

    async def _dispatch_backtest(self, conn, task: dict, node_id: str) -> None:
        """单节点派发 backtest：事务内标记 task + 占用节点，事务外发 HTTP"""
        task_id = task["id"]
        payload = task["payload"]
        generation = task.get("generation", 0) + 1

        # 同一事务：任务 running + 节点 running（避免竞态）
        # 重派时清空残留的 error/preempted_by
        await conn.execute("""
            UPDATE task_queue
            SET status = 'running', started_at = datetime('now'),
                generation = ?,
                error = NULL,
                preempted_by = NULL
            WHERE id = ?
        """, generation, task_id)

        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'running', current_task_id = ?, task_type = 'backtest',
                generation = ?, updated_at = datetime('now')
            WHERE node_id = ?
        """, task_id, generation, node_id)

        # 事务外发起 HTTP
        asyncio.create_task(self._execute_backtest(node_id, payload, task_id, generation))

    async def _execute_backtest(self, node_id: str, payload: dict, task_id: int, generation: int) -> None:
        """事务外执行 HTTP，成功写 job_id + assigned_node，失败回滚节点状态"""
        from .dispatcher import dispatch_backtest
        from .db import execute
        success = False
        try:
            result = await dispatch_backtest(node_id, payload)
            job_id = result["job_id"]
            
            await execute("""
                UPDATE task_queue
                SET cluster_job_id = ?, assigned_node = ?
                WHERE id = ? AND generation = ?
            """, job_id, node_id, task_id, generation)
            success = True
        except Exception as e:
            await execute("""
                UPDATE task_queue SET status = 'failed', finished_at = datetime('now'), error = ? WHERE id = ? AND generation = ?
            """, str(e), task_id, generation)
        finally:
            # 无论成功失败，只要没写上 job_id 就释放节点（避免僵尸 running）
            if not success:
                await execute("""
                    UPDATE cluster_nodes 
                    SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                        generation = generation + 1, updated_at = datetime('now') 
                    WHERE node_id = ? AND current_task_id = ?
                """, node_id, task_id)

    # ═══════════════════════════════════════════════════════════
    # 抢占与恢复
    # ═══════════════════════════════════════════════════════════

    async def _preempt_backtest(
        self,
        conn,
        task_id: int,
        job_id: str,
        node_id: str,
        preempted_by: int,
        old_generation: int,
    ) -> bool:
        """Actually stop one backtest, then requeue/release it.

        The node-side cancel endpoint is cooperative: it sets a thread-safe
        cancellation event and the BacktestEngine exits at a safe trading-day
        boundary. We wait for that terminal state before touching scheduler
        resource state, preventing selection from overlapping old compute.
        """
        if not job_id or not node_id:
            # The dispatch HTTP call may still be in flight. Keep the task
            # running and retry on the next scheduler cycle once job_id exists.
            return False

        from .dispatcher import cancel_task

        stopped = await cancel_task(node_id, job_id, "preempted_by_selection")
        if not stopped:
            return False

        # Only after actual worker termination do we change scheduler state.
        err = f"preempted by selection #{preempted_by}"
        result = await conn.execute(
            """
            UPDATE task_queue
            SET status = 'pending',
                error = ?,
                preempted_by = ?,
                retry_count = retry_count + 1,
                assigned_node = NULL,
                cluster_job_id = NULL,
                queued_at = NULL,
                started_at = NULL,
                finished_at = NULL,
                generation = generation + 1
            WHERE id = ?
              AND generation = ?
              AND status = 'running'
            """,
            err,
            preempted_by,
            task_id,
            old_generation,
        )
        log.info(
            "Preempted task %s (gen=%s) by selection #%s, rows=%s",
            task_id,
            old_generation,
            preempted_by,
            result,
        )

        await conn.execute(
            """
            UPDATE cluster_nodes
            SET status = 'idle',
                current_task_id = NULL,
                task_type = NULL,
                generation = generation + 1,
                updated_at = datetime('now')
            WHERE node_id = ?
              AND current_task_id = ?
            """,
            node_id,
            task_id,
        )
        log.info("Released node %s after confirmed backtest preemption", node_id)
        return True

    async def _recover_stuck(self, conn) -> None:
        """回收超时任务 & 心跳丢失节点 & 任务终态但节点未释放"""
        # 1. running 超过 30min 无心跳 → 重置 pending
        #    注意：仅 heartbeat_at IS NOT NULL 且超时才回收；IS NULL 由 2.5 墙钟兜底
        await conn.execute("""
            UPDATE task_queue
            SET status = 'pending', assigned_node = NULL,
                retry_count = retry_count + 1,
                generation = generation + 1
            WHERE status = 'running'
              AND assigned_node IN (
                  SELECT node_id FROM cluster_nodes
                  WHERE heartbeat_at IS NOT NULL
                    AND heartbeat_at < datetime('now', '-30 minutes')
              )
              AND retry_count < max_retries
        """)

        # 2. 节点心跳超时 → unhealthy
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'unhealthy', last_error = 'heartbeat timeout',
                generation = generation + 1
            WHERE heartbeat_at IS NOT NULL
              AND heartbeat_at < datetime('now', '-60 seconds')
              AND status IN ('idle', 'running')
        """)

        # 2.5 按行动态墙钟超时回收（不依赖心跳）
        # 每个任务用自身的 timeout_sec（创建时计算），NULL 则回退全局默认
        cursor = await conn.execute("""
            SELECT id, timeout_sec, started_at, assigned_node, cluster_job_id,
                   retry_count, max_retries, generation
            FROM task_queue
            WHERE status = 'running'
              AND task_type = 'backtest'
              AND started_at IS NOT NULL
        """)
        running_backtests = await cursor.fetchall()

        timed_out_ids = []
        for row in running_backtests:
            limit = row["timeout_sec"] or TASK_RUNNING_TIMEOUT_SEC
            # 用 SQLite datetime 比较避免 Python isoformat T-vs-space 问题
            check = await conn.execute(
                "SELECT 1 FROM task_queue WHERE id = ? AND started_at < datetime('now', ?)",
                row["id"], f"-{limit} seconds"
            )
            if await check.fetchone():
                timed_out_ids.append(row)
                log.warning(
                    "Wall-clock timeout: task %s (timeout_sec=%s, started_at=%s)",
                    row["id"], limit, row["started_at"],
                )

        for row in timed_out_ids:
            # 回收任务：pending 重试 或 failed
            if row["retry_count"] < row["max_retries"]:
                await conn.execute("""
                    UPDATE task_queue
                    SET status = 'pending', finished_at = NULL, error = NULL,
                        assigned_node = NULL, cluster_job_id = NULL,
                        queued_at = NULL, started_at = NULL,
                        retry_count = retry_count + 1,
                        generation = generation + 1
                    WHERE id = ?
                """, row["id"])
            else:
                await conn.execute("""
                    UPDATE task_queue
                    SET status = 'failed', finished_at = datetime('now'),
                        error = 'timeout: running exceeded wall-clock limit',
                        assigned_node = NULL, cluster_job_id = NULL,
                        queued_at = NULL, started_at = NULL,
                        generation = generation + 1
                    WHERE id = ?
                """, row["id"])

            # 释放节点
            if row["assigned_node"]:
                await conn.execute("""
                    UPDATE cluster_nodes
                    SET status = 'idle', current_task_id = NULL, task_type = NULL,
                        generation = generation + 1, updated_at = datetime('now')
                    WHERE node_id = ?
                """, row["assigned_node"])

        # 2.7 兜底：节点指向已非 running 的任务 → 释放
        # 覆盖 step 2.5 将任务改为 pending/failed 后节点未同步释放的窗口
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL,
                generation = generation + 1, updated_at = datetime('now')
            WHERE current_task_id IS NOT NULL
              AND current_task_id NOT IN (
                  SELECT id FROM task_queue WHERE status = 'running'
              )
        """)

        # 3. draining 超时（取消超时）→ idle
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL,
                generation = generation + 1
            WHERE status = 'draining'
              AND updated_at < datetime('now', '-30 seconds')
        """)

        # 4. 任务已终态但节点仍指向它 → 强制释放节点
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                generation = generation + 1, updated_at = datetime('now')
            WHERE current_task_id IS NOT NULL
              AND current_task_id IN (
                  SELECT id FROM task_queue
                  WHERE status IN ('done', 'failed', 'cancelled', 'preempted')
              )
        """)

        # 5. 僵尸 running：无 cluster_job_id 超过 2 分钟 → 打回 pending
        await conn.execute("""
            UPDATE task_queue
            SET status = 'pending', assigned_node = NULL,
                error = NULL, started_at = NULL,
                generation = generation + 1
            WHERE status = 'running'
              AND task_type = 'backtest'
              AND (cluster_job_id IS NULL OR cluster_job_id = '')
              AND started_at < datetime('now', '-2 minutes')
              AND assigned_node IN (
                  SELECT node_id FROM cluster_nodes
                  WHERE status = 'running'
                    AND current_task_id = task_queue.id
              )
        """)
        # 对应节点也释放
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL,
                generation = generation + 1, updated_at = datetime('now')
            WHERE current_task_id IN (
                SELECT id FROM task_queue
                WHERE status = 'running'
                  AND task_type = 'backtest'
                  AND (cluster_job_id IS NULL OR cluster_job_id = '')
                  AND started_at < datetime('now', '-2 minutes')
            )
        """)

        # 6. 孤儿 running：任务 running 但无任何节点认领（节点已 idle/释放）
        # 适用于 backtest 和 selection
        await conn.execute("""
            UPDATE task_queue
            SET status = 'pending',
                assigned_node = NULL,
                cluster_job_id = NULL,
                error = COALESCE(error, 'orphan running: no node owns this task'),
                started_at = NULL,
                generation = generation + 1
            WHERE status = 'running'
              AND started_at < datetime('now', '-2 minutes')
              AND NOT EXISTS (
                SELECT 1 FROM cluster_nodes
                WHERE current_task_id = task_queue.id
              )
        """)

        # 7. 断链 running：节点认领了但 task 无 job_id/assigned_node → 释放节点 + 重入队
        # 典型场景：调度事务写 node 成功，HTTP 派发成功，但 job_id 回写失败
        await conn.execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL,
                generation = generation + 1, updated_at = datetime('now')
            WHERE current_task_id IN (
                SELECT id FROM task_queue
                WHERE status = 'running'
                  AND (cluster_job_id IS NULL OR cluster_job_id = '')
                  AND started_at < datetime('now', '-2 minutes')
            )
        """)
        await conn.execute("""
            UPDATE task_queue
            SET status = 'pending',
                assigned_node = NULL,
                cluster_job_id = NULL,
                error = 'disconnected: node claimed but no job_id written',
                started_at = NULL,
                generation = generation + 1
            WHERE status = 'running'
              AND (cluster_job_id IS NULL OR cluster_job_id = '')
              AND started_at < datetime('now', '-2 minutes')
        """)

    # ═══════════════════════════════════════════════════════════
    # 轮询正在运行的 backtest（并发 + 短 timeout）
    # ═══════════════════════════════════════════════════════════

    async def _poll_running_backtests(self, conn) -> None:
        """并发轮询 running backtest 的 HF job 状态，单节点 30s timeout"""
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
                # 30s timeout，防止单慢节点拖住整轮调度（长回测节点可能响应慢）
                result = await asyncio.wait_for(poll_backtest_job(node_id, job_id), timeout=30.0)
                status = result.get("status")
                
                if status == "done":
                    payload = result.get("data") or result
                    if result.get("summary") is not None:
                        payload = result
                    await self._complete_backtest(
                        task_id, payload, generation,
                        node_id=node_id, job_id=job_id,
                    )
                elif status in ("failed", "cancelled", "expired"):
                    await self._fail_backtest(task_id, result.get("error") or status, generation)
                elif status in ("queued", "running", None):
                    # 透传进度
                    await self._update_task_progress(task_id, result.get("progress"))
                    return
                else:
                    log.warning("Poll job %s returned unknown status %r, failing", task_id, status)
                    await self._fail_backtest(task_id, f"poll returned unknown status {status!r}", generation)
            except asyncio.TimeoutError:
                # 节点单次 poll 超时：不直接 fail，仅跳过本轮
                # 终态由 2.5 墙钟回收兜底，避免节点短暂繁忙导致误杀
                log.warning("Poll job %s timeout (30s), skipping this round", task_id)
            except HTTPStatusError as e:
                # HTTP 404 / 50x：job 在节点内存 dict 中丢失（进程重启/OOM）→ 收尾
                detail = f"poll job failed: HTTP {e.response.status_code}"
                log.warning("%s (task %s)", detail, task_id)
                await self._fail_backtest(task_id, detail, generation)
            except Exception as e:
                log.warning("Poll job %s failed: %s", task_id, e)
                # 连接失败等：跳过本轮，由墙钟兜底
                log.warning("Poll job %s connection error, skipping this round", task_id)

        # 并发轮询所有 running backtest
        await asyncio.gather(*[poll_one(row) for row in rows], return_exceptions=True)

    async def _complete_backtest(self, task_id: int, data: dict, generation: int, *,
                                  node_id: str | None = None, job_id: str | None = None) -> None:
        from .db import execute, fetchrow
        from .config import RESULT_DIR
        from .result_store import persist, make_result_uri, dir_size
        from .dispatcher import fetch_backtest_artifact

        trow = await fetchrow("SELECT user_id FROM task_queue WHERE id = ?", task_id)
        user_id = trow["user_id"] if trow else None
        if not user_id:
            log.error("task %s missing user_id, cannot persist", task_id)
            await self._fail_backtest(task_id, "missing user_id", generation)
            return

        nbytes = 0
        if data and "summary" in data and "meta" in data:
            summary = data["summary"]
            meta = data["meta"]
            uri = make_result_uri(user_id, task_id)
            task_dir = os.path.join(RESULT_DIR, uri)
            os.makedirs(task_dir, exist_ok=True)
            try:
                with open(os.path.join(task_dir, "meta.json"), "w") as f:
                    json.dump({**meta, "task_id": task_id, "user_id": user_id}, f,
                              ensure_ascii=False, indent=2, default=str)
            except Exception:
                log.exception("meta.json write failed task=%s", task_id)
            if node_id and job_id and data.get("artifacts"):
                for name in data["artifacts"]:
                    try:
                        raw = await fetch_backtest_artifact(node_id, job_id, name)
                        with open(os.path.join(task_dir, f"{name}.parquet"), "wb") as f:
                            f.write(raw)
                    except Exception:
                        log.exception("artifact %s download failed task=%s", name, task_id)
            nbytes = dir_size(task_dir)
        else:
            summary, uri, nbytes = persist(task_id, data or {}, RESULT_DIR, user_id=user_id)

        await execute("""
            UPDATE task_queue
            SET status = 'done', finished_at = datetime('now'),
                result_summary = ?, result_uri = ?, result_bytes = ?, result = NULL,
                progress_pct = 100.0
            WHERE id = ? AND generation = ?
        """, json.dumps(summary), uri, nbytes, task_id, generation)

        await execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL,
                generation = ?, updated_at = datetime('now')
            WHERE current_task_id = ? AND generation = ?
        """, generation, task_id, generation)

        try:
            await self._enforce_user_quota(user_id, keep_task_id=task_id)
        except Exception:
            log.exception("quota enforce failed user=%s", user_id)

    async def _fail_backtest(self, task_id: int, error: str, generation: int) -> None:
        from .db import execute
        log.warning("Failing task %s (gen=%s): %s", task_id, generation, error)
        await execute("""
            UPDATE task_queue
            SET status = 'failed', finished_at = datetime('now'), error = ?
            WHERE id = ? AND generation = ?
        """, error, task_id, generation)
        
        await execute("""
            UPDATE cluster_nodes
            SET status = 'idle', current_task_id = NULL, task_type = NULL, 
                generation = ?, updated_at = datetime('now')
            WHERE current_task_id = ? AND generation = ?
        """, generation, task_id, generation)

    async def _enforce_user_quota(self, user_id: str, *, keep_task_id: int) -> list[int]:
        """超 2GB 则删最旧任务（文件+行），不删 keep_task_id。返回被删 id 列表。"""
        from .db import fetch, execute
        from .config import RESULT_DIR, RESULT_QUOTA_BYTES_PER_USER, ADMIN_USER_IDS
        from .result_store import delete_result_dir

        if user_id in ADMIN_USER_IDS:
            return []

        rows = await fetch(
            "SELECT COALESCE(SUM(result_bytes), 0) AS used FROM task_queue "
            "WHERE user_id = ? AND result_uri IS NOT NULL",
            user_id,
        )
        used = int(rows[0]["used"] if rows else 0)
        if used <= RESULT_QUOTA_BYTES_PER_USER:
            return []

        victims = await fetch(
            """
            SELECT id, result_uri, result_bytes FROM task_queue
            WHERE user_id = ? AND result_uri IS NOT NULL AND id != ?
            ORDER BY finished_at ASC NULLS LAST, id ASC
            """,
            user_id,
            keep_task_id,
        )
        evicted: list[int] = []
        for v in victims:
            if used <= RESULT_QUOTA_BYTES_PER_USER:
                break
            delete_result_dir(v.get("result_uri"), RESULT_DIR)
            await execute("DELETE FROM task_queue WHERE id = ?", v["id"])
            used -= int(v.get("result_bytes") or 0)
            evicted.append(v["id"])
            log.info("quota GC: deleted task %s for user %s", v["id"], user_id)
        return evicted

    async def _update_task_progress(self, task_id: int, progress: dict | None) -> None:
        """Write progress dict to task_queue progress_pct / progress_json."""
        if not progress:
            return
        from .db import execute
        pct = progress.get("pct")
        await execute(
            """
            UPDATE task_queue
            SET progress_pct = ?,
                progress_json = ?
            WHERE id = ? AND status = 'running'
            """,
            float(pct) if pct is not None else None,
            json.dumps(progress),
            task_id,
        )

    async def _mark_task_failed(self, task_id: int, error: str) -> None:
        from .db import execute
        await execute("""
            UPDATE task_queue
            SET status = 'failed', finished_at = datetime('now'), error = ?
            WHERE id = ?
        """, error, task_id)

    async def run_forever(self) -> None:
        await self.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await self.stop()