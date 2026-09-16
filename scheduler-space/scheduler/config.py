import os
from dataclasses import dataclass
from enum import Enum

# 环境变量
POSTGRES_URL = os.getenv("POSTGRES_URL")
SCHEDULER_HOST = os.getenv("SCHEDULER_HOST", "0.0.0.0")
SCHEDULER_PORT = int(os.getenv("SCHEDULER_PORT", "7860"))
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "internal-secret-change-me")

# HF 节点端点 (使用环境变量配置，默认使用公网域名)
HF_NODES = {
    "node1": os.getenv("NODE1_URL", "https://scanli-blinkquant-node1.hf.space"),
    "node2": os.getenv("NODE2_URL", "https://scanli-blinkquant-node2.hf.space"),
    "node3": os.getenv("NODE3_URL", "https://scanli-blinkquant-node3.hf.space"),
}

# 调度参数
SCHEDULE_INTERVAL_SEC = 1          # 调度循环间隔
HEARTBEAT_TIMEOUT_SEC = 30         # 心跳超时判定 unhealthy
TASK_RUNNING_TIMEOUT_SEC = 30 * 60 # running 超过 30 分钟无心跳 → 回收
DISPATCH_TIMEOUT_SEC = 240         # 派发超时（含冷启动）
POLL_JOB_TIMEOUT_SEC = 60          # 轮询 HF job 状态超时
MAX_RETRIES_DEFAULT = 2
PREEMPT_CANCEL_TIMEOUT_SEC = 10    # 抢占取消等待

class NodeStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    DRAINING = "draining"
    UNHEALTHY = "unhealthy"
    MAINTENANCE = "maintenance"

class TaskType(str, Enum):
    SELECTION = "selection"
    BACKTEST = "backtest"

class TaskStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PREEMPTED = "preempted"


# 动态窗口超时
TASK_TIMEOUT_BASE_SEC = int(os.getenv("TASK_TIMEOUT_BASE_SEC", str(15 * 60)))
TASK_TIMEOUT_SEC_PER_YEAR = int(os.getenv("TASK_TIMEOUT_SEC_PER_YEAR", str(20 * 60)))
TASK_TIMEOUT_MIN_SEC = int(os.getenv("TASK_TIMEOUT_MIN_SEC", str(15 * 60)))
TASK_TIMEOUT_MAX_SEC = int(os.getenv("TASK_TIMEOUT_MAX_SEC", str(120 * 60)))
SELECTION_RUNNING_TIMEOUT_SEC = int(os.getenv("SELECTION_RUNNING_TIMEOUT_SEC", str(15 * 60)))


def compute_task_timeout_sec(
    task_type: str,
    payload: dict | None,
    *,
    base: int = TASK_TIMEOUT_BASE_SEC,
    per_year: int = TASK_TIMEOUT_SEC_PER_YEAR,
    min_sec: int = TASK_TIMEOUT_MIN_SEC,
    max_sec: int = TASK_TIMEOUT_MAX_SEC,
    selection_sec: int = SELECTION_RUNNING_TIMEOUT_SEC,
    fallback: int = TASK_RUNNING_TIMEOUT_SEC,
) -> int:
    """按任务 payload 的日期跨度计算墙钟超时秒数。"""
    from datetime import date

    if task_type == "selection":
        return selection_sec

    if not payload:
        return fallback

    start_s = payload.get("start_date") or payload.get("start")
    end_s = (
        payload.get("end_signal_date")
        or payload.get("end_date")
        or payload.get("signal_end_date")
    )
    try:
        start = date.fromisoformat(str(start_s)[:10])
        end = date.fromisoformat(str(end_s)[:10])
        years = max(0.0, (end - start).days / 365.25)
    except Exception:
        return fallback

    raw = int(base + years * per_year)
    return max(min_sec, min(max_sec, raw))