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