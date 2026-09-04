# backend/scheduler/config.py
import os
from dataclasses import dataclass
from enum import Enum

POSTGRES_URL = os.getenv("POSTGRES_URL")
SCHEDULER_HOST = os.getenv("SCHEDULER_HOST", "0.0.0.0")
SCHEDULER_PORT = int(os.getenv("SCHEDULER_PORT", "8080"))

HF_NODES = {
    "node1": "https://scanli-blinkquant-node1.hf.space",
    "node2": "https://scanli-blinkquant-node2.hf.space",
    "node3": "https://scanli-blinkquant-node3.hf.space",
}

SCHEDULE_INTERVAL_SEC = 1
HEARTBEAT_TIMEOUT_SEC = 30
TASK_RUNNING_TIMEOUT_SEC = 30 * 60
DISPATCH_TIMEOUT_SEC = 240
POLL_JOB_TIMEOUT_SEC = 60
MAX_RETRIES_DEFAULT = 2
PREEMPT_CANCEL_TIMEOUT_SEC = 10

class NodeStatus(str, Enum):
    IDLE        = "idle"
    RUNNING     = "running"
    DRAINING    = "draining"
    UNHEALTHY   = "unhealthy"
    MAINTENANCE = "maintenance"

class TaskType(str, Enum):
    SELECTION = "selection"
    BACKTEST  = "backtest"

class TaskStatus(str, Enum):
    PENDING    = "pending"
    QUEUED     = "queued"
    RUNNING    = "running"
    DONE       = "done"
    FAILED     = "failed"
    CANCELLED  = "cancelled"
    PREEMPTED  = "preempted"

class TaskPriority(int, Enum):
    LOW    = -10
    NORMAL = 0
    HIGH   = 10