# backend/scheduler/state.py
from __future__ import annotations
from enum import Enum
from typing import Optional

class NodeAction(str, Enum):
    IDLE        = "idle"
    START_TASK  = "start_task"
    PREEMPT     = "preempt"
    HEARTBEAT   = "heartbeat"
    UNHEALTHY   = "unhealthy"
    RECOVER     = "recover"

class TaskAction(str, Enum):
    QUEUE       = "queue"
    START       = "start"
    PREEMPT     = "preempt"
    COMPLETE    = "complete"
    FAIL        = "fail"
    CANCEL      = "cancel"
    REQUEUE     = "requeue"

VALID_NODE_TRANSITIONS: dict[str, set[str]] = {
    "idle":        {"running", "unhealthy", "maintenance"},
    "running":     {"idle", "draining", "unhealthy"},
    "draining":    {"idle", "unhealthy"},
    "unhealthy":   {"idle", "maintenance"},
    "maintenance": {"idle"},
}

VALID_TASK_TRANSITIONS: dict[str, set[str]] = {
    "pending":    {"queued", "cancelled"},
    "queued":     {"running", "cancelled"},
    "running":    {"done", "failed", "cancelled", "preempted"},
    "preempted":  {"pending"},
}

def can_transition_node(current: str, target: str) -> bool:
    return target in VALID_NODE_TRANSITIONS.get(current, set())

def can_transition_task(current: str, target: str) -> bool:
    return target in VALID_TASK_TRANSITIONS.get(current, set())