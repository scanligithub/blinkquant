# backend/scheduler/models.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

from .config import TaskType, TaskStatus, NodeStatus, TaskPriority

@dataclass(slots=True)
class NodeRow:
    node_id: str
    name: str
    endpoint: str
    weight: int
    status: str
    current_task_id: int | None
    task_type: str | None
    heartbeat_at: datetime | None
    last_error: str | None

    @property
    def is_idle(self) -> bool:
        return self.status == "idle"

    @property
    def is_healthy(self) -> bool:
        return self.status not in ("unhealthy", "maintenance")

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "name": self.name,
            "status": self.status,
            "current_task_id": self.current_task_id,
            "task_type": self.task_type,
            "heartbeat_at": self.heartbeat_at.isoformat() if self.heartbeat_at else None,
            "load": 0.0,
        }


@dataclass(slots=True)
class TaskRow:
    id: int
    user_id: UUID
    task_type: str
    payload: dict
    priority: int
    status: str
    assigned_node: str | None
    cluster_job_id: str | None
    result: dict | None
    error: str | None
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    retry_count: int
    max_retries: int
    preempted_by: int | None

    @property
    def is_terminal(self) -> bool:
        return self.status in ("done", "failed", "cancelled", "preempted")

    @property
    def needs_requeue(self) -> bool:
        return self.status == "preempted" and self.retry_count < self.max_retries

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "user_id": str(self.user_id),
            "task_type": self.task_type,
            "status": self.status,
            "assigned_node": self.assigned_node,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }