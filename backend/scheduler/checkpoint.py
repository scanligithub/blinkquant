# backend/scheduler/checkpoint.py
from __future__ import annotations
import os
import hashlib
import asyncio
import shutil
import tempfile
from pathlib import Path
from typing import Optional

from huggingface_hub import HfApi

from .config import (
    SCHEDULER_DB_PATH,
    SCHEDULER_HF_REPO,
    SCHEDULER_HF_FILE,
    CHECKPOINT_INTERVAL_SEC,
)
from .db import close_pool

_last_uploaded_sha: Optional[str] = None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


async def _checkpoint_once(force: bool = False) -> bool:
    """
    执行一次 checkpoint：
    1. wal_checkpoint(TRUNCATE) 把 WAL 刷回主库
    2. backup 到临时文件
    3. sha256；若与上次相同且非 force → 跳过
    4. 上传 HF Dataset
    返回 True=上传了，False=跳过
    """
    global _last_uploaded_sha

    db_path = Path(SCHEDULER_DB_PATH)
    if not db_path.exists():
        return False

    # 1) 创建临时文件并 backup（在线程池避免阻塞事件循环）
    def _backup() -> Path:
        import sqlite3
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db", prefix="scheduler_snap_")
        os.close(tmp_fd)
        src = sqlite3.connect(db_path)
        dst = sqlite3.connect(tmp_path)
        try:
            # 先强制 checkpoint，确保主库包含所有已提交事务
            src.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        return Path(tmp_path)

    snap_path = await asyncio.to_thread(_backup)

    try:
        sha = await asyncio.to_thread(_sha256_file, snap_path)
        if not force and sha == _last_uploaded_sha:
            return False

        # 2) 上传 HF
        def _upload() -> None:
            api = HfApi(token=os.getenv("HF_TOKEN"))
            api.upload_file(
                path_or_fileobj=str(snap_path),
                path_in_repo=SCHEDULER_HF_FILE,
                repo_id=SCHEDULER_HF_REPO,
                repo_type="dataset",
            )

        await asyncio.to_thread(_upload)
        _last_uploaded_sha = sha
        return True
    finally:
        snap_path.unlink(missing_ok=True)


async def checkpoint_loop() -> None:
    """后台循环：每 CHECKPOINT_INTERVAL_SEC 跑一次"""
    while True:
        try:
            await asyncio.sleep(CHECKPOINT_INTERVAL_SEC)
            await _checkpoint_once(force=False)
        except asyncio.CancelledError:
            # shutdown 时最后一次强制
            try:
                await _checkpoint_once(force=True)
            finally:
                raise
        except Exception as e:
            # 不阻塞调度主循环
            import logging
            logging.getLogger("scheduler").warning("checkpoint_loop error: %s", e)


async def shutdown_checkpoint() -> None:
    """关机前最后一次 checkpoint（force=True）"""
    await _checkpoint_once(force=True)
    await close_pool()