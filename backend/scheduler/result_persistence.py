"""Durable persistence for Node1 Artifact result files.

Local /data is a cache. Completed backtest result files are mirrored to a
Hugging Face Dataset and restored on demand after a Node1 Space restart.
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.utils import EntryNotFoundError

from .config import RESULT_DIR, SCHEDULER_RESULTS_HF_REPO

log = logging.getLogger("scheduler.result_persistence")

_ALLOWED = {
    "equity_curve": "equity_curve.parquet",
    "trades": "trades.parquet",
    "positions_daily": "positions_daily.parquet",
    "signal_trace": "signal_trace.json",
}
_REQUIRED = ("equity_curve", "trades", "positions_daily")
_REMOTE_ALLOW_PATTERNS = ["*.parquet", "meta.json", "signal_trace.json"]


def _safe_uri(result_uri: str) -> str:
    uri = str(result_uri or "").strip()
    if not uri.startswith("by_user/") or ".." in uri or "\\" in uri:
        raise ValueError(f"unsafe result_uri: {result_uri!r}")
    return uri


def _token() -> str:
    token = os.getenv("HF_TOKEN")
    if not token:
        raise RuntimeError("HF_TOKEN is not configured for durable result storage")
    return token


def _upload_api() -> HfApi:
    api = HfApi(token=_token())
    api.create_repo(
        repo_id=SCHEDULER_RESULTS_HF_REPO,
        repo_type="dataset",
        exist_ok=True,
    )
    return api


def _upload_tree_sync(result_uri: str, result_dir: str) -> None:
    uri = _safe_uri(result_uri)
    local_dir = Path(result_dir) / uri
    if not local_dir.is_dir():
        raise FileNotFoundError(f"result directory missing: {local_dir}")

    missing = [
        name for name in _REQUIRED
        if not (local_dir / _ALLOWED[name]).is_file()
    ]
    if missing:
        raise FileNotFoundError(
            f"required durable result files missing in {local_dir}: {', '.join(missing)}"
        )

    _upload_api().upload_folder(
        repo_id=SCHEDULER_RESULTS_HF_REPO,
        repo_type="dataset",
        folder_path=str(local_dir),
        path_in_repo=uri,
        allow_patterns=_REMOTE_ALLOW_PATTERNS,
        commit_message=f"Persist BlinkQuant artifact {uri}",
    )
    log.info("durable result uploaded uri=%s repo=%s", uri, SCHEDULER_RESULTS_HF_REPO)


async def persist_result_tree(result_uri: str, result_dir: str = RESULT_DIR) -> None:
    await asyncio.to_thread(_upload_tree_sync, result_uri, result_dir)


def _restore_part_sync(result_uri: str, name: str, result_dir: str) -> bool:
    uri = _safe_uri(result_uri)
    filename = _ALLOWED.get(name)
    if not filename:
        raise ValueError(f"unsupported durable result part: {name}")

    target = Path(result_dir) / uri / filename
    if target.is_file() and target.stat().st_size > 0:
        return True

    downloaded = hf_hub_download(
        repo_id=SCHEDULER_RESULTS_HF_REPO,
        filename=f"{uri}/{filename}",
        repo_type="dataset",
        local_dir=str(result_dir),
        token=_token(),
    )
    restored = Path(downloaded)
    if not restored.is_file() or restored.stat().st_size <= 0:
        raise IOError(f"HF restore returned invalid file: {restored}")
    log.info("durable result restored uri=%s name=%s path=%s", uri, name, restored)
    return True


async def ensure_result_part_local(
    result_uri: str, name: str, result_dir: str = RESULT_DIR
) -> bool:
    try:
        return await asyncio.to_thread(_restore_part_sync, result_uri, name, result_dir)
    except EntryNotFoundError:
        log.warning(
            "durable result not found uri=%s name=%s repo=%s",
            result_uri, name, SCHEDULER_RESULTS_HF_REPO,
        )
        return False
    except Exception:
        log.exception(
            "durable result restore failed uri=%s name=%s repo=%s",
            result_uri, name, SCHEDULER_RESULTS_HF_REPO,
        )
        raise


async def ensure_result_tree_local(
    result_uri: str, result_dir: str = RESULT_DIR
) -> bool:
    uri = _safe_uri(result_uri)
    for name in _REQUIRED:
        if not await ensure_result_part_local(uri, name, result_dir):
            return False
    return True
