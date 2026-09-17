"""回测结果存储：summary 提取 + Parquet(ZSTD) 持久化。"""
from __future__ import annotations
import json
import logging
import os
import re
import shutil
from typing import Optional

import polars as pl

from .config import RESULT_ZSTD_LEVEL

log = logging.getLogger("result_store")

ARTIFACT_NAMES = ("equity_curve", "trades", "positions_daily")
_USER_ID_RE = re.compile(r"^[\w-]+$")


def assert_safe_user_id(user_id: str) -> str:
    uid = (user_id or "").strip()
    if not uid or not _USER_ID_RE.match(uid):
        raise ValueError(f"invalid user_id for path: {user_id!r}")
    if ".." in uid or "/" in uid or "\\" in uid:
        raise ValueError(f"unsafe user_id: {user_id!r}")
    return uid


def make_result_uri(user_id: str, task_id: int) -> str:
    uid = assert_safe_user_id(str(user_id))
    return f"by_user/{uid}/task_{int(task_id)}"


def dir_size(path: str) -> int:
    total = 0
    if not os.path.isdir(path):
        return 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def delete_result_dir(result_uri: str | None, result_dir: str) -> bool:
    """删除结果目录。uri 为空或不存在视为成功。返回是否实际删过目录。"""
    if not result_uri:
        return False
    if not str(result_uri).startswith("by_user/"):
        log.warning("refuse delete non-by_user uri: %s", result_uri)
        return False
    path = os.path.join(result_dir, result_uri)
    if not os.path.isdir(path):
        return False
    try:
        shutil.rmtree(path)
        parent = os.path.dirname(path)
        try:
            if os.path.isdir(parent) and not os.listdir(parent):
                os.rmdir(parent)
        except OSError:
            pass
        return True
    except Exception:
        log.exception("rmtree failed: %s", path)
        return False


def build_result_summary(data: dict) -> dict:
    m = data.get("metrics") or {}
    ec = data.get("equity_curve")
    trades = data.get("trades")

    final = None
    n_equity = None
    if isinstance(ec, list) and ec:
        final = ec[-1].get("equity")
        n_equity = len(ec)
    elif isinstance(ec, dict):
        final = ec.get("final_equity")
        n_equity = ec.get("n")

    n_trades = m.get("trade_count")
    if n_trades is None:
        if isinstance(trades, list):
            n_trades = len(trades)
        elif isinstance(trades, int):
            n_trades = trades
        elif isinstance(trades, dict):
            n_trades = trades.get("n")

    if final is None:
        final = data.get("final_equity") or m.get("final_equity")

    return {
        "final_equity": final,
        "total_return": m.get("total_return"),
        "max_drawdown": m.get("max_drawdown"),
        "n_trades": n_trades,
        "n_equity_points": n_equity,
        "initial_cash": data.get("initial_cash"),
    }


def persist(task_id: int, data: dict, result_dir: str, *, user_id: str) -> tuple[dict, str, int]:
    """写 Parquet，返回 (summary, uri, bytes)。"""
    uri = make_result_uri(user_id, task_id)
    task_dir = os.path.join(result_dir, uri)
    os.makedirs(task_dir, exist_ok=True)

    meta = {
        "task_id": task_id,
        "user_id": str(user_id),
        "formula": data.get("formula"),
        "start_date": data.get("start_date"),
        "signal_end_date": data.get("signal_end_date"),
        "valuation_end_date": data.get("valuation_end_date"),
        "initial_cash": data.get("initial_cash"),
        "metrics": data.get("metrics"),
    }
    try:
        with open(os.path.join(task_dir, "meta.json"), "w") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        log.exception("Failed to write meta.json for task %s", task_id)

    for name in ARTIFACT_NAMES:
        rows = data.get(name)
        if not rows:
            continue
        try:
            df = pl.DataFrame(rows)
            path = os.path.join(task_dir, f"{name}.parquet")
            df.write_parquet(path, compression="zstd", compression_level=RESULT_ZSTD_LEVEL)
        except Exception:
            log.exception("Failed to write %s for task %s", name, task_id)

    summary = build_result_summary(data)
    nbytes = dir_size(task_dir)
    return summary, uri, nbytes


def persist_frames(
    task_id: int,
    *,
    user_id: str,
    result_dir: str,
    meta: dict,
    equity_curve: pl.DataFrame | None = None,
    trades: pl.DataFrame | None = None,
    positions_daily: pl.DataFrame | None = None,
    summary: dict | None = None,
) -> tuple[dict, str, int]:
    """不经 list[dict]，直接从 DataFrame 写 Parquet。返回 (summary, uri, bytes)。"""
    uri = make_result_uri(user_id, task_id)
    task_dir = os.path.join(result_dir, uri)
    os.makedirs(task_dir, exist_ok=True)

    meta = {**meta, "task_id": task_id, "user_id": str(user_id)}
    try:
        with open(os.path.join(task_dir, "meta.json"), "w") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        log.exception("Failed to write meta.json for task %s", task_id)

    frames = {
        "equity_curve": equity_curve,
        "trades": trades,
        "positions_daily": positions_daily,
    }
    for name, df in frames.items():
        if df is None or df.is_empty():
            continue
        try:
            df.write_parquet(
                os.path.join(task_dir, f"{name}.parquet"),
                compression="zstd",
                compression_level=RESULT_ZSTD_LEVEL,
            )
        except Exception:
            log.exception("Failed to write %s for task %s", name, task_id)

    if summary is None:
        summary = build_result_summary({
            **meta,
            "equity_curve": {"n": equity_curve.height if equity_curve is not None else 0,
                             "final_equity": float(equity_curve["equity"][-1]) if equity_curve is not None and equity_curve.height else None},
            "trades": trades.height if trades is not None else 0,
            "metrics": meta.get("metrics") or {},
        })
    nbytes = dir_size(task_dir)
    return summary, uri, nbytes


def load_part(result_uri: str, name: str, result_dir: str) -> Optional[pl.DataFrame]:
    if name not in ARTIFACT_NAMES:
        raise ValueError(f"Unknown artifact: {name}")
    path = os.path.join(result_dir, result_uri, f"{name}.parquet")
    if not os.path.exists(path):
        return None
    try:
        return pl.read_parquet(path)
    except Exception:
        log.exception("Failed to read %s from %s", name, result_uri)
        return None


def load_as_legacy_json(result_uri: str, result_dir: str) -> dict:
    meta_path = os.path.join(result_dir, result_uri, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)
    result = dict(meta)
    for name in ARTIFACT_NAMES:
        df = load_part(result_uri, name, result_dir)
        result[name] = df.to_dicts() if df is not None else []
    return result
