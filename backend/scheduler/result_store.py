"""回测结果存储：summary 提取 + Parquet(ZSTD) 持久化。"""
from __future__ import annotations
import json
import logging
import os
from typing import Optional

import polars as pl

from .config import RESULT_ZSTD_LEVEL

log = logging.getLogger("result_store")

# Artifact names that can be persisted
ARTIFACT_NAMES = ("equity_curve", "trades", "positions_daily")


def build_result_summary(data: dict) -> dict:
    """从回测完整 data 中提取轻量摘要（存入 SQLite result_summary 列）。"""
    ec = data.get("equity_curve") or []
    trades = data.get("trades") or []
    m = data.get("metrics") or {}

    final = ec[-1]["equity"] if ec else None
    return {
        "final_equity": final,
        "total_return": m.get("total_return"),
        "max_drawdown": m.get("max_drawdown"),
        "n_trades": len(trades) if isinstance(trades, list) else m.get("trade_count"),
        "n_equity_points": len(ec) if isinstance(ec, list) else None,
        "initial_cash": data.get("initial_cash"),
    }


def persist(task_id: int, data: dict, result_dir: str) -> tuple[dict, str]:
    """将回测结果写入 Parquet 文件，返回 (summary, uri)。

    Args:
        task_id: 任务 ID
        data: 回测完整结果字典
        result_dir: 结果根目录

    Returns:
        (summary_dict, relative_uri)
    """
    uri = f"task_{task_id}"
    task_dir = os.path.join(result_dir, uri)
    os.makedirs(task_dir, exist_ok=True)

    # 写 meta.json（formula、日期、metrics 等元数据）
    meta = {
        "task_id": task_id,
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

    # 写 Parquet 文件
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
    return summary, uri


def load_part(result_uri: str, name: str, result_dir: str) -> Optional[pl.DataFrame]:
    """按名加载单个 Parquet 文件。"""
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
    """过渡兼容：读 Parquet 拼回旧 JSON 形状。"""
    meta_path = os.path.join(result_dir, result_uri, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)

    result = dict(meta)
    for name in ARTIFACT_NAMES:
        df = load_part(result_uri, name, result_dir)
        if df is not None:
            result[name] = df.to_dicts()
        else:
            result[name] = []
    return result
