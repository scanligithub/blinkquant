"""回测结果存储：summary 提取 + Parquet(ZSTD) 持久化。"""
from __future__ import annotations
import json
import logging
import os
import re
import shutil
import io
import zipfile
from typing import Optional

import polars as pl

from .config import RESULT_ZSTD_LEVEL

log = logging.getLogger("result_store")

ARTIFACT_NAMES = ("equity_curve", "trades", "positions_daily")
SIGNAL_TRACE_ARTIFACT = "signal_trace"

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

    positions = data.get("positions_daily")
    n_positions = None
    if isinstance(positions, list):
        n_positions = len(positions)
    elif isinstance(positions, int):
        n_positions = positions
    elif isinstance(positions, dict):
        n_positions = positions.get("n")

    if final is None:
        final = data.get("final_equity") or m.get("final_equity")

    diag = data.get("execution_diagnostics") or {}
    rej = diag.get("rej_counters") or data.get("rej_counters") or {}
    partial_fill_count = diag.get("partial_fill_count")
    if partial_fill_count is None:
        partial_fill_count = data.get("partial_fill_count")

    return {
        "final_equity": final,
        "total_return": m.get("total_return"),
        "max_drawdown": m.get("max_drawdown"),
        "cagr": m.get("cagr", m.get("annualized_return")),
        "sharpe": m.get("sharpe"),
        "sortino": m.get("sortino"),
        "calmar": m.get("calmar"),
        "drawdown_duration": m.get("drawdown_duration"),
        "turnover": m.get("turnover"),
        "total_fees": m.get("total_fees"),
        "buy_count": m.get("buy_count"),
        "sell_count": m.get("sell_count"),
        "n_trades": n_trades,
        "n_positions": n_positions,
        "n_equity_points": n_equity,
        "initial_cash": data.get("initial_cash"),
        "rej_counters": {str(k): int(v) for k, v in rej.items()},
        "partial_fill_count": int(partial_fill_count or 0),
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
        "strategy": data.get("strategy"),
        "backtest_config": data.get("backtest_config"),
        "start_date": data.get("start_date"),
        "signal_end_date": data.get("signal_end_date"),
        "valuation_end_date": data.get("valuation_end_date"),
        "initial_cash": data.get("initial_cash"),
        "metrics": data.get("metrics"),
        "strategy_template_id": data.get("strategy_template_id"),
        "strategy_template_name": data.get("strategy_template_name"),
        "strategy_template_updated_at": data.get("strategy_template_updated_at"),
        "source_task_id": data.get("source_task_id"),
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

    signal_trace = data.get(SIGNAL_TRACE_ARTIFACT)
    if signal_trace:
        try:
            with open(os.path.join(task_dir, "signal_trace.json"), "w", encoding="utf-8") as f:
                json.dump(signal_trace, f, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            log.exception("Failed to write signal_trace for task %s", task_id)

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


def load_signal_trace(result_uri: str, result_dir: str) -> Optional[dict]:
    """Load the optional persisted SignalTrace JSON artifact."""
    path = os.path.join(result_dir, result_uri, "signal_trace.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            value = json.load(f)
        return value if isinstance(value, dict) else None
    except Exception:
        log.exception("Failed to read signal_trace from %s", result_uri)
        return None


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
    trace = load_signal_trace(result_uri, result_dir)
    if trace is not None:
        result[SIGNAL_TRACE_ARTIFACT] = trace
    return result


PORTABLE_ID_KEYS = frozenset({
    "user_id",
    "task_id",
    "artifact_id",
    "source_task_id",
    "strategy_template_id",
    "cluster_job_id",
    "assigned_node",
})


def _portable_value(value):
    """Strip internal identity/provenance IDs from portable artifact metadata."""
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            if key in PORTABLE_ID_KEYS:
                continue
            if key in ("source_selection_strategy", "selection_strategy_snapshot") and isinstance(child, dict):
                child = {
                    k: v for k, v in child.items()
                    if k not in {"id", "strategy_id", "source_task_id", "artifact_id"}
                }
            result[key] = _portable_value(child)
        return result
    if isinstance(value, list):
        return [_portable_value(item) for item in value]
    if isinstance(value, tuple):
        return [_portable_value(item) for item in value]
    return value


def build_artifact_bundle(row: dict, result_dir: str) -> bytes:
    """Create a portable ZIP bundle for one artifact without exposing owner/internal IDs."""
    artifact_type = str(row.get("artifact_type") or "")
    if artifact_type not in {"selection", "backtest"}:
        raise ValueError("invalid artifact type")

    try:
        metadata = json.loads(row.get("metadata") or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    try:
        summary = json.loads(row.get("summary") or "null")
    except (TypeError, json.JSONDecodeError):
        summary = None
    try:
        result_json = json.loads(row.get("result_json") or "null")
    except (TypeError, json.JSONDecodeError):
        result_json = None

    files = []
    result_uri = row.get("result_uri")
    base = os.path.join(result_dir, str(result_uri)) if result_uri else None
    if base and os.path.isdir(base):
        for name in ARTIFACT_NAMES:
            if os.path.isfile(os.path.join(base, f"{name}.parquet")):
                files.append(f"parts/{name}.parquet")
        if os.path.isfile(os.path.join(base, "signal_trace.json")):
            files.append("parts/signal_trace.json")

    if artifact_type == "selection":
        files.append("result.json")

    from datetime import datetime, timezone
    manifest = {
        "format": "blinkquant-artifact-v1",
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "artifact_type": artifact_type,
        "title": str(row.get("title") or ""),
        "created_at": row.get("created_at"),
        "finished_at": row.get("finished_at"),
        "metadata": _portable_value(metadata),
        "summary": _portable_value(summary),
        "files": files,
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "manifest.json",
            json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        if artifact_type == "selection" and result_json is not None:
            zf.writestr(
                "result.json",
                json.dumps(_portable_value(result_json), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            )
        if base and os.path.isdir(base):
            for name in ARTIFACT_NAMES:
                path = os.path.join(base, f"{name}.parquet")
                if os.path.isfile(path):
                    zf.write(path, f"parts/{name}.parquet")
            trace_path = os.path.join(base, "signal_trace.json")
            if os.path.isfile(trace_path):
                zf.write(trace_path, "parts/signal_trace.json")
    return buf.getvalue()


ALLOWED_BUNDLE_FILES = frozenset({
    "manifest.json",
    "result.json",
    "parts/equity_curve.parquet",
    "parts/trades.parquet",
    "parts/positions_daily.parquet",
    "parts/signal_trace.json",
})


def validate_artifact_bundle(zf: zipfile.ZipFile) -> dict:
    """Validate and return the portable manifest before filesystem mutation."""
    names = zf.namelist()
    if "manifest.json" not in names:
        raise ValueError("artifact bundle is missing manifest.json")
    if any(name not in ALLOWED_BUNDLE_FILES for name in names):
        raise ValueError("artifact bundle contains an unsupported file")
    manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
    if not isinstance(manifest, dict) or manifest.get("format") != "blinkquant-artifact-v1":
        raise ValueError("unsupported artifact bundle format")
    if manifest.get("artifact_type") not in {"selection", "backtest"}:
        raise ValueError("invalid artifact type")
    title = str(manifest.get("title") or "").strip()
    if not title or len(title) > 80:
        raise ValueError("invalid artifact title")

    files = manifest.get("files")
    if not isinstance(files, list) or any(path not in ALLOWED_BUNDLE_FILES for path in files):
        raise ValueError("invalid artifact file manifest")
    if len(set(files)) != len(files):
        raise ValueError("duplicate artifact files")
    if set(files) != (set(names) - {"manifest.json"}):
        raise ValueError("artifact file manifest mismatch")

    total_uncompressed = 0
    for info in zf.infolist():
        if info.filename not in ALLOWED_BUNDLE_FILES:
            raise ValueError("unsupported artifact file")
        if info.file_size > 2 * 1024**3:
            raise ValueError("artifact member is too large")
        total_uncompressed += info.file_size
        if total_uncompressed > 2 * 1024**3:
            raise ValueError("artifact bundle is too large")
    return manifest


def extract_artifact_bundle(zf: zipfile.ZipFile, temp_dir: str, manifest: dict) -> int:
    """Extract validated result files to a temporary directory and return byte size."""
    os.makedirs(temp_dir, exist_ok=True)
    for name in manifest["files"]:
        if name == "result.json":
            continue
        out_name = os.path.basename(name)
        target = os.path.join(temp_dir, out_name)
        with zf.open(name) as src, open(target, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
    return dir_size(temp_dir)
