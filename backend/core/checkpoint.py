"""BacktestCheckpoint: portable, deterministic checkpoint serialization."""

import datetime
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Union, List, Dict, Any

import polars as pl

from core.backtest_types import FeeConfig, ExecutionConfig
from core.execution import OrderIntent
from core.corporate_actions import CorporateAction


@dataclass
class BacktestCheckpoint:
    """Portable checkpoint state for BacktestEngine."""
    # Metadata
    schema_version: str = "1.0.0"
    engine_version: str = "unknown"
    created_at: str = ""  # ISO8601 UTC
    current_date: str = ""  # YYYY-MM-DD
    phase: str = "CHECKPOINT"
    description: str = ""

    # Portfolio state
    cash: float = 0.0
    positions: List[Dict[str, Any]] = None  # list of {code, total_qty, available_qty, frozen_qty, avg_cost, market_value}

    # Pending orders (signal→execution pipeline)
    pending_signal_date: Optional[str] = None
    pending_execution_date: Optional[str] = None
    pending_intents: List[Dict[str, Any]] = None  # list of {code, side, target_qty, target_weight}
    pending_prices: Dict[str, Dict[str, float]] = None  # code -> {open, close}

    # Corporate action cursor
    processed_dividends: List[Dict[str, Any]] = None
    processed_splits: List[Dict[str, Any]] = None
    ca_cursor_date: str = ""

    # Diagnostics accumulators
    diagnostics: Dict[str, Any] = None

    # Carry-forward prices for suspended stocks
    last_close: List[Dict[str, Any]] = None  # list of {code, close}

    # Engine cursors
    thru_thaw: Optional[str] = None
    selected_thru: Optional[str] = None
    random_seed: int = 42

    def __post_init__(self):
        if self.positions is None:
            self.positions = []
        if self.pending_intents is None:
            self.pending_intents = []
        if self.pending_prices is None:
            self.pending_prices = {}
        if self.processed_dividends is None:
            self.processed_dividends = []
        if self.processed_splits is None:
            self.processed_splits = []
        if self.diagnostics is None:
            self.diagnostics = {}
        if self.last_close is None:
            self.last_close = []
        if not self.created_at:
            self.created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # ------------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------------
    def to_json_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict with deterministic ordering."""
        return {
            "schema_version": self.schema_version,
            "engine_version": self.engine_version,
            "created_at": self.created_at,
            "current_date": self.current_date,
            "phase": self.phase,
            "description": self.description,
            "cash": round(self.cash, 10),
            "positions": self._sort_positions(self.positions),
            "pending_signal_date": self.pending_signal_date,
            "pending_execution_date": self.pending_execution_date,
            "pending_intents": self._sort_intents(self.pending_intents),
            "pending_prices": self._sort_prices(self.pending_prices),
            "processed_dividends": self._sort_cas(self.processed_dividends),
            "processed_splits": self._sort_cas(self.processed_splits),
            "ca_cursor_date": self.ca_cursor_date,
            "diagnostics": self._sort_diagnostics(self.diagnostics),
            "last_close": self._sort_last_close(self.last_close),
            "thru_thaw": self.thru_thaw,
            "selected_thru": self.selected_thru,
            "random_seed": self.random_seed,
        }

    def _sort_positions(self, positions: List[Dict]) -> List[Dict]:
        return sorted(
            [{**p, "avg_cost": round(p.get("avg_cost", 0), 10), "market_value": round(p.get("market_value", 0), 10)} for p in positions],
            key=lambda x: x["code"]
        )

    def _sort_intents(self, intents: List[Dict]) -> List[Dict]:
        return sorted(
            [{**i, "target_weight": round(i.get("target_weight", 0), 10)} for i in intents],
            key=lambda x: (x["code"], x["side"])
        )

    def _sort_prices(self, prices: Dict[str, Dict]) -> Dict[str, Dict]:
        return {k: {"open": round(v.get("open", 0), 10), "close": round(v.get("close", 0), 10)}
                for k, v in sorted(prices.items())}

    def _sort_cas(self, cas: List[Dict]) -> List[Dict]:
        return sorted(
            [{**ca, "amount": round(ca.get("amount", 0), 10), "ratio": round(ca.get("ratio", 0), 10)} for ca in cas],
            key=lambda x: (x["code"], x["ex_date"])
        )

    def _sort_diagnostics(self, diag: Dict) -> Dict:
        result = {}
        for k, v in sorted(diag.items()):
            if isinstance(v, dict):
                # Convert date keys to strings for JSON compatibility
                # Also handle mixed key types (date and str) by converting all to str for sorting
                sorted_items = sorted(v.items(), key=lambda item: str(item[0]))
                result[k] = {str(k2): round(v2, 10) if isinstance(v2, float) else v2 for k2, v2 in sorted_items}
            elif isinstance(v, float):
                result[k] = round(v, 10)
            else:
                result[k] = v
        return result

    def _sort_last_close(self, last_close: List[Dict]) -> List[Dict]:
        return sorted([{**lc, "close": round(lc.get("close", 0), 10)} for lc in last_close],
                      key=lambda x: x["code"])

    def to_json(self) -> str:
        """Deterministic JSON string (sorted keys, no whitespace)."""
        return json.dumps(self.to_json_dict(), sort_keys=True, separators=(',', ':'))

    def save_json(self, path: Union[str, Path]) -> None:
        Path(path).write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def from_json_dict(cls, data: Dict[str, Any]) -> "BacktestCheckpoint":
        return cls(
            schema_version=data.get("schema_version", "1.0.0"),
            engine_version=data.get("engine_version", "unknown"),
            created_at=data.get("created_at", ""),
            current_date=data.get("current_date", ""),
            phase=data.get("phase", "CHECKPOINT"),
            description=data.get("description", ""),
            cash=float(data.get("cash", 0)),
            positions=data.get("positions", []),
            pending_signal_date=data.get("pending_signal_date"),
            pending_execution_date=data.get("pending_execution_date"),
            pending_intents=data.get("pending_intents", []),
            pending_prices=data.get("pending_prices", {}),
            processed_dividends=data.get("processed_dividends", []),
            processed_splits=data.get("processed_splits", []),
            ca_cursor_date=data.get("ca_cursor_date", ""),
            diagnostics=data.get("diagnostics", {}),
            last_close=data.get("last_close", []),
            thru_thaw=data.get("thru_thaw"),
            selected_thru=data.get("selected_thru"),
            random_seed=int(data.get("random_seed", 42)),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "BacktestCheckpoint":
        return cls.from_json_dict(json.loads(json_str))

    @classmethod
    def load_json(cls, path: Union[str, Path]) -> "BacktestCheckpoint":
        return cls.from_json(Path(path).read_text(encoding="utf-8"))

    # ------------------------------------------------------------------------
    # Parquet helpers
    # ------------------------------------------------------------------------
    def last_close_to_parquet(self) -> pl.DataFrame:
        return pl.DataFrame(self.last_close) if self.last_close else pl.DataFrame(schema={"code": pl.Utf8, "close": pl.Float64})

    @classmethod
    def last_close_from_parquet(cls, path: Union[str, Path]) -> List[Dict]:
        df = pl.read_parquet(path)
        return df.to_dicts() if df.height > 0 else []


def save_checkpoint(checkpoint: BacktestCheckpoint, directory: Union[str, Path]) -> None:
    """Save checkpoint to directory with multiple files."""
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)

    # meta.json
    meta = {
        "schema_version": checkpoint.schema_version,
        "engine_version": checkpoint.engine_version,
        "created_at": checkpoint.created_at,
        "current_date": checkpoint.current_date,
        "phase": checkpoint.phase,
        "description": checkpoint.description,
    }
    (dir_path / "meta.json").write_text(json.dumps(meta, sort_keys=True, separators=(',', ':')), encoding="utf-8")

    # portfolio.json
    portfolio_data = {
        "cash": round(checkpoint.cash, 10),
        "positions": checkpoint._sort_positions(checkpoint.positions),
    }
    (dir_path / "portfolio.json").write_text(
        json.dumps(portfolio_data, sort_keys=True, separators=(',', ':')), encoding="utf-8"
    )

    # pending.json
    pending_data = {
        "signal_date": checkpoint.pending_signal_date,
        "execution_date": checkpoint.pending_execution_date,
        "intents": checkpoint._sort_intents(checkpoint.pending_intents),
        "prices": checkpoint._sort_prices(checkpoint.pending_prices),
    }
    (dir_path / "pending.json").write_text(
        json.dumps(pending_data, sort_keys=True, separators=(',', ':')), encoding="utf-8"
    )

    # corporate_actions.json
    ca_data = {
        "processed_dividends": checkpoint._sort_cas(checkpoint.processed_dividends),
        "processed_splits": checkpoint._sort_cas(checkpoint.processed_splits),
        "cursor_date": checkpoint.ca_cursor_date,
    }
    (dir_path / "corporate_actions.json").write_text(
        json.dumps(ca_data, sort_keys=True, separators=(',', ':')), encoding="utf-8"
    )

    # diagnostics.json
    (dir_path / "diagnostics.json").write_text(
        json.dumps(checkpoint._sort_diagnostics(checkpoint.diagnostics), sort_keys=True, separators=(',', ':')), encoding="utf-8"
    )

    # last_close.parquet
    checkpoint.last_close_to_parquet().write_parquet(dir_path / "last_close.parquet", compression="zstd")

    # engine_state.json
    engine_data = {
        "thru_thaw": checkpoint.thru_thaw,
        "selected_thru": checkpoint.selected_thru,
        "random_seed": checkpoint.random_seed,
    }
    (dir_path / "engine_state.json").write_text(
        json.dumps(engine_data, sort_keys=True, separators=(',', ':')), encoding="utf-8"
    )


def load_checkpoint(directory: Union[str, Path]) -> BacktestCheckpoint:
    """Load checkpoint from directory."""
    dir_path = Path(directory)

    # Load meta
    meta = json.loads((dir_path / "meta.json").read_text(encoding="utf-8"))

    # Load components
    portfolio = json.loads((dir_path / "portfolio.json").read_text(encoding="utf-8"))
    pending = json.loads((dir_path / "pending.json").read_text(encoding="utf-8"))
    ca = json.loads((dir_path / "corporate_actions.json").read_text(encoding="utf-8"))
    diagnostics = json.loads((dir_path / "diagnostics.json").read_text(encoding="utf-8"))
    last_close = BacktestCheckpoint.last_close_from_parquet(dir_path / "last_close.parquet")
    engine_state = json.loads((dir_path / "engine_state.json").read_text(encoding="utf-8"))

    return BacktestCheckpoint(
        schema_version=meta["schema_version"],
        engine_version=meta["engine_version"],
        created_at=meta["created_at"],
        current_date=meta["current_date"],
        phase=meta["phase"],
        description=meta.get("description", ""),
        cash=portfolio["cash"],
        positions=portfolio["positions"],
        pending_signal_date=pending["signal_date"],
        pending_execution_date=pending["execution_date"],
        pending_intents=pending["intents"],
        pending_prices=pending["prices"],
        processed_dividends=ca["processed_dividends"],
        processed_splits=ca["processed_splits"],
        ca_cursor_date=ca["cursor_date"],
        diagnostics=diagnostics,
        last_close=last_close,
        thru_thaw=engine_state["thru_thaw"],
        selected_thru=engine_state["selected_thru"],
        random_seed=engine_state["random_seed"],
    )


def checkpoint_eq(a: BacktestCheckpoint, b: BacktestCheckpoint, tol: float = 1e-9) -> bool:
    """Compare two checkpoints for equality (used in regression tests)."""
    if a.schema_version != b.schema_version:
        return False
    if a.current_date != b.current_date:
        return False
    if a.phase != b.phase:
        return False
    if abs(a.cash - b.cash) > tol:
        return False

    # Compare positions
    if len(a.positions) != len(b.positions):
        return False
    for p1, p2 in zip(sorted(a.positions, key=lambda x: x["code"]), sorted(b.positions, key=lambda x: x["code"])):
        if p1["code"] != p2["code"]:
            return False
        for k in ("total_qty", "available_qty", "frozen_qty"):
            if p1[k] != p2[k]:
                return False
        for k in ("avg_cost", "market_value"):
            if abs(p1.get(k, 0) - p2.get(k, 0)) > tol:
                return False

    # Compare pending
    if a.pending_signal_date != b.pending_signal_date:
        return False
    if a.pending_execution_date != b.pending_execution_date:
        return False
    if len(a.pending_intents) != len(b.pending_intents):
        return False
    for i1, i2 in zip(sorted(a.pending_intents, key=lambda x: (x["code"], x["side"])),
                       sorted(b.pending_intents, key=lambda x: (x["code"], x["side"]))):
        if i1 != i2:
            return False

    # Compare CA
    if a.ca_cursor_date != b.ca_cursor_date:
        return False

    # Compare diagnostics (simplified)
    if a.diagnostics != b.diagnostics:
        return False

    # Compare last_close
    if len(a.last_close) != len(b.last_close):
        return False
    for l1, l2 in zip(sorted(a.last_close, key=lambda x: x["code"]), sorted(b.last_close, key=lambda x: x["code"])):
        if l1["code"] != l2["code"]:
            return False
        if abs(l1["close"] - l2["close"]) > tol:
            return False

    # Compare cursors
    if a.thru_thaw != b.thru_thaw:
        return False
    if a.selected_thru != b.selected_thru:
        return False

    return True